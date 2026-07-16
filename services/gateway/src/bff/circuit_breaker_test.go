package bff

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"unoarena/shared/correlation"
)

type breakerRoundTripFunc func(*http.Request) (*http.Response, error)

func (f breakerRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestCircuitBreakerClassificationAndHalfOpen(t *testing.T) {
	now := time.Unix(100, 0)
	statuses := []int{http.StatusBadRequest, http.StatusInternalServerError, http.StatusBadGateway, http.StatusOK}
	calls := 0
	events := make([]CircuitEvent, 0)
	breaker := &circuitBreakerTransport{
		next: breakerRoundTripFunc(func(*http.Request) (*http.Response, error) {
			status := statuses[calls]
			calls++
			return &http.Response{StatusCode: status, Body: http.NoBody, Header: make(http.Header)}, nil
		}),
		upstream: "identity", failureThreshold: 2, openTimeout: time.Second,
		observer: func(event CircuitEvent) { events = append(events, event) }, state: CircuitClosed,
		now: func() time.Time { return now },
	}
	client := &http.Client{Transport: breaker}
	request, _ := http.NewRequest(http.MethodGet, "http://identity.test/ready", nil)

	for range 3 {
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
	}
	if calls != 3 || breaker.state != CircuitOpen {
		t.Fatalf("calls=%d state=%s", calls, breaker.state)
	}
	if _, err := client.Do(request); !errors.Is(err, ErrCircuitOpen) || calls != 3 {
		t.Fatalf("open call err=%v calls=%d", err, calls)
	}
	now = now.Add(time.Second)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if breaker.state != CircuitClosed || calls != 4 {
		t.Fatalf("half-open recovery state=%s calls=%d", breaker.state, calls)
	}
	if len(events) != 3 || events[0].To != CircuitOpen || events[1].To != CircuitHalfOpen || events[2].To != CircuitClosed {
		t.Fatalf("events=%+v", events)
	}
}

func TestCircuitBreakerTripsOnTransportErrors(t *testing.T) {
	breaker := NewCircuitBreakingHTTPClient(&http.Client{Transport: breakerRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("dial failed")
	})}, "room", CircuitBreakerConfig{FailureThreshold: 1, OpenTimeout: time.Minute})
	request, _ := http.NewRequest(http.MethodGet, "http://room.test", nil)
	if _, err := breaker.Do(request); err == nil {
		t.Fatal("first transport error missing")
	}
	if _, err := breaker.Do(request); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("second error=%v", err)
	}
}

func TestCircuitBreakerTreatsTimeoutAsFailure(t *testing.T) {
	breaker := NewCircuitBreakingHTTPClient(&http.Client{Transport: breakerRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, context.DeadlineExceeded
	})}, "analytics", CircuitBreakerConfig{FailureThreshold: 1, OpenTimeout: time.Minute})
	request, _ := http.NewRequest(http.MethodGet, "http://analytics.test", nil)
	if _, err := breaker.Do(request); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first error=%v", err)
	}
	if _, err := breaker.Do(request); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("second error=%v", err)
	}
}

func TestCircuitBreakerAllowsOnlyOneHalfOpenProbe(t *testing.T) {
	now := time.Unix(100, 0)
	probeStarted := make(chan struct{})
	releaseProbe := make(chan struct{})
	calls := 0
	breaker := &circuitBreakerTransport{
		next: breakerRoundTripFunc(func(*http.Request) (*http.Response, error) {
			calls++
			if calls == 1 {
				return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: http.NoBody, Header: make(http.Header)}, nil
			}
			close(probeStarted)
			<-releaseProbe
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Header: make(http.Header)}, nil
		}),
		upstream: "ranking", failureThreshold: 1, openTimeout: time.Second,
		state: CircuitClosed, now: func() time.Time { return now },
	}
	client := &http.Client{Transport: breaker}
	request, _ := http.NewRequest(http.MethodGet, "http://ranking.test", nil)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	now = now.Add(time.Second)
	probeDone := make(chan error, 1)
	go func() {
		response, err := client.Do(request)
		if response != nil {
			_ = response.Body.Close()
		}
		probeDone <- err
	}()
	<-probeStarted
	if _, err := client.Do(request); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("concurrent half-open error=%v", err)
	}
	close(releaseProbe)
	if err := <-probeDone; err != nil {
		t.Fatalf("half-open probe: %v", err)
	}
	if calls != 2 || breaker.state != CircuitClosed {
		t.Fatalf("calls=%d state=%s", calls, breaker.state)
	}
}

func TestReadModelCircuitsKeepRankingAndAnalyticsIndependent(t *testing.T) {
	base := &http.Client{Transport: breakerRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		status := http.StatusOK
		body := `{"requests":1}`
		if request.URL.Host == "ranking.test" {
			status = http.StatusServiceUnavailable
			body = `{"code":"ranking_unavailable"}`
		}
		return &http.Response{
			StatusCode: status,
			Body:       io.NopCloser(strings.NewReader(body)),
			Header:     make(http.Header),
		}, nil
	})}
	reads := NewHTTPReadModelClientWithClients(
		"http://ranking.test",
		"http://analytics.test",
		NewCircuitBreakingHTTPClient(base, "ranking", CircuitBreakerConfig{FailureThreshold: 1, OpenTimeout: time.Minute}),
		NewCircuitBreakingHTTPClient(base, "analytics", CircuitBreakerConfig{FailureThreshold: 1, OpenTimeout: time.Minute}),
	)
	if _, err := reads.Leaderboard(context.Background(), "", correlation.Headers{}); err == nil {
		t.Fatal("ranking 503 should fail")
	}
	if _, err := reads.Leaderboard(context.Background(), "", correlation.Headers{}); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("ranking circuit error=%v", err)
	}
	if _, err := reads.PublicAnalytics(context.Background(), correlation.Headers{}); err != nil {
		t.Fatalf("analytics was affected by ranking circuit: %v", err)
	}
}

func TestCircuitObserverSanitizesUpstreamLabel(t *testing.T) {
	events := make(chan CircuitEvent, 1)
	client := NewCircuitBreakingHTTPClient(&http.Client{Transport: breakerRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("unavailable")
	})}, "https://secret.example/path", CircuitBreakerConfig{
		FailureThreshold: 1,
		OpenTimeout:      time.Minute,
		Observer:         func(event CircuitEvent) { events <- event },
	})
	request, _ := http.NewRequest(http.MethodGet, "http://identity.test", nil)
	_, _ = client.Do(request)
	if event := <-events; event.Upstream != "unknown" {
		t.Fatalf("observer upstream=%q", event.Upstream)
	}
}

func TestLateClosedRequestCannotCompleteHalfOpenProbe(t *testing.T) {
	now := time.Unix(100, 0)
	slowStarted := make(chan struct{})
	releaseSlow := make(chan struct{})
	probeStarted := make(chan struct{})
	releaseProbe := make(chan struct{})
	breaker := &circuitBreakerTransport{
		next: breakerRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			switch request.URL.Path {
			case "/slow":
				close(slowStarted)
				<-releaseSlow
				return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Header: make(http.Header)}, nil
			case "/fail":
				return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: http.NoBody, Header: make(http.Header)}, nil
			case "/probe":
				close(probeStarted)
				<-releaseProbe
				return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Header: make(http.Header)}, nil
			default:
				return nil, errors.New("unexpected transport call")
			}
		}),
		upstream: "identity", failureThreshold: 1, openTimeout: time.Second,
		state: CircuitClosed, now: func() time.Time { return now },
	}
	client := &http.Client{Transport: breaker}
	doAsync := func(path string) <-chan error {
		done := make(chan error, 1)
		go func() {
			response, err := client.Get("http://identity.test" + path)
			if response != nil {
				_ = response.Body.Close()
			}
			done <- err
		}()
		return done
	}
	slowDone := doAsync("/slow")
	<-slowStarted
	response, err := client.Get("http://identity.test/fail")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	now = now.Add(time.Second)
	probeDone := doAsync("/probe")
	<-probeStarted
	close(releaseSlow)
	if err := <-slowDone; err != nil {
		t.Fatalf("late request: %v", err)
	}
	if _, err := client.Get("http://identity.test/extra"); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("late request completed the probe: %v", err)
	}
	close(releaseProbe)
	if err := <-probeDone; err != nil {
		t.Fatalf("probe: %v", err)
	}
}
