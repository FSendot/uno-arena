package app

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

type resilienceRoundTripFunc func(*http.Request) (*http.Response, error)

func (f resilienceRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestIntegrityCircuitBreakerOpenHalfOpenAndDefinitive4xx(t *testing.T) {
	now := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	status := http.StatusServiceUnavailable
	calls := 0
	breaker := newCircuitRoundTripper(resilienceRoundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: status, Body: http.NoBody, Header: make(http.Header)}, nil
	}), 2, 10*time.Second, func() time.Time { return now })
	client := &http.Client{Transport: breaker}
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://integrity.test", nil)
	for i := 0; i < 2; i++ {
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	if _, err := client.Do(req); !errors.Is(err, ErrIntegrityCircuitOpen) || calls != 2 {
		t.Fatalf("open err=%v calls=%d", err, calls)
	}
	now = now.Add(10 * time.Second)
	status = http.StatusBadRequest
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	resp, err = client.Do(req)
	if err != nil || calls != 4 {
		t.Fatalf("definitive 4xx must close breaker: err=%v calls=%d", err, calls)
	}
	resp.Body.Close()
}

func TestDefaultIntegrityClientHasStrictTimeouts(t *testing.T) {
	client := NewIntegrityHTTPClient(2400 * time.Millisecond)
	if client.Timeout != 2400*time.Millisecond {
		t.Fatalf("timeout=%s", client.Timeout)
	}
	breaker, ok := client.Transport.(*circuitRoundTripper)
	if !ok {
		t.Fatalf("transport=%T", client.Transport)
	}
	transport, ok := breaker.next.(*http.Transport)
	if !ok || transport.ResponseHeaderTimeout <= 0 || transport.TLSHandshakeTimeout <= 0 || transport.DialContext == nil {
		t.Fatalf("strict transport=%#v", breaker.next)
	}
}

func TestIntegrityClientBoundsLockHoldingTimeout(t *testing.T) {
	if got := NewIntegrityHTTPClient(time.Millisecond).Timeout; got != 250*time.Millisecond {
		t.Fatalf("minimum timeout=%s", got)
	}
	if got := NewIntegrityHTTPClient(time.Minute).Timeout; got != 4*time.Second {
		t.Fatalf("maximum timeout=%s", got)
	}
}
