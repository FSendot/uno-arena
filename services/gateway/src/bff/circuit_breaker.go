package bff

import (
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"
)

var ErrCircuitOpen = errors.New("upstream circuit open")

type CircuitState string

const (
	CircuitClosed   CircuitState = "closed"
	CircuitOpen     CircuitState = "open"
	CircuitHalfOpen CircuitState = "half_open"
)

type CircuitEvent struct {
	Upstream string
	From     CircuitState
	To       CircuitState
	Reason   string
}

type CircuitObserver func(CircuitEvent)

type CircuitBreakerConfig struct {
	FailureThreshold int
	OpenTimeout      time.Duration
	Observer         CircuitObserver
}

func NewCircuitBreakingHTTPClient(base *http.Client, upstream string, cfg CircuitBreakerConfig) *http.Client {
	if base == nil {
		base = &http.Client{Timeout: defaultHTTPTimeout}
	}
	threshold := cfg.FailureThreshold
	if threshold <= 0 {
		threshold = 5
	}
	openTimeout := cfg.OpenTimeout
	if openTimeout <= 0 {
		openTimeout = 30 * time.Second
	}
	transport := base.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	clone := *base
	clone.Transport = &circuitBreakerTransport{
		next: transport, upstream: sanitizeCircuitUpstream(upstream), failureThreshold: threshold,
		openTimeout: openTimeout, observer: cfg.Observer, state: CircuitClosed, now: time.Now,
	}
	return &clone
}

func sanitizeCircuitUpstream(upstream string) string {
	upstream = strings.TrimSpace(upstream)
	if upstream == "" || len(upstream) > 32 {
		return "unknown"
	}
	for _, r := range upstream {
		if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_') {
			return "unknown"
		}
	}
	return upstream
}

type circuitBreakerTransport struct {
	next             http.RoundTripper
	upstream         string
	failureThreshold int
	openTimeout      time.Duration
	observer         CircuitObserver
	now              func() time.Time

	mu          sync.Mutex
	state       CircuitState
	failures    int
	openedAt    time.Time
	halfOpenRun bool
}

func (c *circuitBreakerTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	allowed, halfOpenProbe := c.allow()
	if !allowed {
		return nil, ErrCircuitOpen
	}
	response, err := c.next.RoundTrip(request)
	failed := err != nil || (response != nil && response.StatusCode >= http.StatusInternalServerError)
	c.complete(failed, halfOpenProbe)
	return response, err
}

func (c *circuitBreakerTransport) allow() (bool, bool) {
	c.mu.Lock()
	var event *CircuitEvent
	if c.state == CircuitOpen && c.now().Sub(c.openedAt) >= c.openTimeout {
		event = c.transitionLocked(CircuitHalfOpen, "open_timeout_elapsed")
	}
	if c.state == CircuitOpen || (c.state == CircuitHalfOpen && c.halfOpenRun) {
		c.mu.Unlock()
		c.observe(event)
		return false, false
	}
	if c.state == CircuitHalfOpen {
		c.halfOpenRun = true
		c.mu.Unlock()
		c.observe(event)
		return true, true
	}
	c.mu.Unlock()
	c.observe(event)
	return true, false
}

func (c *circuitBreakerTransport) complete(failed, halfOpenProbe bool) {
	c.mu.Lock()
	var event *CircuitEvent
	if halfOpenProbe && c.state == CircuitHalfOpen {
		c.halfOpenRun = false
		if failed {
			c.openedAt = c.now()
			event = c.transitionLocked(CircuitOpen, "probe_failed")
		} else {
			c.failures = 0
			event = c.transitionLocked(CircuitClosed, "probe_succeeded")
		}
	} else if c.state != CircuitClosed {
		// Ignore requests admitted before another request opened the circuit. In
		// particular, they must not be mistaken for the single half-open probe.
	} else if !failed {
		c.failures = 0
	} else {
		c.failures++
		if c.failures >= c.failureThreshold {
			c.openedAt = c.now()
			event = c.transitionLocked(CircuitOpen, "failure_threshold_reached")
		}
	}
	c.mu.Unlock()
	c.observe(event)
}

func (c *circuitBreakerTransport) transitionLocked(next CircuitState, reason string) *CircuitEvent {
	previous := c.state
	if previous == next {
		return nil
	}
	c.state = next
	event := CircuitEvent{Upstream: c.upstream, From: previous, To: next, Reason: reason}
	return &event
}

func (c *circuitBreakerTransport) observe(event *CircuitEvent) {
	if event != nil && c.observer != nil {
		c.observer(*event)
	}
}
