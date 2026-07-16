package app

import (
	"errors"
	"net"
	"net/http"
	"sync"
	"time"
)

const (
	DefaultIntegrityHTTPTimeout = 3 * time.Second
	defaultCircuitFailures      = 3
	defaultCircuitOpenFor       = 5 * time.Second
)

var ErrIntegrityCircuitOpen = errors.New("game integrity circuit open")

// NewIntegrityHTTPClient owns the Room->Game Integrity transport policy. Its
// total timeout is intentionally shorter than the caller's external deadline,
// bounding the ADR-0019 interval in which a Room row lock may span a GI call.
func NewIntegrityHTTPClient(timeout time.Duration) *http.Client {
	if timeout <= 0 {
		timeout = DefaultIntegrityHTTPTimeout
	}
	if timeout < 250*time.Millisecond {
		timeout = 250 * time.Millisecond
	}
	if timeout > 4*time.Second {
		timeout = 4 * time.Second
	}
	dialTimeout := timeout / 3
	if dialTimeout > time.Second {
		dialTimeout = time.Second
	}
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: dialTimeout, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          32,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       30 * time.Second,
		TLSHandshakeTimeout:   dialTimeout,
		ResponseHeaderTimeout: timeout / 2,
		ExpectContinueTimeout: time.Second,
	}
	return &http.Client{
		Timeout:   timeout,
		Transport: newCircuitRoundTripper(transport, defaultCircuitFailures, defaultCircuitOpenFor, time.Now),
	}
}

type circuitRoundTripper struct {
	next      http.RoundTripper
	threshold int
	openFor   time.Duration
	now       func() time.Time

	mu        sync.Mutex
	failures  int
	openUntil time.Time
	probe     bool
}

func newCircuitRoundTripper(next http.RoundTripper, threshold int, openFor time.Duration, now func() time.Time) *circuitRoundTripper {
	if next == nil {
		next = http.DefaultTransport
	}
	if threshold < 1 {
		threshold = 1
	}
	if openFor <= 0 {
		openFor = time.Second
	}
	if now == nil {
		now = time.Now
	}
	return &circuitRoundTripper{next: next, threshold: threshold, openFor: openFor, now: now}
}

func (c *circuitRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if !c.admit() {
		return nil, ErrIntegrityCircuitOpen
	}
	resp, err := c.next.RoundTrip(req)
	failed := err != nil || (resp != nil && resp.StatusCode >= http.StatusInternalServerError)
	c.complete(failed)
	return resp, err
}

func (c *circuitRoundTripper) admit() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	if c.openUntil.IsZero() {
		return true
	}
	if now.Before(c.openUntil) || c.probe {
		return false
	}
	c.probe = true
	return true
}

func (c *circuitRoundTripper) complete(failed bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	wasProbe := c.probe
	c.probe = false
	if !failed {
		// A definitive 4xx proves the transport is healthy and must not trip the
		// breaker; callers retain its typed no-write semantics.
		c.failures = 0
		c.openUntil = time.Time{}
		return
	}
	c.failures++
	if wasProbe || c.failures >= c.threshold {
		c.openUntil = c.now().Add(c.openFor)
	}
}
