package bff

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"time"
)

// ClientIPResolver derives the quota key for one edge request.
type ClientIPResolver func(*http.Request) string

// MaxRequestBodyBytes bounds JSON command/auth bodies at the edge.
const MaxRequestBodyBytes = 64 << 10 // 64 KiB

// DefaultMemoryLimiterKeys caps distinct rate-limit keys before eviction.
const DefaultMemoryLimiterKeys = 4096

// ErrRateLimiterUnavailable means the distributed limiter adapter failed.
// Callers must fail closed with HTTP 503 — never treat this as quota exhaustion.
var ErrRateLimiterUnavailable = errors.New("rate_limiter_unavailable")

// RateLimiter is an injectable edge/principal throttle.
// Offline mode uses the bounded/evicting in-memory limiter; durable mode uses Redis.
type RateLimiter interface {
	// Allow reports whether the key may proceed.
	// err != nil (typically ErrRateLimiterUnavailable) means adapter failure → 503.
	// allowed=false with err=nil means quota exhaustion → 429 with advisory retryAfter.
	Allow(ctx context.Context, key string) (allowed bool, retryAfter time.Duration, err error)
}

// MemoryRateLimiter is a fixed-window in-process limiter with bounded key eviction.
// Intended for explicit offline/demo mode only — not a stand-in for distributed Redis.
type MemoryRateLimiter struct {
	mu       sync.Mutex
	limit    int
	window   time.Duration
	maxKeys  int
	clock    func() time.Time
	counters map[string]*windowCounter
	order    []string
}

type windowCounter struct {
	resetAt time.Time
	count   int
}

// NewMemoryRateLimiter creates a bounded/evicting in-memory limiter (limit events per window).
func NewMemoryRateLimiter(limit int, window time.Duration) *MemoryRateLimiter {
	if limit < 1 {
		limit = 1
	}
	if window <= 0 {
		window = time.Second
	}
	return &MemoryRateLimiter{
		limit:    limit,
		window:   window,
		maxKeys:  DefaultMemoryLimiterKeys,
		clock:    func() time.Time { return time.Now() },
		counters: make(map[string]*windowCounter),
		order:    make([]string, 0),
	}
}

// SetMaxKeys configures the eviction bound (tests may lower it).
func (m *MemoryRateLimiter) SetMaxKeys(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n < 1 {
		n = 1
	}
	m.maxKeys = n
}

// Allow implements RateLimiter. Memory limiter never returns an adapter error.
func (m *MemoryRateLimiter) Allow(_ context.Context, key string) (bool, time.Duration, error) {
	if key == "" {
		key = "anonymous"
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.clock()
	c, ok := m.counters[key]
	if !ok || !now.Before(c.resetAt) {
		if !ok {
			m.order = append(m.order, key)
			m.evictLocked()
		}
		c = &windowCounter{resetAt: now.Add(m.window), count: 0}
		m.counters[key] = c
	}
	if c.count >= m.limit {
		retry := c.resetAt.Sub(now)
		if retry < 0 {
			retry = 0
		}
		return false, retry, nil
	}
	c.count++
	return true, 0, nil
}

func (m *MemoryRateLimiter) evictLocked() {
	for len(m.order) > m.maxKeys {
		old := m.order[0]
		m.order = m.order[1:]
		delete(m.counters, old)
	}
}

// AllowAll is a no-op limiter for tests that do not exercise throttling.
type AllowAll struct{}

func (AllowAll) Allow(context.Context, string) (bool, time.Duration, error) { return true, 0, nil }

// clientIP extracts a coarse edge key from the request.
func directClientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		if r.RemoteAddr != "" {
			return r.RemoteAddr
		}
		return "unknown"
	}
	return host
}

// NewTrustedProxyClientIPResolver trusts forwarding headers only when the
// immediate peer belongs to an explicitly configured proxy CIDR.
func NewTrustedProxyClientIPResolver(cidrs []string) (ClientIPResolver, error) {
	trusted := make([]netip.Prefix, 0, len(cidrs))
	for _, raw := range cidrs {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		prefix, err := netip.ParsePrefix(raw)
		if err != nil {
			return nil, fmt.Errorf("trusted proxy CIDR %q: %w", raw, err)
		}
		trusted = append(trusted, prefix)
	}
	return func(r *http.Request) string {
		direct := directClientIP(r)
		peer, err := netip.ParseAddr(direct)
		if err != nil || !addressInPrefixes(peer, trusted) {
			return direct
		}
		chain, ok := forwardedChain(r)
		if !ok || len(chain) == 0 {
			return direct
		}
		for i := len(chain) - 1; i >= 0; i-- {
			if !addressInPrefixes(chain[i], trusted) {
				return chain[i].String()
			}
		}
		return chain[0].String()
	}, nil
}

func addressInPrefixes(addr netip.Addr, prefixes []netip.Prefix) bool {
	addr = addr.Unmap()
	for _, prefix := range prefixes {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}

func forwardedChain(r *http.Request) ([]netip.Addr, bool) {
	if raw := strings.TrimSpace(r.Header.Get("Forwarded")); raw != "" {
		parts := strings.Split(raw, ",")
		out := make([]netip.Addr, 0, len(parts))
		for _, part := range parts {
			var value string
			for _, directive := range strings.Split(part, ";") {
				key, candidate, found := strings.Cut(strings.TrimSpace(directive), "=")
				if found && strings.EqualFold(key, "for") {
					value = strings.Trim(strings.TrimSpace(candidate), `"`)
					break
				}
			}
			addr, err := parseForwardedAddress(value)
			if err != nil {
				return nil, false
			}
			out = append(out, addr)
		}
		return out, true
	}
	raw := strings.TrimSpace(r.Header.Get("X-Forwarded-For"))
	if raw == "" {
		return nil, false
	}
	parts := strings.Split(raw, ",")
	out := make([]netip.Addr, 0, len(parts))
	for _, part := range parts {
		addr, err := parseForwardedAddress(strings.TrimSpace(part))
		if err != nil {
			return nil, false
		}
		out = append(out, addr)
	}
	return out, true
}

func parseForwardedAddress(raw string) (netip.Addr, error) {
	if raw == "" || strings.HasPrefix(raw, "_") || strings.EqualFold(raw, "unknown") {
		return netip.Addr{}, errors.New("invalid forwarded address")
	}
	if addrPort, err := netip.ParseAddrPort(raw); err == nil {
		return addrPort.Addr().Unmap(), nil
	}
	addr, err := netip.ParseAddr(strings.Trim(raw, "[]"))
	if err != nil {
		return netip.Addr{}, err
	}
	return addr.Unmap(), nil
}
