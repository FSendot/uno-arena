package bff

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"
)

// MaxRequestBodyBytes bounds JSON command/auth bodies at the edge.
const MaxRequestBodyBytes = 64 << 10 // 64 KiB

// DefaultMemoryLimiterKeys caps distinct rate-limit keys before eviction.
const DefaultMemoryLimiterKeys = 4096

// RateLimiter is an injectable edge/principal throttle.
// Offline mode uses the bounded/evicting in-memory limiter; a Redis adapter is not faked.
type RateLimiter interface {
	// Allow reports whether the key may proceed. retryAfter is advisory when denied.
	Allow(ctx context.Context, key string) (allowed bool, retryAfter time.Duration)
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

// Allow implements RateLimiter.
func (m *MemoryRateLimiter) Allow(_ context.Context, key string) (bool, time.Duration) {
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
		return false, retry
	}
	c.count++
	return true, 0
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

func (AllowAll) Allow(context.Context, string) (bool, time.Duration) { return true, 0 }

// clientIP extracts a coarse edge key from the request.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		if r.RemoteAddr != "" {
			return r.RemoteAddr
		}
		return "unknown"
	}
	return host
}
