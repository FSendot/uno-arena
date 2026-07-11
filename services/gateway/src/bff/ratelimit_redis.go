package bff

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const defaultRateLimitKeyPrefix = "gateway:rl:v1:"

// fixedWindowRedis is the Redis surface used by RedisRateLimiter (unit-testable).
type fixedWindowRedis interface {
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
}

// RedisRateLimiter is a distributed fixed-window limiter backed by Redis.
// Redis errors return ErrRateLimiterUnavailable (fail closed); quota denial is distinct.
type RedisRateLimiter struct {
	rdb    fixedWindowRedis
	limit  int
	window time.Duration
	prefix string
}

// NewRedisRateLimiter constructs a Redis-backed limiter.
func NewRedisRateLimiter(rdb redis.UniversalClient, limit int, window time.Duration) *RedisRateLimiter {
	return newRedisRateLimiter(rdb, limit, window, defaultRateLimitKeyPrefix)
}

func newRedisRateLimiter(rdb fixedWindowRedis, limit int, window time.Duration, prefix string) *RedisRateLimiter {
	if limit < 1 {
		limit = 1
	}
	if window <= 0 {
		window = time.Second
	}
	p := strings.TrimSpace(prefix)
	if p == "" {
		p = defaultRateLimitKeyPrefix
	}
	if !strings.HasSuffix(p, ":") {
		p += ":"
	}
	return &RedisRateLimiter{rdb: rdb, limit: limit, window: window, prefix: p}
}

// rateLimitScript atomically INCR + PEXPIRE on first hit and returns {count, pttl_ms}.
const rateLimitScript = `
local n = redis.call('INCR', KEYS[1])
if n == 1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
end
local ttl = redis.call('PTTL', KEYS[1])
return {n, ttl}
`

// Allow implements RateLimiter.
func (r *RedisRateLimiter) Allow(ctx context.Context, key string) (bool, time.Duration, error) {
	if key == "" {
		key = "anonymous"
	}
	redisKey := r.prefix + key
	windowMS := r.window.Milliseconds()
	if windowMS < 1 {
		windowMS = 1
	}
	res, err := r.rdb.Eval(ctx, rateLimitScript, []string{redisKey}, windowMS).Result()
	if err != nil {
		return false, 0, fmt.Errorf("%w: %v", ErrRateLimiterUnavailable, err)
	}
	arr, ok := res.([]interface{})
	if !ok || len(arr) != 2 {
		return false, 0, fmt.Errorf("%w: unexpected script result", ErrRateLimiterUnavailable)
	}
	count, err := toInt64(arr[0])
	if err != nil {
		return false, 0, fmt.Errorf("%w: %v", ErrRateLimiterUnavailable, err)
	}
	pttl, err := toInt64(arr[1])
	if err != nil {
		return false, 0, fmt.Errorf("%w: %v", ErrRateLimiterUnavailable, err)
	}
	if count > int64(r.limit) {
		retry := time.Duration(pttl) * time.Millisecond
		if retry < 0 {
			retry = 0
		}
		return false, retry, nil
	}
	return true, 0, nil
}

func toInt64(v interface{}) (int64, error) {
	switch t := v.(type) {
	case int64:
		return t, nil
	case int:
		return int64(t), nil
	case string:
		var n int64
		_, err := fmt.Sscan(t, &n)
		return n, err
	default:
		return 0, fmt.Errorf("unsupported numeric type %T", v)
	}
}
