package store

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

// NewRedisFromURL opens a go-redis v9 client from REDIS_URL.
func NewRedisFromURL(redisURL string) (*redis.Client, error) {
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	return redis.NewClient(opt), nil
}

// PingRedis verifies connectivity.
func PingRedis(ctx context.Context, rdb redis.UniversalClient) error {
	return rdb.Ping(ctx).Err()
}

// ParseStreamMaxLenEnv parses SPECTATOR_REDIS_STREAM_MAXLEN.
// Empty → ok=false (caller keeps default). Non-positive values error.
func ParseStreamMaxLenEnv(raw string) (n int64, ok bool, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false, nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("invalid integer %q", raw)
	}
	if v <= 0 {
		return 0, false, fmt.Errorf("must be positive, got %d", v)
	}
	return v, true, nil
}
