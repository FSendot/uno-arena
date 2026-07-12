package store

import (
	"context"
	"fmt"

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
	if rdb == nil {
		return fmt.Errorf("nil redis client")
	}
	return rdb.Ping(ctx).Err()
}
