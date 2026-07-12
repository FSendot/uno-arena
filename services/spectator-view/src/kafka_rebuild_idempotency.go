package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"unoarena/services/spectator-view/domain"
	"unoarena/services/spectator-view/store"
)

// RebuildIdempotencyStore records completed (recoveryJobId, roomId, failedCheckpoint) work.
type RebuildIdempotencyStore interface {
	AlreadyDone(ctx context.Context, key string) (bool, error)
	MarkDone(ctx context.Context, key string) error
}

// MemoryRebuildIdempotency is process-local fallback (tests / miswired Redis).
// Production worker prefers RedisRebuildIdempotency for cross-pod durability.
type MemoryRebuildIdempotency struct {
	mu   sync.Mutex
	done map[string]struct{}
}

func NewMemoryRebuildIdempotency() *MemoryRebuildIdempotency {
	return &MemoryRebuildIdempotency{done: map[string]struct{}{}}
}

func (m *MemoryRebuildIdempotency) AlreadyDone(ctx context.Context, key string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.done[key]
	return ok, nil
}

func (m *MemoryRebuildIdempotency) MarkDone(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.done == nil {
		m.done = map[string]struct{}{}
	}
	m.done[key] = struct{}{}
	return nil
}

// RedisRebuildIdempotency stores durable completion keys under the Spectator Redis prefix.
// Keys are room-hash-tagged and match the atomic recovery Lua marker so AlreadyDone can
// observe completions written inside the fenced swap transaction.
type RedisRebuildIdempotency struct {
	rdb  redis.UniversalClient
	keys store.KeySpace
	ttl  time.Duration
}

func NewRedisRebuildIdempotency(rdb redis.UniversalClient, keyPrefix string) *RedisRebuildIdempotency {
	return &RedisRebuildIdempotency{
		rdb:  rdb,
		keys: store.NewKeySpace(keyPrefix),
		ttl:  store.RebuildDoneRetention,
	}
}

func parseRebuildIdempotencyRoom(identity string) (domain.RoomID, error) {
	parts := strings.Split(identity, "|")
	if len(parts) != 3 {
		return "", fmt.Errorf("rebuild idempotency identity must be recoveryJobId|roomId|failedCheckpoint")
	}
	room := domain.RoomID(strings.TrimSpace(parts[1]))
	if err := store.ValidateRoomID(room); err != nil {
		return "", err
	}
	return room, nil
}

func (r *RedisRebuildIdempotency) key(identity string) (string, error) {
	room, err := parseRebuildIdempotencyRoom(identity)
	if err != nil {
		return "", err
	}
	return r.keys.RebuildDone(room, identity), nil
}

func (r *RedisRebuildIdempotency) AlreadyDone(ctx context.Context, key string) (bool, error) {
	if r == nil || r.rdb == nil {
		return false, fmt.Errorf("redis idempotency not configured")
	}
	redisKey, err := r.key(key)
	if err != nil {
		return false, err
	}
	n, err := r.rdb.Exists(ctx, redisKey).Result()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (r *RedisRebuildIdempotency) MarkDone(ctx context.Context, key string) error {
	if r == nil || r.rdb == nil {
		return fmt.Errorf("redis idempotency not configured")
	}
	redisKey, err := r.key(key)
	if err != nil {
		return err
	}
	return r.rdb.Set(ctx, redisKey, "1", r.ttl).Err()
}

func strconvFormatInt64(n int64) string {
	return strconv.FormatInt(n, 10)
}
