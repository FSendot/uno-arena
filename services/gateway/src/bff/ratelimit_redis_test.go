package bff

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type fakeFixedWindow struct {
	counts map[string]int64
	ttls   map[string]int64
	fail   error
}

func (f *fakeFixedWindow) Eval(_ context.Context, _ string, keys []string, args ...interface{}) *redis.Cmd {
	cmd := redis.NewCmd(context.Background())
	if f.fail != nil {
		cmd.SetErr(f.fail)
		return cmd
	}
	if f.counts == nil {
		f.counts = make(map[string]int64)
		f.ttls = make(map[string]int64)
	}
	key := keys[0]
	f.counts[key]++
	if f.counts[key] == 1 {
		var windowMS int64 = 60000
		if len(args) > 0 {
			switch v := args[0].(type) {
			case int64:
				windowMS = v
			case int:
				windowMS = int64(v)
			}
		}
		f.ttls[key] = windowMS
	}
	cmd.SetVal([]interface{}{f.counts[key], f.ttls[key]})
	return cmd
}

func TestRedisRateLimiter_AllowsUntilExhausted(t *testing.T) {
	fake := &fakeFixedWindow{}
	lim := newRedisRateLimiter(fake, 2, time.Minute, "test:rl:")
	ctx := context.Background()
	ok, _, err := lim.Allow(ctx, "edge:1.1.1.1")
	if err != nil || !ok {
		t.Fatalf("first allow=%v err=%v", ok, err)
	}
	ok, _, err = lim.Allow(ctx, "edge:1.1.1.1")
	if err != nil || !ok {
		t.Fatalf("second allow=%v err=%v", ok, err)
	}
	ok, retry, err := lim.Allow(ctx, "edge:1.1.1.1")
	if err != nil {
		t.Fatalf("exhaustion must not be adapter error: %v", err)
	}
	if ok {
		t.Fatal("third must be denied")
	}
	if retry <= 0 {
		t.Fatalf("retryAfter=%v", retry)
	}
}

func TestRedisRateLimiter_AdapterFailureDistinctFromQuota(t *testing.T) {
	fake := &fakeFixedWindow{fail: errors.New("redis down")}
	lim := newRedisRateLimiter(fake, 10, time.Minute, "test:rl:")
	ok, retry, err := lim.Allow(context.Background(), "k")
	if ok {
		t.Fatal("must deny on failure")
	}
	if retry != 0 {
		t.Fatalf("retryAfter=%v", retry)
	}
	if !errors.Is(err, ErrRateLimiterUnavailable) {
		t.Fatalf("err=%v want ErrRateLimiterUnavailable", err)
	}
}

func TestMemoryRateLimiter_NeverReturnsAdapterError(t *testing.T) {
	lim := NewMemoryRateLimiter(1, time.Minute)
	ok, _, err := lim.Allow(context.Background(), "a")
	if err != nil || !ok {
		t.Fatalf("first=%v err=%v", ok, err)
	}
	ok, _, err = lim.Allow(context.Background(), "a")
	if err != nil {
		t.Fatalf("quota denial must have nil err, got %v", err)
	}
	if ok {
		t.Fatal("expected denial")
	}
}
