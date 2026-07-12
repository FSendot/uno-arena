package bff

import (
	"context"
	"errors"
	"testing"
)

type memSIChecker struct {
	invalid map[string]bool
	err     error
}

func (m *memSIChecker) IsInvalidated(_ context.Context, sessionID string) (bool, error) {
	if m.err != nil {
		return false, m.err
	}
	return m.invalid[sessionID], nil
}

func TestDenyIfSessionInvalidated_HubAndStore(t *testing.T) {
	hub := NewHub()
	hub.InvalidateSession("sess-hub")
	feed := newRedisLiveFeed(nil, nil, "spectator:", hub).WithSessionInvalidationStore(&memSIChecker{
		invalid: map[string]bool{"sess-redis": true},
	})

	if err := feed.denyIfSessionInvalidated(context.Background(), ""); err != nil {
		t.Fatalf("anonymous must skip: %v", err)
	}
	if err := feed.denyIfSessionInvalidated(context.Background(), "sess-hub"); err == nil || !errors.Is(err, ErrStreamDenied) {
		t.Fatalf("hub: %v", err)
	}
	if err := feed.denyIfSessionInvalidated(context.Background(), "sess-redis"); err == nil || !errors.Is(err, ErrStreamDenied) {
		t.Fatalf("store: %v", err)
	}
	if err := feed.denyIfSessionInvalidated(context.Background(), "sess-ok"); err != nil {
		t.Fatalf("ok: %v", err)
	}
}

func TestDenyIfSessionInvalidated_RedisErrorFailClosed(t *testing.T) {
	hub := NewHub()
	feed := newRedisLiveFeed(nil, nil, "spectator:", hub).WithSessionInvalidationStore(&memSIChecker{
		err: errors.New("redis down"),
	})
	err := feed.denyIfSessionInvalidated(context.Background(), "sess-1")
	if err == nil || !errors.Is(err, ErrLiveFeedUnavailable) {
		t.Fatalf("err=%v", err)
	}
}

func TestCheckSessionInvalidation_FailClosedOnStoreError(t *testing.T) {
	hub := NewHub()
	feed := newRedisLiveFeed(nil, nil, "spectator:", hub).WithSessionInvalidationStore(&memSIChecker{
		err: errors.New("redis down"),
	})
	sess := &redisLiveSession{
		feed:      feed,
		sessionID: "sess-1",
		ch:        make(chan StreamEvent, 1),
		cancelCh:  make(chan struct{}),
	}
	closed, failClosed := sess.checkSessionInvalidation(context.Background())
	if closed || !failClosed {
		t.Fatalf("closed=%v failClosed=%v", closed, failClosed)
	}
}

func TestCheckSessionInvalidation_EmitsOnStoreHit(t *testing.T) {
	hub := NewHub()
	feed := newRedisLiveFeed(nil, nil, "spectator:", hub).WithSessionInvalidationStore(&memSIChecker{
		invalid: map[string]bool{"sess-1": true},
	})
	sess := &redisLiveSession{
		feed:      feed,
		sessionID: "sess-1",
		ch:        make(chan StreamEvent, 1),
		cancelCh:  make(chan struct{}),
	}
	closed, failClosed := sess.checkSessionInvalidation(context.Background())
	if !closed || failClosed {
		t.Fatalf("closed=%v failClosed=%v", closed, failClosed)
	}
	select {
	case ev := <-sess.ch:
		if ev.Event != "session_invalidated" {
			t.Fatalf("event=%q", ev.Event)
		}
	default:
		t.Fatal("expected control event")
	}
}
