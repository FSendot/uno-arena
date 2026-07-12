//go:build redis_integration

package store_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"unoarena/services/gateway/bff"
	"unoarena/services/gateway/bff/store"
)

func openSIIntegrationStore(t *testing.T) (*store.SessionInvalidationStore, context.Context, string) {
	t.Helper()
	url := strings.TrimSpace(os.Getenv("GATEWAY_REDIS_URL"))
	if url == "" {
		t.Skip("GATEWAY_REDIS_URL not set")
	}
	// Prefer DB 11 (gateway integration isolation) when present; accept DB 6 for kind SI proof.
	if !strings.Contains(url, "/11") && !strings.Contains(url, "/6") {
		t.Fatalf("GATEWAY_REDIS_URL must target Redis DB 11 or 6, got %q", url)
	}
	rdb, err := store.NewRedisFromURL(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	ctx := context.Background()
	if err := store.PingRedis(ctx, rdb); err != nil {
		t.Fatalf("redis unavailable: %v", err)
	}
	prefix := "gateway:si:test:" + t.Name() + ":"
	s := store.NewSessionInvalidationStore(rdb, time.Minute).WithKeyPrefix(prefix)
	if err := s.LoadScripts(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		iter := rdb.Scan(ctx, 0, prefix+"*", 100).Iterator()
		for iter.Next(ctx) {
			_ = rdb.Del(ctx, iter.Val())
		}
	})
	return s, ctx, prefix
}

func TestSessionInvalidationStore_ApplyDuplicateConflictAndNotify(t *testing.T) {
	s, ctx, _ := openSIIntegrationStore(t)
	rec := store.SessionInvalidationRecord{
		EventID: "evt1", SessionID: "sess1", PlayerID: "player1",
		Reason: "superseded", CorrelationID: "corr1", OccurredAt: time.Now().UTC(),
	}
	kind, err := s.Apply(ctx, rec)
	if err != nil || kind != store.SessionInvalidationAccepted {
		t.Fatalf("kind=%s err=%v", kind, err)
	}
	kind, err = s.Apply(ctx, rec)
	if err != nil || kind != store.SessionInvalidationDuplicate {
		t.Fatalf("dup kind=%s err=%v", kind, err)
	}
	conflict := rec
	conflict.EventID = "evt2"
	kind, err = s.Apply(ctx, conflict)
	if err != nil || kind != store.SessionInvalidationConflict {
		t.Fatalf("conflict kind=%s err=%v", kind, err)
	}
	ok, err := s.IsInvalidated(ctx, "sess1")
	if err != nil || !ok {
		t.Fatalf("invalidated ok=%v err=%v", ok, err)
	}
}

func TestSessionInvalidation_CrossReplicaAdmissionReject(t *testing.T) {
	// Equivalent multi-replica proof: writer store (consumer replica) vs reader LiveFeed
	// on a second Hub that never received Pub/Sub / Kafka.
	s, ctx, _ := openSIIntegrationStore(t)
	hubA := bff.NewHub()
	hubB := bff.NewHub()

	rec := store.SessionInvalidationRecord{
		EventID: "evt-x", SessionID: "sess-x", PlayerID: "player-x",
		Reason: "revoked", CorrelationID: "corr-x", OccurredAt: time.Now().UTC(),
	}
	kind, err := s.Apply(ctx, rec)
	if err != nil || kind != store.SessionInvalidationAccepted {
		t.Fatalf("kind=%s err=%v", kind, err)
	}
	_, _, _ = hubA.ApplySessionInvalidation(rec.EventID, rec.SessionID)

	feedB := bff.NewRedisLiveFeed(nil, nil, "spectator:", hubB).WithSessionInvalidationStore(s)
	// beginPlayer needs player redis; exercise denyIfSessionInvalidated via admission helper path.
	if err := denyViaFeed(feedB, ctx, "sess-x"); err == nil {
		t.Fatal("replica B must reject from durable Redis without local Hub mark")
	}
	if hubB.IsSessionInvalidated("sess-x") {
		t.Fatal("hub B must not have been marked without Pub/Sub")
	}
}

func TestSessionInvalidationStore_RestoreMissingSessionHash(t *testing.T) {
	s, ctx, prefix := openSIIntegrationStore(t)
	keys := store.NewSessionInvalidationKeySpace(prefix)

	rec := store.SessionInvalidationRecord{
		EventID: "evt-restore", SessionID: "sess-restore", PlayerID: "player-restore",
		Reason: "superseded", CorrelationID: "corr-restore", OccurredAt: time.Now().UTC(),
	}
	kind, err := s.Apply(ctx, rec)
	if err != nil || kind != store.SessionInvalidationAccepted {
		t.Fatalf("accept kind=%s err=%v", kind, err)
	}

	rdbURL := strings.TrimSpace(os.Getenv("GATEWAY_REDIS_URL"))
	rdb, err := store.NewRedisFromURL(rdbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rdb.Close() })

	sessKey := keys.Session("sess-restore")
	if n, err := rdb.Del(ctx, sessKey).Result(); err != nil || n != 1 {
		t.Fatalf("delete session hash n=%d err=%v key=%s", n, err, sessKey)
	}
	ok, err := s.IsInvalidated(ctx, "sess-restore")
	if err != nil || ok {
		t.Fatalf("after delete invalidated=%v err=%v", ok, err)
	}

	kind, err = s.Apply(ctx, rec)
	if err != nil || kind != store.SessionInvalidationRestored {
		t.Fatalf("restore kind=%s err=%v", kind, err)
	}
	ok, err = s.IsInvalidated(ctx, "sess-restore")
	if err != nil || !ok {
		t.Fatalf("after restore invalidated=%v err=%v", ok, err)
	}

	conflict := rec
	conflict.Reason = "revoked"
	kind, err = s.Apply(ctx, conflict)
	if err != nil || kind != store.SessionInvalidationConflict {
		t.Fatalf("changed reason must conflict kind=%s err=%v", kind, err)
	}

	if _, err := rdb.Del(ctx, sessKey).Result(); err != nil {
		t.Fatal(err)
	}
	conflictPlayer := rec
	conflictPlayer.PlayerID = "player-other"
	kind, err = s.Apply(ctx, conflictPlayer)
	if err != nil || kind != store.SessionInvalidationConflict {
		t.Fatalf("changed player must conflict kind=%s err=%v", kind, err)
	}
}

func denyViaFeed(feed *bff.RedisLiveFeed, ctx context.Context, sessionID string) error {
	// Use BeginSession StreamControl which checks durable store before Hub subscribe.
	_, err := feed.BeginSession(ctx, bff.StreamControl, "", sessionID, "p", "")
	return err
}
