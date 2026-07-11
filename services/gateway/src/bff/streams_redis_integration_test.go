//go:build redis_integration

package bff_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"unoarena/services/gateway/bff"
	"unoarena/services/gateway/bff/store"
)

func openIntegrationClients(t *testing.T) (rate, player, spectator *redis.Client, ctx context.Context) {
	t.Helper()
	rateURL := strings.TrimSpace(os.Getenv("GATEWAY_REDIS_URL"))
	playerURL := strings.TrimSpace(os.Getenv("GATEWAY_PLAYER_FEED_REDIS_URL"))
	specURL := strings.TrimSpace(os.Getenv("GATEWAY_SPECTATOR_REDIS_URL"))
	if rateURL == "" || playerURL == "" || specURL == "" {
		t.Skip("GATEWAY_REDIS_URL / GATEWAY_PLAYER_FEED_REDIS_URL / GATEWAY_SPECTATOR_REDIS_URL not set")
	}
	// Isolation: rate DB 11, player DB 12, spectator DB 13 (avoid prod DBs 2/5/6 and other test DBs 14/15).
	if !strings.Contains(rateURL, "/11") {
		t.Fatalf("GATEWAY_REDIS_URL must target Redis DB 11, got %q", rateURL)
	}
	if !strings.Contains(playerURL, "/12") {
		t.Fatalf("GATEWAY_PLAYER_FEED_REDIS_URL must target Redis DB 12, got %q", playerURL)
	}
	if !strings.Contains(specURL, "/13") {
		t.Fatalf("GATEWAY_SPECTATOR_REDIS_URL must target Redis DB 13, got %q", specURL)
	}
	var err error
	rate, err = store.NewRedisFromURL(rateURL)
	if err != nil {
		t.Fatal(err)
	}
	player, err = store.NewRedisFromURL(playerURL)
	if err != nil {
		t.Fatal(err)
	}
	spectator, err = store.NewRedisFromURL(specURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = rate.Close()
		_ = player.Close()
		_ = spectator.Close()
	})
	ctx = context.Background()
	for _, c := range []*redis.Client{rate, player, spectator} {
		if err := c.Ping(ctx).Err(); err != nil {
			t.Fatalf("redis unavailable (must not skip): %v", err)
		}
	}
	return rate, player, spectator, ctx
}

func TestRedisIntegration_RateLimiterFailClosedAndQuota(t *testing.T) {
	rate, _, _, ctx := openIntegrationClients(t)
	prefix := "gwtest:rl:" + t.Name() + ":"
	t.Cleanup(func() {
		iter := rate.Scan(ctx, 0, prefix+"*", 100).Iterator()
		for iter.Next(ctx) {
			_ = rate.Del(ctx, iter.Val())
		}
	})
	lim := bff.NewRedisRateLimiter(rate, 2, time.Minute)
	// Override prefix via constructing with Eval surface is internal; use unique keys.
	ok, _, err := lim.Allow(ctx, prefix+"a")
	if err != nil || !ok {
		t.Fatalf("first=%v err=%v", ok, err)
	}
	ok, _, err = lim.Allow(ctx, prefix+"a")
	if err != nil || !ok {
		t.Fatalf("second=%v err=%v", ok, err)
	}
	ok, _, err = lim.Allow(ctx, prefix+"a")
	if err != nil {
		t.Fatalf("quota denial must not be adapter error: %v", err)
	}
	if ok {
		t.Fatal("expected quota denial")
	}
}

func TestRedisIntegration_PlayerAudienceAndResume(t *testing.T) {
	_, player, spectator, ctx := openIntegrationClients(t)
	room := "room_gw_integ"
	stream, err := bff.PlayerFeedTargetStream(room)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = player.Del(ctx, stream) })
	_ = player.Del(ctx, stream)

	id1, err := player.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		ID:     "1-0",
		Values: map[string]interface{}{
			"event": "CardPlayed", "data": `{"n":1}`, "sequence": "1", "eventId": "e1",
			"playerId": "p1", "sessionId": "s1", "schemaVersion": "1",
		},
	}).Result()
	if err != nil {
		t.Fatal(err)
	}
	_, err = player.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		ID:     "2-0",
		Values: map[string]interface{}{
			"event": "CardPlayed", "data": `{"n":2}`, "sequence": "2", "eventId": "e2",
			"playerId": "p2", "sessionId": "s2", "schemaVersion": "1",
		},
	}).Result()
	if err != nil {
		t.Fatal(err)
	}
	id3, err := player.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		ID:     "3-0",
		Values: map[string]interface{}{
			"event": "CardPlayed", "data": `{"n":3}`, "sequence": "3", "eventId": "e3",
			"playerId": "p1", "sessionId": "s1", "schemaVersion": "1",
		},
	}).Result()
	if err != nil {
		t.Fatal(err)
	}
	_ = id1

	hub := bff.NewHub()
	feed := bff.NewRedisLiveFeed(player, spectator, "spectator:", hub)

	_, err = feed.BeginSession(ctx, bff.StreamPlayer, room, "s1", "p1", "missing-1")
	if !errors.Is(err, bff.ErrSnapshotRequired) {
		t.Fatalf("unknown id err=%v", err)
	}

	sess, err := feed.BeginSession(ctx, bff.StreamPlayer, room, "s1", "p1", "1-0")
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	replay := sess.Replay()
	if len(replay) != 1 {
		t.Fatalf("replay len=%d want 1 (own audience only)", len(replay))
	}
	if replay[0].ID != id3 {
		t.Fatalf("replay id=%q want %q", replay[0].ID, id3)
	}

	sess2, err := feed.BeginSession(ctx, bff.StreamPlayer, room, "s1", "p1", "")
	if err != nil {
		t.Fatal(err)
	}
	defer sess2.Close()
	if len(sess2.Replay()) != 0 {
		t.Fatal("empty Last-Event-ID must live-tail only")
	}
}

func TestRedisIntegration_SpectatorTrimRequiresSnapshot(t *testing.T) {
	_, player, spectator, ctx := openIntegrationClients(t)
	room := "room_spec_integ"
	if err := bff.ValidateSpectatorRoomID(room); err != nil {
		t.Fatal(err)
	}
	ks := bff.NewSpectatorKeySpace("spectator:")
	meta := ks.Meta(room)
	stream := ks.Stream(room, "1")
	t.Cleanup(func() {
		_ = spectator.Del(ctx, meta, stream)
	})
	_ = spectator.HSet(ctx, meta, "generation", "1")
	_ = spectator.Del(ctx, stream)
	_, err := spectator.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		ID:     "1-0",
		Values: map[string]interface{}{
			"event": "projection_updated", "id": "seq_1", "data": `{}`, "sequence": "1", "closed": "0",
		},
	}).Result()
	if err != nil {
		t.Fatal(err)
	}
	_, err = spectator.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		ID:     "2-0",
		Values: map[string]interface{}{
			"event": "projection_updated", "id": "seq_2", "data": `{}`, "sequence": "2", "closed": "0",
		},
	}).Result()
	if err != nil {
		t.Fatal(err)
	}
	// Trim away 1-0.
	if err := spectator.XTrimMaxLen(ctx, stream, 1).Err(); err != nil {
		t.Fatal(err)
	}

	feed := bff.NewRedisLiveFeed(player, spectator, "spectator:", bff.NewHub())
	_, err = feed.BeginSession(ctx, bff.StreamSpectator, room, "", "", "1-0")
	if !errors.Is(err, bff.ErrSnapshotRequired) {
		t.Fatalf("trimmed id err=%v", err)
	}

	sess, err := feed.BeginSession(ctx, bff.StreamSpectator, room, "", "", "2-0")
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	if len(sess.Replay()) != 0 {
		b, _ := json.Marshal(sess.Replay())
		t.Fatalf("resume at tip should have empty replay: %s", b)
	}
}
