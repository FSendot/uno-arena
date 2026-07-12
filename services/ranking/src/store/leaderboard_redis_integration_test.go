//go:build redis_integration

package store_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"strings"
	"testing"
	"time"

	"unoarena/services/ranking/domain"
	"unoarena/services/ranking/store"
)

func openRealRedisLeaderboard(t *testing.T) (*store.RedisLeaderboardStore, context.Context) {
	t.Helper()
	url := strings.TrimSpace(os.Getenv("RANKING_REDIS_URL"))
	if url == "" {
		t.Skip("RANKING_REDIS_URL not set")
	}
	if !strings.Contains(url, "/15") {
		t.Fatalf("RANKING_REDIS_URL must target isolated Redis DB 15, got %q", url)
	}
	rdb, err := store.NewRedisFromURL(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rdb.Close() })

	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		t.Fatal(err)
	}
	prefix := "rankingitest:" + hex.EncodeToString(suffix[:]) + ":"
	lb := store.NewRedisLeaderboardStore(rdb, prefix)
	ctx := context.Background()
	if err := lb.Ping(ctx); err != nil {
		t.Fatalf("real Redis unavailable: %v", err)
	}
	if err := lb.LoadScripts(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		var cursor uint64
		for {
			keys, next, err := rdb.Scan(context.Background(), cursor, prefix+"*", 200).Result()
			if err != nil {
				t.Errorf("scan integration prefix: %v", err)
				return
			}
			if len(keys) > 0 {
				if err := rdb.Del(context.Background(), keys...).Err(); err != nil {
					t.Errorf("delete integration prefix: %v", err)
					return
				}
			}
			cursor = next
			if cursor == 0 {
				return
			}
		}
	})
	return lb, ctx
}

func TestRealRedisLeaderboard_RebuildPageAndVersionFencedUpdate(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("real-redis-integration-cursor-key")
	defer restore()
	lb, ctx := openRealRedisLeaderboard(t)
	board := domain.SourceCasualElo

	if err := lb.RebuildFromPostgres(ctx, board, &memBatchSource{entries: []domain.LeaderboardEntry{
		{PlayerID: "p_a", Rating: 1600},
		{PlayerID: "p_b", Rating: 1600},
		{PlayerID: "p_c", Rating: 1500},
	}}, 2, 10); err != nil {
		t.Fatal(err)
	}

	page, err := lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 2 || page.Entries[0].PlayerID != "p_a" || page.Entries[1].PlayerID != "p_b" {
		t.Fatalf("real Redis tie ordering mismatch: %+v", page.Entries)
	}
	if page.ProjectionVersion != 10 || page.NextCursor == "" {
		t.Fatalf("unexpected first page metadata: %+v", page)
	}

	if err := lb.UpsertPlayer(ctx, board, "p_c", 1500, 1700, time.Now().UTC(), 11); err != nil {
		t.Fatal(err)
	}
	if err := lb.UpsertPlayer(ctx, board, "p_c", 1500, 1400, time.Now().UTC(), 9); err != nil {
		t.Fatal(err)
	}
	page, err = lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 3 || page.Entries[0].PlayerID != "p_c" || page.Entries[0].Rating != 1700 {
		t.Fatalf("real Redis version fence mismatch: %+v", page.Entries)
	}
	if page.ProjectionVersion != 11 {
		t.Fatalf("projectionVersion want 11 got %d", page.ProjectionVersion)
	}
}
