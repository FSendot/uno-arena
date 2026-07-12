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

	"unoarena/services/tournament-orchestration/store"
)

func openRealRedisBracket(t *testing.T) (*store.RedisBracketStore, context.Context) {
	t.Helper()
	url := strings.TrimSpace(os.Getenv("TOURNAMENT_REDIS_URL"))
	if url == "" {
		t.Skip("TOURNAMENT_REDIS_URL not set")
	}
	if !strings.Contains(url, "/14") {
		t.Fatalf("TOURNAMENT_REDIS_URL must target isolated Redis DB 14, got %q", url)
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
	prefix := "tournamentitest:" + hex.EncodeToString(suffix[:]) + ":"
	br := store.NewRedisBracketStore(rdb, prefix)
	ctx := context.Background()
	if err := br.Ping(ctx); err != nil {
		t.Fatalf("real Redis unavailable: %v", err)
	}
	if err := br.LoadScripts(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := br.FlushPrefixedKeys(context.Background()); err != nil {
			t.Errorf("flush integration prefix: %v", err)
		}
	})
	return br, ctx
}

func TestRealRedisBracket_RebuildPageAndVersionFence(t *testing.T) {
	restore := store.SetBracketCursorMACKeyForTest("real-redis-bracket-cursor")
	defer restore()
	br, ctx := openRealRedisBracket(t)
	now := time.Now().UTC()
	tid := "t-real"

	src := &memBracketSource{
		summary: sampleSummary("in_progress", 3),
		ver:     10,
		at:      now,
		refs:    []store.BracketChunkRef{{RoundNumber: 1, BatchID: "b0"}},
		chunks:  map[string][]store.BracketSlotView{"1:b0": sampleSlots("b0", 0, 2)},
	}
	if err := br.RebuildFromPostgres(ctx, tid, src, 10); err != nil {
		t.Fatal(err)
	}
	page, err := br.Page(ctx, store.BracketPageQuery{TournamentID: tid, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Slots) != 2 || page.Slots[0].SlotIndex != 0 || page.NextCursor == "" {
		t.Fatalf("page=%+v", page)
	}
	if page.ProjectionVersion != 10 {
		t.Fatalf("ver=%d", page.ProjectionVersion)
	}
	if err := br.UpsertSummary(ctx, tid, sampleSummary("in_progress", 9), 11, now); err != nil {
		t.Fatal(err)
	}
	if err := br.UpsertSummary(ctx, tid, sampleSummary("in_progress", 1), 9, now); err != nil {
		t.Fatal(err)
	}
	page, err = br.Page(ctx, store.BracketPageQuery{TournamentID: tid, Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if page.Summary.RegisteredCount != 9 || page.ProjectionVersion != 11 {
		t.Fatalf("fence mismatch: %+v", page)
	}
}
