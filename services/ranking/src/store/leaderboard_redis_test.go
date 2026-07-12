package store_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"unoarena/services/ranking/domain"
	"unoarena/services/ranking/store"
)

func newTestRedisLB(t *testing.T) (*store.RedisLeaderboardStore, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	lb := store.NewRedisLeaderboardStore(rdb, "ranking:")
	if err := lb.LoadScripts(context.Background()); err != nil {
		t.Fatal(err)
	}
	return lb, mr
}

// bootstrapProjection creates an empty live generation via rebuild so CDC upserts can apply.
func bootstrapProjection(t *testing.T, lb *store.RedisLeaderboardStore, board domain.RatingSourceType) {
	t.Helper()
	if err := lb.RebuildFromPostgres(context.Background(), board, &memBatchSource{}, 10, 0); err != nil {
		t.Fatal(err)
	}
}

func seedViaRebuild(t *testing.T, lb *store.RedisLeaderboardStore, board domain.RatingSourceType, entries []domain.LeaderboardEntry) {
	t.Helper()
	if err := lb.RebuildFromPostgres(context.Background(), board, &memBatchSource{entries: entries}, 100, 0); err != nil {
		t.Fatal(err)
	}
}

func TestRedisLeaderboard_OrderingAndTies(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("test-lb-cursor")
	defer restore()
	lb, _ := newTestRedisLB(t)
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0).UTC()
	board := domain.SourceCasualElo
	bootstrapProjection(t, lb, board)
	for i, e := range []struct {
		id     string
		rating int
	}{
		{"p_c", 1500},
		{"p_a", 1500},
		{"p_b", 1600},
		{"p_d", 1400},
	} {
		if err := lb.UpsertPlayer(ctx, board, domain.PlayerID(e.id), 0, e.rating, now, int64(i+1)); err != nil {
			t.Fatal(err)
		}
	}
	page, err := lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"p_b", "p_a", "p_c", "p_d"}
	if len(page.Entries) != len(want) {
		t.Fatalf("entries=%+v", page.Entries)
	}
	for i, id := range want {
		if string(page.Entries[i].PlayerID) != id {
			t.Fatalf("rank %d: got %s want %s (%+v)", i+1, page.Entries[i].PlayerID, id, page.Entries)
		}
		if page.Entries[i].Rank != i+1 {
			t.Fatalf("rank field: %+v", page.Entries[i])
		}
	}
}

func TestRedisLeaderboard_CursorPagingAndEOF(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("test-lb-cursor")
	defer restore()
	lb, _ := newTestRedisLB(t)
	ctx := context.Background()
	now := time.Now().UTC()
	board := domain.SourceCasualElo
	bootstrapProjection(t, lb, board)
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("p%02d", i)
		if err := lb.UpsertPlayer(ctx, board, domain.PlayerID(id), 0, 1000-i, now.Add(time.Duration(i)*time.Second), int64(i+1)); err != nil {
			t.Fatal(err)
		}
	}
	page1, err := lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page1.Entries) != 2 || page1.NextCursor == "" {
		t.Fatalf("page1=%+v", page1)
	}
	page2, err := lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Cursor: page1.NextCursor, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.Entries) != 2 || page2.NextCursor == "" {
		t.Fatalf("page2=%+v", page2)
	}
	page3, err := lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Cursor: page2.NextCursor, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page3.Entries) != 1 || page3.NextCursor != "" {
		t.Fatalf("final page should omit nextCursor: %+v", page3)
	}
}

func TestLeaderboardCursor_TamperAndLeakage(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("test-lb-cursor")
	defer restore()
	enc, err := store.EncodeLeaderboardCursor(store.LeaderboardCursor{Rating: 1200, PlayerID: "p1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DecodeLeaderboardCursor(enc + "x"); err == nil {
		t.Fatal("expected tamper reject")
	}
	if _, err := store.DecodeLeaderboardCursor("lb1.notbase64.mac"); err == nil {
		t.Fatal("expected invalid")
	}
	if _, err := store.DecodeLeaderboardCursor("redis:offset=1"); !errors.Is(err, store.ErrInvalidLeaderboardCursor) {
		t.Fatalf("leakage: %v", err)
	}
	if _, err := store.DecodeLeaderboardCursor("lb1.abc.ranking:v1:lb"); err == nil {
		t.Fatal("expected ranking key leakage reject")
	}
	got, err := store.DecodeLeaderboardCursor(enc)
	if err != nil || got.PlayerID != "p1" || got.Rating != 1200 {
		t.Fatalf("roundtrip=%+v err=%v", got, err)
	}
}

func TestLeaderboardCursor_ProdRequiresSecret(t *testing.T) {
	t.Setenv("DEPLOYMENT_ENV", "production")
	t.Setenv("RANKING_LEADERBOARD_CURSOR_SECRET", "")
	store.SetLeaderboardCursorMACKeyForTest("")
	if _, err := store.EncodeLeaderboardCursor(store.LeaderboardCursor{Rating: 1, PlayerID: "p"}); !errors.Is(err, store.ErrLeaderboardCursorSecretRequired) {
		t.Fatalf("want secret required, got %v", err)
	}
}

func TestRedisLeaderboard_StaleAndDuplicateCDC(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("test-lb-cursor")
	defer restore()
	lb, _ := newTestRedisLB(t)
	ctx := context.Background()
	board := domain.SourceCasualElo
	bootstrapProjection(t, lb, board)
	t1 := time.Unix(1_700_000_000, 0).UTC()
	t2 := t1.Add(time.Minute)
	if err := lb.UpsertPlayer(ctx, board, "p1", 1000, 1100, t2, 2); err != nil {
		t.Fatal(err)
	}
	// Stale older event must not regress.
	if err := lb.UpsertPlayer(ctx, board, "p1", 1000, 1050, t1, 1); err != nil {
		t.Fatal(err)
	}
	page, err := lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 1 || page.Entries[0].Rating != 1100 {
		t.Fatalf("stale regress: %+v", page.Entries)
	}
	// Duplicate same fence is idempotent.
	if err := lb.UpsertPlayer(ctx, board, "p1", 1000, 1100, t2, 2); err != nil {
		t.Fatal(err)
	}
	// Zero-delta no-op.
	if err := lb.UpsertPlayer(ctx, board, "p1", 1100, 1100, t2.Add(time.Second), 0); err != nil {
		t.Fatal(err)
	}
}

func TestRedisLeaderboard_ConcurrentRebuildAndUpsert(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("test-lb-cursor")
	defer restore()
	lb, _ := newTestRedisLB(t)
	ctx := context.Background()
	board := domain.SourceCasualElo
	src := &memBatchSource{entries: make([]domain.LeaderboardEntry, 0, 200)}
	for i := 0; i < 200; i++ {
		src.entries = append(src.entries, domain.LeaderboardEntry{
			PlayerID: domain.PlayerID(fmt.Sprintf("p%04d", i)),
			Rating:   1000,
		})
	}
	seedViaRebuild(t, lb, board, src.entries[:50])
	now := time.Now().UTC()

	ready := make(chan struct{})
	blocked := &blockingBatchSource{ready: ready, release: make(chan struct{}), inner: src}
	errCh := make(chan error, 2)
	go func() {
		errCh <- lb.RebuildFromPostgres(ctx, board, blocked, 50, 0)
	}()
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("rebuild did not start")
	}
	// CDC before rebuild batches: must win for overlapping players.
	for i := 0; i < 20; i++ {
		id := domain.PlayerID(fmt.Sprintf("p%04d", i))
		if err := lb.UpsertPlayer(ctx, board, id, 1000, 2500+i, now.Add(time.Minute+time.Duration(i)*time.Millisecond), int64(100+i)); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			id := domain.PlayerID(fmt.Sprintf("live%02d", i))
			_ = lb.UpsertPlayer(ctx, board, id, 0, 3000+i, now.Add(time.Duration(i)*time.Millisecond), int64(200+i))
		}
	}()
	close(blocked.release)
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	var all []store.RankedLeaderboardEntry
	cursor := ""
	for {
		page, err := lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Cursor: cursor, Limit: 500})
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, page.Entries...)
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if len(all) < 200 {
		t.Fatalf("after rebuild expect >=200 got %d", len(all))
	}
	byID := map[string]int{}
	for _, e := range all {
		byID[string(e.PlayerID)] = e.Rating
	}
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("p%04d", i)
		want := 2500 + i
		if byID[id] != want {
			t.Fatalf("%s rating=%d want CDC %d", id, byID[id], want)
		}
	}
	foundLive := 0
	for i := 0; i < 10; i++ {
		if _, ok := byID[fmt.Sprintf("live%02d", i)]; ok {
			foundLive++
		}
	}
	if foundLive == 0 {
		t.Fatal("expected concurrent CDC-only upserts to dual-write into rebuild staging")
	}
}

func TestRedisLeaderboard_LargeTieKeysetContinuity(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("test-lb-cursor")
	defer restore()
	lb, _ := newTestRedisLB(t)
	ctx := context.Background()
	board := domain.SourceCasualElo
	const n = 600
	const rating = 1500
	entries := make([]domain.LeaderboardEntry, 0, n)
	for i := 0; i < n; i++ {
		entries = append(entries, domain.LeaderboardEntry{
			PlayerID: domain.PlayerID(fmt.Sprintf("tie%04d", i)),
			Rating:   rating,
		})
	}
	seedViaRebuild(t, lb, board, entries)
	_ = ctx
	// Mid-tie cursor: after tie0299, next page must start at tie0300 with no gaps.
	mid := store.LeaderboardCursor{Rating: rating, PlayerID: "tie0299"}
	enc, err := store.EncodeLeaderboardCursor(mid)
	if err != nil {
		t.Fatal(err)
	}
	page, err := lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Cursor: enc, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 100 {
		t.Fatalf("entries=%d", len(page.Entries))
	}
	for i, e := range page.Entries {
		want := fmt.Sprintf("tie%04d", 300+i)
		if string(e.PlayerID) != want {
			t.Fatalf("gap at i=%d got %s want %s", i, e.PlayerID, want)
		}
		if e.Rating != rating {
			t.Fatalf("rating drift %+v", e)
		}
	}
	// Walk remaining pages — full continuity through EOF.
	seen := map[string]bool{}
	for _, e := range page.Entries {
		seen[string(e.PlayerID)] = true
	}
	cursor := page.NextCursor
	for cursor != "" {
		page, err = lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Cursor: cursor, Limit: 100})
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range page.Entries {
			id := string(e.PlayerID)
			if seen[id] {
				t.Fatalf("duplicate %s", id)
			}
			seen[id] = true
		}
		cursor = page.NextCursor
	}
	for i := 300; i < n; i++ {
		id := fmt.Sprintf("tie%04d", i)
		if !seen[id] {
			t.Fatalf("missing %s after mid-tie cursor walk", id)
		}
	}
}

func TestLeaderboardKeys_HashTag(t *testing.T) {
	ks := store.NewLeaderboardKeySpace("ranking:")
	meta := ks.Meta(domain.SourceCasualElo)
	z := ks.GenZSet(domain.SourceCasualElo, 1)
	if !strings.Contains(meta, "{casual_elo}") || !strings.Contains(z, "{casual_elo}") {
		t.Fatalf("expected boardType hash-tag keys meta=%s z=%s", meta, z)
	}
	if !strings.HasPrefix(z, ks.BoardRoot(domain.SourceCasualElo)) {
		t.Fatalf("zset must share board root")
	}
}

func TestRedisLeaderboard_PageLimitBounded(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("test-lb-cursor")
	defer restore()
	lb, _ := newTestRedisLB(t)
	ctx := context.Background()
	board := domain.SourceCasualElo
	entries := make([]domain.LeaderboardEntry, 0, 1200)
	for i := 0; i < 1200; i++ {
		entries = append(entries, domain.LeaderboardEntry{
			PlayerID: domain.PlayerID(fmt.Sprintf("p%05d", i)),
			Rating:   5000 - i,
		})
	}
	seedViaRebuild(t, lb, board, entries)
	_ = ctx
	page, err := lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Limit: 9999})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) > store.MaxLeaderboardPageLimit {
		t.Fatalf("page exceeded max: %d", len(page.Entries))
	}
	if len(page.Entries) != store.MaxLeaderboardPageLimit {
		t.Fatalf("clamped limit want %d got %d", store.MaxLeaderboardPageLimit, len(page.Entries))
	}
	if page.NextCursor == "" {
		t.Fatal("expected nextCursor for truncated page")
	}
}

func TestClampLeaderboardLimit(t *testing.T) {
	if store.ClampLeaderboardLimit(0) != 100 {
		t.Fatal("default")
	}
	if store.ClampLeaderboardLimit(501) != 500 {
		t.Fatal("max")
	}
}

type memBatchSource struct {
	entries []domain.LeaderboardEntry
}

func (m *memBatchSource) LeaderboardKeysetPage(_ context.Context, _ domain.RatingSourceType, after *store.LeaderboardCursor, limit int) ([]domain.LeaderboardEntry, error) {
	start := 0
	if after != nil {
		start = len(m.entries)
		for i, e := range m.entries {
			if e.Rating < after.Rating || (e.Rating == after.Rating && string(e.PlayerID) > after.PlayerID) {
				start = i
				break
			}
		}
	}
	end := start + limit
	if end > len(m.entries) {
		end = len(m.entries)
	}
	out := make([]domain.LeaderboardEntry, end-start)
	copy(out, m.entries[start:end])
	return out, nil
}
