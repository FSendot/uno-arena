package store_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"unoarena/services/ranking/domain"
	"unoarena/services/ranking/store"
)

func TestUpsert_SameBoardVersionMultiPlayerDoesNotCountPlayers(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("test-lb-cursor")
	defer restore()
	lb, _ := newTestRedisLB(t)
	ctx := context.Background()
	board := domain.SourceCasualElo
	bootstrapProjection(t, lb, board)
	now := time.Now().UTC()
	const boardVer int64 = 42
	for i, id := range []string{"p1", "p2", "p3"} {
		if err := lb.UpsertPlayer(ctx, board, domain.PlayerID(id), 0, 1000+i, now, boardVer); err != nil {
			t.Fatal(err)
		}
	}
	meta, err := lb.Meta(ctx, board)
	if err != nil {
		t.Fatal(err)
	}
	if meta.ProjectionVersion != boardVer {
		t.Fatalf("projectionVersion must be board dirty_version %d, not player count; got %d", boardVer, meta.ProjectionVersion)
	}
	page, err := lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 3 {
		t.Fatalf("entries=%+v", page.Entries)
	}
}

func TestUpsert_StaleVersionCannotRegress(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("test-lb-cursor")
	defer restore()
	lb, _ := newTestRedisLB(t)
	ctx := context.Background()
	board := domain.SourceCasualElo
	bootstrapProjection(t, lb, board)
	now := time.Now().UTC()
	if err := lb.UpsertPlayer(ctx, board, "p1", 1000, 1100, now, 5); err != nil {
		t.Fatal(err)
	}
	if err := lb.UpsertPlayer(ctx, board, "p1", 1000, 1050, now.Add(-time.Minute), 4); err != nil {
		t.Fatal(err)
	}
	page, err := lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 1 || page.Entries[0].Rating != 1100 {
		t.Fatalf("stale version regressed: %+v", page.Entries)
	}
	meta, err := lb.Meta(ctx, board)
	if err != nil {
		t.Fatal(err)
	}
	if meta.ProjectionVersion != 5 {
		t.Fatalf("meta version want 5 got %d", meta.ProjectionVersion)
	}
}

func TestUpsert_EqualVersionConflictErrors(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("test-lb-cursor")
	defer restore()
	lb, _ := newTestRedisLB(t)
	ctx := context.Background()
	board := domain.SourceCasualElo
	bootstrapProjection(t, lb, board)
	now := time.Now().UTC()
	if err := lb.UpsertPlayer(ctx, board, "p1", 1000, 1100, now, 7); err != nil {
		t.Fatal(err)
	}
	err := lb.UpsertPlayer(ctx, board, "p1", 1000, 1200, now, 7)
	if !errors.Is(err, store.ErrLeaderboardProjectionConflict) {
		t.Fatalf("want conflict, got %v", err)
	}
	page, err := lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if page.Entries[0].Rating != 1100 {
		t.Fatalf("conflict must not change score: %+v", page.Entries)
	}
}

func TestUpsert_EqualVersionDuplicateIdempotent(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("test-lb-cursor")
	defer restore()
	lb, _ := newTestRedisLB(t)
	ctx := context.Background()
	board := domain.SourceCasualElo
	bootstrapProjection(t, lb, board)
	now := time.Now().UTC()
	if err := lb.UpsertPlayer(ctx, board, "p1", 1000, 1100, now, 3); err != nil {
		t.Fatal(err)
	}
	if err := lb.UpsertPlayer(ctx, board, "p1", 1000, 1100, now.Add(time.Second), 3); err != nil {
		t.Fatal(err)
	}
	meta, err := lb.Meta(ctx, board)
	if err != nil {
		t.Fatal(err)
	}
	if meta.ProjectionVersion != 3 {
		t.Fatalf("idempotent duplicate must not bump version, got %d", meta.ProjectionVersion)
	}
}

func TestRebuild_WatermarkBeatsDelayedOldCDC(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("test-lb-cursor")
	defer restore()
	lb, _ := newTestRedisLB(t)
	ctx := context.Background()
	board := domain.SourceCasualElo
	if err := lb.RebuildFromPostgres(ctx, board, &memBatchSource{entries: []domain.LeaderboardEntry{
		{PlayerID: "p1", Rating: 2000},
	}}, 10, 10); err != nil {
		t.Fatal(err)
	}
	// Delayed CDC from an older dirty_version must not regress rebuild state.
	if err := lb.UpsertPlayer(ctx, board, "p1", 1500, 1600, time.Now().UTC(), 5); err != nil {
		t.Fatal(err)
	}
	page, err := lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 1 || page.Entries[0].Rating != 2000 {
		t.Fatalf("delayed old CDC overwrote watermark rebuild: %+v", page.Entries)
	}
	meta, err := lb.Meta(ctx, board)
	if err != nil {
		t.Fatal(err)
	}
	if meta.ProjectionVersion != 10 {
		t.Fatalf("projectionVersion want watermark 10 got %d", meta.ProjectionVersion)
	}
}

func TestRebuild_ConcurrentHigherCDCBeatsBatch(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("test-lb-cursor")
	defer restore()
	lb, _ := newTestRedisLB(t)
	ctx := context.Background()
	board := domain.SourceCasualElo
	bootstrapProjection(t, lb, board)

	src := &memBatchSource{entries: []domain.LeaderboardEntry{
		{PlayerID: "pKeep", Rating: 1000},
		{PlayerID: "pOther", Rating: 900},
	}}
	ready := make(chan struct{})
	blocked := &blockingBatchSource{ready: ready, release: make(chan struct{}), inner: src}
	errCh := make(chan error, 1)
	go func() {
		errCh <- lb.RebuildFromPostgres(ctx, board, blocked, 10, 5)
	}()
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("rebuild did not start")
	}
	now := time.Now().UTC()
	// Higher dirty_version CDC during rebuild must win over batch watermark 5.
	if err := lb.UpsertPlayer(ctx, board, "pKeep", 1000, 2500, now, 9); err != nil {
		t.Fatal(err)
	}
	close(blocked.release)
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	page, err := lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range page.Entries {
		if e.PlayerID == "pKeep" && e.Rating != 2500 {
			t.Fatalf("higher CDC lost to batch: %+v", page.Entries)
		}
	}
	ks := store.NewLeaderboardKeySpace("ranking:")
	meta, err := lb.Meta(ctx, board)
	if err != nil {
		t.Fatal(err)
	}
	applied, err := lb.Client().HGet(ctx, ks.GenApplied(board, meta.Generation), "pKeep").Result()
	if err != nil {
		t.Fatal(err)
	}
	if applied != "9" {
		t.Fatalf("applied fence want CDC version 9 got %q", applied)
	}
}

func TestUpsert_RebuildConvergenceAllowsMatchingDelayedEvent(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("test-lb-cursor")
	defer restore()
	lb, _ := newTestRedisLB(t)
	ctx := context.Background()
	board := domain.SourceCasualElo
	if err := lb.RebuildFromPostgres(ctx, board, &memBatchSource{entries: []domain.LeaderboardEntry{
		{PlayerID: "p1", Rating: 2000},
	}}, 10, 8); err != nil {
		t.Fatal(err)
	}
	// Delayed matching event at the rebuild watermark converges idempotently.
	if err := lb.UpsertPlayer(ctx, board, "p1", 1500, 2000, time.Now().UTC(), 8); err != nil {
		t.Fatal(err)
	}
	page, err := lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if page.Entries[0].Rating != 2000 {
		t.Fatalf("convergence failed: %+v", page.Entries)
	}
}

func TestUpsert_CausalGapOnHigherVersionConflicts(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("test-lb-cursor")
	defer restore()
	lb, _ := newTestRedisLB(t)
	ctx := context.Background()
	board := domain.SourceCasualElo
	bootstrapProjection(t, lb, board)
	now := time.Now().UTC()
	if err := lb.UpsertPlayer(ctx, board, "p1", 1000, 2000, now, 2); err != nil {
		t.Fatal(err)
	}
	err := lb.UpsertPlayer(ctx, board, "p1", 1500, 2100, now, 3)
	if !errors.Is(err, store.ErrLeaderboardProjectionConflict) {
		t.Fatalf("causal gap want conflict, got %v", err)
	}
}

func TestRebuildBatch_DoesNotOverwriteHigherAppliedVersion(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("test-lb-cursor")
	defer restore()
	lb, _ := newTestRedisLB(t)
	ctx := context.Background()
	board := domain.SourceCasualElo
	seedViaRebuild(t, lb, board, []domain.LeaderboardEntry{
		{PlayerID: "p1", Rating: 1000},
	})
	meta, err := lb.Meta(ctx, board)
	if err != nil {
		t.Fatal(err)
	}
	newGen := meta.Generation + 1
	token := "tok-batch"
	ks := store.NewLeaderboardKeySpace("ranking:")
	stagingZ := ks.GenZSet(board, newGen)
	stagingApplied := ks.GenApplied(board, newGen)
	if err := lb.Client().HSet(ctx, ks.Meta(board),
		"rebuilding_gen", strconv.FormatInt(newGen, 10),
		"rebuild_token", token,
	).Err(); err != nil {
		t.Fatal(err)
	}
	if err := lb.Client().ZAdd(ctx, stagingZ, redis.Z{
		Score: store.RedisScoreForRating(2500), Member: "p1",
	}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := lb.Client().HSet(ctx, stagingApplied, "p1", "12").Err(); err != nil {
		t.Fatal(err)
	}
	if err := lb.RunRebuildMemberBatchForTest(ctx, board, newGen, token, 5, []domain.LeaderboardEntry{
		{PlayerID: "p1", Rating: 1000},
	}); err != nil {
		t.Fatal(err)
	}
	score, err := lb.Client().ZScore(ctx, stagingZ, "p1").Result()
	if err != nil {
		t.Fatal(err)
	}
	if store.RatingFromRedisScore(score) != 2500 {
		t.Fatalf("batch overwrote higher CDC applied version: score=%v", score)
	}
	applied, err := lb.Client().HGet(ctx, stagingApplied, "p1").Result()
	if err != nil {
		t.Fatal(err)
	}
	if applied != "12" {
		t.Fatalf("applied want 12 got %q", applied)
	}
}
