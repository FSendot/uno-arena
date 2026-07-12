package store_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"unoarena/services/ranking/domain"
	"unoarena/services/ranking/store"
)

func TestEmptyProjection_PageUnavailableAndUpsertNoop(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("test-lb-cursor")
	defer restore()
	lb, _ := newTestRedisLB(t)
	ctx := context.Background()
	board := domain.SourceCasualElo

	_, err := lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Limit: 10})
	if !errors.Is(err, store.ErrLeaderboardProjectionUnavailable) {
		t.Fatalf("empty Page want unavailable, got %v", err)
	}

	now := time.Now().UTC()
	if err := lb.UpsertPlayer(ctx, board, "p1", 0, 1500, now, 1); err != nil {
		t.Fatal(err)
	}
	_, err = lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Limit: 10})
	if !errors.Is(err, store.ErrLeaderboardProjectionUnavailable) {
		t.Fatalf("after empty upsert Page still unavailable, got %v", err)
	}
	meta, err := lb.Meta(ctx, board)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Generation != 0 {
		t.Fatalf("upsert must not create generation, got %d", meta.Generation)
	}

	seedViaRebuild(t, lb, board, []domain.LeaderboardEntry{
		{PlayerID: "p1", Rating: 1500},
		{PlayerID: "p2", Rating: 1400},
	})
	page, err := lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Limit: 10})
	if err != nil || len(page.Entries) != 2 {
		t.Fatalf("after rebuild page=%+v err=%v", page, err)
	}
}

func TestCDCDuringRebuild_DualWritesStagingOnlyWhenEmptyLive(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("test-lb-cursor")
	defer restore()
	lb, _ := newTestRedisLB(t)
	ctx := context.Background()
	board := domain.SourceCasualElo
	rdb := lb.Client()

	ks := store.NewLeaderboardKeySpace("ranking:")
	metaKey := ks.Meta(board)
	zkey := ks.GenZSet(board, 1)
	if err := rdb.HSet(ctx, metaKey, "rebuilding_gen", "1").Err(); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	if err := lb.UpsertPlayer(ctx, board, "pCDC", 0, 2200, now, 1); err != nil {
		t.Fatal(err)
	}
	score, err := rdb.ZScore(ctx, zkey, "pCDC").Result()
	if err != nil {
		t.Fatalf("CDC should write staging during rebuild-only: %v", err)
	}
	if store.RatingFromRedisScore(score) != 2200 {
		t.Fatalf("staging score=%v", score)
	}
	meta, _ := lb.Meta(ctx, board)
	if meta.Generation != 0 {
		t.Fatalf("live gen must stay 0 during rebuild-only upsert, got %d", meta.Generation)
	}
}

func TestRebuildSkipsExistingStagingApplied(t *testing.T) {
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
		errCh <- lb.RebuildFromPostgres(ctx, board, blocked, 10, 0)
	}()
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("rebuild did not start")
	}
	now := time.Now().UTC()
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
	found := false
	for _, e := range page.Entries {
		if e.PlayerID == "pKeep" {
			found = true
			if e.Rating != 2500 {
				t.Fatalf("pKeep rating=%d want CDC 2500", e.Rating)
			}
		}
	}
	if !found {
		t.Fatal("pKeep missing after cutover")
	}
}

type blockingBatchSource struct {
	ready   chan struct{}
	release chan struct{}
	inner   *memBatchSource
	once    bool
}

func (b *blockingBatchSource) LeaderboardKeysetPage(ctx context.Context, board domain.RatingSourceType, after *store.LeaderboardCursor, limit int) ([]domain.LeaderboardEntry, error) {
	if !b.once {
		b.once = true
		close(b.ready)
		<-b.release
	}
	return b.inner.LeaderboardKeysetPage(ctx, board, after, limit)
}

func TestAbortRebuild_DoesNotDeleteLiveAfterCutover(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("test-lb-cursor")
	defer restore()
	lb, _ := newTestRedisLB(t)
	ctx := context.Background()
	board := domain.SourceCasualElo
	seedViaRebuild(t, lb, board, []domain.LeaderboardEntry{
		{PlayerID: "p1", Rating: 1200},
	})
	meta, err := lb.Meta(ctx, board)
	if err != nil {
		t.Fatal(err)
	}
	liveGen := meta.Generation
	if liveGen < 1 {
		t.Fatal("expected live gen")
	}
	if err := lb.AbortRebuildForTest(ctx, board, liveGen); err != nil {
		t.Fatal(err)
	}
	page, err := lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 1 || page.Entries[0].PlayerID != "p1" {
		t.Fatalf("live board deleted by abort: %+v", page.Entries)
	}
}

func TestCutover_PreservesHigherProjectionVersion(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("test-lb-cursor")
	defer restore()
	lb, _ := newTestRedisLB(t)
	ctx := context.Background()
	board := domain.SourceCasualElo
	bootstrapProjection(t, lb, board)
	now := time.Now().UTC()
	for i := 0; i < 5; i++ {
		if err := lb.UpsertPlayer(ctx, board, domain.PlayerID("p"+strconv.Itoa(i)), 0, 1000+i, now.Add(time.Duration(i)*time.Second), 10); err != nil {
			t.Fatal(err)
		}
	}
	before, err := lb.Meta(ctx, board)
	if err != nil {
		t.Fatal(err)
	}
	if before.ProjectionVersion != 10 {
		t.Fatalf("same board version must not count players, ver=%d", before.ProjectionVersion)
	}
	ks := store.NewLeaderboardKeySpace("ranking:")
	if err := lb.Client().HSet(ctx, ks.Meta(board), "projectionVersion", "999").Err(); err != nil {
		t.Fatal(err)
	}
	src := &memBatchSource{entries: []domain.LeaderboardEntry{
		{PlayerID: "p0", Rating: 1000},
		{PlayerID: "p1", Rating: 1001},
	}}
	// watermark 0 → bump current+1 (never ZCARD); must not regress below 999
	if err := lb.RebuildFromPostgres(ctx, board, src, 10, 0); err != nil {
		t.Fatal(err)
	}
	after, err := lb.Meta(ctx, board)
	if err != nil {
		t.Fatal(err)
	}
	if after.ProjectionVersion < 999 {
		t.Fatalf("cutover regressed projectionVersion to %d", after.ProjectionVersion)
	}
	if after.ProjectionVersion != 1000 {
		t.Fatalf("watermark 0 should bump 999→1000, got %d", after.ProjectionVersion)
	}
	if err := lb.Client().HSet(ctx, ks.Meta(board), "projectionVersion", "1000").Err(); err != nil {
		t.Fatal(err)
	}
	// Postgres dirty_version watermark must raise version when higher than current
	if err := lb.RebuildFromPostgres(ctx, board, src, 10, 5000); err != nil {
		t.Fatal(err)
	}
	after2, err := lb.Meta(ctx, board)
	if err != nil {
		t.Fatal(err)
	}
	if after2.ProjectionVersion != 5000 {
		t.Fatalf("watermark 5000 want projectionVersion 5000 got %d", after2.ProjectionVersion)
	}
	// Lower watermark must not regress
	if err := lb.RebuildFromPostgres(ctx, board, src, 10, 42); err != nil {
		t.Fatal(err)
	}
	after3, err := lb.Meta(ctx, board)
	if err != nil {
		t.Fatal(err)
	}
	if after3.ProjectionVersion != 5000 {
		t.Fatalf("lower watermark must keep 5000, got %d", after3.ProjectionVersion)
	}
}

func TestPage_RetriesWhenZSetVanishesUnderCutover(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("test-lb-cursor")
	defer restore()
	lb, _ := newTestRedisLB(t)
	ctx := context.Background()
	board := domain.SourceCasualElo
	seedViaRebuild(t, lb, board, []domain.LeaderboardEntry{
		{PlayerID: "a", Rating: 10},
		{PlayerID: "b", Rating: 9},
	})
	meta, _ := lb.Meta(ctx, board)
	oldGen := meta.Generation
	ks := store.NewLeaderboardKeySpace("ranking:")
	oldZ := ks.GenZSet(board, oldGen)
	newGen := oldGen + 1
	newZ := ks.GenZSet(board, newGen)
	rdb := lb.Client()
	if err := rdb.ZAdd(ctx, newZ, redis.Z{Score: store.RedisScoreForRating(10), Member: "a"}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.ZAdd(ctx, newZ, redis.Z{Score: store.RedisScoreForRating(9), Member: "b"}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.HSet(ctx, ks.Meta(board),
		"generation", strconv.FormatInt(newGen, 10),
		"ready", "1",
		"memberCount", "2",
	).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Del(ctx, oldZ).Err(); err != nil {
		t.Fatal(err)
	}

	page, err := lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 2 {
		t.Fatalf("expected page from new gen, got %+v", page.Entries)
	}
}

func TestStaleCDC_DoesNotPoisonStagingDuringRebuild(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("test-lb-cursor")
	defer restore()
	lb, _ := newTestRedisLB(t)
	ctx := context.Background()
	board := domain.SourceCasualElo
	seedViaRebuild(t, lb, board, []domain.LeaderboardEntry{
		{PlayerID: "p1", Rating: 2000},
	})
	t1 := time.Unix(1_700_000_000, 0).UTC()
	t2 := t1.Add(time.Minute)
	if err := lb.UpsertPlayer(ctx, board, "p1", 1500, 2000, t2, 2); err != nil {
		t.Fatal(err)
	}

	src := &memBatchSource{entries: []domain.LeaderboardEntry{
		{PlayerID: "p1", Rating: 2000},
	}}
	ready := make(chan struct{})
	blocked := &blockingBatchSource{ready: ready, release: make(chan struct{}), inner: src}
	errCh := make(chan error, 1)
	go func() {
		errCh <- lb.RebuildFromPostgres(ctx, board, blocked, 10, 0)
	}()
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("rebuild did not start")
	}
	// Stale CDC relative to live fence must not write staging.
	if err := lb.UpsertPlayer(ctx, board, "p1", 2000, 1500, t1, 1); err != nil {
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
	if len(page.Entries) != 1 || page.Entries[0].Rating != 2000 {
		t.Fatalf("stale CDC poisoned cutover: %+v", page.Entries)
	}
}

func TestPage_CursorPathAtomicAcrossGenerationFlip(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("test-lb-cursor")
	defer restore()
	lb, _ := newTestRedisLB(t)
	ctx := context.Background()
	board := domain.SourceCasualElo
	entries := make([]domain.LeaderboardEntry, 0, 6)
	for i := 0; i < 6; i++ {
		entries = append(entries, domain.LeaderboardEntry{
			PlayerID: domain.PlayerID("p" + strconv.Itoa(i)),
			Rating:   1000 - i,
		})
	}
	seedViaRebuild(t, lb, board, entries)

	page1, err := lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if page1.NextCursor == "" || len(page1.Entries) != 2 {
		t.Fatalf("page1=%+v", page1)
	}

	meta, _ := lb.Meta(ctx, board)
	oldGen := meta.Generation
	ks := store.NewLeaderboardKeySpace("ranking:")
	oldZ := ks.GenZSet(board, oldGen)
	newGen := oldGen + 1
	newZ := ks.GenZSet(board, newGen)
	rdb := lb.Client()
	for _, e := range entries {
		if err := rdb.ZAdd(ctx, newZ, redis.Z{
			Score:  store.RedisScoreForRating(e.Rating),
			Member: string(e.PlayerID),
		}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	// Flip generation then delete old zset — cursor page must retry atomically.
	if err := rdb.HSet(ctx, ks.Meta(board),
		"generation", strconv.FormatInt(newGen, 10),
		"ready", "1",
		"memberCount", strconv.Itoa(len(entries)),
		"projectionVersion", "42",
		"generatedAt", time.Now().UTC().Format(time.RFC3339),
	).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Del(ctx, oldZ).Err(); err != nil {
		t.Fatal(err)
	}

	page2, err := lb.Page(ctx, store.LeaderboardPageQuery{
		BoardType: board, Cursor: page1.NextCursor, Limit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.Entries) != 2 {
		t.Fatalf("cursor page after flip: %+v", page2.Entries)
	}
	if page2.ProjectionVersion != 42 {
		t.Fatalf("version from atomic page script want 42 got %d", page2.ProjectionVersion)
	}
	if page2.Entries[0].Rank != 3 {
		t.Fatalf("rankBase after cursor want 3 got %d (%+v)", page2.Entries[0].Rank, page2.Entries)
	}
	if string(page2.Entries[0].PlayerID) != "p2" || string(page2.Entries[1].PlayerID) != "p3" {
		t.Fatalf("unexpected cursor page: %+v", page2.Entries)
	}
}

func TestBeginRebuild_SameTokenAmbiguousSuccessContinues(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("test-lb-cursor")
	defer restore()
	lb, _ := newTestRedisLB(t)
	ctx := context.Background()
	board := domain.SourceCasualElo
	seedViaRebuild(t, lb, board, []domain.LeaderboardEntry{
		{PlayerID: "p1", Rating: 1200},
	})
	meta, err := lb.Meta(ctx, board)
	if err != nil {
		t.Fatal(err)
	}
	newGen := meta.Generation + 1
	ourToken := "tok-ambiguous-owner"
	restoreTok := store.SetRebuildTokenForTest(ourToken)
	defer restoreTok()

	ks := store.NewLeaderboardKeySpace("ranking:")
	// Simulate begin that applied on Redis (our token) before the client saw an error.
	if err := lb.Client().HSet(ctx, ks.Meta(board),
		"rebuilding_gen", strconv.FormatInt(newGen, 10),
		"rebuild_token", ourToken,
	).Err(); err != nil {
		t.Fatal(err)
	}
	if err := lb.Client().Del(ctx, ks.GenZSet(board, newGen), ks.GenApplied(board, newGen)).Err(); err != nil {
		t.Fatal(err)
	}

	src := &memBatchSource{entries: []domain.LeaderboardEntry{
		{PlayerID: "p1", Rating: 1300},
		{PlayerID: "p2", Rating: 1100},
	}}
	if err := lb.RebuildFromPostgres(ctx, board, src, 10, 0); err != nil {
		t.Fatalf("same-token ambiguous begin should reconcile and continue: %v", err)
	}
	page, err := lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 2 || page.Entries[0].Rating != 1300 {
		t.Fatalf("rebuild after reconciled begin: %+v", page.Entries)
	}
	after, err := lb.Meta(ctx, board)
	if err != nil {
		t.Fatal(err)
	}
	if after.RebuildingGen != 0 || after.RebuildToken != "" {
		t.Fatalf("rebuild ownership fields stuck: gen=%d token=%q", after.RebuildingGen, after.RebuildToken)
	}
	if after.Generation != newGen {
		t.Fatalf("generation want %d got %d", newGen, after.Generation)
	}
	if after.MemberCount != 2 {
		t.Fatalf("memberCount want 2 got %d", after.MemberCount)
	}
}

func TestBeginRebuild_DifferentTokenDoesNotJoinOrAbortOther(t *testing.T) {
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
	winnerToken := "tok-winner"
	ks := store.NewLeaderboardKeySpace("ranking:")
	stagingZ := ks.GenZSet(board, newGen)
	stagingApplied := ks.GenApplied(board, newGen)
	if err := lb.Client().HSet(ctx, ks.Meta(board),
		"rebuilding_gen", strconv.FormatInt(newGen, 10),
		"rebuild_token", winnerToken,
	).Err(); err != nil {
		t.Fatal(err)
	}
	if err := lb.Client().ZAdd(ctx, stagingZ, redis.Z{
		Score: store.RedisScoreForRating(9999), Member: "winnerOnly",
	}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := lb.Client().HSet(ctx, stagingApplied, "winnerOnly", "0").Err(); err != nil {
		t.Fatal(err)
	}

	restoreTok := store.SetRebuildTokenForTest("tok-loser")
	defer restoreTok()
	err = lb.RebuildFromPostgres(ctx, board, &memBatchSource{entries: []domain.LeaderboardEntry{
		{PlayerID: "p1", Rating: 1000},
	}}, 10, 0)
	if err == nil {
		t.Fatal("expected already-in-progress for different token")
	}
	if !errors.Is(err, store.ErrLeaderboardProjectionUnavailable) {
		t.Fatalf("want unavailable, got %v", err)
	}
	if !strings.Contains(err.Error(), "rebuild already in progress") {
		t.Fatalf("want clear stuck message, got %v", err)
	}

	// Winner's staging must be untouched.
	score, err := lb.Client().ZScore(ctx, stagingZ, "winnerOnly").Result()
	if err != nil {
		t.Fatalf("loser must not abort winner staging: %v", err)
	}
	if store.RatingFromRedisScore(score) != 9999 {
		t.Fatalf("staging score corrupted: %v", score)
	}
	after, err := lb.Meta(ctx, board)
	if err != nil {
		t.Fatal(err)
	}
	if after.RebuildingGen != newGen || after.RebuildToken != winnerToken {
		t.Fatalf("winner ownership must remain: %+v", after)
	}
	if after.Generation != meta.Generation {
		t.Fatalf("live generation must not change, got %d", after.Generation)
	}
}

func TestBeginRebuild_StuckOtherGenReturnsClearError(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("test-lb-cursor")
	defer restore()
	lb, _ := newTestRedisLB(t)
	ctx := context.Background()
	board := domain.SourceCasualElo
	seedViaRebuild(t, lb, board, []domain.LeaderboardEntry{
		{PlayerID: "p1", Rating: 1000},
	})
	ks := store.NewLeaderboardKeySpace("ranking:")
	if err := lb.Client().HSet(ctx, ks.Meta(board),
		"rebuilding_gen", "99",
		"rebuild_token", "tok-other",
	).Err(); err != nil {
		t.Fatal(err)
	}
	err := lb.RebuildFromPostgres(ctx, board, &memBatchSource{entries: []domain.LeaderboardEntry{
		{PlayerID: "p1", Rating: 1000},
	}}, 10, 0)
	if err == nil {
		t.Fatal("expected clear stuck-rebuild error")
	}
	if !errors.Is(err, store.ErrLeaderboardProjectionUnavailable) {
		t.Fatalf("want unavailable wrap, got %v", err)
	}
	if !strings.Contains(err.Error(), "rebuild already in progress") {
		t.Fatalf("want clear stuck message, got %v", err)
	}
}

func TestEmptyRebuild_PageReturnsEmptySuccess(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("test-lb-cursor")
	defer restore()
	lb, _ := newTestRedisLB(t)
	ctx := context.Background()
	board := domain.SourceCasualElo
	if err := lb.RebuildFromPostgres(ctx, board, &memBatchSource{}, 10, 7); err != nil {
		t.Fatal(err)
	}
	meta, err := lb.Meta(ctx, board)
	if err != nil {
		t.Fatal(err)
	}
	if !meta.Ready || meta.Generation < 1 {
		t.Fatalf("empty rebuild must set ready+generation, got ready=%v gen=%d", meta.Ready, meta.Generation)
	}
	if !meta.MemberCountSet || meta.MemberCount != 0 {
		t.Fatalf("empty rebuild memberCount want 0 set, got set=%v count=%d", meta.MemberCountSet, meta.MemberCount)
	}
	ks := store.NewLeaderboardKeySpace("ranking:")
	n, err := lb.Client().Exists(ctx, ks.GenZSet(board, meta.Generation)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected empty zset key dropped, exists=%d", n)
	}
	page, err := lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Limit: 10})
	if err != nil {
		t.Fatalf("empty live board must Page successfully, got %v", err)
	}
	if len(page.Entries) != 0 {
		t.Fatalf("want empty entries, got %+v", page.Entries)
	}
	if page.NextCursor != "" {
		t.Fatalf("empty page must not set nextCursor: %q", page.NextCursor)
	}
	if page.ProjectionVersion != 7 {
		t.Fatalf("projectionVersion want watermark 7 got %d", page.ProjectionVersion)
	}
}

func TestPage_ReadyMemberCountSemantics(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("test-lb-cursor")
	defer restore()
	lb, _ := newTestRedisLB(t)
	ctx := context.Background()
	board := domain.SourceCasualElo
	ks := store.NewLeaderboardKeySpace("ranking:")
	metaKey := ks.Meta(board)

	// generation set without ready/memberCount + missing zset → unavailable
	if err := lb.Client().HSet(ctx, metaKey,
		"generation", "1",
		"projectionVersion", "1",
	).Err(); err != nil {
		t.Fatal(err)
	}
	_, err := lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Limit: 10})
	if !errors.Is(err, store.ErrLeaderboardProjectionUnavailable) {
		t.Fatalf("no ready → unavailable, got %v", err)
	}

	// ready + memberCount 0 + missing zset → empty success
	if err := lb.Client().HSet(ctx, metaKey,
		"generation", "1",
		"ready", "1",
		"memberCount", "0",
		"projectionVersion", "3",
		"generatedAt", time.Now().UTC().Format(time.RFC3339),
	).Err(); err != nil {
		t.Fatal(err)
	}
	page, err := lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Limit: 10})
	if err != nil || len(page.Entries) != 0 {
		t.Fatalf("ready+memberCount0 want empty success, page=%+v err=%v", page, err)
	}

	// ready + memberCount 5 + missing zset → unavailable
	if err := lb.Client().HSet(ctx, metaKey, "memberCount", "5").Err(); err != nil {
		t.Fatal(err)
	}
	_, err = lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Limit: 10})
	if !errors.Is(err, store.ErrLeaderboardProjectionUnavailable) {
		t.Fatalf("ready+memberCount5+missing zset → unavailable, got %v", err)
	}

	// ready + absent memberCount + missing zset → unavailable (absent ≠ zero)
	if err := lb.Client().HDel(ctx, metaKey, "memberCount").Err(); err != nil {
		t.Fatal(err)
	}
	_, err = lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Limit: 10})
	if !errors.Is(err, store.ErrLeaderboardProjectionUnavailable) {
		t.Fatalf("absent memberCount → unavailable, got %v", err)
	}
}

func TestRebuildBatch_WrongTokenDoesNotWrite(t *testing.T) {
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
	tokenA := "tok-A"
	ks := store.NewLeaderboardKeySpace("ranking:")
	stagingZ := ks.GenZSet(board, newGen)
	if err := lb.Client().HSet(ctx, ks.Meta(board),
		"rebuilding_gen", strconv.FormatInt(newGen, 10),
		"rebuild_token", tokenA,
	).Err(); err != nil {
		t.Fatal(err)
	}
	// Ownership switches to B while A still thinks it owns the rebuild.
	if err := lb.Client().HSet(ctx, ks.Meta(board), "rebuild_token", "tok-B").Err(); err != nil {
		t.Fatal(err)
	}
	err = lb.RunRebuildMemberBatchForTest(ctx, board, newGen, tokenA, 0, []domain.LeaderboardEntry{
		{PlayerID: "intruder", Rating: 9999},
	})
	if err == nil {
		t.Fatal("expected ownership-lost error for wrong token")
	}
	n, err := lb.Client().Exists(ctx, stagingZ).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("wrong-token batch must not write staging, exists=%d", n)
	}
	score, err := lb.Client().ZScore(ctx, stagingZ, "intruder").Result()
	if err == nil {
		t.Fatalf("intruder must not be present, score=%v", score)
	}
}

func TestPage_MemberCountMismatchUnavailable(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("test-lb-cursor")
	defer restore()
	lb, _ := newTestRedisLB(t)
	ctx := context.Background()
	board := domain.SourceCasualElo
	seedViaRebuild(t, lb, board, []domain.LeaderboardEntry{
		{PlayerID: "a", Rating: 10},
		{PlayerID: "b", Rating: 9},
	})
	ks := store.NewLeaderboardKeySpace("ranking:")
	if err := lb.Client().HSet(ctx, ks.Meta(board), "memberCount", "5").Err(); err != nil {
		t.Fatal(err)
	}
	_, err := lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Limit: 10})
	if !errors.Is(err, store.ErrLeaderboardProjectionUnavailable) {
		t.Fatalf("ZCARD!=memberCount must be unavailable, got %v", err)
	}
}

func TestPage_SameScoreScanCapUnavailable(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("test-lb-cursor")
	defer restore()
	restoreScan := store.SetLeaderboardPageMaxScanForTest(3)
	defer restoreScan()
	lb, _ := newTestRedisLB(t)
	ctx := context.Background()
	board := domain.SourceCasualElo
	entries := make([]domain.LeaderboardEntry, 0, 8)
	for i := 0; i < 8; i++ {
		entries = append(entries, domain.LeaderboardEntry{
			PlayerID: domain.PlayerID(fmt.Sprintf("p%02d", i)),
			Rating:   1500,
		})
	}
	seedViaRebuild(t, lb, board, entries)
	// Cursor member missing forces same-score fallback; cap=3 < cohort size.
	cur, err := store.EncodeLeaderboardCursor(store.LeaderboardCursor{
		Rating: 1500, PlayerID: "missing",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Cursor: cur, Limit: 2})
	if !errors.Is(err, store.ErrLeaderboardProjectionUnavailable) {
		t.Fatalf("scan cap must fail closed, got %v", err)
	}
}

func TestPage_CursorEncodeFailureFailClosed(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("test-lb-cursor")
	defer restore()
	lb, _ := newTestRedisLB(t)
	ctx := context.Background()
	board := domain.SourceCasualElo
	seedViaRebuild(t, lb, board, []domain.LeaderboardEntry{
		{PlayerID: "p0", Rating: 100},
		{PlayerID: "p1", Rating: 90},
		{PlayerID: "p2", Rating: 80},
	})
	// After seed, strip cursor secret so NextCursor encode fails (false EOF must not occur).
	store.SetLeaderboardCursorMACKeyForTest("")
	t.Setenv("DEPLOYMENT_ENV", "production")
	t.Setenv("RANKING_LEADERBOARD_CURSOR_SECRET", "")
	_, err := lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Limit: 2})
	if err == nil {
		t.Fatal("expected cursor encode error, got success with possible false EOF")
	}
	if !errors.Is(err, store.ErrLeaderboardCursorSecretRequired) {
		t.Fatalf("want cursor secret required, got %v", err)
	}
}

func TestPage_InvalidGeneratedAtUnavailable(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("test-lb-cursor")
	defer restore()
	lb, _ := newTestRedisLB(t)
	ctx := context.Background()
	board := domain.SourceCasualElo
	seedViaRebuild(t, lb, board, []domain.LeaderboardEntry{
		{PlayerID: "p1", Rating: 1200},
	})
	ks := store.NewLeaderboardKeySpace("ranking:")
	if err := lb.Client().HSet(ctx, ks.Meta(board), "generatedAt", "not-a-timestamp").Err(); err != nil {
		t.Fatal(err)
	}
	_, err := lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Limit: 10})
	if !errors.Is(err, store.ErrLeaderboardProjectionUnavailable) {
		t.Fatalf("invalid generatedAt must be unavailable, got %v", err)
	}
	if err := lb.Client().HDel(ctx, ks.Meta(board), "generatedAt").Err(); err != nil {
		t.Fatal(err)
	}
	_, err = lb.Page(ctx, store.LeaderboardPageQuery{BoardType: board, Limit: 10})
	if !errors.Is(err, store.ErrLeaderboardProjectionUnavailable) {
		t.Fatalf("missing generatedAt must be unavailable, got %v", err)
	}
}
