package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"unoarena/services/tournament-orchestration/store"
)

func newTestRedisBracket(t *testing.T) (*store.RedisBracketStore, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	br := store.NewRedisBracketStore(rdb, "tournament:")
	if err := br.LoadScripts(context.Background()); err != nil {
		t.Fatal(err)
	}
	return br, mr
}

func sampleSummary(phase string, reg int) store.BracketSummary {
	return sampleSummarySlots(phase, reg, 2)
}

func sampleSummarySlots(phase string, reg, slotCount int) store.BracketSummary {
	return store.BracketSummary{
		Phase: phase, Capacity: 16, RegisteredCount: reg, CurrentRound: 1,
		Rounds: []store.BracketRoundSummary{{
			RoundNumber: 1, Status: "seeded", SlotCount: slotCount, BatchCount: 1,
		}},
	}
}

func sampleSlots(batchID string, from, to int) []store.BracketSlotView {
	out := make([]store.BracketSlotView, 0, to-from+1)
	for i := from; i <= to; i++ {
		out = append(out, store.BracketSlotView{
			RoundNumber: 1, SlotIndex: i, SlotID: fmt.Sprintf("slot_%d", i),
			Status: "ready", SeededPlayerIDs: []string{fmt.Sprintf("p%d", i)},
			BatchID: batchID,
		})
	}
	return out
}

type memBracketSource struct {
	summary store.BracketSummary
	ver     int64
	at      time.Time
	chunks  map[string][]store.BracketSlotView // "round:batch" → slots
	refs    []store.BracketChunkRef
}

func (m *memBracketSource) LoadBracketSummary(context.Context, string) (store.BracketSummary, bool, error) {
	return m.summary, true, nil
}
func (m *memBracketSource) LoadProjectionCheckpoint(context.Context, string) (int64, time.Time, error) {
	return m.ver, m.at, nil
}
func (m *memBracketSource) ListVisibleBracketChunks(context.Context, string) ([]store.BracketChunkRef, error) {
	return m.refs, nil
}
func (m *memBracketSource) LoadBatchChunkForProjection(_ context.Context, _ string, roundNumber int, batchID string) ([]store.BracketSlotView, error) {
	return m.chunks[fmt.Sprintf("%d:%s", roundNumber, batchID)], nil
}

func TestRedisBracket_ChunkBoundariesAndExactOrder(t *testing.T) {
	restore := store.SetBracketCursorMACKeyForTest("test-bracket-cursor")
	defer restore()
	br, _ := newTestRedisBracket(t)
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0).UTC()
	tid := "t-order"

	sum := sampleSummarySlots("in_progress", 8, 11)
	if err := br.UpsertSummary(ctx, tid, sum, 1, now); err != nil {
		t.Fatal(err)
	}
	// Two chunks; slot_10 must not precede slot_2 (numeric order via score).
	if err := br.UpsertChunk(ctx, tid, 1, "batch_0", sampleSlots("batch_0", 0, 1), 1, now); err != nil {
		t.Fatal(err)
	}
	if err := br.UpsertChunk(ctx, tid, 1, "batch_1", sampleSlots("batch_1", 2, 10), 1, now); err != nil {
		t.Fatal(err)
	}

	page, err := br.Page(ctx, store.BracketPageQuery{TournamentID: tid, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Slots) != 11 {
		t.Fatalf("slots=%d want 11", len(page.Slots))
	}
	for i, sl := range page.Slots {
		if sl.SlotIndex != i {
			t.Fatalf("slot[%d].SlotIndex=%d want %d", i, sl.SlotIndex, i)
		}
	}
	if page.ProjectionVersion != 1 || page.Summary.RegisteredCount != 8 {
		t.Fatalf("page meta=%+v", page)
	}
}

func TestRedisBracket_CursorStableAcrossVersions(t *testing.T) {
	restore := store.SetBracketCursorMACKeyForTest("test-bracket-cursor")
	defer restore()
	br, _ := newTestRedisBracket(t)
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0).UTC()
	tid := "t-cursor"

	if err := br.UpsertSummary(ctx, tid, sampleSummarySlots("in_progress", 4, 4), 1, now); err != nil {
		t.Fatal(err)
	}
	if err := br.UpsertChunk(ctx, tid, 1, "b0", sampleSlots("b0", 0, 3), 1, now); err != nil {
		t.Fatal(err)
	}
	page1, err := br.Page(ctx, store.BracketPageQuery{TournamentID: tid, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if page1.NextCursor == "" {
		t.Fatal("expected next cursor")
	}
	// Bump version; cursor must still work (version change must NOT invalidate cursor).
	sum2 := sampleSummarySlots("in_progress", 5, 4)
	if err := br.UpsertSummary(ctx, tid, sum2, 2, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	page2, err := br.Page(ctx, store.BracketPageQuery{TournamentID: tid, Cursor: page1.NextCursor, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.Slots) != 2 || page2.Slots[0].SlotIndex != 2 || page2.Slots[1].SlotIndex != 3 {
		t.Fatalf("page2=%+v", page2.Slots)
	}
	if page2.ProjectionVersion != 2 {
		t.Fatalf("version=%d", page2.ProjectionVersion)
	}
}

func TestRedisBracket_StaleEqualHigherVersions(t *testing.T) {
	br, _ := newTestRedisBracket(t)
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0).UTC()
	tid := "t-fence"
	sum := sampleSummary("open", 1)
	if err := br.UpsertSummary(ctx, tid, sum, 5, now); err != nil {
		t.Fatal(err)
	}
	// Stale no-op.
	if err := br.UpsertSummary(ctx, tid, sampleSummary("open", 99), 4, now); err != nil {
		t.Fatal(err)
	}
	meta, err := br.Meta(ctx, tid)
	if err != nil {
		t.Fatal(err)
	}
	if meta.ProjectionVersion != 5 {
		t.Fatalf("stale applied: ver=%d", meta.ProjectionVersion)
	}
	// Equal + same JSON idempotent.
	if err := br.UpsertSummary(ctx, tid, sum, 5, now); err != nil {
		t.Fatal(err)
	}
	// Equal + different JSON conflict.
	err = br.UpsertSummary(ctx, tid, sampleSummary("open", 2), 5, now)
	if !errors.Is(err, store.ErrBracketProjectionConflict) {
		t.Fatalf("want conflict, got %v", err)
	}
	// Higher wins.
	if err := br.UpsertSummary(ctx, tid, sampleSummary("closed", 3), 6, now); err != nil {
		t.Fatal(err)
	}
	meta, _ = br.Meta(ctx, tid)
	if meta.ProjectionVersion != 6 {
		t.Fatalf("ver=%d", meta.ProjectionVersion)
	}
}

func TestRedisBracket_ConcurrentRebuildAndUpdate(t *testing.T) {
	restore := store.SetBracketCursorMACKeyForTest("test-bracket-cursor")
	defer restore()
	br, _ := newTestRedisBracket(t)
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0).UTC()
	tid := "t-race"

	src := &memBracketSource{
		summary: sampleSummary("in_progress", 2),
		ver:     10,
		at:      now,
		refs:    []store.BracketChunkRef{{RoundNumber: 1, BatchID: "b0"}},
		chunks: map[string][]store.BracketSlotView{
			"1:b0": sampleSlots("b0", 0, 1),
		},
	}
	blocked := &blockingBracketSource{inner: src, gate: make(chan struct{})}
	errCh := make(chan error, 1)
	go func() {
		errCh <- br.RebuildFromPostgres(ctx, tid, blocked, 10)
	}()
	// Wait until rebuild has begun (rebuilding_gen set), then dual-write higher version.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		m, _ := br.Meta(ctx, tid)
		if m.RebuildingGen > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := br.UpsertSummary(ctx, tid, sampleSummary("in_progress", 99), 20, now.Add(time.Second)); err != nil {
		close(blocked.gate)
		t.Fatal(err)
	}
	close(blocked.gate)
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	page, err := br.Page(ctx, store.BracketPageQuery{TournamentID: tid, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	// Higher-version incremental must not be lost across cutover (dual-write + watermark).
	if page.Summary.RegisteredCount != 99 || page.ProjectionVersion < 20 {
		t.Fatalf("lost concurrent update: page=%+v", page)
	}
}

type blockingBracketSource struct {
	inner *memBracketSource
	gate  chan struct{}
}

func (b *blockingBracketSource) LoadBracketSummary(ctx context.Context, tid string) (store.BracketSummary, bool, error) {
	<-b.gate
	return b.inner.LoadBracketSummary(ctx, tid)
}
func (b *blockingBracketSource) LoadProjectionCheckpoint(ctx context.Context, tid string) (int64, time.Time, error) {
	return b.inner.LoadProjectionCheckpoint(ctx, tid)
}
func (b *blockingBracketSource) ListVisibleBracketChunks(ctx context.Context, tid string) ([]store.BracketChunkRef, error) {
	return b.inner.ListVisibleBracketChunks(ctx, tid)
}
func (b *blockingBracketSource) LoadBatchChunkForProjection(ctx context.Context, tid string, rn int, bid string) ([]store.BracketSlotView, error) {
	return b.inner.LoadBatchChunkForProjection(ctx, tid, rn, bid)
}

func TestRedisBracket_EmptyTournamentAndChunkSizeBound(t *testing.T) {
	restore := store.SetBracketCursorMACKeyForTest("test-bracket-cursor")
	defer restore()
	br, _ := newTestRedisBracket(t)
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0).UTC()
	tid := "t-empty"
	empty := store.BracketSummary{Phase: "open", Capacity: 8, Rounds: []store.BracketRoundSummary{}}
	if err := br.UpsertSummary(ctx, tid, empty, 1, now); err != nil {
		t.Fatal(err)
	}
	page, err := br.Page(ctx, store.BracketPageQuery{TournamentID: tid, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Slots) != 0 || page.Summary.Phase != "open" {
		t.Fatalf("empty page=%+v", page)
	}

	// Exact max batch size is allowed; over-size is rejected.
	tid2 := "t-big"
	maxSlots := sampleSlots("b0", 0, 999)
	if err := br.UpsertSummary(ctx, tid2, sampleSummarySlots("in_progress", 1000, 1000), 1, now); err != nil {
		t.Fatal(err)
	}
	if err := br.UpsertChunk(ctx, tid2, 1, "b0", maxSlots, 1, now); err != nil {
		t.Fatal(err)
	}
	page, err = br.Page(ctx, store.BracketPageQuery{TournamentID: tid2, Limit: store.MaxBracketPageLimit})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Slots) != store.MaxBracketPageLimit {
		t.Fatalf("want %d slots, got %d", store.MaxBracketPageLimit, len(page.Slots))
	}
	over := sampleSlots("b1", 0, 1100)
	err = br.UpsertChunk(ctx, tid2, 1, "b1", over, 1, now)
	if err == nil || !strings.Contains(err.Error(), "chunk exceeds max size") {
		t.Fatalf("want chunk size rejection, got %v", err)
	}
}

func TestRedisBracket_ReadySemanticsO1(t *testing.T) {
	restore := store.SetBracketCursorMACKeyForTest("test-bracket-cursor")
	defer restore()
	br, _ := newTestRedisBracket(t)
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0).UTC()

	// Empty tournament (expectedSlots=0, ZCARD=0) becomes ready.
	emptyTID := "t-ready-empty"
	empty := store.BracketSummary{Phase: "open", Capacity: 8, Rounds: []store.BracketRoundSummary{}}
	if err := br.UpsertSummary(ctx, emptyTID, empty, 1, now); err != nil {
		t.Fatal(err)
	}
	meta, err := br.Meta(ctx, emptyTID)
	if err != nil || !meta.Ready {
		t.Fatalf("empty want ready, meta=%+v err=%v", meta, err)
	}
	if _, err := br.Page(ctx, store.BracketPageQuery{TournamentID: emptyTID, Limit: 10}); err != nil {
		t.Fatal(err)
	}

	// Partial populate: expectedSlots > ZCARD → not ready.
	tid := "t-ready-partial"
	if err := br.UpsertSummary(ctx, tid, sampleSummarySlots("in_progress", 4, 4), 1, now); err != nil {
		t.Fatal(err)
	}
	if err := br.UpsertChunk(ctx, tid, 1, "b0", sampleSlots("b0", 0, 1), 1, now); err != nil {
		t.Fatal(err)
	}
	meta, err = br.Meta(ctx, tid)
	if err != nil || meta.Ready {
		t.Fatalf("partial must not be ready, meta=%+v err=%v", meta, err)
	}
	if _, err := br.Page(ctx, store.BracketPageQuery{TournamentID: tid, Limit: 10}); !errors.Is(err, store.ErrBracketProjectionUnavailable) {
		t.Fatalf("want unavailable while partial, got %v", err)
	}

	// Full populate → ready.
	if err := br.UpsertChunk(ctx, tid, 1, "b1", sampleSlots("b1", 2, 3), 1, now); err != nil {
		t.Fatal(err)
	}
	meta, err = br.Meta(ctx, tid)
	if err != nil || !meta.Ready {
		t.Fatalf("full populate want ready, meta=%+v err=%v", meta, err)
	}

	// Incremental chunk patch clears ready and leaves the published page version
	// unchanged until the matching summary upsert commits the refresh.
	patched := sampleSlots("b0", 0, 1)
	patched[0].Status = "completed"
	if err := br.UpsertChunk(ctx, tid, 1, "b0", patched, 2, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	meta, err = br.Meta(ctx, tid)
	if err != nil || meta.Ready || meta.ProjectionVersion != 1 {
		t.Fatalf("partial incremental refresh must be hidden: meta=%+v err=%v", meta, err)
	}
	if _, err := br.Page(ctx, store.BracketPageQuery{TournamentID: tid, Limit: 10}); !errors.Is(err, store.ErrBracketProjectionUnavailable) {
		t.Fatalf("partial incremental refresh must fall back, got %v", err)
	}
	if err := br.UpsertSummary(ctx, tid, sampleSummarySlots("in_progress", 4, 4), 2, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	meta, err = br.Meta(ctx, tid)
	if err != nil || !meta.Ready || meta.ProjectionVersion != 2 {
		t.Fatalf("summary commit marker did not publish refresh: meta=%+v err=%v", meta, err)
	}
	page, err := br.Page(ctx, store.BracketPageQuery{TournamentID: tid, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Slots) != 4 || page.Slots[0].Status != "completed" {
		t.Fatalf("page=%+v", page)
	}
}

func TestRedisBracket_IncompleteProjectionFallsBack(t *testing.T) {
	restore := store.SetBracketCursorMACKeyForTest("test-bracket-cursor")
	defer restore()
	br, _ := newTestRedisBracket(t)
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0).UTC()
	tid := "t-incomplete"
	ks := store.NewBracketKeySpace("tournament:")

	// Summary-only with expectedSlots>0 is not ready → unavailable.
	if err := br.UpsertSummary(ctx, tid, sampleSummarySlots("in_progress", 2, 2), 1, now); err != nil {
		t.Fatal(err)
	}
	meta, _ := br.Meta(ctx, tid)
	if meta.Ready {
		t.Fatal("summary-only with pending slots must not be ready")
	}
	if _, err := br.Page(ctx, store.BracketPageQuery{TournamentID: tid, Limit: 10}); !errors.Is(err, store.ErrBracketProjectionUnavailable) {
		t.Fatalf("want unavailable, got %v", err)
	}

	// Forced ready + dangling index/chunk still fails closed at page time.
	rdb := br.Client()
	member := store.BracketIndexMember(1, 0)
	if err := rdb.ZAdd(ctx, ks.GenIndex(tid, 1), redis.Z{
		Score: store.BracketIndexScore(1, 0), Member: member,
	}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.HSet(ctx, ks.GenSlotMap(tid, 1), member, "missing_batch").Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.HSet(ctx, ks.Meta(tid), "ready", "1").Err(); err != nil {
		t.Fatal(err)
	}
	_, err := br.Page(ctx, store.BracketPageQuery{TournamentID: tid, Limit: 10})
	if !errors.Is(err, store.ErrBracketProjectionUnavailable) {
		t.Fatalf("want unavailable for dangling chunk, got %v", err)
	}

	pg := &stubPGLoader{page: store.BracketPage{
		TournamentID: tid, ProjectionVersion: 9, GeneratedAt: now,
		Summary: sampleSummary("in_progress", 2),
		Slots:   sampleSlots("pg", 0, 1),
	}}
	comp := &store.CompositeBracketPageLoader{Redis: br, PG: pg}
	page, err := comp.LoadBracketPage(ctx, store.BracketPageQuery{TournamentID: tid, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if !pg.called || page.ProjectionVersion != 9 || len(page.Slots) != 2 {
		t.Fatalf("composite fallback failed: called=%v page=%+v", pg.called, page)
	}
}

func TestRedisBracket_StagingDualWriteConflict(t *testing.T) {
	br, _ := newTestRedisBracket(t)
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0).UTC()
	tid := "t-staging-conflict"
	ks := store.NewBracketKeySpace("tournament:")
	sumLive := sampleSummary("in_progress", 2)
	if err := br.UpsertSummary(ctx, tid, sumLive, 9, now); err != nil {
		t.Fatal(err)
	}
	if err := br.UpsertChunk(ctx, tid, 1, "b0", sampleSlots("b0", 0, 1), 9, now); err != nil {
		t.Fatal(err)
	}

	// Mark rebuild in progress so incremental dual-writes staging.
	newGen := int64(2)
	if err := br.Client().HSet(ctx, ks.Meta(tid), "rebuilding_gen", "2", "rebuild_token", "tok-staging").Err(); err != nil {
		t.Fatal(err)
	}

	// Plant equal-version different payload on staging at ver 10.
	stagingSum, _ := json.Marshal(sampleSummary("in_progress", 99))
	if err := br.Client().Set(ctx, ks.GenSummary(tid, newGen), string(stagingSum), 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := br.Client().Set(ctx, ks.GenSummary(tid, newGen)+":ver", "10", 0).Err(); err != nil {
		t.Fatal(err)
	}

	before, err := br.Client().Get(ctx, ks.GenSummary(tid, 1)).Result()
	if err != nil {
		t.Fatal(err)
	}

	// Live would apply ver 10, staging conflicts → fail closed with no live mutation.
	err = br.UpsertSummary(ctx, tid, sampleSummary("in_progress", 50), 10, now.Add(time.Second))
	if !errors.Is(err, store.ErrBracketProjectionConflict) {
		t.Fatalf("want staging conflict, got %v", err)
	}
	after, err := br.Client().Get(ctx, ks.GenSummary(tid, 1)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("live mutated despite staging conflict:\nbefore=%s\nafter=%s", before, after)
	}
	meta, _ := br.Meta(ctx, tid)
	if meta.ProjectionVersion != 9 {
		t.Fatalf("meta version mutated: %d", meta.ProjectionVersion)
	}
}

func TestRedisBracket_FallbackComposite(t *testing.T) {
	restore := store.SetBracketCursorMACKeyForTest("test-bracket-cursor")
	defer restore()
	br, mr := newTestRedisBracket(t)
	mr.Close() // force Redis failure
	pg := &stubPGLoader{page: store.BracketPage{
		TournamentID: "t-fb", ProjectionVersion: 3, GeneratedAt: time.Unix(1, 0).UTC(),
		Summary: sampleSummary("open", 0),
	}}
	comp := &store.CompositeBracketPageLoader{Redis: br, PG: pg}
	page, err := comp.LoadBracketPage(context.Background(), store.BracketPageQuery{TournamentID: "t-fb", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if page.ProjectionVersion != 3 || !pg.called {
		t.Fatalf("fallback failed: page=%+v called=%v", page, pg.called)
	}
}

func TestRedisBracket_CompositeRejectsMissedRefreshVersion(t *testing.T) {
	restore := store.SetBracketCursorMACKeyForTest("test-bracket-cursor")
	defer restore()
	br, _ := newTestRedisBracket(t)
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0).UTC()
	tid := "t-stale-version"
	if err := br.UpsertSummary(ctx, tid, sampleSummary("open", 0), 1, now); err != nil {
		t.Fatal(err)
	}
	pg := &checkpointPGLoader{
		stubPGLoader: stubPGLoader{page: store.BracketPage{
			TournamentID: tid, ProjectionVersion: 2, GeneratedAt: now.Add(time.Second),
			Summary: sampleSummary("closed", 0),
		}},
		version: 2,
	}
	comp := &store.CompositeBracketPageLoader{Redis: br, PG: pg}
	page, err := comp.LoadBracketPage(ctx, store.BracketPageQuery{TournamentID: tid, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if !pg.called || page.ProjectionVersion != 2 || page.Summary.Phase != "closed" {
		t.Fatalf("missed refresh must fall back to authoritative page: called=%v page=%+v", pg.called, page)
	}
}

type stubPGLoader struct {
	page   store.BracketPage
	called bool
}

func (s *stubPGLoader) LoadBracketPage(context.Context, store.BracketPageQuery) (store.BracketPage, error) {
	s.called = true
	return s.page, nil
}

type checkpointPGLoader struct {
	stubPGLoader
	version int64
}

func (s *checkpointPGLoader) LoadProjectionCheckpoint(context.Context, string) (int64, time.Time, error) {
	return s.version, s.page.GeneratedAt, nil
}

func TestRedisBracket_FlushPrefixedKeys(t *testing.T) {
	br, _ := newTestRedisBracket(t)
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0).UTC()
	if err := br.UpsertSummary(ctx, "t-flush", sampleSummary("open", 0), 1, now); err != nil {
		t.Fatal(err)
	}
	if err := br.FlushPrefixedKeys(ctx); err != nil {
		t.Fatal(err)
	}
	_, err := br.Page(ctx, store.BracketPageQuery{TournamentID: "t-flush", Limit: 10})
	if !errors.Is(err, store.ErrBracketProjectionUnavailable) {
		t.Fatalf("want unavailable after flush, got %v", err)
	}
}

func TestRedisBracket_RebuildFromSource(t *testing.T) {
	restore := store.SetBracketCursorMACKeyForTest("test-bracket-cursor")
	defer restore()
	br, _ := newTestRedisBracket(t)
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0).UTC()
	tid := "t-rebuild"
	src := &memBracketSource{
		summary: sampleSummary("seeded", 4),
		ver:     7,
		at:      now,
		refs:    []store.BracketChunkRef{{RoundNumber: 1, BatchID: "b0"}},
		chunks:  map[string][]store.BracketSlotView{"1:b0": sampleSlots("b0", 0, 1)},
	}
	if err := br.RebuildFromPostgres(ctx, tid, src, 7); err != nil {
		t.Fatal(err)
	}
	page, err := br.Page(ctx, store.BracketPageQuery{TournamentID: tid, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if page.ProjectionVersion != 7 || len(page.Slots) != 2 {
		t.Fatalf("page=%+v", page)
	}
	// Ensure chunk JSON round-trips fields.
	raw, _ := json.Marshal(page.Slots[0])
	if !json.Valid(raw) {
		t.Fatal("slot json invalid")
	}
}

func TestBracketKeySpace_DocumentsChunkedLayout(t *testing.T) {
	ks := store.NewBracketKeySpace("")
	root := ks.TournamentRoot("tid")
	if root != "tournament:v1:br:{tid}:" {
		t.Fatalf("root=%q", root)
	}
	if ks.GenChunk("tid", 2, 1, "batch_0") != "tournament:v1:br:{tid}:gen:2:chunk:1:batch_0" {
		t.Fatal(ks.GenChunk("tid", 2, 1, "batch_0"))
	}
	if ks.GenChunkSet("tid", 2) != "tournament:v1:br:{tid}:gen:2:chunkset" {
		t.Fatal(ks.GenChunkSet("tid", 2))
	}
}
