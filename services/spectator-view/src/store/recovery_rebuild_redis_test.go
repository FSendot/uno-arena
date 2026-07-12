package store_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"unoarena/services/spectator-view/domain"
	"unoarena/services/spectator-view/store"
)

func openRecoveryStore(t *testing.T) (*store.RedisProjectionStore, context.Context) {
	t.Helper()
	url := strings.TrimSpace(os.Getenv("SPECTATOR_REDIS_URL"))
	if url == "" {
		t.Skip("SPECTATOR_REDIS_URL not set")
	}
	rdb, err := store.NewRedisFromURL(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis unavailable: %v", err)
	}
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	prefix := "specrec_" + hex.EncodeToString(b) + ":"
	s := store.NewRedisProjectionStore(rdb, prefix)
	if err := s.LoadScripts(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = s.FlushPrefixedKeys(context.Background())
	})
	if err := s.FlushPrefixedKeys(ctx); err != nil {
		t.Fatal(err)
	}
	return s, ctx
}

func recoverySnapshot(room domain.RoomID, eid string, seq uint64) domain.SpectatorSafeEvent {
	return domain.SpectatorSafeEvent{
		EventID: domain.EventID(eid), EventType: domain.EventSnapshotSanitized, SchemaVersion: 1,
		RoomID: room, Sequence: domain.SequenceNumber(seq),
		Payload: map[string]any{
			"status":     "waiting",
			"visibility": "public",
			"roster": []any{
				map[string]any{"seatIndex": 0, "playerId": "p1", "displayName": "Alice", "cardCount": 0, "occupied": true},
			},
		},
	}
}

func seedRoomSeq1(t *testing.T, s *store.RedisProjectionStore, ctx context.Context, room domain.RoomID) {
	t.Helper()
	out, err := s.Apply(ctx, room, []domain.SpectatorSafeEvent{{
		EventID: "seed", EventType: domain.EventRoomCreated, SchemaVersion: 1,
		RoomID: room, Sequence: 1,
		Payload: map[string]any{
			"visibility": "public",
			"seats": []any{
				map[string]any{"seatIndex": 0, "playerId": "p1", "displayName": "Alice", "cardCount": 0},
			},
		},
	}})
	if err != nil || out.Kind != domain.OutcomeAccepted {
		t.Fatalf("seed: %+v err=%v", out, err)
	}
}

func TestRedis_RecoveryStaleNewerApplyWins(t *testing.T) {
	s, ctx := openRecoveryStore(t)
	room := domain.RoomID("room_stale")
	seedRoomSeq1(t, s, ctx, room)

	// Live Apply advances past the fence.
	out, err := s.Apply(ctx, room, []domain.SpectatorSafeEvent{{
		EventID: "live2", EventType: domain.EventRoomLocked, SchemaVersion: 1,
		RoomID: room, Sequence: 2, Payload: map[string]any{"status": "locked"},
	}})
	if err != nil || out.Kind != domain.OutcomeAccepted {
		t.Fatalf("live apply: %+v err=%v", out, err)
	}

	res, err := s.RecoveryRebuildFromSnapshot(ctx, room, recoverySnapshot(room, "snap1", 1),
		"1", 1, nil, store.RecoveryRelease{
			Enabled: true, Note: store.ReleaseNoteRecoveryContinuityProven,
		})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Stale {
		t.Fatalf("want stale, got %+v", res)
	}
	if res.QuarantineReleased {
		t.Fatal("stale must not release quarantine")
	}
	seq, status, _, _, _, err := s.RoomMeta(ctx, room)
	if err != nil || seq != 2 || status != "locked" {
		t.Fatalf("must not regress: seq=%d status=%s err=%v", seq, status, err)
	}
}

func TestRedis_RecoveryEqualSeqGenerationConflict(t *testing.T) {
	s, ctx := openRecoveryStore(t)
	room := domain.RoomID("room_conflict")
	seedRoomSeq1(t, s, ctx, room)

	// First recovery advances generation 1→2 at same sequence family.
	res, err := s.RecoveryRebuildFromSnapshot(ctx, room, recoverySnapshot(room, "snap1", 1),
		"1", 1, nil, store.RecoveryRelease{})
	if err != nil || res.Stale || res.Generation != "2" {
		t.Fatalf("first recovery: %+v err=%v", res, err)
	}

	// Second recovery still fences old generation at equal sequence → conflict.
	_, err = s.RecoveryRebuildFromSnapshot(ctx, room, recoverySnapshot(room, "snap1b", 1),
		"1", 1, nil, store.RecoveryRelease{})
	if !errors.Is(err, store.ErrRecoveryConflict) {
		t.Fatalf("want conflict, got %v", err)
	}
	gen, seq, err := s.CurrentFence(ctx, room)
	if err != nil || gen != "2" || seq != 1 {
		t.Fatalf("fence gen=%s seq=%d err=%v", gen, seq, err)
	}
}

func TestRedis_RecoverySuccessfulFencedAdvance(t *testing.T) {
	s, ctx := openRecoveryStore(t)
	room := domain.RoomID("room_ok")
	seedRoomSeq1(t, s, ctx, room)

	held := []domain.SpectatorSafeEvent{{
		EventID: "h2", EventType: domain.EventRoomLocked, SchemaVersion: 1,
		RoomID: room, Sequence: 2, Payload: map[string]any{"status": "locked"},
	}}
	res, err := s.RecoveryRebuildFromSnapshot(ctx, room, recoverySnapshot(room, "snap1", 1),
		"1", 1, held, store.RecoveryRelease{})
	if err != nil || res.Stale {
		t.Fatalf("recovery: %+v err=%v", res, err)
	}
	if res.Generation != "2" || res.Sequence != 2 || res.Status != "locked" {
		t.Fatalf("result=%+v", res)
	}
	seq, status, _, count, _, err := s.RoomMeta(ctx, room)
	if err != nil || seq != 2 || status != "locked" || count != 2 {
		t.Fatalf("meta seq=%d status=%s count=%d err=%v", seq, status, count, err)
	}
	gen, _, err := s.CurrentFence(ctx, room)
	if err != nil || gen != "2" {
		t.Fatalf("gen=%s err=%v", gen, err)
	}
}

func TestRedis_RecoveryConcurrentApplyVsRebuild(t *testing.T) {
	s, ctx := openRecoveryStore(t)
	room := domain.RoomID("room_race")
	seedRoomSeq1(t, s, ctx, room)

	var wg sync.WaitGroup
	var applyOK, rebuildOK, rebuildStale atomic.Int64
	const n = 12
	for i := 0; i < n; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			out, err := s.Apply(ctx, room, []domain.SpectatorSafeEvent{{
				EventID:   domain.EventID(fmt.Sprintf("race_apply_%d", i)),
				EventType: domain.EventTurnAdvanced, SchemaVersion: 1,
				RoomID: room, Sequence: 2,
				Payload: map[string]any{"currentPlayerId": "p1"},
			}})
			if err != nil {
				t.Errorf("apply: %v", err)
				return
			}
			if out.Kind == domain.OutcomeAccepted {
				applyOK.Add(1)
			}
		}(i)
		go func(i int) {
			defer wg.Done()
			// Rebuild that would keep seq=1 — must not regress if Apply won.
			res, err := s.RecoveryRebuildFromSnapshot(ctx, room,
				recoverySnapshot(room, fmt.Sprintf("race_snap_%d", i), 1),
				"1", 1, nil, store.RecoveryRelease{})
			if errors.Is(err, store.ErrRecoveryConflict) {
				return
			}
			if err != nil {
				t.Errorf("rebuild: %v", err)
				return
			}
			if res.Stale {
				rebuildStale.Add(1)
				return
			}
			rebuildOK.Add(1)
		}(i)
	}
	wg.Wait()

	seq, _, _, _, _, err := s.RoomMeta(ctx, room)
	if err != nil {
		t.Fatal(err)
	}
	if applyOK.Load() > 0 && seq < 2 {
		t.Fatalf("Apply won but sequence regressed to %d (apply=%d rebuildOK=%d stale=%d)",
			seq, applyOK.Load(), rebuildOK.Load(), rebuildStale.Load())
	}
	if seq < 1 {
		t.Fatalf("invalid final seq=%d", seq)
	}
}

func TestRedis_RecoveryQuarantineReleaseOnlyOnSuccess(t *testing.T) {
	s, ctx := openRecoveryStore(t)
	room := domain.RoomID("room_qrel")
	seedRoomSeq1(t, s, ctx, room)

	if err := s.QuarantineKafkaAggregate(ctx, store.KafkaAggregateQuarantine{
		ConsumerGroup:  "spectator-view",
		SourceTopic:    "room.spectator-safe.events",
		AggregateKey:   string(room),
		Classification: store.QuarantineClassApplication,
		Reason:         "gap",
		QuarantinedAt:  time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}

	// Gap in held → no Redis write, quarantine stays active.
	_, err := s.RecoveryRebuildFromSnapshot(ctx, room, recoverySnapshot(room, "snap1", 1),
		"1", 1, []domain.SpectatorSafeEvent{{
			EventID: "gap", EventType: domain.EventRoomLocked, SchemaVersion: 1,
			RoomID: room, Sequence: 3, Payload: map[string]any{"status": "locked"},
		}}, store.RecoveryRelease{
			Enabled: true, Note: store.ReleaseNoteRecoveryContinuityProven,
		})
	if !errors.Is(err, domain.ErrHeldContinuityGap) {
		t.Fatalf("want gap, got %v", err)
	}
	ok, err := s.IsKafkaAggregateQuarantined(ctx, "spectator-view", "room.spectator-safe.events", string(room))
	if err != nil || !ok {
		t.Fatalf("quarantine must remain: ok=%v err=%v", ok, err)
	}

	// Stale fence → no release.
	_, _ = s.Apply(ctx, room, []domain.SpectatorSafeEvent{{
		EventID: "live2", EventType: domain.EventRoomLocked, SchemaVersion: 1,
		RoomID: room, Sequence: 2, Payload: map[string]any{"status": "locked"},
	}})
	res, err := s.RecoveryRebuildFromSnapshot(ctx, room, recoverySnapshot(room, "snap1b", 1),
		"1", 1, nil, store.RecoveryRelease{
			Enabled: true, Note: store.ReleaseNoteRecoveryContinuityProven,
		})
	if err != nil || !res.Stale {
		t.Fatalf("want stale: %+v err=%v", res, err)
	}
	ok, err = s.IsKafkaAggregateQuarantined(ctx, "spectator-view", "room.spectator-safe.events", string(room))
	if err != nil || !ok {
		t.Fatalf("stale must keep quarantine: ok=%v err=%v", ok, err)
	}

	// Successful contiguous recovery at current fence releases quarantine.
	gen, seq, err := s.CurrentFence(ctx, room)
	if err != nil {
		t.Fatal(err)
	}
	res, err = s.RecoveryRebuildFromSnapshot(ctx, room, recoverySnapshot(room, "snap2", seq),
		gen, seq, nil, store.RecoveryRelease{
			Enabled: true, Note: store.ReleaseNoteRecoveryContinuityProven,
		})
	if err != nil || res.Stale || !res.QuarantineReleased {
		t.Fatalf("success release: %+v err=%v", res, err)
	}
	ok, err = s.IsKafkaAggregateQuarantined(ctx, "spectator-view", "room.spectator-safe.events", string(room))
	if err != nil || ok {
		t.Fatalf("quarantine must be released: ok=%v err=%v", ok, err)
	}
	fields, err := s.KafkaQuarantineFields(ctx, room)
	if err != nil {
		t.Fatal(err)
	}
	if fields["active"] != "0" || fields["release_note"] != store.ReleaseNoteRecoveryContinuityProven {
		t.Fatalf("audit fields=%v", fields)
	}
	if fields["released_at"] == "" {
		t.Fatal("released_at required")
	}
}

func TestRedis_RecoveryRejectsRawReleaseNote(t *testing.T) {
	s, ctx := openRecoveryStore(t)
	room := domain.RoomID("room_note")
	seedRoomSeq1(t, s, ctx, room)
	_, err := s.RecoveryRebuildFromSnapshot(ctx, room, recoverySnapshot(room, "snap1", 1),
		"1", 1, nil, store.RecoveryRelease{
			Enabled: true, Note: "please release this room now",
		})
	if !errors.Is(err, store.ErrInvalidReleaseNote) {
		t.Fatalf("got %v", err)
	}
	err = s.ReleaseKafkaAggregateQuarantine(ctx, room, "manual ops override")
	if !errors.Is(err, store.ErrInvalidReleaseNote) {
		t.Fatalf("standalone got %v", err)
	}
}

func TestRedis_RecoveryConflictKeepsQuarantine(t *testing.T) {
	s, ctx := openRecoveryStore(t)
	room := domain.RoomID("room_qconflict")
	seedRoomSeq1(t, s, ctx, room)
	_ = s.QuarantineKafkaAggregate(ctx, store.KafkaAggregateQuarantine{
		ConsumerGroup: "spectator-view", SourceTopic: "room.spectator-safe.events",
		AggregateKey: string(room), Classification: store.QuarantineClassApplication,
		Reason: "gap", QuarantinedAt: time.Now().UTC(),
	})
	_, _ = s.RecoveryRebuildFromSnapshot(ctx, room, recoverySnapshot(room, "snap1", 1),
		"1", 1, nil, store.RecoveryRelease{})
	_, err := s.RecoveryRebuildFromSnapshot(ctx, room, recoverySnapshot(room, "snap1b", 1),
		"1", 1, nil, store.RecoveryRelease{
			Enabled: true, Note: store.ReleaseNoteRebuildJobComplete,
		})
	if !errors.Is(err, store.ErrRecoveryConflict) {
		t.Fatalf("got %v", err)
	}
	ok, err := s.IsKafkaAggregateQuarantined(ctx, "spectator-view", "room.spectator-safe.events", string(room))
	if err != nil || !ok {
		t.Fatalf("conflict must keep quarantine: ok=%v err=%v", ok, err)
	}
}

func TestRedis_RecoveryAtomicIdempotency_TwoWorkersOneMutation(t *testing.T) {
	s, ctx := openRecoveryStore(t)
	room := domain.RoomID("room_idemp_race")
	seedRoomSeq1(t, s, ctx, room)
	if err := s.QuarantineKafkaAggregate(ctx, store.KafkaAggregateQuarantine{
		ConsumerGroup: "spectator-view", SourceTopic: "room.spectator-safe.events",
		AggregateKey: string(room), Classification: store.QuarantineClassApplication,
		Reason: "gap", QuarantinedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}

	identity := "job-race|" + string(room) + "|1"
	idemp := store.RecoveryIdempotency{Identity: identity}
	release := store.RecoveryRelease{Enabled: true, Note: store.ReleaseNoteRecoveryContinuityProven}

	var wg sync.WaitGroup
	var mutated, already, stale atomic.Int64
	const workers = 2
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := s.RecoveryRebuildFromSnapshotWithIdempotency(ctx, room,
				recoverySnapshot(room, "snap_idemp", 1), "1", 1, nil, release, idemp)
			if err != nil {
				errs <- err
				return
			}
			switch {
			case res.AlreadyDone:
				already.Add(1)
			case res.Stale:
				stale.Add(1)
			default:
				mutated.Add(1)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("worker err: %v", err)
	}
	if mutated.Load() != 1 {
		t.Fatalf("want exactly one mutation, got mutated=%d already=%d stale=%d",
			mutated.Load(), already.Load(), stale.Load())
	}
	if already.Load()+stale.Load() != 1 {
		t.Fatalf("duplicate must be already_done (or rare stale), already=%d stale=%d",
			already.Load(), stale.Load())
	}

	gen, seq, err := s.CurrentFence(ctx, room)
	if err != nil || gen != "2" || seq != 1 {
		t.Fatalf("fence gen=%s seq=%d err=%v", gen, seq, err)
	}
	ok, err := s.IsKafkaAggregateQuarantined(ctx, "spectator-view", "room.spectator-safe.events", string(room))
	if err != nil || ok {
		t.Fatalf("quarantine must be released once: ok=%v err=%v", ok, err)
	}
	fields, err := s.KafkaQuarantineFields(ctx, room)
	if err != nil {
		t.Fatal(err)
	}
	if fields["active"] != "0" || fields["release_note"] != store.ReleaseNoteRecoveryContinuityProven || fields["released_at"] == "" {
		t.Fatalf("quarantine audit corrupted: %v", fields)
	}
	done, err := s.IsRebuildIdempotencyDone(ctx, room, identity)
	if err != nil || !done {
		t.Fatalf("atomic marker required after success: done=%v err=%v", done, err)
	}

	// Third delivery: identical key is idempotent success; must not re-release / regress.
	before := fields
	res, err := s.RecoveryRebuildFromSnapshotWithIdempotency(ctx, room,
		recoverySnapshot(room, "snap_idemp_dup", 1), "1", 1, nil, release, idemp)
	if err != nil || !res.AlreadyDone {
		t.Fatalf("want already_done, got %+v err=%v", res, err)
	}
	after, err := s.KafkaQuarantineFields(ctx, room)
	if err != nil {
		t.Fatal(err)
	}
	if after["released_at"] != before["released_at"] || after["release_note"] != before["release_note"] {
		t.Fatalf("duplicate must not mutate quarantine audit: before=%v after=%v", before, after)
	}
	gen, seq, err = s.CurrentFence(ctx, room)
	if err != nil || gen != "2" || seq != 1 {
		t.Fatalf("duplicate must not regress fence gen=%s seq=%d err=%v", gen, seq, err)
	}
}

func TestRedis_RecoveryAtomicIdempotency_MarkerOnlyOnSuccess(t *testing.T) {
	s, ctx := openRecoveryStore(t)
	room := domain.RoomID("room_idemp_fail")
	seedRoomSeq1(t, s, ctx, room)
	identity := "job-fail|" + string(room) + "|1"
	idemp := store.RecoveryIdempotency{Identity: identity}
	release := store.RecoveryRelease{Enabled: true, Note: store.ReleaseNoteRecoveryContinuityProven}

	// Continuity gap — no Lua success, no marker.
	_, err := s.RecoveryRebuildFromSnapshotWithIdempotency(ctx, room, recoverySnapshot(room, "snap1", 1),
		"1", 1, []domain.SpectatorSafeEvent{{
			EventID: "gap", EventType: domain.EventRoomLocked, SchemaVersion: 1,
			RoomID: room, Sequence: 3, Payload: map[string]any{"status": "locked"},
		}}, release, idemp)
	if !errors.Is(err, domain.ErrHeldContinuityGap) {
		t.Fatalf("want gap, got %v", err)
	}
	done, err := s.IsRebuildIdempotencyDone(ctx, room, identity)
	if err != nil || done {
		t.Fatalf("gap must not mark done: done=%v err=%v", done, err)
	}

	// Stale — no marker.
	_, _ = s.Apply(ctx, room, []domain.SpectatorSafeEvent{{
		EventID: "live2", EventType: domain.EventRoomLocked, SchemaVersion: 1,
		RoomID: room, Sequence: 2, Payload: map[string]any{"status": "locked"},
	}})
	res, err := s.RecoveryRebuildFromSnapshotWithIdempotency(ctx, room, recoverySnapshot(room, "snap1b", 1),
		"1", 1, nil, release, idemp)
	if err != nil || !res.Stale {
		t.Fatalf("want stale: %+v err=%v", res, err)
	}
	done, err = s.IsRebuildIdempotencyDone(ctx, room, identity)
	if err != nil || done {
		t.Fatalf("stale must not mark done: done=%v err=%v", done, err)
	}

	// Conflict after a successful non-idempotent recovery — losing fence must not mark.
	gen, seq, err := s.CurrentFence(ctx, room)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.RecoveryRebuildFromSnapshot(ctx, room, recoverySnapshot(room, "snap_ok", seq),
		gen, seq, nil, store.RecoveryRelease{})
	if err != nil {
		t.Fatal(err)
	}
	conflictID := "job-conflict|" + string(room) + "|2"
	_, err = s.RecoveryRebuildFromSnapshotWithIdempotency(ctx, room, recoverySnapshot(room, "snap_c", seq),
		gen, seq, nil, release, store.RecoveryIdempotency{Identity: conflictID})
	if !errors.Is(err, store.ErrRecoveryConflict) {
		t.Fatalf("want conflict, got %v", err)
	}
	done, err = s.IsRebuildIdempotencyDone(ctx, room, conflictID)
	if err != nil || done {
		t.Fatalf("conflict must not mark done: done=%v err=%v", done, err)
	}
}
