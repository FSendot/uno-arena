package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"unoarena/services/spectator-view/domain"
	"unoarena/services/spectator-view/store"
)

type scriptedHeldScanner struct {
	batches [][]ConsumerRecord
	polls   atomic.Int32
	closed  atomic.Bool
}

func (s *scriptedHeldScanner) Poll(ctx context.Context) ([]ConsumerRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	i := int(s.polls.Add(1) - 1)
	if i >= len(s.batches) {
		return nil, nil
	}
	return s.batches[i], nil
}

func (s *scriptedHeldScanner) Close() error {
	s.closed.Store(true)
	return nil
}

func heldDLQHeaders(partition, offset int64) map[string]string {
	return map[string]string{
		dlqHeaderConsumer:        DefaultSpectatorKafkaGroup,
		dlqHeaderSourceTopic:     DefaultSpectatorSafeTopic,
		dlqHeaderSourcePartition: strconv.FormatInt(partition, 10),
		dlqHeaderSourceOffset:    strconv.FormatInt(offset, 10),
	}
}

func targetHeldRec(room string, seq, offset int64, mut ...func(map[string]any)) ConsumerRecord {
	return ConsumerRecord{
		Topic:     DefaultSpectatorSafeDLQTopic,
		Partition: 0,
		Offset:    offset,
		Key:       []byte(room),
		Value: canonicalSpectatorSafeJSON(append([]func(map[string]any){func(m map[string]any) {
			m["roomId"] = room
			m["eventId"] = fmt.Sprintf("evt-%s-%d", room, seq)
			m["sequenceNumber"] = float64(seq)
		}}, mut...)...),
		Headers: heldDLQHeaders(0, offset),
	}
}

func unrelatedHeldRec(room string, offset int64) ConsumerRecord {
	return ConsumerRecord{
		Topic:     DefaultSpectatorSafeDLQTopic,
		Partition: 0,
		Offset:    offset,
		Key:       []byte(room),
		Value: canonicalSpectatorSafeJSON(func(m map[string]any) {
			m["roomId"] = room
			m["eventId"] = fmt.Sprintf("evt-%s-%d", room, offset)
			m["sequenceNumber"] = float64(offset + 1)
		}),
		Headers: heldDLQHeaders(0, offset),
	}
}

func heldSourceWithBatches(batches ...[]ConsumerRecord) *KafkaHeldSpectatorDLQSource {
	scanner := &scriptedHeldScanner{batches: batches}
	return &KafkaHeldSpectatorDLQSource{
		Brokers:        []string{"localhost:9092"},
		DLQTopic:       DefaultSpectatorSafeDLQTopic,
		IdleEmptyPolls: 2,
		PollTimeout:    50 * time.Millisecond,
		MaxPollCycles:  10,
		newClient: func(brokers []string, topic, group string) (heldKafkaScanner, error) {
			return scanner, nil
		},
	}
}

func TestKafkaHeldSpectatorDLQSource_ContinuousUnrelatedBatchesTerminate(t *testing.T) {
	t.Parallel()
	const maxPolls = 8
	batches := make([][]ConsumerRecord, 0, 200)
	for i := 0; i < 200; i++ {
		batches = append(batches, []ConsumerRecord{unrelatedHeldRec("other-room", int64(i))})
	}
	scanner := &scriptedHeldScanner{batches: batches}
	src := &KafkaHeldSpectatorDLQSource{
		Brokers:        []string{"localhost:9092"},
		DLQTopic:       DefaultSpectatorSafeDLQTopic,
		IdleEmptyPolls: 3,
		PollTimeout:    50 * time.Millisecond,
		MaxPollCycles:  maxPolls,
		MaxScanRecords: 10_000,
		newClient: func(brokers []string, topic, group string) (heldKafkaScanner, error) {
			return scanner, nil
		},
	}
	_, err := src.LoadHeldAfterCheckpoint(context.Background(), HeldSpectatorRecordQuery{
		RoomID: "target-room", RecoveryJobID: "job-1", FailedCheckpoint: 10, ResumeCheckpoint: 9,
	})
	if !errors.Is(err, domain.ErrHeldContinuityBound) {
		t.Fatalf("want ErrHeldContinuityBound, got %v", err)
	}
	polls := int(scanner.polls.Load())
	if polls > maxPolls {
		t.Fatalf("polls=%d exceeded MaxPollCycles=%d", polls, maxPolls)
	}
	if polls < maxPolls {
		t.Fatalf("expected scan to hit poll budget; polls=%d", polls)
	}
	if !scanner.closed.Load() {
		t.Fatal("scanner must close")
	}
}

func TestKafkaHeldSpectatorDLQSource_RecordBudgetBoundsUnrelatedTraffic(t *testing.T) {
	t.Parallel()
	big := make([]ConsumerRecord, 0, 50)
	for i := 0; i < 50; i++ {
		big = append(big, unrelatedHeldRec("other-room", int64(i)))
	}
	scanner := &scriptedHeldScanner{batches: [][]ConsumerRecord{big, big, big}}
	src := &KafkaHeldSpectatorDLQSource{
		Brokers:        []string{"localhost:9092"},
		DLQTopic:       DefaultSpectatorSafeDLQTopic,
		IdleEmptyPolls: 100,
		MaxPollCycles:  100,
		MaxScanRecords: 40,
		newClient: func(brokers []string, topic, group string) (heldKafkaScanner, error) {
			return scanner, nil
		},
	}
	_, err := src.LoadHeldAfterCheckpoint(context.Background(), HeldSpectatorRecordQuery{
		RoomID: "target-room", RecoveryJobID: "job-2", FailedCheckpoint: 5, ResumeCheckpoint: 4,
	})
	if !errors.Is(err, domain.ErrHeldContinuityBound) {
		t.Fatalf("want ErrHeldContinuityBound, got %v", err)
	}
}

func TestKafkaHeldSpectatorDLQSource_FailClosedWrongSourceTopic(t *testing.T) {
	t.Parallel()
	ok := targetHeldRec("room-a", 2, 2)
	wrongSrc := ok
	wrongSrc.Offset = 1
	wrongSrc.Headers = heldDLQHeaders(0, 1)
	wrongSrc.Headers[dlqHeaderSourceTopic] = "wrong.topic"
	src := heldSourceWithBatches([]ConsumerRecord{wrongSrc, ok}, nil, nil)
	_, err := src.LoadHeldAfterCheckpoint(context.Background(), HeldSpectatorRecordQuery{
		RoomID: "room-a", RecoveryJobID: "job-3", FailedCheckpoint: 2, ResumeCheckpoint: 1,
	})
	if !errors.Is(err, domain.ErrHeldContinuityInvalid) {
		t.Fatalf("want fail-closed ErrHeldContinuityInvalid, got %v", err)
	}
}

func TestKafkaHeldSpectatorDLQSource_FailClosedMissingOriginalHeaders(t *testing.T) {
	t.Parallel()
	rec := targetHeldRec("room-a", 2, 1)
	delete(rec.Headers, dlqHeaderSourcePartition)
	src := heldSourceWithBatches([]ConsumerRecord{rec}, nil, nil)
	_, err := src.LoadHeldAfterCheckpoint(context.Background(), HeldSpectatorRecordQuery{
		RoomID: "room-a", RecoveryJobID: "job-miss", FailedCheckpoint: 2, ResumeCheckpoint: 1,
	})
	if !errors.Is(err, domain.ErrHeldContinuityInvalid) {
		t.Fatalf("want fail-closed, got %v", err)
	}
}

func TestKafkaHeldSpectatorDLQSource_FailClosedWrongConsumerIdentity(t *testing.T) {
	t.Parallel()
	rec := targetHeldRec("room-a", 2, 1)
	rec.Headers[dlqHeaderConsumer] = "not-spectator-view"
	src := heldSourceWithBatches([]ConsumerRecord{rec}, nil, nil)
	_, err := src.LoadHeldAfterCheckpoint(context.Background(), HeldSpectatorRecordQuery{
		RoomID: "room-a", RecoveryJobID: "job-cons", FailedCheckpoint: 2, ResumeCheckpoint: 1,
	})
	if !errors.Is(err, domain.ErrHeldContinuityInvalid) {
		t.Fatalf("want fail-closed, got %v", err)
	}
}

func TestKafkaHeldSpectatorDLQSource_FailClosedNegativeOffset(t *testing.T) {
	t.Parallel()
	rec := targetHeldRec("room-a", 2, 1)
	rec.Headers[dlqHeaderSourceOffset] = "-1"
	src := heldSourceWithBatches([]ConsumerRecord{rec}, nil, nil)
	_, err := src.LoadHeldAfterCheckpoint(context.Background(), HeldSpectatorRecordQuery{
		RoomID: "room-a", RecoveryJobID: "job-neg", FailedCheckpoint: 2, ResumeCheckpoint: 1,
	})
	if !errors.Is(err, domain.ErrHeldContinuityInvalid) {
		t.Fatalf("want fail-closed, got %v", err)
	}
}

func TestKafkaHeldSpectatorDLQSource_FailClosedMalformedTargetSchema(t *testing.T) {
	t.Parallel()
	rec := ConsumerRecord{
		Topic: DefaultSpectatorSafeDLQTopic, Partition: 0, Offset: 1,
		Key: []byte("room-a"), Value: []byte(`{"roomId":"room-a","not":"valid"}`),
		Headers: heldDLQHeaders(0, 1),
	}
	src := heldSourceWithBatches([]ConsumerRecord{rec}, nil, nil)
	_, err := src.LoadHeldAfterCheckpoint(context.Background(), HeldSpectatorRecordQuery{
		RoomID: "room-a", RecoveryJobID: "job-schema", FailedCheckpoint: 2, ResumeCheckpoint: 1,
	})
	if !errors.Is(err, domain.ErrHeldContinuityInvalid) {
		t.Fatalf("want fail-closed, got %v", err)
	}
}

func TestKafkaHeldSpectatorDLQSource_FailClosedTargetKeyMustEqualRoomID(t *testing.T) {
	t.Parallel()
	rec := targetHeldRec("room-a", 2, 1)
	rec.Key = nil // identifiable only via envelope peek
	src := heldSourceWithBatches([]ConsumerRecord{rec}, nil, nil)
	_, err := src.LoadHeldAfterCheckpoint(context.Background(), HeldSpectatorRecordQuery{
		RoomID: "room-a", RecoveryJobID: "job-key", FailedCheckpoint: 2, ResumeCheckpoint: 1,
	})
	if !errors.Is(err, domain.ErrHeldContinuityInvalid) {
		t.Fatalf("want fail-closed, got %v", err)
	}
}

func TestKafkaHeldSpectatorDLQSource_ValidReplay(t *testing.T) {
	t.Parallel()
	ok := targetHeldRec("room-a", 2, 2)
	src := heldSourceWithBatches(
		[]ConsumerRecord{unrelatedHeldRec("other", 0), ok},
		nil, nil,
	)
	evts, err := src.LoadHeldAfterCheckpoint(context.Background(), HeldSpectatorRecordQuery{
		RoomID: "room-a", RecoveryJobID: "job-ok", FailedCheckpoint: 2, ResumeCheckpoint: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(evts) != 1 || string(evts[0].EventID) != "evt-room-a-2" {
		t.Fatalf("evts=%+v", evts)
	}
}

func TestKafkaHeldSpectatorDLQSource_UnrelatedMalformedIgnored(t *testing.T) {
	t.Parallel()
	junk := ConsumerRecord{
		Topic: DefaultSpectatorSafeDLQTopic, Partition: 0, Offset: 1,
		Key: []byte("other-room"), Value: []byte("{{{not-json"),
		// Missing headers on purpose — must not fail the target scan.
	}
	ok := targetHeldRec("room-a", 2, 2)
	src := heldSourceWithBatches([]ConsumerRecord{junk, ok}, nil, nil)
	evts, err := src.LoadHeldAfterCheckpoint(context.Background(), HeldSpectatorRecordQuery{
		RoomID: "room-a", RecoveryJobID: "job-junk", FailedCheckpoint: 2, ResumeCheckpoint: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(evts) != 1 {
		t.Fatalf("evts=%+v", evts)
	}
}

func TestKafkaHeldSpectatorDLQSource_IdempotentDuplicateSequence(t *testing.T) {
	t.Parallel()
	a := targetHeldRec("room-a", 2, 1)
	b := targetHeldRec("room-a", 2, 2) // same eventId/content, different DLQ offset
	src := heldSourceWithBatches([]ConsumerRecord{a, b}, nil, nil)
	evts, err := src.LoadHeldAfterCheckpoint(context.Background(), HeldSpectatorRecordQuery{
		RoomID: "room-a", RecoveryJobID: "job-dup", FailedCheckpoint: 2, ResumeCheckpoint: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(evts) != 1 {
		t.Fatalf("want idempotent single event, got %+v", evts)
	}
}

func TestKafkaHeldSpectatorDLQSource_ConflictingDuplicateSequenceFailClosed(t *testing.T) {
	t.Parallel()
	a := targetHeldRec("room-a", 2, 1)
	b := targetHeldRec("room-a", 2, 2, func(m map[string]any) {
		m["eventId"] = "evt-room-a-2-conflict"
	})
	src := heldSourceWithBatches([]ConsumerRecord{a, b}, nil, nil)
	_, err := src.LoadHeldAfterCheckpoint(context.Background(), HeldSpectatorRecordQuery{
		RoomID: "room-a", RecoveryJobID: "job-conflict", FailedCheckpoint: 2, ResumeCheckpoint: 1,
	})
	if !errors.Is(err, domain.ErrHeldContinuityInvalid) {
		t.Fatalf("want conflict fail-closed, got %v", err)
	}
}

func TestCollectHeldFromRecords_MatchedHardMax(t *testing.T) {
	t.Parallel()
	recs := make([]ConsumerRecord, 0, domain.MaxHeldRecoveryEvents+1)
	for i := 0; i < domain.MaxHeldRecoveryEvents+1; i++ {
		seq := int64(i + 2)
		recs = append(recs, ConsumerRecord{
			Key: []byte("room-b"),
			Value: canonicalSpectatorSafeJSON(func(m map[string]any) {
				m["roomId"] = "room-b"
				m["eventId"] = fmt.Sprintf("e-%d", seq)
				m["sequenceNumber"] = float64(seq)
			}),
		})
	}
	_, err := collectHeldFromRecords(recs, HeldSpectatorRecordQuery{
		RoomID: "room-b", ResumeCheckpoint: 1,
	})
	if !errors.Is(err, domain.ErrHeldContinuityBound) {
		t.Fatalf("want bound, got %v", err)
	}
}

type stubRoomRecovery struct {
	snap RoomSpectatorRecoverySnapshot
}

func (s stubRoomRecovery) FetchSpectatorRecoverySnapshot(
	ctx context.Context, roomID string, failedCheckpoint int64, recoveryJobID string,
) (RoomSpectatorRecoverySnapshot, error) {
	return s.snap, nil
}

type stubHeldSource struct {
	err  error
	evts []domain.SpectatorSafeEvent
}

func (s stubHeldSource) LoadHeldAfterCheckpoint(
	ctx context.Context, q HeldSpectatorRecordQuery,
) ([]domain.SpectatorSafeEvent, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.evts, nil
}

func TestStoreBackedProjectionRebuildExecutor_HeldFailureDoesNotReleaseQuarantine(t *testing.T) {
	t.Parallel()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	rs := store.NewRedisProjectionStore(rdb, "spec:")
	rs = rs.WithKafkaIdentity(DefaultSpectatorKafkaGroup, DefaultSpectatorSafeTopic)
	ctx := context.Background()
	if err := rs.LoadScripts(ctx); err != nil {
		t.Fatal(err)
	}
	room := domain.RoomID("room-held-fail")
	out, err := rs.Apply(ctx, room, []domain.SpectatorSafeEvent{{
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
	if err := rs.QuarantineKafkaAggregate(ctx, store.KafkaAggregateQuarantine{
		ConsumerGroup:  DefaultSpectatorKafkaGroup,
		SourceTopic:    DefaultSpectatorSafeTopic,
		AggregateKey:   string(room),
		Classification: store.QuarantineClassApplication,
		Reason:         "gap",
		QuarantinedAt:  time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}

	exec := &StoreBackedProjectionRebuildExecutor{
		Room: stubRoomRecovery{snap: RoomSpectatorRecoverySnapshot{
			SchemaVersion: 1, RoomID: string(room), RecoveryJobID: "job-1",
			FailedCheckpoint: 2, SequenceNumber: 1, ResumeCheckpoint: 1,
			Payload: map[string]any{
				"status": "waiting", "visibility": "public",
				"roster": []any{
					map[string]any{"seatIndex": 0, "playerId": "p1", "displayName": "Alice", "cardCount": 0, "occupied": true},
				},
			},
		}},
		Held:  stubHeldSource{err: fmt.Errorf("%w: bad dlq meta", domain.ErrHeldContinuityInvalid)},
		Store: rs,
		Release: store.RecoveryRelease{
			Enabled: true, Note: store.ReleaseNoteRecoveryContinuityProven,
		},
	}
	err = exec.ExecuteRebuild(ctx, ParsedProjectionRebuildRequest{
		RoomID: string(room), RecoveryJobID: "job-1", FailedCheckpoint: 2,
	})
	if !errors.Is(err, domain.ErrHeldContinuityInvalid) {
		t.Fatalf("want held fail-closed, got %v", err)
	}
	ok, err := rs.IsKafkaAggregateQuarantined(ctx, DefaultSpectatorKafkaGroup, DefaultSpectatorSafeTopic, string(room))
	if err != nil || !ok {
		t.Fatalf("quarantine must remain after held failure: ok=%v err=%v", ok, err)
	}
}
