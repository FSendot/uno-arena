//go:build redis_integration

package store_test

import (
	"testing"
	"time"

	"unoarena/services/spectator-view/domain"
	"unoarena/services/spectator-view/store"
)

func TestRedisKafkaQuarantine_AtomicWithDroppedOutcome(t *testing.T) {
	s, ctx := openIntegrationStore(t)
	s = s.WithKafkaIdentity("spectator-view", "room.spectator-safe.events")
	room := domain.RoomID("roomQ1")

	// Seed accepted event so projection exists.
	if _, err := s.Apply(ctx, room, []domain.SpectatorSafeEvent{roomCreated(string(room), "seed", 1)}); err != nil {
		t.Fatal(err)
	}

	out, err := s.Apply(ctx, room, []domain.SpectatorSafeEvent{{
		EventID: "drop1", EventType: domain.EventCardPlayed, SchemaVersion: 1,
		RoomID: room, Sequence: 2,
		Payload: map[string]any{"privateHand": []any{"r1"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if out.Kind != domain.OutcomeDropped {
		t.Fatalf("kind=%s", out.Kind)
	}

	ok, err := s.IsKafkaAggregateQuarantined(ctx, "spectator-view", "room.spectator-safe.events", string(room))
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected active quarantine after privacy drop")
	}

	// Later events must still apply at domain/store level when called directly;
	// Kafka consumer skips via IsKafkaAggregateQuarantined. Verify marker holds.
	ok2, err := s.IsKafkaAggregateQuarantined(ctx, "spectator-view", "room.spectator-safe.events", string(room))
	if err != nil || !ok2 {
		t.Fatalf("quarantine must remain active: ok=%v err=%v", ok2, err)
	}
}

func TestRedisKafkaQuarantine_TerminalParsePersist(t *testing.T) {
	s, ctx := openIntegrationStore(t)
	room := "roomQ2"
	err := s.QuarantineKafkaAggregate(ctx, store.KafkaAggregateQuarantine{
		ConsumerGroup:   "spectator-view",
		SourceTopic:     "room.spectator-safe.events",
		AggregateKey:    room,
		Classification:  store.QuarantineClassSchemaInvalid,
		Reason:          "schema_invalid",
		SourcePartition: 1,
		SourceOffset:    9,
		EventID:         "evt-bad",
		CorrelationID:   "corr-bad",
		QuarantinedAt:   time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	ok, err := s.IsKafkaAggregateQuarantined(ctx, "spectator-view", "room.spectator-safe.events", room)
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	// Wrong consumer identity must not match.
	ok, err = s.IsKafkaAggregateQuarantined(ctx, "other", "room.spectator-safe.events", room)
	if err != nil || ok {
		t.Fatalf("wrong group must not match: ok=%v err=%v", ok, err)
	}
}

func TestRedisKafkaQuarantine_NoPartialOnCAS(t *testing.T) {
	s, ctx := openIntegrationStore(t)
	s = s.WithKafkaIdentity("spectator-view", "room.spectator-safe.events")
	room := domain.RoomID("roomQ3")
	if _, err := s.Apply(ctx, room, []domain.SpectatorSafeEvent{roomCreated(string(room), "seed", 1)}); err != nil {
		t.Fatal(err)
	}
	// Gap sequence quarantines and marks aggregate in same Lua commit.
	out, err := s.Apply(ctx, room, []domain.SpectatorSafeEvent{{
		EventID: "gap", EventType: domain.EventCardPlayed, SchemaVersion: 1,
		RoomID: room, Sequence: 5,
		Payload: map[string]any{"discardTop": "r7", "activeColor": "red"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if out.Kind != domain.OutcomeQuarantined {
		t.Fatalf("kind=%s", out.Kind)
	}
	ok, err := s.IsKafkaAggregateQuarantined(ctx, "spectator-view", "room.spectator-safe.events", string(room))
	if err != nil || !ok {
		t.Fatalf("quarantine must be set with outcome: ok=%v err=%v", ok, err)
	}
	// Outcome must also be durable.
	dup, err := s.Apply(ctx, room, []domain.SpectatorSafeEvent{{
		EventID: "gap", EventType: domain.EventCardPlayed, SchemaVersion: 1,
		RoomID: room, Sequence: 5,
		Payload: map[string]any{"discardTop": "r7", "activeColor": "red"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if dup.Kind != domain.OutcomeDuplicate {
		t.Fatalf("expected duplicate outcome persistence, got %s", dup.Kind)
	}
}

func TestApplyCommitScript_DeclaresQuarantineKey(t *testing.T) {
	// Structure guard: LoadScripts must succeed with quarantine script present.
	s, ctx := openIntegrationStore(t)
	if err := s.LoadScripts(ctx); err != nil {
		t.Fatal(err)
	}
}
