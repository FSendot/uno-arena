//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"testing"

	"unoarena/services/room-gameplay/app"
	"unoarena/services/room-gameplay/domain"
	"unoarena/services/room-gameplay/store"
)

func TestIntegration_FirstTournamentRuntimeReadyPublishesOnceAcrossReplacement(t *testing.T) {
	pool := openPool(t)
	sessions := store.NewSessionStore(pool)
	room, outcome := domain.ProvisionTournamentRoom(domain.ProvisionTournamentRoomCommand{
		CommandID: "provision-ready", RoomID: "room-ready", TournamentID: "t-ready",
		RoundNumber: 2, SlotID: "slot-7", HostID: "p1", Visibility: domain.VisibilityPrivate, MaxSeats: 2,
	})
	if room == nil || !outcome.Accepted() {
		t.Fatalf("provision outcome=%+v", outcome)
	}
	if err := sessions.Commit(context.Background(), app.CommitRequest{Session: domain.OpenSession(room)}); err != nil {
		t.Fatal(err)
	}
	if err := sessions.MarkRuntimePodReady(context.Background(), "room-ready", 1, "10.0.0.1"); err != nil {
		t.Fatal(err)
	}
	if _, err := sessions.AdvanceRuntimeGeneration(context.Background(), "room-ready", 1); err != nil {
		t.Fatal(err)
	}
	if err := sessions.MarkRuntimePodReady(context.Background(), "room-ready", 2, "10.0.0.2"); err != nil {
		t.Fatal(err)
	}

	var count int
	var eventID, topic, partitionKey string
	var payload []byte
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*)::int, min(event_id), min(topic), min(partition_key), min(payload::text)::bytea
		FROM integration_outbox_events WHERE event_type='RoomRuntimeReady'
	`).Scan(&count, &eventID, &topic, &partitionKey, &payload); err != nil {
		t.Fatal(err)
	}
	if count != 1 || eventID != "room-runtime-ready:room-ready" || topic != "room.runtime.ready" || partitionKey != "room-ready" {
		t.Fatalf("outbox count=%d event=%q topic=%q key=%q", count, eventID, topic, partitionKey)
	}
	var body map[string]any
	if err := json.Unmarshal(payload, &body); err != nil {
		t.Fatal(err)
	}
	if body["roomId"] != "room-ready" || body["tournamentId"] != "t-ready" || body["slotId"] != "slot-7" || body["roundNumber"] != float64(2) || body["generation"] != float64(1) {
		t.Fatalf("payload=%v", body)
	}
}

func TestIntegration_CasualRuntimeReadyDoesNotPublishTournamentFact(t *testing.T) {
	pool := openPool(t)
	sessions := store.NewSessionStore(pool)
	room, outcome := domain.CreateRoom(domain.CreateRoomCommand{CommandID: "casual-ready", RoomID: "casual-ready", HostID: "p1", Visibility: domain.VisibilityPublic, MaxSeats: 2})
	if room == nil || !outcome.Accepted() {
		t.Fatalf("create outcome=%+v", outcome)
	}
	if err := sessions.Commit(context.Background(), app.CommitRequest{Session: domain.OpenSession(room)}); err != nil {
		t.Fatal(err)
	}
	if err := sessions.MarkRuntimePodReady(context.Background(), "casual-ready", 1, "10.0.0.3"); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM integration_outbox_events WHERE event_type='RoomRuntimeReady'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("casual readiness facts=%d", count)
	}
}
