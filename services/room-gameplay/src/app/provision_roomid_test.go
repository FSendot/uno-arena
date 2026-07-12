package app

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"unoarena/shared/envelope"
)

func TestProvision_FirstSuccessAndDuplicateIncludeAuthoritativeRoomID(t *testing.T) {
	clock := NewFixedClock(time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC))
	sessions := NewMemorySessionRepository()
	svc := NewService(ServiceDeps{
		Sessions:  sessions,
		Integrity: NewFakeGameIntegrity(),
		Publisher: NewFakeEventPublisher(),
		Audit:     NewFakeAuditSink(),
		Deals:     NewFakeDealSource(),
		Clock:     clock,
		SessionsV: AllowAllSessionValidator{},
	})

	wantRoom := "room_auth_1"
	first := svc.Provision(context.Background(), ProvisionInput{
		CommandID: "prov-first", TournamentID: "t-auth", RoundNumber: 1, SlotID: "slot_0",
		RoomID: wantRoom, HostID: "host", Visibility: "private", MaxSeats: 2,
		PlayerIDs: []string{"host", "p2"},
	})
	if first.Err != nil || first.Result.Status != envelope.StatusAccepted {
		t.Fatalf("first: %+v err=%v", first.Result, first.Err)
	}
	gotFirst := mustProvisionPayloadRoomID(t, first.Result.Payload)
	if gotFirst != wantRoom {
		t.Fatalf("first success roomId=%q want %q (payload=%s)", gotFirst, wantRoom, first.Result.Payload)
	}

	dup := svc.Provision(context.Background(), ProvisionInput{
		CommandID: "prov-dup", TournamentID: "t-auth", RoundNumber: 1, SlotID: "slot_0",
		RoomID: "room_other", HostID: "host", MaxSeats: 2,
	})
	if dup.Err != nil || dup.Result.Status != envelope.StatusAccepted {
		t.Fatalf("dup: %+v err=%v", dup.Result, dup.Err)
	}
	gotDup := mustProvisionPayloadRoomID(t, dup.Result.Payload)
	if gotDup != wantRoom {
		t.Fatalf("duplicate roomId=%q want %q", gotDup, wantRoom)
	}
}

func mustProvisionPayloadRoomID(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("payload: %v", err)
	}
	roomID, _ := payload["roomId"].(string)
	if roomID == "" {
		t.Fatalf("missing roomId in payload %s", raw)
	}
	return roomID
}
