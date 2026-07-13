package main

import "testing"

func TestParseRoomRuntimeReadyRecord_Contract(t *testing.T) {
	evt, err := ParseRoomRuntimeReadyRecord([]byte(`{"schemaVersion":1,"eventId":"room-runtime-ready:r1","eventType":"RoomRuntimeReady","correlationId":"corr-1","occurredAt":"2026-07-13T10:00:00Z","roomId":"r1","tournamentId":"t1","roundNumber":2,"slotId":"s7","generation":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if evt.RoomID != "r1" || evt.TournamentID != "t1" || evt.RoundNumber != 2 || evt.SlotID != "s7" || evt.Generation != 1 {
		t.Fatalf("event=%+v", evt)
	}
}

func TestParseRoomRuntimeReadyRecord_RejectsWrongTypeOrGeneration(t *testing.T) {
	for _, raw := range []string{
		`{"schemaVersion":1,"eventId":"e","eventType":"Other","correlationId":"c","occurredAt":"2026-07-13T10:00:00Z","roomId":"r","tournamentId":"t","roundNumber":1,"slotId":"s","generation":1}`,
		`{"schemaVersion":1,"eventId":"e","eventType":"RoomRuntimeReady","correlationId":"c","occurredAt":"2026-07-13T10:00:00Z","roomId":"r","tournamentId":"t","roundNumber":1,"slotId":"s","generation":0}`,
	} {
		if _, err := ParseRoomRuntimeReadyRecord([]byte(raw)); err == nil || !IsTerminalKafkaConsumeError(err) {
			t.Fatalf("want terminal contract rejection: %s err=%v", raw, err)
		}
	}
}
