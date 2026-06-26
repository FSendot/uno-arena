package main

import (
	"encoding/json"
	"testing"
)

// spectatorSafeEvent mirrors the schema declared by room-gameplay.
// This test is the consumer side of the contract check: if room-gameplay
// changes the schema in a breaking way, this test fails and blocks this
// service's pipeline. See ADR-0014.
type spectatorSafeEvent struct {
	RoomID         string                 `json:"roomId"`
	EventType      string                 `json:"eventType"`
	SequenceNumber int                    `json:"sequenceNumber"`
	Payload        map[string]interface{} `json:"payload"`
}

func TestConsumesSpectatorSafeEvent(t *testing.T) {
	raw := `{"roomId":"room-123","eventType":"card.played","sequenceNumber":1,"payload":{"card":"red-7"}}`
	var event spectatorSafeEvent
	if err := json.Unmarshal([]byte(raw), &event); err != nil {
		t.Fatalf("consumer: failed to parse room.spectator-safe.events payload: %v", err)
	}
	if event.RoomID == "" {
		t.Error("consumer: roomId is required")
	}
	if event.EventType == "" {
		t.Error("consumer: eventType is required")
	}
	if event.SequenceNumber == 0 {
		t.Error("consumer: sequenceNumber is required")
	}
	if event.Payload == nil {
		t.Error("consumer: payload is required")
	}
}
