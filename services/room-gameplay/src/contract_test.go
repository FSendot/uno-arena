package main

import (
	"encoding/json"
	"testing"
)

// spectatorSafeEvent mirrors the room.spectator-safe.events schema owned by
// this service. A change here that breaks spectator-view's consumer test
// blocks both pipelines. See ADR-0014.
type spectatorSafeEvent struct {
	RoomID         string                 `json:"roomId"`
	EventType      string                 `json:"eventType"`
	SequenceNumber int                    `json:"sequenceNumber"`
	Payload        map[string]interface{} `json:"payload"`
}

func TestSpectatorSafeEventSchema(t *testing.T) {
	event := spectatorSafeEvent{
		RoomID:         "room-123",
		EventType:      "card.played",
		SequenceNumber: 1,
		Payload:        map[string]interface{}{"card": "red-7"},
	}
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("failed to marshal spectator-safe event: %v", err)
	}
	var check spectatorSafeEvent
	if err := json.Unmarshal(data, &check); err != nil {
		t.Fatalf("failed to unmarshal spectator-safe event: %v", err)
	}
	if check.RoomID == "" {
		t.Error("contract: roomId is required")
	}
	if check.EventType == "" {
		t.Error("contract: eventType is required")
	}
	if check.SequenceNumber == 0 {
		t.Error("contract: sequenceNumber is required")
	}
	if check.Payload == nil {
		t.Error("contract: payload is required")
	}
}
