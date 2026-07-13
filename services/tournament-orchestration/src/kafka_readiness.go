package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// RoomRuntimeReadyEvent is Room's first-playable integration fact.
type RoomRuntimeReadyEvent struct {
	EventID, CorrelationID, RoomID, TournamentID, SlotID string
	SchemaVersion, RoundNumber                           int
	Generation                                           int64
	OccurredAt                                           time.Time
}

// ParseRoomRuntimeReadyRecord validates the canonical AsyncAPI envelope.
func ParseRoomRuntimeReadyRecord(value []byte) (RoomRuntimeReadyEvent, error) {
	var raw map[string]any
	if err := json.Unmarshal(value, &raw); err != nil {
		return RoomRuntimeReadyEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("invalid json envelope"))
	}
	required := func(name string) (string, error) { return requireNonEmptyString(raw, name) }
	eventID, err := required("eventId")
	if err != nil {
		return RoomRuntimeReadyEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, err)
	}
	eventType, err := required("eventType")
	if err != nil || eventType != "RoomRuntimeReady" {
		return RoomRuntimeReadyEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("eventType must be RoomRuntimeReady"))
	}
	schema, err := requireIntegralInt(raw["schemaVersion"], "schemaVersion")
	if err != nil || schema != 1 {
		return RoomRuntimeReadyEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("schemaVersion must be 1"))
	}
	corr, err := required("correlationId")
	if err != nil {
		return RoomRuntimeReadyEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	occurred, err := required("occurredAt")
	if err != nil {
		return RoomRuntimeReadyEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	at, err := parseKafkaTime(occurred)
	if err != nil {
		return RoomRuntimeReadyEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("invalid occurredAt"))
	}
	roomID, err := required("roomId")
	if err != nil {
		return RoomRuntimeReadyEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	tournamentID, err := required("tournamentId")
	if err != nil {
		return RoomRuntimeReadyEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	slotID, err := required("slotId")
	if err != nil {
		return RoomRuntimeReadyEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	round, err := requireIntegralInt(raw["roundNumber"], "roundNumber")
	if err != nil || round < 1 {
		return RoomRuntimeReadyEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("roundNumber must be > 0"))
	}
	generation, err := requireIntegralInt64(raw["generation"], "generation")
	if err != nil || generation < 1 {
		return RoomRuntimeReadyEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("generation must be > 0"))
	}
	if strings.TrimSpace(eventID) == "" {
		return RoomRuntimeReadyEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("eventId required"))
	}
	return RoomRuntimeReadyEvent{EventID: eventID, CorrelationID: corr, RoomID: roomID, TournamentID: tournamentID, SlotID: slotID, SchemaVersion: schema, RoundNumber: round, Generation: generation, OccurredAt: at}, nil
}
