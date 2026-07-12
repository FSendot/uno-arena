package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	DefaultProjectionRebuildTopic    = "spectator.projection.rebuild_requested"
	DefaultProjectionRebuildGroup    = "spectator-view-projection-rebuilder"
	DefaultProjectionRebuildDLQTopic = "spectator.projection.rebuild_requested.spectator-view.dlq"
	EventTypeProjectionRebuildReq    = "SpectatorProjectionRebuildRequested"
	ExpectedSpectatorSafeSourceTopic = DefaultSpectatorSafeTopic
)

// ParsedProjectionRebuildRequest is the strict AsyncAPI rebuild-request envelope.
type ParsedProjectionRebuildRequest struct {
	EventID             string
	EventType           string
	SchemaVersion       int
	CorrelationID       string
	CausationID         string
	OccurredAt          time.Time
	RecoveryJobID       string
	RoomID              string
	FailedCheckpoint    int64
	ExpectedSourceTopic string
	Attempt             int // optional; 0 means absent
}

// ParseSpectatorProjectionRebuildRequested maps rebuild-request Kafka JSON strictly.
// NEVER accepts embedded event arrays.
func ParseSpectatorProjectionRebuildRequested(value []byte) (ParsedProjectionRebuildRequest, error) {
	var raw map[string]any
	if err := json.Unmarshal(value, &raw); err != nil {
		return ParsedProjectionRebuildRequest{},
			newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("invalid json envelope"))
	}

	// Reject event arrays / held payloads — rebuild messages must stay bounded control plane.
	for _, forbidden := range []string{"events", "heldEvents", "records", "payloads"} {
		if v, ok := raw[forbidden]; ok && v != nil {
			return ParsedProjectionRebuildRequest{},
				newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("%s must not be present on rebuild requests", forbidden))
		}
	}

	eventID, err := requireNonEmptyString(raw, "eventId")
	if err != nil {
		return ParsedProjectionRebuildRequest{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, err)
	}
	eventType, err := requireNonEmptyString(raw, "eventType")
	if err != nil {
		return ParsedProjectionRebuildRequest{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, err)
	}
	if eventType != EventTypeProjectionRebuildReq {
		return ParsedProjectionRebuildRequest{},
			newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("eventType must be %s", EventTypeProjectionRebuildReq))
	}
	schemaVersion, err := requireIntegralInt(raw["schemaVersion"], "schemaVersion")
	if err != nil {
		return ParsedProjectionRebuildRequest{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, err)
	}
	if schemaVersion != 1 {
		return ParsedProjectionRebuildRequest{},
			newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("schemaVersion must be 1"))
	}
	correlationID, err := requireNonEmptyString(raw, "correlationId")
	if err != nil {
		return ParsedProjectionRebuildRequest{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	occurredRaw, ok := raw["occurredAt"]
	if !ok || occurredRaw == nil {
		return ParsedProjectionRebuildRequest{},
			newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("occurredAt is required"))
	}
	occurredStr, ok := occurredRaw.(string)
	if !ok || strings.TrimSpace(occurredStr) == "" {
		return ParsedProjectionRebuildRequest{},
			newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("occurredAt must be a date-time string"))
	}
	occurredAt, err := parseKafkaTime(occurredStr)
	if err != nil {
		return ParsedProjectionRebuildRequest{},
			newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("invalid occurredAt"))
	}

	recoveryJobID, err := requireNonEmptyString(raw, "recoveryJobId")
	if err != nil {
		return ParsedProjectionRebuildRequest{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	roomID, err := requireNonEmptyString(raw, "roomId")
	if err != nil {
		return ParsedProjectionRebuildRequest{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	if _, ok := raw["failedCheckpoint"]; !ok || raw["failedCheckpoint"] == nil {
		return ParsedProjectionRebuildRequest{},
			newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("failedCheckpoint is required"))
	}
	failedCheckpoint, err := requireIntegralInt64(raw["failedCheckpoint"], "failedCheckpoint")
	if err != nil {
		return ParsedProjectionRebuildRequest{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	if failedCheckpoint < 1 {
		return ParsedProjectionRebuildRequest{},
			newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("failedCheckpoint must be >= 1"))
	}
	expectedSource, err := requireNonEmptyString(raw, "expectedSourceTopic")
	if err != nil {
		return ParsedProjectionRebuildRequest{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	if expectedSource != ExpectedSpectatorSafeSourceTopic {
		return ParsedProjectionRebuildRequest{},
			newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("expectedSourceTopic must be %s", ExpectedSpectatorSafeSourceTopic))
	}

	parsed := ParsedProjectionRebuildRequest{
		EventID:             eventID,
		EventType:           eventType,
		SchemaVersion:       schemaVersion,
		CorrelationID:       correlationID,
		OccurredAt:          occurredAt,
		RecoveryJobID:       recoveryJobID,
		RoomID:              roomID,
		FailedCheckpoint:    failedCheckpoint,
		ExpectedSourceTopic: expectedSource,
	}
	if _, ok := raw["causationId"]; ok {
		causationID, err := optionalString(raw, "causationId")
		if err != nil {
			return ParsedProjectionRebuildRequest{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
		}
		parsed.CausationID = causationID
	}
	if _, ok := raw["attempt"]; ok && raw["attempt"] != nil {
		attempt, err := requireIntegralInt(raw["attempt"], "attempt")
		if err != nil {
			return ParsedProjectionRebuildRequest{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
		}
		if attempt < 1 {
			return ParsedProjectionRebuildRequest{},
				newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("attempt must be >= 1"))
		}
		parsed.Attempt = attempt
	}
	return parsed, nil
}

// IdempotencyKey returns the durable (recoveryJobId, roomId, failedCheckpoint) identity.
func (p ParsedProjectionRebuildRequest) IdempotencyKey() string {
	return p.RecoveryJobID + "|" + p.RoomID + "|" + formatInt64(p.FailedCheckpoint)
}

func formatInt64(n int64) string {
	return strconvFormatInt64(n)
}
