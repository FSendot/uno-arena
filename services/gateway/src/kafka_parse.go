package main

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"
)

// ParsedSessionInvalidated is the strict AsyncAPI Kafka envelope for
// identity.session.invalidated.
type ParsedSessionInvalidated struct {
	EventID       string
	EventType     string
	SchemaVersion int
	CorrelationID string
	CausationID   string
	OccurredAt    time.Time
	PlayerID      string
	SessionID     string
	Reason        string
}

// ParseSessionInvalidatedRecord maps canonical AsyncAPI Kafka JSON.
// Requires schemaVersion=1, eventType=SessionInvalidated, EventMetadata, and
// playerId/sessionId/reason.
func ParseSessionInvalidatedRecord(value []byte) (ParsedSessionInvalidated, error) {
	var raw map[string]any
	if err := json.Unmarshal(value, &raw); err != nil {
		return ParsedSessionInvalidated{},
			newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("invalid json envelope"))
	}

	eventID, err := requireNonEmptyString(raw, "eventId")
	if err != nil {
		return ParsedSessionInvalidated{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, err)
	}
	eventType, err := requireNonEmptyString(raw, "eventType")
	if err != nil {
		return ParsedSessionInvalidated{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, err)
	}
	if eventType != "SessionInvalidated" {
		return ParsedSessionInvalidated{}, newTerminalKafkaError(KafkaFailureSchemaInvalid,
			fmt.Errorf("eventType must be SessionInvalidated"))
	}
	schemaVersion, err := requireIntegralInt(raw["schemaVersion"], "schemaVersion")
	if err != nil {
		return ParsedSessionInvalidated{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, err)
	}
	if schemaVersion != 1 {
		return ParsedSessionInvalidated{}, newTerminalKafkaError(KafkaFailureSchemaInvalid,
			fmt.Errorf("schemaVersion must be 1"))
	}
	correlationID, err := requireNonEmptyString(raw, "correlationId")
	if err != nil {
		return ParsedSessionInvalidated{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	occurredRaw, ok := raw["occurredAt"]
	if !ok || occurredRaw == nil {
		return ParsedSessionInvalidated{}, newTerminalKafkaError(KafkaFailurePayloadInvalid,
			fmt.Errorf("occurredAt is required"))
	}
	occurredStr, ok := occurredRaw.(string)
	if !ok || strings.TrimSpace(occurredStr) == "" {
		return ParsedSessionInvalidated{}, newTerminalKafkaError(KafkaFailurePayloadInvalid,
			fmt.Errorf("occurredAt must be a date-time string"))
	}
	occurredAt, err := parseKafkaTime(occurredStr)
	if err != nil {
		return ParsedSessionInvalidated{}, newTerminalKafkaError(KafkaFailurePayloadInvalid,
			fmt.Errorf("invalid occurredAt"))
	}

	playerID, err := requireNonEmptyString(raw, "playerId")
	if err != nil {
		return ParsedSessionInvalidated{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	sessionID, err := requireNonEmptyString(raw, "sessionId")
	if err != nil {
		return ParsedSessionInvalidated{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	reason, err := requireNonEmptyString(raw, "reason")
	if err != nil {
		return ParsedSessionInvalidated{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}

	parsed := ParsedSessionInvalidated{
		EventID:       eventID,
		EventType:     eventType,
		SchemaVersion: schemaVersion,
		CorrelationID: correlationID,
		OccurredAt:    occurredAt,
		PlayerID:      playerID,
		SessionID:     sessionID,
		Reason:        reason,
	}
	if _, ok := raw["causationId"]; ok {
		causationID, err := optionalString(raw, "causationId")
		if err != nil {
			return ParsedSessionInvalidated{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
		}
		parsed.CausationID = causationID
	}
	return parsed, nil
}

func parseKafkaTime(ts string) (time.Time, error) {
	ts = strings.TrimSpace(ts)
	if at, err := time.Parse(time.RFC3339Nano, ts); err == nil {
		return at.UTC(), nil
	}
	if at, err := time.Parse(time.RFC3339, ts); err == nil {
		return at.UTC(), nil
	}
	return time.Time{}, fmt.Errorf("invalid time")
}

func requireNonEmptyString(m map[string]any, key string) (string, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return "", fmt.Errorf("%s is required", key)
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("%s must be a string", key)
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return "", fmt.Errorf("%s is required", key)
	}
	return s, nil
}

func optionalString(m map[string]any, key string) (string, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return "", nil
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("%s must be a string", key)
	}
	return strings.TrimSpace(s), nil
}

func requireIntegralInt(v any, field string) (int, error) {
	n, err := requireIntegralInt64(v, field)
	if err != nil {
		return 0, err
	}
	if n < math.MinInt || n > math.MaxInt {
		return 0, fmt.Errorf("%s must be an integer", field)
	}
	return int(n), nil
}

func requireIntegralInt64(v any, field string) (int64, error) {
	switch t := v.(type) {
	case float64:
		if math.IsNaN(t) || math.IsInf(t, 0) || t != math.Trunc(t) {
			return 0, fmt.Errorf("%s must be an integer", field)
		}
		return int64(t), nil
	case json.Number:
		n, err := t.Int64()
		if err != nil {
			return 0, fmt.Errorf("%s must be an integer", field)
		}
		return n, nil
	case int:
		return int64(t), nil
	case int64:
		return t, nil
	default:
		return 0, fmt.Errorf("%s must be an integer", field)
	}
}

func peekSafeCorrelationID(value []byte) string {
	var peek struct {
		CorrelationID string `json:"correlationId"`
	}
	if err := json.Unmarshal(value, &peek); err != nil {
		return ""
	}
	return strings.TrimSpace(peek.CorrelationID)
}

func peekSafeEventID(value []byte) string {
	var peek struct {
		EventID string `json:"eventId"`
	}
	if err := json.Unmarshal(value, &peek); err != nil {
		return ""
	}
	return strings.TrimSpace(peek.EventID)
}
