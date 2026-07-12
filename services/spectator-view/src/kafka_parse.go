package main

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"unoarena/services/spectator-view/domain"
)

// ParsedSpectatorSafeEvent is the strict AsyncAPI Kafka envelope for room.spectator-safe.events.
type ParsedSpectatorSafeEvent struct {
	EventID       string
	EventType     string
	SchemaVersion int
	CorrelationID string
	CausationID   string
	OccurredAt    time.Time
	RoomID        string
	Sequence      int64
	Payload       map[string]any
}

// ParseSpectatorSafeRecord maps canonical nested AsyncAPI Kafka JSON into a domain event.
// Only the nested payload object becomes domain.SpectatorSafeEvent.Payload.
func ParseSpectatorSafeRecord(value []byte) (ParsedSpectatorSafeEvent, domain.SpectatorSafeEvent, error) {
	var raw map[string]any
	if err := json.Unmarshal(value, &raw); err != nil {
		return ParsedSpectatorSafeEvent{}, domain.SpectatorSafeEvent{},
			newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("invalid json envelope"))
	}

	eventID, err := requireNonEmptyString(raw, "eventId")
	if err != nil {
		return ParsedSpectatorSafeEvent{}, domain.SpectatorSafeEvent{},
			newTerminalKafkaError(KafkaFailureSchemaInvalid, err)
	}
	eventType, err := requireNonEmptyString(raw, "eventType")
	if err != nil {
		return ParsedSpectatorSafeEvent{}, domain.SpectatorSafeEvent{},
			newTerminalKafkaError(KafkaFailureSchemaInvalid, err)
	}
	schemaVersion, err := requireIntegralInt(raw["schemaVersion"], "schemaVersion")
	if err != nil {
		return ParsedSpectatorSafeEvent{}, domain.SpectatorSafeEvent{},
			newTerminalKafkaError(KafkaFailureSchemaInvalid, err)
	}
	if schemaVersion != 1 {
		return ParsedSpectatorSafeEvent{}, domain.SpectatorSafeEvent{},
			newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("schemaVersion must be 1"))
	}
	correlationID, err := requireNonEmptyString(raw, "correlationId")
	if err != nil {
		return ParsedSpectatorSafeEvent{}, domain.SpectatorSafeEvent{},
			newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	occurredRaw, ok := raw["occurredAt"]
	if !ok || occurredRaw == nil {
		return ParsedSpectatorSafeEvent{}, domain.SpectatorSafeEvent{},
			newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("occurredAt is required"))
	}
	occurredStr, ok := occurredRaw.(string)
	if !ok || strings.TrimSpace(occurredStr) == "" {
		return ParsedSpectatorSafeEvent{}, domain.SpectatorSafeEvent{},
			newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("occurredAt must be a date-time string"))
	}
	occurredAt, err := parseKafkaTime(occurredStr)
	if err != nil {
		return ParsedSpectatorSafeEvent{}, domain.SpectatorSafeEvent{},
			newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("invalid occurredAt"))
	}

	roomID, err := requireNonEmptyString(raw, "roomId")
	if err != nil {
		return ParsedSpectatorSafeEvent{}, domain.SpectatorSafeEvent{},
			newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	if _, ok := raw["sequenceNumber"]; !ok || raw["sequenceNumber"] == nil {
		return ParsedSpectatorSafeEvent{}, domain.SpectatorSafeEvent{},
			newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("sequenceNumber is required"))
	}
	seq, err := requireIntegralInt64(raw["sequenceNumber"], "sequenceNumber")
	if err != nil {
		return ParsedSpectatorSafeEvent{}, domain.SpectatorSafeEvent{},
			newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	if seq < 1 {
		return ParsedSpectatorSafeEvent{}, domain.SpectatorSafeEvent{},
			newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("sequenceNumber must be >= 1"))
	}

	payloadRaw, ok := raw["payload"]
	if !ok || payloadRaw == nil {
		return ParsedSpectatorSafeEvent{}, domain.SpectatorSafeEvent{},
			newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("payload is required"))
	}
	payload, ok := payloadRaw.(map[string]any)
	if !ok {
		return ParsedSpectatorSafeEvent{}, domain.SpectatorSafeEvent{},
			newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("payload must be an object"))
	}
	// Defensive copy so top-level merge cannot mutate the raw envelope map.
	payload = cloneStringAnyMap(payload)

	var topUno map[string]any
	if v, has := raw["unoWindow"]; has && v != nil {
		uw, ok := v.(map[string]any)
		if !ok {
			return ParsedSpectatorSafeEvent{}, domain.SpectatorSafeEvent{},
				newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("unoWindow must be an object"))
		}
		if err := validateUnoWindowObject(uw); err != nil {
			return ParsedSpectatorSafeEvent{}, domain.SpectatorSafeEvent{},
				newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
		}
		topUno = uw
	}

	if err := mergeUnoWindow(payload, topUno); err != nil {
		return ParsedSpectatorSafeEvent{}, domain.SpectatorSafeEvent{},
			newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}

	parsed := ParsedSpectatorSafeEvent{
		EventID:       eventID,
		EventType:     eventType,
		SchemaVersion: schemaVersion,
		CorrelationID: correlationID,
		OccurredAt:    occurredAt,
		RoomID:        roomID,
		Sequence:      seq,
		Payload:       payload,
	}
	if _, ok := raw["causationId"]; ok {
		causationID, err := optionalString(raw, "causationId")
		if err != nil {
			return ParsedSpectatorSafeEvent{}, domain.SpectatorSafeEvent{},
				newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
		}
		parsed.CausationID = causationID
	}

	dom := domain.SpectatorSafeEvent{
		EventID:       domain.EventID(eventID),
		EventType:     domain.EventType(eventType),
		SchemaVersion: schemaVersion,
		RoomID:        domain.RoomID(roomID),
		Sequence:      domain.SequenceNumber(seq),
		Payload:       payload,
	}
	return parsed, dom, nil
}

func mergeUnoWindow(payload map[string]any, topUno map[string]any) error {
	if topUno == nil {
		if v, ok := payload["unoWindow"]; ok && v != nil {
			uw, ok := v.(map[string]any)
			if !ok {
				return fmt.Errorf("payload.unoWindow must be an object")
			}
			return validateUnoWindowObject(uw)
		}
		return nil
	}
	if v, ok := payload["unoWindow"]; ok && v != nil {
		nested, ok := v.(map[string]any)
		if !ok {
			return fmt.Errorf("payload.unoWindow must be an object")
		}
		if err := validateUnoWindowObject(nested); err != nil {
			return err
		}
		if !unoWindowsAgree(topUno, nested) {
			return fmt.Errorf("top-level unoWindow conflicts with payload.unoWindow")
		}
		return nil
	}
	payload["unoWindow"] = cloneStringAnyMap(topUno)
	return nil
}

func validateUnoWindowObject(uw map[string]any) error {
	if _, err := requireNonEmptyString(uw, "playerId"); err != nil {
		return fmt.Errorf("unoWindow.%w", err)
	}
	if _, ok := uw["openingSequence"]; !ok || uw["openingSequence"] == nil {
		// openingRoomSequence is an accepted ingest alias per AsyncAPI.
		if _, ok := uw["openingRoomSequence"]; !ok || uw["openingRoomSequence"] == nil {
			return fmt.Errorf("unoWindow.openingSequence is required")
		}
		n, err := requireIntegralInt64(uw["openingRoomSequence"], "openingRoomSequence")
		if err != nil {
			return fmt.Errorf("unoWindow.%w", err)
		}
		if n < 1 {
			return fmt.Errorf("unoWindow.openingSequence must be >= 1")
		}
	} else {
		n, err := requireIntegralInt64(uw["openingSequence"], "openingSequence")
		if err != nil {
			return fmt.Errorf("unoWindow.%w", err)
		}
		if n < 1 {
			return fmt.Errorf("unoWindow.openingSequence must be >= 1")
		}
	}
	expiresRaw, ok := uw["expiresAt"]
	if !ok || expiresRaw == nil {
		return fmt.Errorf("unoWindow.expiresAt is required")
	}
	expiresStr, ok := expiresRaw.(string)
	if !ok || strings.TrimSpace(expiresStr) == "" {
		return fmt.Errorf("unoWindow.expiresAt must be a date-time string")
	}
	if _, err := parseKafkaTime(expiresStr); err != nil {
		return fmt.Errorf("unoWindow.invalid expiresAt")
	}
	return nil
}

func unoWindowsAgree(a, b map[string]any) bool {
	norm := func(m map[string]any) map[string]any {
		out := cloneStringAnyMap(m)
		if _, ok := out["openingSequence"]; !ok {
			if v, ok := out["openingRoomSequence"]; ok {
				out["openingSequence"] = v
			}
		}
		delete(out, "openingRoomSequence")
		return out
	}
	return reflect.DeepEqual(norm(a), norm(b))
}

func cloneStringAnyMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
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

func peekSafeRoomID(value []byte) string {
	var peek struct {
		RoomID string `json:"roomId"`
	}
	if err := json.Unmarshal(value, &peek); err != nil {
		return ""
	}
	return strings.TrimSpace(peek.RoomID)
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
