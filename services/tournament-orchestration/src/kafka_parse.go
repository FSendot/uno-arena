package main

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"
)

// ParseMatchCompletedRecord maps a canonical AsyncAPI Kafka JSON envelope into
// MatchCompletedEvent with strict JSON typing for required contract fields.
func ParseMatchCompletedRecord(value []byte) (MatchCompletedEvent, error) {
	var raw map[string]any
	if err := json.Unmarshal(value, &raw); err != nil {
		return MatchCompletedEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("invalid json envelope"))
	}

	eventID, err := requireNonEmptyString(raw, "eventId")
	if err != nil {
		return MatchCompletedEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, err)
	}
	eventType, err := requireNonEmptyString(raw, "eventType")
	if err != nil {
		return MatchCompletedEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, err)
	}
	schemaVersion, err := requireIntegralInt(raw["schemaVersion"], "schemaVersion")
	if err != nil {
		return MatchCompletedEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, err)
	}
	if schemaVersion != 1 {
		return MatchCompletedEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("schemaVersion must be 1"))
	}
	correlationID, err := requireNonEmptyString(raw, "correlationId")
	if err != nil {
		return MatchCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	occurredRaw, ok := raw["occurredAt"]
	if !ok || occurredRaw == nil {
		return MatchCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("occurredAt is required"))
	}
	occurredStr, ok := occurredRaw.(string)
	if !ok || strings.TrimSpace(occurredStr) == "" {
		return MatchCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("occurredAt must be a date-time string"))
	}
	occurredAt, err := parseKafkaTime(occurredStr)
	if err != nil {
		return MatchCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("invalid occurredAt"))
	}

	roomID, err := requireNonEmptyString(raw, "roomId")
	if err != nil {
		return MatchCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	tournamentID, err := requireNonEmptyString(raw, "tournamentId")
	if err != nil {
		return MatchCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	completionVersion, err := requireIntegralInt64(raw["completionVersion"], "completionVersion")
	if err != nil {
		return MatchCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	if completionVersion <= 0 {
		return MatchCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("completionVersion must be > 0"))
	}

	isAbandonedRaw, ok := raw["isAbandoned"]
	if !ok || isAbandonedRaw == nil {
		return MatchCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("isAbandoned is required"))
	}
	isAbandoned, ok := isAbandonedRaw.(bool)
	if !ok {
		return MatchCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("isAbandoned must be a boolean"))
	}

	evt := MatchCompletedEvent{
		EventID:           eventID,
		EventType:         eventType,
		SchemaVersion:     schemaVersion,
		CorrelationID:     correlationID,
		OccurredAt:        occurredAt,
		RoomID:            roomID,
		TournamentID:      tournamentID,
		CompletionVersion: uint64(completionVersion),
		IsAbandoned:       isAbandoned,
		HasIsAbandoned:    true,
	}

	if _, ok := raw["causationId"]; ok {
		causationID, err := optionalString(raw, "causationId")
		if err != nil {
			return MatchCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
		}
		evt.CausationID = causationID
	}
	if _, ok := raw["slotId"]; ok {
		slotID, err := optionalString(raw, "slotId")
		if err != nil {
			return MatchCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
		}
		evt.SlotID = slotID
	}
	if _, ok := raw["roundNumber"]; ok && raw["roundNumber"] != nil {
		roundNumber, err := requireIntegralInt(raw["roundNumber"], "roundNumber")
		if err != nil {
			return MatchCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
		}
		evt.RoundNumber = roundNumber
	}

	playersRaw, ok := raw["players"]
	if !ok || playersRaw == nil {
		return MatchCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("players are required"))
	}
	players, err := parseMatchCompletedPlayers(playersRaw)
	if err != nil {
		return MatchCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	evt.Players = players

	if forfeitsRaw, ok := raw["forfeits"]; ok && forfeitsRaw != nil {
		forfeits, err := parseMatchCompletedForfeits(forfeitsRaw)
		if err != nil {
			return MatchCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
		}
		evt.Forfeits = forfeits
	}

	if err := validateMatchCompletedEvent(evt); err != nil {
		class := KafkaFailurePayloadInvalid
		msg := err.Error()
		if strings.Contains(msg, "schemaVersion") || strings.Contains(msg, "eventType") || strings.Contains(msg, "eventId") {
			class = KafkaFailureSchemaInvalid
		}
		return MatchCompletedEvent{}, newTerminalKafkaError(class, err)
	}
	return evt, nil
}

func parseMatchCompletedPlayers(raw any) ([]MatchPlayerStanding, error) {
	arr, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("players must be an array")
	}
	if len(arr) == 0 {
		return nil, fmt.Errorf("players are required")
	}
	out := make([]MatchPlayerStanding, 0, len(arr))
	for _, item := range arr {
		pm, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("players entries must be objects")
		}
		playerID, err := requireNonEmptyString(pm, "playerId")
		if err != nil {
			return nil, err
		}
		if _, ok := pm["matchWins"]; !ok || pm["matchWins"] == nil {
			return nil, fmt.Errorf("matchWins is required")
		}
		matchWins, err := requireIntegralInt(pm["matchWins"], "matchWins")
		if err != nil {
			return nil, err
		}
		if _, ok := pm["cumulativeCardPoints"]; !ok || pm["cumulativeCardPoints"] == nil {
			return nil, fmt.Errorf("cumulativeCardPoints is required")
		}
		cardPoints, err := requireIntegralInt(pm["cumulativeCardPoints"], "cumulativeCardPoints")
		if err != nil {
			return nil, err
		}
		standing := MatchPlayerStanding{
			PlayerID:             playerID,
			MatchWins:            matchWins,
			CumulativeCardPoints: cardPoints,
		}
		if v, ok := pm["forfeited"]; ok && v != nil {
			forfeited, ok := v.(bool)
			if !ok {
				return nil, fmt.Errorf("forfeited must be a boolean")
			}
			standing.Forfeited = forfeited
		}
		if v, ok := pm["finalGameCompletedAt"]; ok && v != nil {
			ts, ok := v.(string)
			if !ok || strings.TrimSpace(ts) == "" {
				return nil, fmt.Errorf("finalGameCompletedAt must be a date-time string")
			}
			at, err := parseKafkaTime(ts)
			if err != nil {
				return nil, fmt.Errorf("invalid finalGameCompletedAt")
			}
			standing.FinalGameCompletedAt = at
		}
		out = append(out, standing)
	}
	return out, nil
}

func parseMatchCompletedForfeits(raw any) ([]string, error) {
	arr, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("forfeits must be an array of strings")
	}
	out := make([]string, 0, len(arr))
	for _, item := range arr {
		s, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("forfeits must be an array of strings")
		}
		s = strings.TrimSpace(s)
		if s == "" {
			return nil, fmt.Errorf("forfeits entries must be nonempty strings")
		}
		out = append(out, s)
	}
	return out, nil
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

// peekSafeRoomID extracts roomId only when present as a nonempty JSON string.
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
