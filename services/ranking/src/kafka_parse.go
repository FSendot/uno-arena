package main

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"unoarena/services/ranking/domain"
)

const eventTypeGameCompleted = "GameCompleted"

// GameCompletedEvent is the strict AsyncAPI Kafka envelope for room.game.completed.
type GameCompletedEvent struct {
	EventID        string
	EventType      string
	SchemaVersion  int
	CorrelationID  string
	CausationID    string
	OccurredAt     time.Time
	RoomID         string
	GameID         string
	RoomType       string
	IsAbandoned    bool
	Authoritative  bool
	Completed      bool
	CommandID      string
	PlacementOrder []string
	Participants   []GameCompletedParticipant
}

// GameCompletedParticipant is one placement fact from the Kafka envelope.
type GameCompletedParticipant struct {
	PlayerID   string
	Placement  int
	CardPoints *int
	Outcome    string
}

// ParseGameCompletedRecord maps canonical AsyncAPI Kafka JSON into GameCompletedEvent
// with strict JSON typing for required contract fields.
func ParseGameCompletedRecord(value []byte) (GameCompletedEvent, error) {
	var raw map[string]any
	if err := json.Unmarshal(value, &raw); err != nil {
		return GameCompletedEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("invalid json envelope"))
	}

	eventID, err := requireNonEmptyString(raw, "eventId")
	if err != nil {
		return GameCompletedEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, err)
	}
	eventType, err := requireNonEmptyString(raw, "eventType")
	if err != nil {
		return GameCompletedEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, err)
	}
	if eventType != eventTypeGameCompleted {
		return GameCompletedEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("eventType must be %s", eventTypeGameCompleted))
	}
	schemaVersion, err := requireIntegralInt(raw["schemaVersion"], "schemaVersion")
	if err != nil {
		return GameCompletedEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, err)
	}
	if schemaVersion != 1 {
		return GameCompletedEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("schemaVersion must be 1"))
	}
	correlationID, err := requireNonEmptyString(raw, "correlationId")
	if err != nil {
		return GameCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	occurredRaw, ok := raw["occurredAt"]
	if !ok || occurredRaw == nil {
		return GameCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("occurredAt is required"))
	}
	occurredStr, ok := occurredRaw.(string)
	if !ok || strings.TrimSpace(occurredStr) == "" {
		return GameCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("occurredAt must be a date-time string"))
	}
	occurredAt, err := parseKafkaTime(occurredStr)
	if err != nil {
		return GameCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("invalid occurredAt"))
	}

	roomID, err := requireNonEmptyString(raw, "roomId")
	if err != nil {
		return GameCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	gameID, err := requireNonEmptyString(raw, "gameId")
	if err != nil {
		return GameCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	roomType, err := requireNonEmptyString(raw, "roomType")
	if err != nil {
		return GameCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	if roomType != string(domain.RoomTypeAdHoc) && roomType != string(domain.RoomTypeTournament) {
		return GameCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("roomType must be ad_hoc or tournament"))
	}

	isAbandoned, err := requireBool(raw, "isAbandoned")
	if err != nil {
		return GameCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	authoritative, err := requireBool(raw, "authoritative")
	if err != nil {
		return GameCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	completed, err := requireBool(raw, "completed")
	if err != nil {
		return GameCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}

	evt := GameCompletedEvent{
		EventID:       eventID,
		EventType:     eventType,
		SchemaVersion: schemaVersion,
		CorrelationID: correlationID,
		OccurredAt:    occurredAt,
		RoomID:        roomID,
		GameID:        gameID,
		RoomType:      roomType,
		IsAbandoned:   isAbandoned,
		Authoritative: authoritative,
		Completed:     completed,
	}

	if _, ok := raw["causationId"]; ok {
		causationID, err := optionalString(raw, "causationId")
		if err != nil {
			return GameCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
		}
		evt.CausationID = causationID
	}
	if _, ok := raw["commandId"]; ok {
		commandID, err := optionalString(raw, "commandId")
		if err != nil {
			return GameCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
		}
		evt.CommandID = commandID
	}

	orderRaw, ok := raw["placementOrder"]
	if !ok || orderRaw == nil {
		return GameCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("placementOrder is required"))
	}
	placementOrder, err := parsePlacementOrder(orderRaw)
	if err != nil {
		return GameCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	evt.PlacementOrder = placementOrder

	partsRaw, ok := raw["participants"]
	if !ok || partsRaw == nil {
		return GameCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("participants are required"))
	}
	participants, err := parseGameCompletedParticipants(partsRaw)
	if err != nil {
		return GameCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	evt.Participants = participants

	if err := validatePlacementAgreement(evt.PlacementOrder, evt.Participants); err != nil {
		return GameCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	return evt, nil
}

// MapGameCompletedToRequest builds the Ranking apply request from a parsed envelope.
// commandId uses canonical commandId when present, otherwise kafka:<eventId>.
// causationId prefers commandId then envelope causationId.
func MapGameCompletedToRequest(evt GameCompletedEvent) GameCompletedRequest {
	commandID := strings.TrimSpace(evt.CommandID)
	if commandID == "" {
		commandID = "kafka:" + evt.EventID
	}
	causation := strings.TrimSpace(evt.CommandID)
	if causation == "" {
		causation = strings.TrimSpace(evt.CausationID)
	}
	parts := make([]domain.RatedPlacement, 0, len(evt.Participants))
	for _, p := range evt.Participants {
		parts = append(parts, domain.RatedPlacement{
			PlayerID:  domain.PlayerID(p.PlayerID),
			Placement: p.Placement,
		})
	}
	return GameCompletedRequest{
		CommandID:     domain.CommandID(commandID),
		EventID:       domain.EventID(evt.EventID),
		GameID:        domain.GameID(evt.GameID),
		RoomID:        domain.RoomID(evt.RoomID),
		RoomType:      domain.RoomType(evt.RoomType),
		IsAbandoned:   evt.IsAbandoned,
		Authoritative: evt.Authoritative,
		Completed:     evt.Completed,
		Participants:  parts,
		CorrelationID: evt.CorrelationID,
		CausationID:   causation,
	}
}

func parsePlacementOrder(raw any) ([]string, error) {
	arr, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("placementOrder must be an array")
	}
	if len(arr) == 0 {
		return nil, fmt.Errorf("placementOrder is required")
	}
	out := make([]string, 0, len(arr))
	seen := map[string]struct{}{}
	for _, item := range arr {
		s, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("placementOrder entries must be strings")
		}
		s = strings.TrimSpace(s)
		if s == "" {
			return nil, fmt.Errorf("placementOrder entries must be nonempty strings")
		}
		if _, dup := seen[s]; dup {
			return nil, fmt.Errorf("placementOrder entries must be unique")
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out, nil
}

func parseGameCompletedParticipants(raw any) ([]GameCompletedParticipant, error) {
	arr, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("participants must be an array")
	}
	if len(arr) == 0 {
		return nil, fmt.Errorf("participants are required")
	}
	out := make([]GameCompletedParticipant, 0, len(arr))
	seen := map[string]struct{}{}
	for _, item := range arr {
		pm, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("participants entries must be objects")
		}
		playerID, err := requireNonEmptyString(pm, "playerId")
		if err != nil {
			return nil, err
		}
		if _, dup := seen[playerID]; dup {
			return nil, fmt.Errorf("participants playerId must be unique")
		}
		seen[playerID] = struct{}{}
		if _, ok := pm["placement"]; !ok || pm["placement"] == nil {
			return nil, fmt.Errorf("placement is required")
		}
		placement, err := requireIntegralInt(pm["placement"], "placement")
		if err != nil {
			return nil, err
		}
		if placement < 1 {
			return nil, fmt.Errorf("placement must be >= 1")
		}
		p := GameCompletedParticipant{PlayerID: playerID, Placement: placement}
		if v, ok := pm["cardPoints"]; ok && v != nil {
			n, err := requireIntegralInt(v, "cardPoints")
			if err != nil {
				return nil, err
			}
			p.CardPoints = &n
		}
		if _, ok := pm["outcome"]; ok {
			outcome, err := optionalString(pm, "outcome")
			if err != nil {
				return nil, err
			}
			p.Outcome = outcome
		}
		out = append(out, p)
	}
	return out, nil
}

func validatePlacementAgreement(order []string, parts []GameCompletedParticipant) error {
	if len(order) != len(parts) {
		return fmt.Errorf("placementOrder and participants must have the same length")
	}
	byID := map[string]GameCompletedParticipant{}
	for _, p := range parts {
		byID[p.PlayerID] = p
	}
	for i, id := range order {
		p, ok := byID[id]
		if !ok {
			return fmt.Errorf("placementOrder and participants sets must agree")
		}
		want := i + 1
		if p.Placement != want {
			return fmt.Errorf("participant placement must match placementOrder position")
		}
	}
	for _, p := range parts {
		found := false
		for _, id := range order {
			if id == p.PlayerID {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("placementOrder and participants sets must agree")
		}
	}
	return nil
}

func requireBool(m map[string]any, key string) (bool, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return false, fmt.Errorf("%s is required", key)
	}
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("%s must be a boolean", key)
	}
	return b, nil
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
