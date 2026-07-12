package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"time"

	"unoarena/services/ranking/domain"
)

const (
	eventTypePlayersAdvanced     = "PlayersAdvanced"
	eventTypeTournamentCompleted = "TournamentCompleted"
)

// PlayersAdvancedEvent is the strict AsyncAPI envelope for tournament.players.advanced.
type PlayersAdvancedEvent struct {
	EventID            string
	EventType          string
	SchemaVersion      int
	CorrelationID      string
	CausationID        string
	OccurredAt         time.Time
	TournamentID       string
	RoundNumber        int
	SourceSlotID       string
	AdvancingPlayerIDs []string
	BusinessKey        string
	PayloadFingerprint string
}

// TournamentCompletedEvent is the strict AsyncAPI envelope for tournament.completed.
type TournamentCompletedEvent struct {
	EventID            string
	EventType          string
	SchemaVersion      int
	CorrelationID      string
	CausationID        string
	OccurredAt         time.Time
	TournamentID       string
	FinalStandings     []string
	BusinessKey        string
	PayloadFingerprint string
}

// ParsePlayersAdvancedRecord maps canonical AsyncAPI JSON into PlayersAdvancedEvent.
func ParsePlayersAdvancedRecord(value []byte) (PlayersAdvancedEvent, error) {
	raw, err := decodeRankingEnvelopeJSON(value)
	if err != nil {
		return PlayersAdvancedEvent{}, err
	}
	meta, err := parseRankingEventMetadata(raw, eventTypePlayersAdvanced)
	if err != nil {
		return PlayersAdvancedEvent{}, err
	}
	tournamentID, err := requireNonEmptyString(raw, "tournamentId")
	if err != nil {
		return PlayersAdvancedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	if _, ok := raw["roundNumber"]; !ok || raw["roundNumber"] == nil {
		return PlayersAdvancedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("roundNumber is required"))
	}
	roundNumber, err := requireIntegralInt(raw["roundNumber"], "roundNumber")
	if err != nil {
		return PlayersAdvancedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	if roundNumber < 1 {
		return PlayersAdvancedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("roundNumber must be >= 1"))
	}
	sourceSlotID, err := requireNonEmptyString(raw, "sourceSlotId")
	if err != nil {
		return PlayersAdvancedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	idsRaw, ok := raw["advancingPlayerIds"]
	if !ok || idsRaw == nil {
		return PlayersAdvancedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("advancingPlayerIds is required"))
	}
	ids, err := parseUniquePlayerIDList(idsRaw, "advancingPlayerIds", 1, 3)
	if err != nil {
		return PlayersAdvancedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	biz, err := playersAdvancedBusinessKey(tournamentID, roundNumber, sourceSlotID)
	if err != nil {
		return PlayersAdvancedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	fp := fingerprintContractBody(map[string]any{
		"eventId": meta.EventID, "eventType": meta.EventType, "schemaVersion": meta.SchemaVersion,
		"correlationId": meta.CorrelationID, "occurredAt": meta.OccurredAt.Format(time.RFC3339Nano),
		"tournamentId": tournamentID, "roundNumber": roundNumber, "sourceSlotId": sourceSlotID,
		"advancingPlayerIds": ids,
	}, raw)
	evt := PlayersAdvancedEvent{
		EventID: meta.EventID, EventType: meta.EventType, SchemaVersion: meta.SchemaVersion,
		CorrelationID: meta.CorrelationID, CausationID: meta.CausationID, OccurredAt: meta.OccurredAt,
		TournamentID: tournamentID, RoundNumber: roundNumber, SourceSlotID: sourceSlotID,
		AdvancingPlayerIDs: ids, BusinessKey: biz, PayloadFingerprint: fp,
	}
	return evt, nil
}

// ParseTournamentCompletedRecord maps canonical AsyncAPI JSON into TournamentCompletedEvent.
// championId is not an accepted contract field.
func ParseTournamentCompletedRecord(value []byte) (TournamentCompletedEvent, error) {
	raw, err := decodeRankingEnvelopeJSON(value)
	if err != nil {
		return TournamentCompletedEvent{}, err
	}
	if _, ok := raw["championId"]; ok {
		return TournamentCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("championId is not an accepted contract field"))
	}
	meta, err := parseRankingEventMetadata(raw, eventTypeTournamentCompleted)
	if err != nil {
		return TournamentCompletedEvent{}, err
	}
	tournamentID, err := requireNonEmptyString(raw, "tournamentId")
	if err != nil {
		return TournamentCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	standingsRaw, ok := raw["finalStandings"]
	if !ok || standingsRaw == nil {
		return TournamentCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("finalStandings is required"))
	}
	standings, err := parseUniquePlayerIDList(standingsRaw, "finalStandings", 1, 10)
	if err != nil {
		return TournamentCompletedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	fp := fingerprintContractBody(map[string]any{
		"eventId": meta.EventID, "eventType": meta.EventType, "schemaVersion": meta.SchemaVersion,
		"correlationId": meta.CorrelationID, "occurredAt": meta.OccurredAt.Format(time.RFC3339Nano),
		"tournamentId": tournamentID, "finalStandings": standings,
	}, raw)
	return TournamentCompletedEvent{
		EventID: meta.EventID, EventType: meta.EventType, SchemaVersion: meta.SchemaVersion,
		CorrelationID: meta.CorrelationID, CausationID: meta.CausationID, OccurredAt: meta.OccurredAt,
		TournamentID: tournamentID, FinalStandings: standings,
		BusinessKey: meta.EventID, PayloadFingerprint: fp,
	}, nil
}

func MapPlayersAdvancedToRequest(evt PlayersAdvancedEvent) TournamentPerformanceRequest {
	players := make([]TournamentPlayerPerformance, 0, len(evt.AdvancingPlayerIDs))
	for _, id := range evt.AdvancingPlayerIDs {
		players = append(players, TournamentPlayerPerformance{
			PlayerID: domain.PlayerID(id), RoundNumber: evt.RoundNumber,
			Reason: domain.ReasonTournamentAdvancement,
		})
	}
	causation := strings.TrimSpace(evt.CausationID)
	return TournamentPerformanceRequest{
		SourceTopic: DefaultPlayersAdvancedTopic, UpstreamEventID: domain.EventID(evt.EventID),
		BusinessKey: evt.BusinessKey, PayloadFingerprint: evt.PayloadFingerprint,
		TournamentID: domain.TournamentID(evt.TournamentID), CorrelationID: evt.CorrelationID,
		CausationID: causation, Players: players,
	}
}

func MapTournamentCompletedToRequest(evt TournamentCompletedEvent) TournamentPerformanceRequest {
	players := make([]TournamentPlayerPerformance, 0, len(evt.FinalStandings))
	for i, id := range evt.FinalStandings {
		players = append(players, TournamentPlayerPerformance{
			PlayerID: domain.PlayerID(id), Placement: i + 1,
			Reason: domain.ReasonTournamentFinalStanding,
		})
	}
	causation := strings.TrimSpace(evt.CausationID)
	return TournamentPerformanceRequest{
		SourceTopic: DefaultTournamentCompletedTopic, UpstreamEventID: domain.EventID(evt.EventID),
		BusinessKey: evt.BusinessKey, PayloadFingerprint: evt.PayloadFingerprint,
		TournamentID: domain.TournamentID(evt.TournamentID), CorrelationID: evt.CorrelationID,
		CausationID: causation, Players: players,
	}
}

func decodeRankingEnvelopeJSON(value []byte) (map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(value))
	dec.UseNumber()
	var raw map[string]any
	if err := dec.Decode(&raw); err != nil {
		return nil, newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("invalid json envelope"))
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return nil, newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("trailing json tokens"))
	}
	if raw == nil {
		return nil, newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("invalid json envelope"))
	}
	return raw, nil
}

type rankingEventMetadata struct {
	EventID       string
	EventType     string
	SchemaVersion int
	CorrelationID string
	CausationID   string
	OccurredAt    time.Time
}

func parseRankingEventMetadata(raw map[string]any, wantType string) (rankingEventMetadata, error) {
	eventID, err := requireNonEmptyString(raw, "eventId")
	if err != nil {
		return rankingEventMetadata{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, err)
	}
	eventType, err := requireNonEmptyString(raw, "eventType")
	if err != nil {
		return rankingEventMetadata{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, err)
	}
	if eventType != wantType {
		return rankingEventMetadata{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("eventType must be %s", wantType))
	}
	schemaVersion, err := requireIntegralInt(raw["schemaVersion"], "schemaVersion")
	if err != nil {
		return rankingEventMetadata{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, err)
	}
	if schemaVersion != 1 {
		return rankingEventMetadata{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("schemaVersion must be 1"))
	}
	correlationID, err := requireNonEmptyString(raw, "correlationId")
	if err != nil {
		return rankingEventMetadata{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	occurredRaw, ok := raw["occurredAt"]
	if !ok || occurredRaw == nil {
		return rankingEventMetadata{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("occurredAt is required"))
	}
	occurredStr, ok := occurredRaw.(string)
	if !ok || strings.TrimSpace(occurredStr) == "" {
		return rankingEventMetadata{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("occurredAt must be a date-time string"))
	}
	occurredAt, err := parseKafkaTime(occurredStr)
	if err != nil {
		return rankingEventMetadata{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("invalid occurredAt"))
	}
	meta := rankingEventMetadata{
		EventID: eventID, EventType: eventType, SchemaVersion: schemaVersion,
		CorrelationID: correlationID, OccurredAt: occurredAt,
	}
	if _, ok := raw["causationId"]; ok {
		causationID, err := optionalString(raw, "causationId")
		if err != nil {
			return rankingEventMetadata{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
		}
		meta.CausationID = causationID
	}
	return meta, nil
}

func parseUniquePlayerIDList(raw any, field string, minN, maxN int) ([]string, error) {
	arr, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("%s must be an array", field)
	}
	if len(arr) < minN || len(arr) > maxN {
		return nil, fmt.Errorf("%s must have %d..%d entries", field, minN, maxN)
	}
	out := make([]string, 0, len(arr))
	seen := map[string]struct{}{}
	for _, item := range arr {
		s, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("%s entries must be strings", field)
		}
		s = strings.TrimSpace(s)
		if s == "" {
			return nil, fmt.Errorf("%s entries must be nonempty strings", field)
		}
		if _, dup := seen[s]; dup {
			return nil, fmt.Errorf("%s entries must be unique", field)
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out, nil
}

func playersAdvancedBusinessKey(tournamentID string, roundNumber int, sourceSlotID string) (string, error) {
	b, err := json.Marshal([]any{tournamentID, roundNumber, sourceSlotID})
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func fingerprintContractBody(identity map[string]any, raw map[string]any) string {
	// Include optional contract body fields when present (rule / completionReason).
	if v, ok := raw["rule"]; ok {
		identity["rule"] = v
	}
	if v, ok := raw["completionReason"]; ok {
		identity["completionReason"] = v
	}
	if v, ok := raw["causationId"]; ok {
		identity["causationId"] = v
	}
	canonical, err := canonicalJSON(identity)
	if err != nil {
		sum := sha256.Sum256([]byte("unfingerprintable"))
		return hex.EncodeToString(sum[:])
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}

func canonicalJSON(v any) ([]byte, error) {
	switch x := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		buf := []byte{'{'}
		for i, k := range keys {
			if i > 0 {
				buf = append(buf, ',')
			}
			kb, err := json.Marshal(k)
			if err != nil {
				return nil, err
			}
			buf = append(buf, kb...)
			buf = append(buf, ':')
			vb, err := canonicalJSON(x[k])
			if err != nil {
				return nil, err
			}
			buf = append(buf, vb...)
		}
		buf = append(buf, '}')
		return buf, nil
	case []any:
		buf := []byte{'['}
		for i, item := range x {
			if i > 0 {
				buf = append(buf, ',')
			}
			vb, err := canonicalJSON(item)
			if err != nil {
				return nil, err
			}
			buf = append(buf, vb...)
		}
		buf = append(buf, ']')
		return buf, nil
	case []string:
		arr := make([]any, len(x))
		for i, s := range x {
			arr[i] = s
		}
		return canonicalJSON(arr)
	case json.Number:
		return []byte(x.String()), nil
	case float64:
		if math.Trunc(x) == x {
			return []byte(fmt.Sprintf("%.0f", x)), nil
		}
		return json.Marshal(x)
	default:
		return json.Marshal(x)
	}
}
