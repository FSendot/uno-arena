package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"unoarena/services/analytics/domain"
)

// AsyncAPI message eventType values for Analytics-subscribed topics.
const (
	asyncEventGameplayMetric                = "GameplayMetric"
	asyncEventMatchCompleted                = "MatchCompleted"
	asyncEventTournamentMatchAssigned       = "TournamentMatchAssigned"
	asyncEventTournamentMatchResultRecorded = "TournamentMatchResultRecorded"
	asyncEventPlayersAdvanced               = "PlayersAdvanced"
	asyncEventTournamentRoundCompleted      = "TournamentRoundCompleted"
	asyncEventTournamentCompleted           = "TournamentCompleted"
	asyncEventPlayerRatingUpdated           = "PlayerRatingUpdated"
	asyncEventLeaderboardSnapshotPublished  = "LeaderboardSnapshotPublished"
)

// ParseAnalyticsRecord maps a Kafka record into an allowlisted domain.UpstreamEvent.
// SourceTopic is derived only from rec.Topic (never from payload).
func ParseAnalyticsRecord(rec ConsumerRecord) (domain.UpstreamEvent, error) {
	source := domain.SourceTopic(strings.TrimSpace(rec.Topic))
	if !source.Valid() {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid,
			fmt.Errorf("unsupported analytics source topic %q", rec.Topic))
	}

	raw, err := decodeAnalyticsEnvelopeJSON(rec.Value)
	if err != nil {
		return domain.UpstreamEvent{}, err
	}

	meta, err := parseEventMetadata(raw)
	if err != nil {
		return domain.UpstreamEvent{}, err
	}
	wantType, ok := expectedAsyncEventType(source)
	if !ok || meta.EventType != wantType {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid,
			fmt.Errorf("eventType must be %s for topic %s", wantType, source))
	}

	if err := rejectForbiddenPrivacyFields(raw); err != nil {
		return domain.UpstreamEvent{}, err
	}

	key := strings.TrimSpace(string(rec.Key))
	if key == "" {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid,
			fmt.Errorf("kafka record key is required"))
	}

	evt, err := mapTopicPayload(source, meta, raw, key)
	if err != nil {
		return domain.UpstreamEvent{}, err
	}
	return evt, nil
}

// decodeAnalyticsEnvelopeJSON uses UseNumber so large/non-integral integers fail
// deterministically in requireIntegral* helpers, and rejects trailing JSON tokens.
func decodeAnalyticsEnvelopeJSON(value []byte) (map[string]any, error) {
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

type eventMetadata struct {
	EventID       string
	EventType     string
	SchemaVersion int
	CorrelationID string
	OccurredAt    time.Time
}

func parseEventMetadata(raw map[string]any) (eventMetadata, error) {
	eventID, err := requireNonEmptyString(raw, "eventId")
	if err != nil {
		return eventMetadata{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, err)
	}
	eventType, err := requireNonEmptyString(raw, "eventType")
	if err != nil {
		return eventMetadata{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, err)
	}
	schemaVersion, err := requireIntegralInt(raw["schemaVersion"], "schemaVersion")
	if err != nil {
		return eventMetadata{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, err)
	}
	if schemaVersion != domain.CurrentSchemaVersion {
		return eventMetadata{}, newTerminalKafkaError(KafkaFailureSchemaInvalid,
			fmt.Errorf("schemaVersion must be %d", domain.CurrentSchemaVersion))
	}
	correlationID, err := requireNonEmptyString(raw, "correlationId")
	if err != nil {
		return eventMetadata{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	occurredRaw, ok := raw["occurredAt"]
	if !ok || occurredRaw == nil {
		return eventMetadata{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("occurredAt is required"))
	}
	occurredStr, ok := occurredRaw.(string)
	if !ok || strings.TrimSpace(occurredStr) == "" {
		return eventMetadata{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("occurredAt must be a date-time string"))
	}
	occurredAt, err := parseKafkaTime(occurredStr)
	if err != nil {
		return eventMetadata{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("invalid occurredAt"))
	}
	return eventMetadata{
		EventID:       eventID,
		EventType:     eventType,
		SchemaVersion: schemaVersion,
		CorrelationID: correlationID,
		OccurredAt:    occurredAt,
	}, nil
}

func expectedAsyncEventType(source domain.SourceTopic) (string, bool) {
	switch source {
	case domain.SourceRoomGameplayMetrics:
		return asyncEventGameplayMetric, true
	case domain.SourceRoomMatchCompleted:
		return asyncEventMatchCompleted, true
	case domain.SourceTournamentMatchAssigned:
		return asyncEventTournamentMatchAssigned, true
	case domain.SourceTournamentMatchResultRecorded:
		return asyncEventTournamentMatchResultRecorded, true
	case domain.SourceTournamentPlayersAdvanced:
		return asyncEventPlayersAdvanced, true
	case domain.SourceTournamentRoundCompleted:
		return asyncEventTournamentRoundCompleted, true
	case domain.SourceTournamentCompleted:
		return asyncEventTournamentCompleted, true
	case domain.SourceRankingPlayerRatingUpdated:
		return asyncEventPlayerRatingUpdated, true
	case domain.SourceRankingLeaderboardSnapshot:
		return asyncEventLeaderboardSnapshotPublished, true
	default:
		return "", false
	}
}

func mapTopicPayload(source domain.SourceTopic, meta eventMetadata, raw map[string]any, key string) (domain.UpstreamEvent, error) {
	switch source {
	case domain.SourceRoomGameplayMetrics:
		return mapGameplayMetric(source, meta, raw, key)
	case domain.SourceRoomMatchCompleted:
		return mapMatchCompleted(source, meta, raw, key)
	case domain.SourceTournamentMatchAssigned:
		return mapTournamentMatchAssigned(source, meta, raw, key)
	case domain.SourceTournamentMatchResultRecorded:
		return mapTournamentMatchResult(source, meta, raw, key)
	case domain.SourceTournamentPlayersAdvanced:
		return mapPlayersAdvanced(source, meta, raw, key)
	case domain.SourceTournamentRoundCompleted:
		return mapTournamentRoundCompleted(source, meta, raw, key)
	case domain.SourceTournamentCompleted:
		return mapTournamentCompleted(source, meta, raw, key)
	case domain.SourceRankingPlayerRatingUpdated:
		return mapPlayerRatingUpdated(source, meta, raw, key)
	case domain.SourceRankingLeaderboardSnapshot:
		return mapLeaderboardSnapshot(source, meta, raw, key)
	default:
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid,
			fmt.Errorf("unsupported topic %s", source))
	}
}

func mapGameplayMetric(source domain.SourceTopic, meta eventMetadata, raw map[string]any, key string) (domain.UpstreamEvent, error) {
	roomID, err := requireNonEmptyString(raw, "roomId")
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	if key != roomID {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid,
			fmt.Errorf("kafka key must equal roomId"))
	}
	visibility, err := requireNonEmptyString(raw, "visibility")
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	metricType, err := requireNonEmptyString(raw, "metricType")
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	identity := map[string]any{
		"roomId":     roomID,
		"visibility": visibility,
		"metricType": metricType,
	}
	payload := map[string]any{
		"visibility": visibility,
		"metricType": metricType,
		"roomId":     roomID,
	}
	for _, field := range []string{"gameId", "tournamentId", "publicCard", "publicCardRank", "publicCardColor", "playerId", "displayName"} {
		if err := copyOptionalString(raw, identity, field); err != nil {
			return domain.UpstreamEvent{}, err
		}
		if err := copyOptionalString(raw, payload, field); err != nil {
			return domain.UpstreamEvent{}, err
		}
	}
	for _, field := range []string{"publicCardCountTotal", "publicCardCount", "roomSequence"} {
		if err := copyOptionalNumber(raw, identity, field); err != nil {
			return domain.UpstreamEvent{}, err
		}
		if err := copyOptionalNumber(raw, payload, field); err != nil {
			return domain.UpstreamEvent{}, err
		}
	}
	return finishMapped(source, domain.EventGameplayMetric, meta, meta.EventID, payload, identity, false), nil
}

func mapMatchCompleted(source domain.SourceTopic, meta eventMetadata, raw map[string]any, key string) (domain.UpstreamEvent, error) {
	roomID, err := requireNonEmptyString(raw, "roomId")
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	if key != roomID {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid,
			fmt.Errorf("kafka key must equal roomId"))
	}
	completionVersion, err := requireIntegralInt64(raw["completionVersion"], "completionVersion")
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	isAbandoned, err := requireBool(raw, "isAbandoned")
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	// AsyncAPI requires players[]; validate presence then strip from projection rows.
	players, err := requireObjectArray(raw, "players")
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	playerIdentity := make([]any, 0, len(players))
	for i, row := range players {
		if err := rejectForbiddenPrivacyFields(row); err != nil {
			return domain.UpstreamEvent{}, err
		}
		playerIdentity = append(playerIdentity, copyAnyMap(row))
		_ = i
	}
	idem := fmt.Sprintf("%s|%d", roomID, completionVersion)

	identity := map[string]any{
		"roomId":            roomID,
		"completionVersion": completionVersion,
		"isAbandoned":       isAbandoned,
		"players":           playerIdentity,
	}
	if err := copyOptionalString(raw, identity, "tournamentId"); err != nil {
		return domain.UpstreamEvent{}, err
	}
	if err := copyOptionalNumber(raw, identity, "roundNumber"); err != nil {
		return domain.UpstreamEvent{}, err
	}
	if err := copyOptionalString(raw, identity, "slotId"); err != nil {
		return domain.UpstreamEvent{}, err
	}
	if err := copyOptionalStringArray(raw, identity, "forfeits"); err != nil {
		return domain.UpstreamEvent{}, err
	}

	tournamentID, err := optionalString(raw, "tournamentId")
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	if tournamentID == "" {
		// Ad-hoc MatchCompleted: durable ignore. Fingerprint identity only (no eventId).
		ignorePayload := map[string]any{
			"roomId":            roomID,
			"completionVersion": completionVersion,
			"reason":            "adhoc_match_completed_ignored",
		}
		return finishMapped(source, domain.EventTournamentStatistic, meta, idem, ignorePayload, identity, true), nil
	}

	payload := map[string]any{
		"tournamentId": tournamentID,
		"roomId":       roomID,
		"eventType":    asyncEventMatchCompleted,
		"phase":        "match_completed",
	}
	if err := copyOptionalNumber(raw, payload, "roundNumber"); err != nil {
		return domain.UpstreamEvent{}, err
	}
	if err := copyOptionalString(raw, payload, "slotId"); err != nil {
		return domain.UpstreamEvent{}, err
	}
	// players[] / forfeits[] / isAbandoned intentionally stripped from projection payload.
	return finishMapped(source, domain.EventTournamentStatistic, meta, idem, payload, identity, false), nil
}

func mapTournamentMatchAssigned(source domain.SourceTopic, meta eventMetadata, raw map[string]any, key string) (domain.UpstreamEvent, error) {
	tournamentID, err := requireNonEmptyString(raw, "tournamentId")
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	if key != tournamentID {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid,
			fmt.Errorf("kafka key must equal tournamentId"))
	}
	roundNumber, err := requireIntegralInt(raw["roundNumber"], "roundNumber")
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	slotID, err := requireNonEmptyString(raw, "slotId")
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	roomID, err := requireNonEmptyString(raw, "roomId")
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	idem := fmt.Sprintf("%s|%d|%s", tournamentID, roundNumber, slotID)
	identity := map[string]any{
		"tournamentId": tournamentID,
		"roundNumber":  roundNumber,
		"slotId":       slotID,
		"roomId":       roomID,
	}
	payload := map[string]any{
		"tournamentId": tournamentID,
		"roundNumber":  roundNumber,
		"slotId":       slotID,
		"roomId":       roomID,
		"eventType":    asyncEventTournamentMatchAssigned,
		"phase":        "assigned",
	}
	return finishMapped(source, domain.EventTournamentStatistic, meta, idem, payload, identity, false), nil
}

func mapTournamentMatchResult(source domain.SourceTopic, meta eventMetadata, raw map[string]any, key string) (domain.UpstreamEvent, error) {
	tournamentID, err := requireNonEmptyString(raw, "tournamentId")
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	if key != tournamentID {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid,
			fmt.Errorf("kafka key must equal tournamentId"))
	}
	roomID, err := requireNonEmptyString(raw, "roomId")
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	completionVersion, err := requireIntegralInt64(raw["completionVersion"], "completionVersion")
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	idem := fmt.Sprintf("%s|%d", roomID, completionVersion)
	identity := map[string]any{
		"tournamentId":      tournamentID,
		"roomId":            roomID,
		"completionVersion": completionVersion,
	}
	if err := copyOptionalString(raw, identity, "slotId"); err != nil {
		return domain.UpstreamEvent{}, err
	}
	payload := map[string]any{
		"tournamentId": tournamentID,
		"roomId":       roomID,
		"eventType":    asyncEventTournamentMatchResultRecorded,
		"phase":        "result_recorded",
	}
	if err := copyOptionalString(raw, payload, "slotId"); err != nil {
		return domain.UpstreamEvent{}, err
	}
	return finishMapped(source, domain.EventTournamentStatistic, meta, idem, payload, identity, false), nil
}

func mapPlayersAdvanced(source domain.SourceTopic, meta eventMetadata, raw map[string]any, key string) (domain.UpstreamEvent, error) {
	tournamentID, err := requireNonEmptyString(raw, "tournamentId")
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	if key != tournamentID {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid,
			fmt.Errorf("kafka key must equal tournamentId"))
	}
	roundNumber, err := requireIntegralInt(raw["roundNumber"], "roundNumber")
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	sourceSlotID, err := requireNonEmptyString(raw, "sourceSlotId")
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	advancing, err := requireStringArray(raw, "advancingPlayerIds")
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	advancingAny := make([]any, len(advancing))
	for i, id := range advancing {
		advancingAny[i] = id
	}
	idem := fmt.Sprintf("%s|%d|%s", tournamentID, roundNumber, sourceSlotID)
	identity := map[string]any{
		"tournamentId":       tournamentID,
		"roundNumber":        roundNumber,
		"sourceSlotId":       sourceSlotID,
		"advancingPlayerIds": advancingAny,
	}
	if err := copyOptionalString(raw, identity, "rule"); err != nil {
		return domain.UpstreamEvent{}, err
	}
	payload := map[string]any{
		"tournamentId":         tournamentID,
		"roundNumber":          roundNumber,
		"slotId":               sourceSlotID,
		"advancingPlayerCount": len(advancing),
		"eventType":            asyncEventPlayersAdvanced,
		"phase":                "advanced",
	}
	// advancingPlayerIds / rule intentionally not forwarded to projection rows.
	return finishMapped(source, domain.EventTournamentStatistic, meta, idem, payload, identity, false), nil
}

func mapTournamentRoundCompleted(source domain.SourceTopic, meta eventMetadata, raw map[string]any, key string) (domain.UpstreamEvent, error) {
	tournamentID, err := requireNonEmptyString(raw, "tournamentId")
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	if key != tournamentID {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid,
			fmt.Errorf("kafka key must equal tournamentId"))
	}
	roundNumber, err := requireIntegralInt(raw["roundNumber"], "roundNumber")
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	idem := fmt.Sprintf("%s|%d", tournamentID, roundNumber)
	identity := map[string]any{
		"tournamentId": tournamentID,
		"roundNumber":  roundNumber,
	}
	payload := map[string]any{
		"tournamentId": tournamentID,
		"roundNumber":  roundNumber,
		"eventType":    asyncEventTournamentRoundCompleted,
		"phase":        "round_completed",
	}
	return finishMapped(source, domain.EventTournamentStatistic, meta, idem, payload, identity, false), nil
}

func mapTournamentCompleted(source domain.SourceTopic, meta eventMetadata, raw map[string]any, key string) (domain.UpstreamEvent, error) {
	tournamentID, err := requireNonEmptyString(raw, "tournamentId")
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	if key != tournamentID {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid,
			fmt.Errorf("kafka key must equal tournamentId"))
	}
	standings, err := requireUniqueStringArrayBounded(raw, "finalStandings", 1, 10)
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	standingsAny := make([]any, len(standings))
	for i, id := range standings {
		standingsAny[i] = id
	}
	identity := map[string]any{
		"tournamentId":   tournamentID,
		"finalStandings": standingsAny,
	}
	if err := copyOptionalString(raw, identity, "completionReason"); err != nil {
		return domain.UpstreamEvent{}, err
	}
	payload := map[string]any{
		"tournamentId": tournamentID,
		"eventType":    asyncEventTournamentCompleted,
		"phase":        "completed",
	}
	completionReason, err := optionalString(raw, "completionReason")
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	// Public rows may derive champion/result from entry 0; full ordered list stays in fingerprint identity only.
	pp := map[string]string{
		"result": standings[0],
	}
	if completionReason != "" {
		pp["status"] = completionReason
	}
	payload["publicPayload"] = pp
	return finishMapped(source, domain.EventTournamentStatistic, meta, meta.EventID, payload, identity, false), nil
}

func mapPlayerRatingUpdated(source domain.SourceTopic, meta eventMetadata, raw map[string]any, key string) (domain.UpstreamEvent, error) {
	playerID, err := requireNonEmptyString(raw, "playerId")
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	if key != playerID {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid,
			fmt.Errorf("kafka key must equal playerId"))
	}
	prev, err := requireIntegralInt(raw["previousRating"], "previousRating")
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	next, err := requireIntegralInt(raw["newRating"], "newRating")
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	gameID, err := optionalString(raw, "gameId")
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	tournamentID, err := optionalString(raw, "tournamentId")
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	placementEventID, err := optionalString(raw, "placementEventId")
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}

	var idem string
	switch {
	case gameID != "":
		idem = fmt.Sprintf("%s|game:%s", playerID, gameID)
	case tournamentID != "" && placementEventID != "":
		idem = fmt.Sprintf("%s|placement:%s|%s", playerID, tournamentID, placementEventID)
	default:
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid,
			fmt.Errorf("rating idempotency requires gameId or tournamentId+placementEventId"))
	}

	identity := map[string]any{
		"playerId":       playerID,
		"previousRating": prev,
		"newRating":      next,
	}
	if gameID != "" {
		identity["gameId"] = gameID
	}
	if tournamentID != "" {
		identity["tournamentId"] = tournamentID
	}
	if placementEventID != "" {
		identity["placementEventId"] = placementEventID
	}

	payload := map[string]any{
		"playerId":       playerID,
		"previousRating": prev,
		"newRating":      next,
		"sourceType":     "player_rating_updated",
	}
	if gameID != "" {
		payload["gameId"] = gameID
	}
	if tournamentID != "" {
		payload["tournamentId"] = tournamentID
	}
	return finishMapped(source, domain.EventRatingStatistic, meta, idem, payload, identity, false), nil
}

func mapLeaderboardSnapshot(source domain.SourceTopic, meta eventMetadata, raw map[string]any, key string) (domain.UpstreamEvent, error) {
	boardType, err := requireNonEmptyString(raw, "boardType")
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	if key != boardType {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid,
			fmt.Errorf("kafka key must equal boardType"))
	}
	snapshotID, err := requireNonEmptyString(raw, "snapshotId")
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}

	entriesRaw, hasEntries := raw["entries"]
	var entries []any
	if !hasEntries || entriesRaw == nil {
		entries = []any{}
	} else {
		arr, ok := entriesRaw.([]any)
		if !ok {
			return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid,
				fmt.Errorf("entries must be an array"))
		}
		entries = make([]any, 0, len(arr))
		for i, item := range arr {
			row, ok := item.(map[string]any)
			if !ok {
				return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid,
					fmt.Errorf("leaderboard entry must be an object at index %d", i))
			}
			if err := rejectForbiddenPrivacyFields(row); err != nil {
				return domain.UpstreamEvent{}, err
			}
			playerID, err := requireNonEmptyString(row, "playerId")
			if err != nil {
				return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
			}
			rating, err := requireIntegralInt(row["rating"], "rating")
			if err != nil {
				return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
			}
			rank, err := requireIntegralInt(row["rank"], "rank")
			if err != nil {
				return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
			}
			if rank < 1 {
				return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid,
					fmt.Errorf("rank must be >= 1"))
			}
			mapped := map[string]any{
				"playerId": playerID,
				"rating":   rating,
				"rank":     rank,
			}
			if err := copyOptionalString(row, mapped, "displayName"); err != nil {
				return domain.UpstreamEvent{}, err
			}
			entries = append(entries, mapped)
		}
	}

	identity := map[string]any{
		"snapshotId": snapshotID,
		"boardType":  boardType,
		"entries":    entries,
	}
	if err := copyOptionalString(raw, identity, "generatedAt"); err != nil {
		return domain.UpstreamEvent{}, err
	}
	payload := map[string]any{
		"snapshotId": snapshotID,
		"boardType":  boardType,
		"sourceType": "leaderboard_snapshot",
		"entries":    entries,
	}
	return finishMapped(source, domain.EventLeaderboardSnapshot, meta, snapshotID, payload, identity, false), nil
}

func finishMapped(source domain.SourceTopic, eventType domain.EventType, meta eventMetadata, idem string, payload, identity map[string]any, ignore bool) domain.UpstreamEvent {
	// Immutable payload fingerprint covers the canonical contract body (identity),
	// not envelope metadata and not privacy-stripped projection fields alone.
	fp := domain.FingerprintPayload(identity)
	return domain.UpstreamEvent{
		EventID:            domain.EventID(meta.EventID),
		EventType:          eventType,
		Source:             source,
		SchemaVersion:      meta.SchemaVersion,
		CorrelationID:      meta.CorrelationID,
		OccurredAt:         meta.OccurredAt,
		Payload:            payload,
		IdempotencyKey:     idem,
		PayloadFingerprint: fp,
		DurableIgnore:      ignore,
	}
}

// rejectForbiddenPrivacyFields fails closed when private/session/audit fields appear.
func rejectForbiddenPrivacyFields(m map[string]any) error {
	forbidden := map[string]struct{}{
		"hand": {}, "hands": {}, "cards": {}, "privatehand": {}, "drawncards": {},
		"drawcards": {}, "cardidentity": {}, "drawncardids": {}, "drawidentity": {},
		"deck": {}, "deckorder": {}, "hiddendeck": {}, "remainingdeck": {},
		"seed": {}, "deckseed": {}, "session": {}, "sessionid": {}, "sessiontoken": {},
		"token": {}, "accesstoken": {}, "refreshtoken": {}, "password": {}, "secret": {},
		"privatepayload": {}, "opponenthands": {}, "opponenthand": {}, "playerhand": {},
		"playeremail": {}, "email": {}, "sessions": {}, "commands": {},
	}
	for k := range m {
		norm := strings.ToLower(strings.ReplaceAll(k, "_", ""))
		if _, bad := forbidden[norm]; bad {
			return newTerminalKafkaError(KafkaFailurePayloadInvalid,
				fmt.Errorf("forbidden private field: %s", k))
		}
	}
	return nil
}

func copyOptionalString(src map[string]any, dst map[string]any, key string) error {
	if _, ok := src[key]; !ok || src[key] == nil {
		return nil
	}
	s, err := optionalString(src, key)
	if err != nil {
		return newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	if s != "" {
		dst[key] = s
	}
	return nil
}

func copyOptionalNumber(src map[string]any, dst map[string]any, key string) error {
	if _, ok := src[key]; !ok || src[key] == nil {
		return nil
	}
	n, err := requireIntegralInt64(src[key], key)
	if err != nil {
		return newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	dst[key] = n
	return nil
}

func copyOptionalStringArray(src map[string]any, dst map[string]any, key string) error {
	if _, ok := src[key]; !ok || src[key] == nil {
		return nil
	}
	arr, err := requireStringArray(src, key)
	if err != nil {
		return newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	out := make([]any, len(arr))
	for i, s := range arr {
		out[i] = s
	}
	dst[key] = out
	return nil
}

func copyAnyMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = deepCopyJSONValue(v)
	}
	return out
}

func deepCopyJSONValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		return copyAnyMap(x)
	case []any:
		out := make([]any, len(x))
		for i, child := range x {
			out[i] = deepCopyJSONValue(child)
		}
		return out
	default:
		return x
	}
}

func requireStringArray(m map[string]any, key string) ([]string, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return nil, fmt.Errorf("%s is required", key)
	}
	arr, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("%s must be an array", key)
	}
	out := make([]string, 0, len(arr))
	for _, item := range arr {
		s, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("%s entries must be strings", key)
		}
		s = strings.TrimSpace(s)
		if s == "" {
			return nil, fmt.Errorf("%s entries must be nonempty strings", key)
		}
		out = append(out, s)
	}
	return out, nil
}

func requireUniqueStringArrayBounded(m map[string]any, key string, minItems, maxItems int) ([]string, error) {
	out, err := requireStringArray(m, key)
	if err != nil {
		return nil, err
	}
	if len(out) < minItems || len(out) > maxItems {
		return nil, fmt.Errorf("%s must contain %d..%d unique entries", key, minItems, maxItems)
	}
	seen := make(map[string]struct{}, len(out))
	for _, s := range out {
		if _, dup := seen[s]; dup {
			return nil, fmt.Errorf("%s entries must be unique", key)
		}
		seen[s] = struct{}{}
	}
	return out, nil
}

func requireObjectArray(m map[string]any, key string) ([]map[string]any, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return nil, fmt.Errorf("%s is required", key)
	}
	arr, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("%s must be an array", key)
	}
	out := make([]map[string]any, 0, len(arr))
	for i, item := range arr {
		row, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s[%d] must be an object", key, i)
		}
		out = append(out, row)
	}
	return out, nil
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

func peekSafeCorrelationID(value []byte) string {
	var peek struct {
		CorrelationID string `json:"correlationId"`
	}
	if err := json.Unmarshal(value, &peek); err != nil {
		return ""
	}
	return strings.TrimSpace(peek.CorrelationID)
}
