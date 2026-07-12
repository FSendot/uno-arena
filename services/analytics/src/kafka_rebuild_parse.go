package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"unoarena/services/analytics/domain"
)

const (
	DefaultProjectionRebuildTopic    = "analytics.projection.rebuild_requested"
	DefaultProjectionRebuildGroup    = "analytics-projection-rebuilder"
	DefaultProjectionRebuildDLQTopic = "analytics.projection.rebuild_requested.analytics.dlq"
	EventTypeProjectionRebuildReq    = "AnalyticsProjectionRebuildRequested"
)

// ParsedAnalyticsProjectionRebuildRequest is the strict AsyncAPI rebuild-request envelope.
type ParsedAnalyticsProjectionRebuildRequest struct {
	EventID             string
	EventType           string
	SchemaVersion       int
	CorrelationID       string
	CausationID         string
	OccurredAt          time.Time
	RecoveryJobID       string
	SourceContext       string
	ExpectedSourceTopic string
	FromCheckpoint      string
	ToCheckpoint        string
	FromOccurredAt      time.Time
	ToOccurredAt        time.Time
	HasCheckpointRange  bool
	HasOccurredRange    bool
	PageCursor          string
	Attempt             int // optional; 0 means absent
}

var analyticsRebuildContextTopics = map[string]map[string]struct{}{
	"room": {
		"room.gameplay.metrics": {},
		"room.match.completed":  {},
	},
	"tournament": {
		"tournament.match.assigned":        {},
		"tournament.match.result_recorded": {},
		"tournament.players.advanced":      {},
		"tournament.round.completed":       {},
		"tournament.completed":             {},
	},
	"ranking": {
		"ranking.player_rating_updated":          {},
		"ranking.leaderboard_snapshot_published": {},
	},
}

// ParseAnalyticsProjectionRebuildRequested maps rebuild-request Kafka JSON strictly.
// NEVER accepts embedded event arrays.
func ParseAnalyticsProjectionRebuildRequested(value []byte) (ParsedAnalyticsProjectionRebuildRequest, error) {
	raw, err := decodeAnalyticsEnvelopeJSON(value)
	if err != nil {
		return ParsedAnalyticsProjectionRebuildRequest{}, err
	}

	for _, forbidden := range []string{"events", "heldEvents", "records", "payloads"} {
		if v, ok := raw[forbidden]; ok && v != nil {
			return ParsedAnalyticsProjectionRebuildRequest{},
				newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("%s must not be present on rebuild requests", forbidden))
		}
	}

	meta, err := parseEventMetadata(raw)
	if err != nil {
		return ParsedAnalyticsProjectionRebuildRequest{}, err
	}
	if meta.EventType != EventTypeProjectionRebuildReq {
		return ParsedAnalyticsProjectionRebuildRequest{},
			newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("eventType must be %s", EventTypeProjectionRebuildReq))
	}

	recoveryJobID, err := requireNonEmptyString(raw, "recoveryJobId")
	if err != nil {
		return ParsedAnalyticsProjectionRebuildRequest{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	sourceContext, err := requireNonEmptyString(raw, "sourceContext")
	if err != nil {
		return ParsedAnalyticsProjectionRebuildRequest{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	expectedSource, err := requireNonEmptyString(raw, "expectedSourceTopic")
	if err != nil {
		return ParsedAnalyticsProjectionRebuildRequest{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	if err := validateAnalyticsRebuildContextTopic(sourceContext, expectedSource); err != nil {
		return ParsedAnalyticsProjectionRebuildRequest{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}

	parsed := ParsedAnalyticsProjectionRebuildRequest{
		EventID:             meta.EventID,
		EventType:           meta.EventType,
		SchemaVersion:       meta.SchemaVersion,
		CorrelationID:       meta.CorrelationID,
		OccurredAt:          meta.OccurredAt,
		RecoveryJobID:       recoveryJobID,
		SourceContext:       sourceContext,
		ExpectedSourceTopic: expectedSource,
	}

	fromCP, hasFromCP, err := optionalNonEmptyString(raw, "fromCheckpoint")
	if err != nil {
		return ParsedAnalyticsProjectionRebuildRequest{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	toCP, hasToCP, err := optionalNonEmptyString(raw, "toCheckpoint")
	if err != nil {
		return ParsedAnalyticsProjectionRebuildRequest{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	if hasFromCP != hasToCP {
		return ParsedAnalyticsProjectionRebuildRequest{},
			newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("fromCheckpoint/toCheckpoint must be paired"))
	}
	if hasFromCP {
		parsed.FromCheckpoint = fromCP
		parsed.ToCheckpoint = toCP
		parsed.HasCheckpointRange = true
	}

	fromOA, hasFromOA, err := optionalOccurredAt(raw, "fromOccurredAt")
	if err != nil {
		return ParsedAnalyticsProjectionRebuildRequest{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	toOA, hasToOA, err := optionalOccurredAt(raw, "toOccurredAt")
	if err != nil {
		return ParsedAnalyticsProjectionRebuildRequest{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	if hasFromOA != hasToOA {
		return ParsedAnalyticsProjectionRebuildRequest{},
			newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("fromOccurredAt/toOccurredAt must be paired"))
	}
	if hasFromOA {
		if toOA.Before(fromOA) {
			return ParsedAnalyticsProjectionRebuildRequest{},
				newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("toOccurredAt must be >= fromOccurredAt"))
		}
		parsed.FromOccurredAt = fromOA
		parsed.ToOccurredAt = toOA
		parsed.HasOccurredRange = true
	}
	if !parsed.HasCheckpointRange && !parsed.HasOccurredRange {
		return ParsedAnalyticsProjectionRebuildRequest{},
			newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("bounded paired range required (checkpoint and/or occurredAt)"))
	}

	if _, ok := raw["pageCursor"]; ok && raw["pageCursor"] != nil {
		cur, err := optionalString(raw, "pageCursor")
		if err != nil {
			return ParsedAnalyticsProjectionRebuildRequest{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
		}
		parsed.PageCursor = cur
	}
	if _, ok := raw["causationId"]; ok {
		causationID, err := optionalString(raw, "causationId")
		if err != nil {
			return ParsedAnalyticsProjectionRebuildRequest{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
		}
		parsed.CausationID = causationID
	}
	if _, ok := raw["attempt"]; ok && raw["attempt"] != nil {
		attempt, err := requireIntegralInt(raw["attempt"], "attempt")
		if err != nil {
			return ParsedAnalyticsProjectionRebuildRequest{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
		}
		if attempt < 1 {
			return ParsedAnalyticsProjectionRebuildRequest{},
				newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("attempt must be >= 1"))
		}
		parsed.Attempt = attempt
	}
	return parsed, nil
}

func validateAnalyticsRebuildContextTopic(ctx, topic string) error {
	allow, ok := analyticsRebuildContextTopics[ctx]
	if !ok {
		return fmt.Errorf("sourceContext must be room|tournament|ranking")
	}
	if _, ok := allow[topic]; !ok {
		return fmt.Errorf("expectedSourceTopic %q is not valid for sourceContext %q", topic, ctx)
	}
	return nil
}

// IdempotencyKey returns durable (recoveryJobId, sourceTopic, pageCursor).
func (p ParsedAnalyticsProjectionRebuildRequest) IdempotencyKey() string {
	return p.RecoveryJobID + "|" + p.ExpectedSourceTopic + "|" + p.PageCursor
}

func optionalNonEmptyString(raw map[string]any, field string) (string, bool, error) {
	if _, ok := raw[field]; !ok || raw[field] == nil {
		return "", false, nil
	}
	s, err := requireNonEmptyString(raw, field)
	if err != nil {
		return "", false, err
	}
	return s, true, nil
}

func optionalOccurredAt(raw map[string]any, field string) (time.Time, bool, error) {
	if _, ok := raw[field]; !ok || raw[field] == nil {
		return time.Time{}, false, nil
	}
	s, ok := raw[field].(string)
	if !ok || strings.TrimSpace(s) == "" {
		return time.Time{}, false, fmt.Errorf("%s must be a date-time string", field)
	}
	t, err := parseKafkaTime(s)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("invalid %s", field)
	}
	return t, true, nil
}

// ParseAnalyticsBackfillRecord reuses ParseAnalyticsRecord with expected topic and derived key.
func ParseAnalyticsBackfillRecord(expectedTopic string, value []byte) (domain.UpstreamEvent, error) {
	expectedTopic = strings.TrimSpace(expectedTopic)
	if !domain.SourceTopic(expectedTopic).Valid() {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid,
			fmt.Errorf("unsupported analytics source topic %q", expectedTopic))
	}
	raw, err := decodeAnalyticsEnvelopeJSON(value)
	if err != nil {
		return domain.UpstreamEvent{}, err
	}
	key, err := partitionKeyFromAnalyticsPayload(domain.SourceTopic(expectedTopic), raw)
	if err != nil {
		return domain.UpstreamEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	return ParseAnalyticsRecord(ConsumerRecord{
		Topic: expectedTopic,
		Key:   []byte(key),
		Value: value,
	})
}

func partitionKeyFromAnalyticsPayload(source domain.SourceTopic, raw map[string]any) (string, error) {
	switch source {
	case domain.SourceRoomGameplayMetrics, domain.SourceRoomMatchCompleted:
		return requireNonEmptyString(raw, "roomId")
	case domain.SourceTournamentMatchAssigned, domain.SourceTournamentMatchResultRecorded,
		domain.SourceTournamentPlayersAdvanced, domain.SourceTournamentRoundCompleted,
		domain.SourceTournamentCompleted:
		return requireNonEmptyString(raw, "tournamentId")
	case domain.SourceRankingPlayerRatingUpdated:
		return requireNonEmptyString(raw, "playerId")
	case domain.SourceRankingLeaderboardSnapshot:
		return requireNonEmptyString(raw, "boardType")
	default:
		return "", fmt.Errorf("unsupported topic %s", source)
	}
}

// EncodeAnalyticsProjectionRebuildRequested marshals a follow-up control envelope.
func EncodeAnalyticsProjectionRebuildRequested(req ParsedAnalyticsProjectionRebuildRequest) ([]byte, error) {
	body := map[string]any{
		"eventId":             req.EventID,
		"eventType":           EventTypeProjectionRebuildReq,
		"schemaVersion":       1,
		"correlationId":       req.CorrelationID,
		"occurredAt":          req.OccurredAt.UTC().Format(time.RFC3339Nano),
		"recoveryJobId":       req.RecoveryJobID,
		"sourceContext":       req.SourceContext,
		"expectedSourceTopic": req.ExpectedSourceTopic,
	}
	if req.CausationID != "" {
		body["causationId"] = req.CausationID
	}
	if req.HasCheckpointRange {
		body["fromCheckpoint"] = req.FromCheckpoint
		body["toCheckpoint"] = req.ToCheckpoint
	}
	if req.HasOccurredRange {
		body["fromOccurredAt"] = req.FromOccurredAt.UTC().Format(time.RFC3339Nano)
		body["toOccurredAt"] = req.ToOccurredAt.UTC().Format(time.RFC3339Nano)
	}
	if req.PageCursor != "" {
		body["pageCursor"] = req.PageCursor
	}
	if req.Attempt > 0 {
		body["attempt"] = req.Attempt
	}
	return json.Marshal(body)
}
