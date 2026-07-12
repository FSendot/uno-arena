package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"unoarena/services/ranking/domain"
)

const eventTypePlayerRatingUpdated = "PlayerRatingUpdated"

// PlayerRatingUpdatedEvent is the Ranking-owned CDC fact for Redis leaderboard maintenance.
// Ranking produces this topic via outbox/Debezium and consumes it for the non-authoritative
// Redis projection (not dual-written inside the Postgres ingest transaction).
type PlayerRatingUpdatedEvent struct {
	EventID           string
	PlayerID          domain.PlayerID
	PreviousRating    int
	NewRating         int
	GameID            string
	TournamentID      string
	PlacementEventID  string
	BoardType         domain.RatingSourceType
	OccurredAt        time.Time
	CorrelationID     string
	ProjectionVersion int64
}

// PlayerRatingUpdatedApplier applies CDC rating facts to the Redis projection.
type PlayerRatingUpdatedApplier interface {
	ApplyPlayerRatingUpdated(ctx context.Context, evt PlayerRatingUpdatedEvent) error
}

// ParsePlayerRatingUpdatedRecord maps a Kafka envelope into a projection update.
// EventMetadata (eventId, eventType, schemaVersion=1, correlationId, occurredAt) is required;
// missing occurredAt is never substituted with wall-clock.
// Score-changing events require a positive projectionVersion (board dirty_version fence).
func ParsePlayerRatingUpdatedRecord(value []byte) (PlayerRatingUpdatedEvent, error) {
	var raw map[string]any
	dec := json.NewDecoder(strings.NewReader(string(value)))
	dec.UseNumber()
	if err := dec.Decode(&raw); err != nil {
		return PlayerRatingUpdatedEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("invalid json: %w", err))
	}
	eventID, err := requireJSONString(raw, "eventId")
	if err != nil {
		return PlayerRatingUpdatedEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, err)
	}
	eventType, err := requireJSONString(raw, "eventType")
	if err != nil {
		return PlayerRatingUpdatedEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, err)
	}
	if eventType != eventTypePlayerRatingUpdated {
		return PlayerRatingUpdatedEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid,
			fmt.Errorf("eventType must be PlayerRatingUpdated"))
	}
	schemaVersion, err := requireJSONInt(raw, "schemaVersion")
	if err != nil {
		return PlayerRatingUpdatedEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid, err)
	}
	if schemaVersion != 1 {
		return PlayerRatingUpdatedEvent{}, newTerminalKafkaError(KafkaFailureSchemaInvalid,
			fmt.Errorf("schemaVersion must be 1"))
	}
	correlationID, err := requireJSONString(raw, "correlationId")
	if err != nil {
		return PlayerRatingUpdatedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	occurredRaw, ok := raw["occurredAt"]
	if !ok || occurredRaw == nil {
		return PlayerRatingUpdatedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid,
			fmt.Errorf("occurredAt is required"))
	}
	occurredStr, ok := occurredRaw.(string)
	if !ok || strings.TrimSpace(occurredStr) == "" {
		return PlayerRatingUpdatedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid,
			fmt.Errorf("occurredAt must be a date-time string"))
	}
	occurredAt, err := time.Parse(time.RFC3339, occurredStr)
	if err != nil {
		return PlayerRatingUpdatedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid,
			fmt.Errorf("invalid occurredAt"))
	}

	playerID, err := requireJSONString(raw, "playerId")
	if err != nil {
		return PlayerRatingUpdatedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	prev, err := requireJSONInt(raw, "previousRating")
	if err != nil {
		return PlayerRatingUpdatedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	next, err := requireJSONInt(raw, "newRating")
	if err != nil {
		return PlayerRatingUpdatedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	var projectionVersion int64
	if prev != next {
		projectionVersion, err = requireJSONInt64(raw, "projectionVersion")
		if err != nil {
			return PlayerRatingUpdatedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
		}
		if projectionVersion <= 0 {
			return PlayerRatingUpdatedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid,
				fmt.Errorf("projectionVersion must be positive for score-changing events"))
		}
	} else if _, ok := raw["projectionVersion"]; ok && raw["projectionVersion"] != nil {
		projectionVersion, err = requireJSONInt64(raw, "projectionVersion")
		if err != nil {
			return PlayerRatingUpdatedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
		}
	}
	gameID := optionalJSONString(raw, "gameId")
	tournamentID := optionalJSONString(raw, "tournamentId")
	placementEventID := optionalJSONString(raw, "placementEventId")

	boardType, err := boardTypeFromPlayerRatingUpdated(gameID, tournamentID, placementEventID)
	if err != nil {
		return PlayerRatingUpdatedEvent{}, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}

	return PlayerRatingUpdatedEvent{
		EventID:           eventID,
		PlayerID:          domain.PlayerID(playerID),
		PreviousRating:    prev,
		NewRating:         next,
		GameID:            gameID,
		TournamentID:      tournamentID,
		PlacementEventID:  placementEventID,
		BoardType:         boardType,
		OccurredAt:        occurredAt.UTC(),
		CorrelationID:     correlationID,
		ProjectionVersion: projectionVersion,
	}, nil
}

func boardTypeFromPlayerRatingUpdated(gameID, tournamentID, placementEventID string) (domain.RatingSourceType, error) {
	hasGame := strings.TrimSpace(gameID) != ""
	hasTour := strings.TrimSpace(tournamentID) != "" && strings.TrimSpace(placementEventID) != ""
	switch {
	case hasGame && !hasTour:
		return domain.SourceCasualElo, nil
	case hasTour && !hasGame:
		return domain.SourceTournamentPlacement, nil
	case hasGame && hasTour:
		return "", fmt.Errorf("rating update must not set both gameId and tournament placement fields")
	default:
		return "", fmt.Errorf("rating update requires gameId or tournamentId+placementEventId")
	}
}

func requireJSONString(m map[string]any, key string) (string, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return "", fmt.Errorf("%s is required", key)
	}
	s, ok := v.(string)
	if !ok || strings.TrimSpace(s) == "" {
		return "", fmt.Errorf("%s must be a non-empty string", key)
	}
	return s, nil
}

func optionalJSONString(m map[string]any, key string) string {
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return ""
	}
	return s
}

func requireJSONInt(m map[string]any, key string) (int, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return 0, fmt.Errorf("%s is required", key)
	}
	switch n := v.(type) {
	case json.Number:
		i, err := n.Int64()
		if err != nil {
			return 0, fmt.Errorf("%s must be an integer", key)
		}
		return int(i), nil
	case float64:
		if n != float64(int(n)) {
			return 0, fmt.Errorf("%s must be an integer", key)
		}
		return int(n), nil
	default:
		return 0, fmt.Errorf("%s must be an integer", key)
	}
}

func requireJSONInt64(m map[string]any, key string) (int64, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return 0, fmt.Errorf("%s is required", key)
	}
	switch n := v.(type) {
	case json.Number:
		i, err := n.Int64()
		if err != nil {
			return 0, fmt.Errorf("%s must be an integer", key)
		}
		return i, nil
	case float64:
		if n != float64(int64(n)) {
			return 0, fmt.Errorf("%s must be an integer", key)
		}
		return int64(n), nil
	case int64:
		return n, nil
	case int:
		return int64(n), nil
	default:
		return 0, fmt.Errorf("%s must be an integer", key)
	}
}
