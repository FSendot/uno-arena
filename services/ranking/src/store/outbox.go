package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"unoarena/services/ranking/domain"
)

// External AsyncAPI message names on ranking.* channels (docs/contracts are source of truth).
const (
	msgPlayerRatingUpdated          = "PlayerRatingUpdated"
	msgLeaderboardSnapshotPublished = "LeaderboardSnapshotPublished"
)

// outboxMeta carries inbound bridge metadata into CDC outbox payloads.
type outboxMeta struct {
	UpstreamEventID string
	CorrelationID   string
	CausationID     string
	Now             time.Time
}

func persistResponses(ctx context.Context, tx pgx.Tx, req GameCompletedRequest, result GameCompletedResult) error {
	body, err := json.Marshal(gameResultDTO(result))
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if req.EventID.Valid() {
		if _, err := tx.Exec(ctx, `
			INSERT INTO ranking_command_responses (dedupe_kind, dedupe_key, response_json, created_at)
			VALUES ('event_id', $1, $2, $3)
			ON CONFLICT DO NOTHING
		`, string(req.EventID), body, now); err != nil {
			return wrapUnavailable(err)
		}
	}
	if req.GameID.Valid() {
		if _, err := tx.Exec(ctx, `
			INSERT INTO ranking_command_responses (dedupe_kind, dedupe_key, response_json, created_at)
			VALUES ('game_id', $1, $2, $3)
			ON CONFLICT DO NOTHING
		`, string(req.GameID), body, now); err != nil {
			return wrapUnavailable(err)
		}
	}
	return nil
}

func loadResponse(ctx context.Context, tx pgx.Tx, kind, key string) (GameCompletedResult, bool, error) {
	var raw []byte
	err := tx.QueryRow(ctx, `
		SELECT response_json FROM ranking_command_responses
		WHERE dedupe_kind = $1 AND dedupe_key = $2
	`, kind, key).Scan(&raw)
	if err == pgx.ErrNoRows {
		return GameCompletedResult{}, false, nil
	}
	if err != nil {
		return GameCompletedResult{}, false, wrapUnavailable(err)
	}
	var dto gameCompletedDTO
	if err := json.Unmarshal(raw, &dto); err != nil {
		return GameCompletedResult{}, false, err
	}
	return dto.toResult(), true, nil
}

func loadOutcomeResponse(ctx context.Context, tx pgx.Tx, kind, key string) (domain.CommandOutcome, bool, error) {
	var raw []byte
	err := tx.QueryRow(ctx, `
		SELECT response_json FROM ranking_command_responses
		WHERE dedupe_kind = $1 AND dedupe_key = $2
	`, kind, key).Scan(&raw)
	if err == pgx.ErrNoRows {
		return domain.CommandOutcome{}, false, nil
	}
	if err != nil {
		return domain.CommandOutcome{}, false, wrapUnavailable(err)
	}
	var dto commandOutcomeDTO
	if err := json.Unmarshal(raw, &dto); err != nil {
		return domain.CommandOutcome{}, false, err
	}
	return dto.toOutcome(), true, nil
}

func insertOutboxEvents(ctx context.Context, tx pgx.Tx, events []OutboxEvent) error {
	for _, e := range events {
		if e.EventID == "" {
			continue
		}
		payload, err := json.Marshal(e.Payload)
		if err != nil {
			return err
		}
		sv := e.SchemaVersion
		if sv <= 0 {
			sv = 1
		}
		created := e.CreatedAt.UTC()
		if created.IsZero() {
			created = time.Now().UTC()
		}
		var player any
		if e.PlayerID != "" {
			player = e.PlayerID
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO outbox_events (
				event_id, event_type, player_id, topic, partition_key, schema_version, payload, created_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT (event_id) DO NOTHING
		`, e.EventID, e.EventType, player, e.Topic, e.PartitionKey, sv, payload, created)
		if err != nil {
			return wrapUnavailable(err)
		}
	}
	return nil
}

func resolveOutboxCorrelationID(corr, eventRoot string) string {
	if v := strings.TrimSpace(corr); v != "" {
		return v
	}
	if v := strings.TrimSpace(eventRoot); v != "" {
		return v
	}
	return "ranking-unspecified"
}

func outboxEventMetadata(eventID, eventType string, meta outboxMeta) map[string]any {
	now := meta.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	payload := map[string]any{
		"eventId":       eventID,
		"eventType":     eventType,
		"schemaVersion": 1,
		"correlationId": resolveOutboxCorrelationID(meta.CorrelationID, eventID),
		"occurredAt":    now.Format(time.RFC3339Nano),
	}
	if c := strings.TrimSpace(meta.CausationID); c != "" {
		payload["causationId"] = c
	}
	return payload
}

func outboxFromFact(f domain.Fact, meta outboxMeta) (OutboxEvent, error) {
	now := meta.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	meta.Now = now

	switch f.Name {
	case domain.FactPlayerRatingUpdated:
		prev, _ := strconv.Atoi(f.Data["previousRating"])
		next, _ := strconv.Atoi(f.Data["newRating"])
		eventID := meta.UpstreamEventID + ":" + f.Data["playerId"] + ":" + msgPlayerRatingUpdated
		if f.Data["eventId"] != "" {
			eventID = f.Data["eventId"] + ":" + msgPlayerRatingUpdated
		}
		payload := outboxEventMetadata(eventID, msgPlayerRatingUpdated, meta)
		payload["playerId"] = f.Data["playerId"]
		payload["previousRating"] = prev
		payload["newRating"] = next
		if g := f.Data["gameId"]; g != "" {
			payload["gameId"] = g
		}
		return OutboxEvent{
			EventID: eventID, EventType: msgPlayerRatingUpdated,
			PlayerID: f.Data["playerId"], Topic: topicPlayerRatingUpdated,
			PartitionKey: f.Data["playerId"], SchemaVersion: 1, Payload: payload, CreatedAt: now,
		}, nil

	case domain.FactTournamentPlacementRatingUpdated:
		// Internal fact name stays TournamentPlacementRatingUpdated; AsyncAPI publishes
		// ranking.player_rating_updated / PlayerRatingUpdated with tournament placement fields.
		prev, _ := strconv.Atoi(f.Data["previousRating"])
		next, _ := strconv.Atoi(f.Data["newRating"])
		eventID := meta.UpstreamEventID + ":" + f.Data["playerId"] + ":" + msgPlayerRatingUpdated
		if f.Data["eventId"] != "" {
			eventID = f.Data["eventId"] + ":" + msgPlayerRatingUpdated
		}
		payload := outboxEventMetadata(eventID, msgPlayerRatingUpdated, meta)
		payload["playerId"] = f.Data["playerId"]
		payload["tournamentId"] = f.Data["tournamentId"]
		payload["placementEventId"] = f.Data["placementEventId"]
		payload["previousRating"] = prev
		payload["newRating"] = next
		return OutboxEvent{
			EventID: eventID, EventType: msgPlayerRatingUpdated,
			PlayerID: f.Data["playerId"], Topic: topicPlayerRatingUpdated,
			PartitionKey: f.Data["playerId"], SchemaVersion: 1, Payload: payload, CreatedAt: now,
		}, nil

	case domain.FactLeaderboardSnapshotPublished:
		eventID := f.Data["snapshotId"] + ":" + msgLeaderboardSnapshotPublished
		payload := outboxEventMetadata(eventID, msgLeaderboardSnapshotPublished, meta)
		payload["snapshotId"] = f.Data["snapshotId"]
		payload["boardType"] = f.Data["boardType"]
		payload["generatedAt"] = now.Format(time.RFC3339Nano)
		return OutboxEvent{
			EventID: eventID, EventType: msgLeaderboardSnapshotPublished,
			Topic: topicLeaderboardSnapshotPublished, PartitionKey: f.Data["boardType"],
			SchemaVersion: 1, Payload: payload, CreatedAt: now,
		}, nil
	default:
		return OutboxEvent{}, fmt.Errorf("unknown fact %s", f.Name)
	}
}

type gameCompletedDTO struct {
	Kind      string              `json:"kind"`
	CommandID string              `json:"commandId"`
	EventID   string              `json:"eventId"`
	Facts     []factDTO           `json:"facts"`
	Rejection *rejectionDTO       `json:"rejection,omitempty"`
	PerPlayer []commandOutcomeDTO `json:"participants,omitempty"`
}

type commandOutcomeDTO struct {
	Kind      string        `json:"kind"`
	CommandID string        `json:"commandId"`
	Facts     []factDTO     `json:"facts"`
	Rejection *rejectionDTO `json:"rejection,omitempty"`
}

type factDTO struct {
	Name string            `json:"name"`
	Data map[string]string `json:"data"`
}

type rejectionDTO struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func gameResultDTO(r GameCompletedResult) gameCompletedDTO {
	dto := gameCompletedDTO{
		Kind:      string(r.Kind),
		CommandID: string(r.CommandID),
		EventID:   string(r.EventID),
		Facts:     factsDTO(r.Facts),
	}
	if r.Rejection != nil {
		dto.Rejection = &rejectionDTO{Code: string(r.Rejection.Code), Message: r.Rejection.Message}
	}
	for _, p := range r.PerPlayer {
		dto.PerPlayer = append(dto.PerPlayer, outcomeDTO(p))
	}
	return dto
}

func outcomeDTO(o domain.CommandOutcome) commandOutcomeDTO {
	dto := commandOutcomeDTO{
		Kind:      string(o.Kind),
		CommandID: string(o.CommandID),
		Facts:     factsDTO(o.Facts),
	}
	if o.Rejection != nil {
		dto.Rejection = &rejectionDTO{Code: string(o.Rejection.Code), Message: o.Rejection.Message}
	}
	return dto
}

func factsDTO(facts []domain.Fact) []factDTO {
	out := make([]factDTO, 0, len(facts))
	for _, f := range facts {
		data := map[string]string{}
		for k, v := range f.Data {
			data[k] = v
		}
		out = append(out, factDTO{Name: string(f.Name), Data: data})
	}
	return out
}

func (d gameCompletedDTO) toResult() GameCompletedResult {
	r := GameCompletedResult{
		Kind:      domain.OutcomeKind(d.Kind),
		CommandID: domain.CommandID(d.CommandID),
		EventID:   domain.EventID(d.EventID),
		Facts:     toFacts(d.Facts),
	}
	if d.Rejection != nil {
		r.Rejection = &domain.Rejection{Code: domain.RejectionCode(d.Rejection.Code), Message: d.Rejection.Message}
	}
	for _, p := range d.PerPlayer {
		r.PerPlayer = append(r.PerPlayer, p.toOutcome())
	}
	return r
}

func (d commandOutcomeDTO) toOutcome() domain.CommandOutcome {
	o := domain.CommandOutcome{
		Kind:      domain.OutcomeKind(d.Kind),
		CommandID: domain.CommandID(d.CommandID),
		Facts:     toFacts(d.Facts),
	}
	if d.Rejection != nil {
		o.Rejection = &domain.Rejection{Code: domain.RejectionCode(d.Rejection.Code), Message: d.Rejection.Message}
	}
	return o
}

func toFacts(in []factDTO) []domain.Fact {
	out := make([]domain.Fact, 0, len(in))
	for _, f := range in {
		data := map[string]string{}
		for k, v := range f.Data {
			data[k] = v
		}
		out = append(out, domain.Fact{Name: domain.FactName(f.Name), Data: data})
	}
	return out
}
