package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/shared/envelope"
)

// OutboxEvent is an append-only tournament fact for Debezium CDC.
type OutboxEvent struct {
	EventID       string
	EventType     string
	TournamentID  string
	Topic         string
	PartitionKey  string
	SchemaVersion int
	Payload       map[string]any
	CreatedAt     time.Time
}

// CommitRequest is one atomic persistence unit for the durable repository.
type CommitRequest struct {
	Tournament        *domain.Tournament
	CommandID         string
	Outcome           envelope.Result
	Events            []OutboxEvent
	MatchResultSource *MatchResultSource
	// ProjectionChanged bumps bracket_projection_versions only for accepted
	// bracket-visible state changes (never rejects or semantic no-ops).
	ProjectionChanged bool
}

// MatchResultSource carries the inbound MatchCompleted event id for the matching room/version.
type MatchResultSource struct {
	EventID           string
	RoomID            string
	CompletionVersion uint64
}

// TournamentStore is the durable Postgres TournamentRepository.
type TournamentStore struct {
	pool *pgxpool.Pool

	// FailNextCommits rolls back the next N Commits (integration fault injection).
	FailNextCommits int
}

// NewTournamentStore constructs a durable tournament aggregate store.
func NewTournamentStore(pool *pgxpool.Pool) *TournamentStore {
	return &TournamentStore{pool: pool}
}

// Get loads and reconstitutes a tournament aggregate.
func (s *TournamentStore) Get(ctx context.Context, id domain.TournamentID) (*domain.Tournament, bool) {
	if s == nil || s.pool == nil || !id.Valid() {
		return nil, false
	}
	t, err := s.loadTournament(ctx, string(id))
	if err != nil || t == nil {
		return nil, false
	}
	return t, true
}

// LookupOutcome returns a prior command outcome from command_idempotency.
func (s *TournamentStore) LookupOutcome(ctx context.Context, commandID string) (envelope.Result, bool) {
	if s == nil || s.pool == nil || commandID == "" {
		return envelope.Result{}, false
	}
	var body []byte
	err := s.pool.QueryRow(ctx, `
		SELECT outcome_body FROM command_idempotency WHERE command_id = $1
	`, commandID).Scan(&body)
	if err != nil {
		return envelope.Result{}, false
	}
	var out envelope.Result
	if err := json.Unmarshal(body, &out); err != nil {
		return envelope.Result{}, false
	}
	return out, true
}

// ListPendingOutbox is capability-only; durable path never polls CDC outbox rows.
func (s *TournamentStore) ListPendingOutbox(context.Context, int) ([]OutboxEvent, error) {
	return nil, ErrCapabilityOnly
}

// MarkOutboxPublished is capability-only; durable path never marks CDC outbox rows.
func (s *TournamentStore) MarkOutboxPublished(context.Context, string, time.Time) error {
	return ErrCapabilityOnly
}

// Commit persists aggregate + command outcome + outbox inserts under one FOR UPDATE lock.
// Prefer BeginExisting/BeginCreate for service mutation paths so hydrate shares the lock.
func (s *TournamentStore) Commit(ctx context.Context, req CommitRequest) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("nil store")
	}
	if req.CommandID == "" {
		return fmt.Errorf("commandId required for commit")
	}

	tid := ""
	if req.Tournament != nil {
		tid = string(req.Tournament.ID())
	}

	var uow *TournamentUnitOfWork
	var err error
	if tid != "" {
		if _, ok := s.Get(ctx, domain.TournamentID(tid)); ok {
			uow, err = s.BeginExisting(ctx, domain.TournamentID(tid))
		} else {
			uow, err = s.BeginCreate(ctx, domain.TournamentID(tid))
		}
		if err != nil {
			return err
		}
		defer func() { _ = uow.Rollback() }()
		return uow.Commit(req)
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return wrapUnavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var existingBody []byte
	err = tx.QueryRow(ctx, `
		SELECT outcome_body FROM command_idempotency WHERE command_id = $1 FOR UPDATE
	`, req.CommandID).Scan(&existingBody)
	if err == nil {
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return wrapUnavailable(err)
	}
	if err := insertCommandOutcome(ctx, tx, req); err != nil {
		return wrapUnavailable(err)
	}
	if err := insertOutboxEvents(ctx, tx, req.Events); err != nil {
		return wrapUnavailable(err)
	}
	if s.FailNextCommits > 0 {
		s.FailNextCommits--
		return fmt.Errorf("injected commit failure")
	}
	if err := tx.Commit(ctx); err != nil {
		return wrapUnavailable(err)
	}
	return nil
}

func insertCommandOutcome(ctx context.Context, tx pgx.Tx, req CommitRequest) error {
	body, err := json.Marshal(req.Outcome)
	if err != nil {
		return err
	}
	status := string(req.Outcome.Status)
	if status == "" {
		status = "accepted"
	}
	cmdType := req.Outcome.Type
	if cmdType == "" {
		cmdType = "unknown"
	}
	var tournamentID any
	if req.Tournament != nil {
		tournamentID = string(req.Tournament.ID())
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO command_idempotency (
			command_id, tournament_id, player_id, command_type, outcome_status, outcome_body
		) VALUES ($1, $2, NULL, $3, $4, $5)
	`, req.CommandID, tournamentID, cmdType, status, body)
	return err
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
		var tid any
		if e.TournamentID != "" {
			tid = e.TournamentID
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO outbox_events (
				event_id, event_type, tournament_id, topic, partition_key, schema_version, payload, created_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT (event_id) DO NOTHING
		`, e.EventID, e.EventType, tid, e.Topic, e.PartitionKey, sv, payload, created)
		if err != nil {
			return err
		}
	}
	return nil
}

type tournamentRules struct {
	RetryBudget  int    `json:"retryBudget"`
	BatchSize    int    `json:"batchSize"`
	CurrentRound int    `json:"currentRound"`
	ChampionID   string `json:"championId,omitempty"`
}

// CountOutbox is a test helper.
func (s *TournamentStore) CountOutbox(ctx context.Context) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&n)
	return n, err
}

// Ping exposes pool health for reconnect tests.
func (s *TournamentStore) Ping(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("nil pool")
	}
	return s.pool.Ping(ctx)
}
