package main

import (
	"context"
	"time"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/services/tournament-orchestration/store"
	"unoarena/shared/envelope"
)

// durableRepo adapts store.TournamentStore to TournamentRepository (no ctx on ports).
type durableRepo struct {
	store *store.TournamentStore
}

type durableUoW struct {
	inner *store.TournamentUnitOfWork
}

func (r *durableRepo) BeginExisting(id domain.TournamentID) (TournamentUnitOfWork, error) {
	uow, err := r.store.BeginExisting(context.Background(), id)
	if err != nil {
		return nil, err
	}
	return &durableUoW{inner: uow}, nil
}

func (r *durableRepo) BeginCreate(id domain.TournamentID) (TournamentUnitOfWork, error) {
	uow, err := r.store.BeginCreate(context.Background(), id)
	if err != nil {
		return nil, err
	}
	return &durableUoW{inner: uow}, nil
}

func (u *durableUoW) Loaded() *domain.Tournament { return u.inner.Loaded() }
func (u *durableUoW) Exists() bool               { return u.inner.Exists() }

func (u *durableUoW) LookupOutcome(commandID string) (envelope.Result, bool) {
	return u.inner.LookupOutcome(commandID)
}

func (u *durableUoW) Commit(req CommitRequest) error {
	return u.inner.Commit(toStoreCommit(req))
}

func (u *durableUoW) Rollback() error { return u.inner.Rollback() }

func (r *durableRepo) Get(id domain.TournamentID) (*domain.Tournament, bool) {
	return r.store.Get(context.Background(), id)
}

func (r *durableRepo) Commit(req CommitRequest) error {
	return r.store.Commit(context.Background(), toStoreCommit(req))
}

func toStoreCommit(req CommitRequest) store.CommitRequest {
	events := make([]store.OutboxEvent, 0, len(req.Events))
	for _, e := range req.Events {
		events = append(events, store.OutboxEvent{
			EventID:       e.EventID,
			EventType:     e.EventType,
			TournamentID:  e.TournamentID,
			Topic:         e.Topic,
			PartitionKey:  e.PartitionKey,
			SchemaVersion: e.SchemaVersion,
			Payload:       e.Payload,
			CreatedAt:     e.CreatedAt,
		})
	}
	out := store.CommitRequest{
		Tournament: req.Tournament,
		CommandID:  req.CommandID,
		Outcome:    req.Outcome,
		Events:     events,
	}
	if req.MatchResultSource != nil {
		out.MatchResultSource = &store.MatchResultSource{
			EventID:           req.MatchResultSource.EventID,
			RoomID:            req.MatchResultSource.RoomID,
			CompletionVersion: req.MatchResultSource.CompletionVersion,
		}
	}
	return out
}

func (r *durableRepo) LookupOutcome(commandID string) (envelope.Result, bool) {
	return r.store.LookupOutcome(context.Background(), commandID)
}

func (r *durableRepo) ListPendingOutbox(limit int) ([]OutboxEvent, error) {
	_, err := r.store.ListPendingOutbox(context.Background(), limit)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (r *durableRepo) MarkOutboxPublished(eventID string, at time.Time) error {
	return r.store.MarkOutboxPublished(context.Background(), eventID, at)
}
