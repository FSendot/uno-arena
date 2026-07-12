package main

import (
	"context"

	"unoarena/services/ranking/domain"
	"unoarena/services/ranking/store"
)

// RatingApplication is the deep application seam for Ranking handlers.
// Durable and capability adapters implement the same transactional contracts.
type RatingApplication interface {
	ApplyCasualGameCompleted(ctx context.Context, req GameCompletedRequest) (GameCompletedResult, error)
	ApplyTournamentPlacement(ctx context.Context, req TournamentPlacementRequest) (domain.CommandOutcome, error)
	ApplyTournamentPerformance(ctx context.Context, req TournamentPerformanceRequest) (TournamentPerformanceResult, error)
	// LeaderboardPage returns a bounded live-keyset page (Redis projection or Postgres/memory fallback).
	LeaderboardPage(ctx context.Context, boardType domain.RatingSourceType, cursor string, limit int) (store.LeaderboardPage, error)
	History(ctx context.Context, playerID domain.PlayerID) ([]domain.RatingHistoryEntry, bool, error)
	RebuildStatus(ctx context.Context) (RebuildStatus, error)
	// RebuildLeaderboardProjection rebuilds the Redis projection from authoritative Postgres (ops).
	// Capability/memory adapters may no-op successfully.
	RebuildLeaderboardProjection(ctx context.Context, boardType domain.RatingSourceType) error
}

// RebuildStatus is the offline/rebuild diagnostic view.
type RebuildStatus struct {
	PlayerCount int
	Mode        string
	TopEntry    *domain.LeaderboardEntry
}

// GameCompletedRequest is one authoritative GameCompleted with all participants.
type GameCompletedRequest struct {
	CommandID     domain.CommandID
	EventID       domain.EventID
	GameID        domain.GameID
	RoomID        domain.RoomID
	RoomType      domain.RoomType
	IsAbandoned   bool
	Authoritative bool
	Completed     bool
	Participants  []domain.RatedPlacement
	CorrelationID string
	CausationID   string
}

// TournamentPlacementRequest wraps the domain placement command with outbound event metadata.
type TournamentPlacementRequest struct {
	Command       domain.ApplyTournamentPlacementUpdateCommand
	CorrelationID string
	CausationID   string
}

// TournamentPlayerPerformance is one affected player in an event-wide tournament apply.
type TournamentPlayerPerformance struct {
	PlayerID    domain.PlayerID
	Placement   int
	RoundNumber int
	Reason      domain.RatingHistoryReason
}

// TournamentPerformanceRequest is the event-wide tournament performance apply contract.
type TournamentPerformanceRequest struct {
	SourceTopic        string
	UpstreamEventID    domain.EventID
	BusinessKey        string
	PayloadFingerprint string
	TournamentID       domain.TournamentID
	CorrelationID      string
	CausationID        string
	Players            []TournamentPlayerPerformance
}

// TournamentPerformanceResult is the atomic multi-player tournament outcome.
type TournamentPerformanceResult struct {
	Kind            domain.OutcomeKind
	UpstreamEventID domain.EventID
	BusinessKey     string
	Facts           []domain.Fact
	Rejection       *domain.Rejection
	PerPlayer       []domain.CommandOutcome
	ScoreChanged    bool
}

// GameCompletedResult is the atomic multi-participant apply outcome.
type GameCompletedResult struct {
	Kind      domain.OutcomeKind
	CommandID domain.CommandID
	EventID   domain.EventID
	Facts     []domain.Fact
	Rejection *domain.Rejection
	PerPlayer []domain.CommandOutcome
}
