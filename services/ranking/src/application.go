package main

import (
	"context"

	"unoarena/services/ranking/domain"
)

// RatingApplication is the deep application seam for Ranking handlers.
// Durable and capability adapters implement the same transactional contracts.
type RatingApplication interface {
	ApplyCasualGameCompleted(ctx context.Context, req GameCompletedRequest) (GameCompletedResult, error)
	ApplyTournamentPlacement(ctx context.Context, req TournamentPlacementRequest) (domain.CommandOutcome, error)
	Leaderboard(ctx context.Context, boardType domain.RatingSourceType) ([]domain.LeaderboardEntry, error)
	History(ctx context.Context, playerID domain.PlayerID) ([]domain.RatingHistoryEntry, bool, error)
	RebuildStatus(ctx context.Context) (RebuildStatus, error)
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

// GameCompletedResult is the atomic multi-participant apply outcome.
type GameCompletedResult struct {
	Kind      domain.OutcomeKind
	CommandID domain.CommandID
	EventID   domain.EventID
	Facts     []domain.Fact
	Rejection *domain.Rejection
	PerPlayer []domain.CommandOutcome
}
