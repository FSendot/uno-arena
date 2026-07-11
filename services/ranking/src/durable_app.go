package main

import (
	"context"

	"unoarena/services/ranking/domain"
	"unoarena/services/ranking/store"
)

// durableApp adapts store.RankingStore to RatingApplication.
type durableApp struct {
	store *store.RankingStore
	mode  string
}

func newDurableApp(s *store.RankingStore) *durableApp {
	return &durableApp{store: s, mode: "durable"}
}

func (a *durableApp) ApplyCasualGameCompleted(ctx context.Context, req GameCompletedRequest) (GameCompletedResult, error) {
	out, err := a.store.ApplyCasualGameCompleted(ctx, store.GameCompletedRequest{
		CommandID: req.CommandID, EventID: req.EventID, GameID: req.GameID, RoomID: req.RoomID,
		RoomType: req.RoomType, IsAbandoned: req.IsAbandoned, Authoritative: req.Authoritative,
		Completed: req.Completed, Participants: req.Participants,
		CorrelationID: req.CorrelationID, CausationID: req.CausationID,
	})
	if err != nil {
		return GameCompletedResult{}, err
	}
	return GameCompletedResult{
		Kind: out.Kind, CommandID: out.CommandID, EventID: out.EventID,
		Facts: out.Facts, Rejection: out.Rejection, PerPlayer: out.PerPlayer,
	}, nil
}

func (a *durableApp) ApplyTournamentPlacement(ctx context.Context, req TournamentPlacementRequest) (domain.CommandOutcome, error) {
	return a.store.ApplyTournamentPlacement(ctx, store.TournamentPlacementRequest{
		Command: req.Command, CorrelationID: req.CorrelationID, CausationID: req.CausationID,
	})
}

func (a *durableApp) Leaderboard(ctx context.Context, boardType domain.RatingSourceType) ([]domain.LeaderboardEntry, error) {
	return a.store.Leaderboard(ctx, boardType)
}

func (a *durableApp) History(ctx context.Context, playerID domain.PlayerID) ([]domain.RatingHistoryEntry, bool, error) {
	return a.store.History(ctx, playerID)
}

func (a *durableApp) RebuildStatus(ctx context.Context) (RebuildStatus, error) {
	n, top, err := a.store.RebuildStatus(ctx)
	if err != nil {
		return RebuildStatus{}, err
	}
	return RebuildStatus{PlayerCount: n, Mode: a.mode, TopEntry: top}, nil
}
