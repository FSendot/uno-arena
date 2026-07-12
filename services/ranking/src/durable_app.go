package main

import (
	"context"
	"errors"
	"log"

	"unoarena/services/ranking/domain"
	"unoarena/services/ranking/store"
)

// durableApp adapts store.RankingStore (+ optional Redis projection) to RatingApplication.
type durableApp struct {
	store *store.RankingStore
	redis *store.RedisLeaderboardStore
	mode  string
}

func newDurableApp(s *store.RankingStore) *durableApp {
	return &durableApp{store: s, mode: "durable"}
}

func (a *durableApp) withRedis(r *store.RedisLeaderboardStore) *durableApp {
	a.redis = r
	return a
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

func (a *durableApp) ApplyTournamentPerformance(ctx context.Context, req TournamentPerformanceRequest) (TournamentPerformanceResult, error) {
	players := make([]store.TournamentPlayerPerformance, 0, len(req.Players))
	for _, p := range req.Players {
		players = append(players, store.TournamentPlayerPerformance{
			PlayerID: p.PlayerID, Placement: p.Placement, RoundNumber: p.RoundNumber, Reason: p.Reason,
		})
	}
	out, err := a.store.ApplyTournamentPerformance(ctx, store.TournamentPerformanceRequest{
		SourceTopic: req.SourceTopic, UpstreamEventID: req.UpstreamEventID,
		BusinessKey: req.BusinessKey, PayloadFingerprint: req.PayloadFingerprint,
		TournamentID: req.TournamentID, CorrelationID: req.CorrelationID, CausationID: req.CausationID,
		Players: players,
	})
	if err != nil {
		return TournamentPerformanceResult{}, err
	}
	return TournamentPerformanceResult{
		Kind: out.Kind, UpstreamEventID: out.UpstreamEventID, BusinessKey: out.BusinessKey,
		Facts: out.Facts, Rejection: out.Rejection, PerPlayer: out.PerPlayer, ScoreChanged: out.ScoreChanged,
	}, nil
}

func (a *durableApp) LeaderboardPage(ctx context.Context, boardType domain.RatingSourceType, cursor string, limit int) (store.LeaderboardPage, error) {
	q := store.LeaderboardPageQuery{BoardType: boardType, Cursor: cursor, Limit: limit}
	if a.redis != nil {
		page, err := a.redis.Page(ctx, q)
		if err == nil {
			return page, nil
		}
		// Architecture: authoritative HTTP leaderboard fallback reads are served from Postgres.
		log.Printf(`{"level":"warn","service":"ranking","event":"leaderboard_redis_fallback","err":%q}`, sanitizeLogErr(err))
	}
	return a.store.LeaderboardPage(ctx, q)
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

func (a *durableApp) RebuildLeaderboardProjection(ctx context.Context, boardType domain.RatingSourceType) error {
	if a.redis == nil {
		return errors.New("redis leaderboard projection not configured")
	}
	st, err := a.store.GetPublicationState(ctx, boardType)
	if err != nil {
		return err
	}
	return a.redis.RebuildFromPostgres(ctx, boardType, a.store, store.DefaultLeaderboardRebuildBatch, st.DirtyVersion)
}

// ApplyPlayerRatingUpdated applies a CDC PlayerRatingUpdated fact to the Redis projection.
func (a *durableApp) ApplyPlayerRatingUpdated(ctx context.Context, evt PlayerRatingUpdatedEvent) error {
	if a.redis == nil {
		return errors.New("redis leaderboard projection not configured")
	}
	err := a.redis.UpsertPlayer(ctx, evt.BoardType, evt.PlayerID, evt.PreviousRating, evt.NewRating, evt.OccurredAt, evt.ProjectionVersion)
	if errors.Is(err, store.ErrLeaderboardProjectionConflict) {
		return newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	return err
}
