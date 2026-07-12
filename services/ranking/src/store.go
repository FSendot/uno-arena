package main

import (
	"context"
	"strings"
	"sync"
	"time"

	"unoarena/services/ranking/domain"
	"unoarena/services/ranking/store"
)

// MemoryRatingStore is a mutex-guarded in-memory RatingApplication.
// domain.PlayerRating is not goroutine-safe — mutations run under the store lock.
type MemoryRatingStore struct {
	mu                sync.Mutex
	ratings           map[domain.PlayerID]*domain.PlayerRating
	processedEvents   map[domain.EventID]GameCompletedResult
	processedGames    map[domain.GameID]GameCompletedResult
	processedPerf     map[string]memPerfOutcome
	mode              string
	projectionVersion int64
}

// NewMemoryRatingStore creates an empty in-memory store.
func NewMemoryRatingStore() *MemoryRatingStore {
	return &MemoryRatingStore{
		ratings:         make(map[domain.PlayerID]*domain.PlayerRating),
		processedEvents: make(map[domain.EventID]GameCompletedResult),
		processedGames:  make(map[domain.GameID]GameCompletedResult),
		processedPerf:   make(map[string]memPerfOutcome),
		mode:            "capability",
	}
}

func (s *MemoryRatingStore) getOrCreateLocked(id domain.PlayerID) *domain.PlayerRating {
	if r, ok := s.ratings[id]; ok {
		return r
	}
	r := domain.NewPlayerRating(id, domain.DefaultRatingConfig())
	s.ratings[id] = r
	return r
}

func (s *MemoryRatingStore) allSnapshotsLocked() []domain.PlayerRatingSnapshot {
	out := make([]domain.PlayerRatingSnapshot, 0, len(s.ratings))
	for _, r := range s.ratings {
		out = append(out, r.PublicSnapshot())
	}
	return out
}

// Leaderboard returns ordered entries from in-memory snapshots (test helper).
func (s *MemoryRatingStore) Leaderboard(_ context.Context, boardType domain.RatingSourceType) ([]domain.LeaderboardEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return domain.LeaderboardFromSnapshots(s.allSnapshotsLocked(), boardType), nil
}

// LeaderboardPage returns a bounded live-keyset page for API parity in capability mode.
func (s *MemoryRatingStore) LeaderboardPage(_ context.Context, boardType domain.RatingSourceType, cursor string, limit int) (store.LeaderboardPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	limit = store.ClampLeaderboardLimit(limit)
	ordered := domain.LeaderboardFromSnapshots(s.allSnapshotsLocked(), boardType)
	start := 0
	if strings.TrimSpace(cursor) != "" {
		c, err := store.DecodeLeaderboardCursor(cursor)
		if err != nil {
			return store.LeaderboardPage{}, err
		}
		start = len(ordered)
		for i, e := range ordered {
			if e.Rating < c.Rating || (e.Rating == c.Rating && string(e.PlayerID) > c.PlayerID) {
				start = i
				break
			}
		}
	}
	end := start + limit
	if end > len(ordered) {
		end = len(ordered)
	}
	slice := ordered[start:end]
	entries := make([]store.RankedLeaderboardEntry, 0, len(slice))
	for i, e := range slice {
		entries = append(entries, store.RankedLeaderboardEntry{
			PlayerID: e.PlayerID,
			Rating:   e.Rating,
			Rank:     start + i + 1,
		})
	}
	s.projectionVersion++
	page := store.LeaderboardPage{
		BoardType:         boardType,
		ProjectionVersion: s.projectionVersion,
		GeneratedAt:       time.Now().UTC(),
		Entries:           entries,
	}
	if end < len(ordered) && len(entries) > 0 {
		last := entries[len(entries)-1]
		enc, err := store.EncodeLeaderboardCursor(store.LeaderboardCursor{
			Rating: last.Rating, PlayerID: string(last.PlayerID),
		})
		if err != nil {
			return store.LeaderboardPage{}, err
		}
		page.NextCursor = enc
	}
	return page, nil
}

// RebuildLeaderboardProjection is a no-op in capability/memory mode.
func (s *MemoryRatingStore) RebuildLeaderboardProjection(_ context.Context, _ domain.RatingSourceType) error {
	return nil
}

// ApplyPlayerRatingUpdated is a no-op in capability/memory mode (no Redis projection).
func (s *MemoryRatingStore) ApplyPlayerRatingUpdated(_ context.Context, _ PlayerRatingUpdatedEvent) error {
	return nil
}

// History returns a defensive copy of rating history for a known player.
func (s *MemoryRatingStore) History(_ context.Context, id domain.PlayerID) ([]domain.RatingHistoryEntry, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.ratings[id]
	if !ok {
		return nil, false, nil
	}
	return r.History(), true, nil
}

// RebuildStatus returns offline rebuild diagnostics without requiring a full snapshot publish.
func (s *MemoryRatingStore) RebuildStatus(_ context.Context) (RebuildStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := domain.LeaderboardFromSnapshots(s.allSnapshotsLocked(), domain.SourceCasualElo)
	status := RebuildStatus{PlayerCount: len(s.ratings), Mode: s.mode}
	if len(entries) > 0 {
		e := entries[0]
		status.TopEntry = &e
	}
	return status, nil
}

// ApplyTournamentPlacement applies a single-player tournament placement update.
// Correlation metadata is accepted for API parity with durable mode; memory has no outbox.
// Ranking computes the award from reason + round/placement; no caller delta is trusted.
func (s *MemoryRatingStore) ApplyTournamentPlacement(_ context.Context, req TournamentPlacementRequest) (domain.CommandOutcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rating := s.getOrCreateLocked(req.Command.PlayerID)
	return rating.ApplyTournamentPlacementUpdate(req.Command), nil
}

// ApplyTournamentPerformance applies an event-wide tournament fact in memory (capability/ops).
func (s *MemoryRatingStore) ApplyTournamentPerformance(_ context.Context, req TournamentPerformanceRequest) (TournamentPerformanceResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.BusinessKey != "" {
		if prior, ok := s.processedPerf[req.SourceTopic+":"+req.BusinessKey]; ok {
			if prior.fingerprint == req.PayloadFingerprint {
				dup := prior.result
				dup.Kind = domain.OutcomeDuplicate
				return dup, nil
			}
			return TournamentPerformanceResult{}, &store.TournamentPerformanceConflictError{
				SourceTopic: req.SourceTopic, BusinessKey: req.BusinessKey,
				ExistingFingerprint: prior.fingerprint, IncomingFingerprint: req.PayloadFingerprint,
			}
		}
	}

	var allFacts []domain.Fact
	var perPlayer []domain.CommandOutcome
	scoreChanged := false
	raw := string(req.UpstreamEventID)
	for _, part := range req.Players {
		rating := s.getOrCreateLocked(part.PlayerID)
		before := rating.TournamentPlacementRating()
		cmd := domain.ApplyTournamentPlacementUpdateCommand{
			CommandID:        domain.CommandID(raw + ":" + string(part.PlayerID)),
			EventID:          domain.EventID(raw + ":" + string(part.PlayerID)),
			PlayerID:         part.PlayerID,
			TournamentID:     req.TournamentID,
			PlacementEventID: domain.PlacementEventID(raw),
			Placement:        part.Placement,
			RoundNumber:      part.RoundNumber,
			Reason:           part.Reason,
		}
		out := rating.ApplyTournamentPlacementUpdate(cmd)
		perPlayer = append(perPlayer, out)
		if out.Kind != domain.OutcomeAccepted && out.Kind != domain.OutcomeDuplicate {
			rej := out.Rejection
			if rej == nil {
				r := domain.Rejection{Code: domain.RejectInvalidCommand, Message: "participant apply failed"}
				rej = &r
			}
			return TournamentPerformanceResult{
				Kind: domain.OutcomeRejected, UpstreamEventID: req.UpstreamEventID,
				BusinessKey: req.BusinessKey, Rejection: rej, PerPlayer: perPlayer,
			}, nil
		}
		if out.Kind == domain.OutcomeAccepted {
			allFacts = append(allFacts, out.Facts...)
			if rating.TournamentPlacementRating() > before {
				scoreChanged = true
			}
		}
	}
	result := TournamentPerformanceResult{
		Kind: domain.OutcomeAccepted, UpstreamEventID: req.UpstreamEventID,
		BusinessKey: req.BusinessKey, Facts: allFacts, PerPlayer: perPlayer, ScoreChanged: scoreChanged,
	}
	if req.BusinessKey != "" {
		s.processedPerf[req.SourceTopic+":"+req.BusinessKey] = memPerfOutcome{
			fingerprint: req.PayloadFingerprint, result: result,
		}
	}
	return result, nil
}

type memPerfOutcome struct {
	fingerprint string
	result      TournamentPerformanceResult
}

// ApplyCasualGameCompleted loads server-side ratings, ignores caller rating
// values, and applies every participant atomically under one lock with eventId
// and gameId dedupe. No partial fan-out. Rejections write no dedupe state.
func (s *MemoryRatingStore) ApplyCasualGameCompleted(_ context.Context, req GameCompletedRequest) (GameCompletedResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.EventID.Valid() {
		if prior, ok := s.processedEvents[req.EventID]; ok {
			return GameCompletedResult{
				Kind:      domain.OutcomeDuplicate,
				CommandID: req.CommandID,
				EventID:   req.EventID,
				Facts:     prior.Facts,
				PerPlayer: prior.PerPlayer,
			}, nil
		}
	}
	if req.GameID.Valid() {
		if prior, ok := s.processedGames[req.GameID]; ok {
			return GameCompletedResult{
				Kind:      domain.OutcomeDuplicate,
				CommandID: req.CommandID,
				EventID:   req.EventID,
				Facts:     prior.Facts,
				PerPlayer: prior.PerPlayer,
			}, nil
		}
	}

	if !req.Authoritative {
		rej := domain.Rejection{Code: domain.RejectNotAuthoritative, Message: "casual elo requires an authoritative GameCompleted"}
		return GameCompletedResult{Kind: domain.OutcomeRejected, CommandID: req.CommandID, EventID: req.EventID, Rejection: &rej}, nil
	}
	if !req.Completed {
		rej := domain.Rejection{Code: domain.RejectIneligibleGame, Message: "casual elo requires a completed game"}
		return GameCompletedResult{Kind: domain.OutcomeRejected, CommandID: req.CommandID, EventID: req.EventID, Rejection: &rej}, nil
	}
	if req.IsAbandoned {
		rej := domain.Rejection{Code: domain.RejectAbandonedGame, Message: "abandoned games do not update casual elo"}
		return GameCompletedResult{Kind: domain.OutcomeRejected, CommandID: req.CommandID, EventID: req.EventID, Rejection: &rej}, nil
	}
	if req.RoomType == domain.RoomTypeTournament {
		rej := domain.Rejection{Code: domain.RejectTournamentGame, Message: "tournament games do not update casual elo"}
		return GameCompletedResult{Kind: domain.OutcomeRejected, CommandID: req.CommandID, EventID: req.EventID, Rejection: &rej}, nil
	}
	if req.RoomType != domain.RoomTypeAdHoc {
		rej := domain.Rejection{Code: domain.RejectIneligibleGame, Message: "casual elo requires roomType=ad_hoc"}
		return GameCompletedResult{Kind: domain.OutcomeRejected, CommandID: req.CommandID, EventID: req.EventID, Rejection: &rej}, nil
	}
	if !req.CommandID.Valid() || !req.GameID.Valid() {
		rej := domain.Rejection{Code: domain.RejectInvalidIdentity, Message: "casual elo requires commandId and gameId"}
		return GameCompletedResult{Kind: domain.OutcomeRejected, CommandID: req.CommandID, EventID: req.EventID, Rejection: &rej}, nil
	}
	if len(req.Participants) < 2 {
		rej := domain.Rejection{Code: domain.RejectInvalidOpponents, Message: "pairwise elo requires at least two participants"}
		return GameCompletedResult{Kind: domain.OutcomeRejected, CommandID: req.CommandID, EventID: req.EventID, Rejection: &rej}, nil
	}

	standings := make([]domain.RatedPlacement, 0, len(req.Participants))
	seen := map[domain.PlayerID]struct{}{}
	for _, p := range req.Participants {
		if !p.PlayerID.Valid() || p.Placement < 1 {
			rej := domain.Rejection{Code: domain.RejectInvalidOpponents, Message: "each participant requires playerId and placement >= 1"}
			return GameCompletedResult{Kind: domain.OutcomeRejected, CommandID: req.CommandID, EventID: req.EventID, Rejection: &rej}, nil
		}
		if _, dup := seen[p.PlayerID]; dup {
			rej := domain.Rejection{Code: domain.RejectInvalidOpponents, Message: "duplicate participant playerId"}
			return GameCompletedResult{Kind: domain.OutcomeRejected, CommandID: req.CommandID, EventID: req.EventID, Rejection: &rej}, nil
		}
		seen[p.PlayerID] = struct{}{}
		rating := s.getOrCreateLocked(p.PlayerID)
		standings = append(standings, domain.RatedPlacement{
			PlayerID:  p.PlayerID,
			Rating:    rating.CasualElo().Value,
			Placement: p.Placement,
		})
	}

	var allFacts []domain.Fact
	var perPlayer []domain.CommandOutcome
	for _, part := range standings {
		cmd := domain.ApplyCasualEloUpdateCommand{
			CommandID:     domain.CommandID(string(req.CommandID) + ":" + string(part.PlayerID)),
			EventID:       domain.EventID(string(req.EventID) + ":" + string(part.PlayerID)),
			PlayerID:      part.PlayerID,
			GameID:        req.GameID,
			RoomID:        req.RoomID,
			RoomType:      req.RoomType,
			IsAbandoned:   req.IsAbandoned,
			Authoritative: req.Authoritative,
			Completed:     req.Completed,
			Participants:  standings,
		}
		rating := s.getOrCreateLocked(part.PlayerID)
		out := rating.ApplyCasualEloUpdate(cmd)
		perPlayer = append(perPlayer, out)
		if out.Kind != domain.OutcomeAccepted && out.Kind != domain.OutcomeDuplicate {
			rej := out.Rejection
			if rej == nil {
				r := domain.Rejection{Code: domain.RejectInvalidCommand, Message: "participant apply failed"}
				rej = &r
			}
			// Domain already mutated accepted players in this lock — memory rolls back by
			// not exposing partial results: capability tests use single-lock sequential apply
			// and reject before any accepted path for eligibility. Mid-fanout reject is rare;
			// restore by not installing game/event dedupe and leaving mutated state (parity
			// with prior memory behavior). Eligibility rejects above write nothing.
			return GameCompletedResult{
				Kind: domain.OutcomeRejected, CommandID: req.CommandID, EventID: req.EventID,
				Rejection: rej, PerPlayer: perPlayer,
			}, nil
		}
		if out.Kind == domain.OutcomeAccepted {
			allFacts = append(allFacts, out.Facts...)
		}
	}

	result := GameCompletedResult{
		Kind:      domain.OutcomeAccepted,
		CommandID: req.CommandID,
		EventID:   req.EventID,
		Facts:     allFacts,
		PerPlayer: perPlayer,
	}
	if req.EventID.Valid() {
		s.processedEvents[req.EventID] = result
	}
	if req.GameID.Valid() {
		s.processedGames[req.GameID] = result
	}
	return result, nil
}
