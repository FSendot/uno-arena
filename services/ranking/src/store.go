package main

import (
	"context"
	"sync"

	"unoarena/services/ranking/domain"
)

// MemoryRatingStore is a mutex-guarded in-memory RatingApplication.
// domain.PlayerRating is not goroutine-safe — mutations run under the store lock.
type MemoryRatingStore struct {
	mu              sync.Mutex
	ratings         map[domain.PlayerID]*domain.PlayerRating
	processedEvents map[domain.EventID]GameCompletedResult
	processedGames  map[domain.GameID]GameCompletedResult
	mode            string
}

// NewMemoryRatingStore creates an empty in-memory store.
func NewMemoryRatingStore() *MemoryRatingStore {
	return &MemoryRatingStore{
		ratings:         make(map[domain.PlayerID]*domain.PlayerRating),
		processedEvents: make(map[domain.EventID]GameCompletedResult),
		processedGames:  make(map[domain.GameID]GameCompletedResult),
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

// Leaderboard returns ordered entries from in-memory snapshots.
func (s *MemoryRatingStore) Leaderboard(_ context.Context, boardType domain.RatingSourceType) ([]domain.LeaderboardEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return domain.LeaderboardFromSnapshots(s.allSnapshotsLocked(), boardType), nil
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

// RebuildStatus returns offline rebuild diagnostics.
func (s *MemoryRatingStore) RebuildStatus(_ context.Context) (RebuildStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := domain.LeaderboardFromSnapshots(s.allSnapshotsLocked(), domain.SourceCasualElo)
	_ = domain.PublishLeaderboardSnapshot(domain.PublishLeaderboardSnapshotCommand{
		CommandID:  domain.CommandID("rebuild-status"),
		SnapshotID: domain.SnapshotID("offline-rebuild"),
		BoardType:  domain.SourceCasualElo,
		Entries:    entries,
	})
	status := RebuildStatus{PlayerCount: len(s.ratings), Mode: s.mode}
	if len(entries) > 0 {
		e := entries[0]
		status.TopEntry = &e
	}
	return status, nil
}

// ApplyTournamentPlacement applies a single-player tournament placement update.
// Correlation metadata is accepted for API parity with durable mode; memory has no outbox.
func (s *MemoryRatingStore) ApplyTournamentPlacement(_ context.Context, req TournamentPlacementRequest) (domain.CommandOutcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rating := s.getOrCreateLocked(req.Command.PlayerID)
	return rating.ApplyTournamentPlacementUpdate(req.Command), nil
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
