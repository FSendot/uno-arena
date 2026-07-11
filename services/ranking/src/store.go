package main

import (
	"sync"

	"unoarena/services/ranking/domain"
)

// RatingStore is the offline persistence seam for PlayerRating aggregates.
type RatingStore interface {
	Get(id domain.PlayerID) (*domain.PlayerRating, bool)
	GetOrCreate(id domain.PlayerID) *domain.PlayerRating
	AllSnapshots() []domain.PlayerRatingSnapshot
}

// MemoryRatingStore is a mutex-guarded in-memory RatingStore.
// domain.PlayerRating is not goroutine-safe — callers must hold the store lock
// across GetOrCreate + Apply + read for a single mutation, or use WithLock /
// ApplyCasualGameCompleted.
type MemoryRatingStore struct {
	mu              sync.Mutex
	ratings         map[domain.PlayerID]*domain.PlayerRating
	processedEvents map[domain.EventID]domain.CommandOutcome
	processedGames  map[domain.GameID]domain.CommandOutcome
}

// NewMemoryRatingStore creates an empty in-memory store.
func NewMemoryRatingStore() *MemoryRatingStore {
	return &MemoryRatingStore{
		ratings:         make(map[domain.PlayerID]*domain.PlayerRating),
		processedEvents: make(map[domain.EventID]domain.CommandOutcome),
		processedGames:  make(map[domain.GameID]domain.CommandOutcome),
	}
}

// WithLock runs fn while holding the store mutex (for handler mutations).
func (s *MemoryRatingStore) WithLock(fn func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn()
}

func (s *MemoryRatingStore) Get(id domain.PlayerID) (*domain.PlayerRating, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.ratings[id]
	return r, ok
}

func (s *MemoryRatingStore) GetOrCreate(id domain.PlayerID) *domain.PlayerRating {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getOrCreateLocked(id)
}

func (s *MemoryRatingStore) getOrCreateLocked(id domain.PlayerID) *domain.PlayerRating {
	if r, ok := s.ratings[id]; ok {
		return r
	}
	r := domain.NewPlayerRating(id, domain.DefaultRatingConfig())
	s.ratings[id] = r
	return r
}

func (s *MemoryRatingStore) AllSnapshots() []domain.PlayerRatingSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.allSnapshotsLocked()
}

// History returns a defensive copy of rating history for a known player.
func (s *MemoryRatingStore) History(id domain.PlayerID) ([]domain.RatingHistoryEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.ratings[id]
	if !ok {
		return nil, false
	}
	return r.History(), true
}

func (s *MemoryRatingStore) allSnapshotsLocked() []domain.PlayerRatingSnapshot {
	out := make([]domain.PlayerRatingSnapshot, 0, len(s.ratings))
	for _, r := range s.ratings {
		out = append(out, r.PublicSnapshot())
	}
	return out
}

func (s *MemoryRatingStore) playerCountLocked() int {
	return len(s.ratings)
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
	Participants  []domain.RatedPlacement // Placement only; Rating ignored
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

// ApplyCasualGameCompleted loads server-side ratings, ignores caller rating
// values, and applies every participant atomically under one lock with eventId
// and gameId dedupe. No partial fan-out.
func (s *MemoryRatingStore) ApplyCasualGameCompleted(req GameCompletedRequest) GameCompletedResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.EventID.Valid() {
		if prior, ok := s.processedEvents[req.EventID]; ok {
			return GameCompletedResult{
				Kind:      domain.OutcomeDuplicate,
				CommandID: req.CommandID,
				EventID:   req.EventID,
				Facts:     prior.Facts,
			}
		}
	}
	if req.GameID.Valid() {
		if prior, ok := s.processedGames[req.GameID]; ok {
			return GameCompletedResult{
				Kind:      domain.OutcomeDuplicate,
				CommandID: req.CommandID,
				EventID:   req.EventID,
				Facts:     prior.Facts,
			}
		}
	}

	// Eligibility once for the whole game before any mutation — no partial fan-out.
	if !req.Authoritative {
		rej := domain.Rejection{Code: domain.RejectNotAuthoritative, Message: "casual elo requires an authoritative GameCompleted"}
		return GameCompletedResult{Kind: domain.OutcomeRejected, CommandID: req.CommandID, EventID: req.EventID, Rejection: &rej}
	}
	if !req.Completed {
		rej := domain.Rejection{Code: domain.RejectIneligibleGame, Message: "casual elo requires a completed game"}
		return GameCompletedResult{Kind: domain.OutcomeRejected, CommandID: req.CommandID, EventID: req.EventID, Rejection: &rej}
	}
	if req.IsAbandoned {
		rej := domain.Rejection{Code: domain.RejectAbandonedGame, Message: "abandoned games do not update casual elo"}
		return GameCompletedResult{Kind: domain.OutcomeRejected, CommandID: req.CommandID, EventID: req.EventID, Rejection: &rej}
	}
	if req.RoomType == domain.RoomTypeTournament {
		rej := domain.Rejection{Code: domain.RejectTournamentGame, Message: "tournament games do not update casual elo"}
		return GameCompletedResult{Kind: domain.OutcomeRejected, CommandID: req.CommandID, EventID: req.EventID, Rejection: &rej}
	}
	if req.RoomType != domain.RoomTypeAdHoc {
		rej := domain.Rejection{Code: domain.RejectIneligibleGame, Message: "casual elo requires roomType=ad_hoc"}
		return GameCompletedResult{Kind: domain.OutcomeRejected, CommandID: req.CommandID, EventID: req.EventID, Rejection: &rej}
	}
	if !req.CommandID.Valid() || !req.GameID.Valid() {
		rej := domain.Rejection{Code: domain.RejectInvalidIdentity, Message: "casual elo requires commandId and gameId"}
		return GameCompletedResult{Kind: domain.OutcomeRejected, CommandID: req.CommandID, EventID: req.EventID, Rejection: &rej}
	}
	if len(req.Participants) < 2 {
		rej := domain.Rejection{Code: domain.RejectInvalidOpponents, Message: "pairwise elo requires at least two participants"}
		return GameCompletedResult{Kind: domain.OutcomeRejected, CommandID: req.CommandID, EventID: req.EventID, Rejection: &rej}
	}

	// Snapshot current ratings server-side; ignore caller Rating fields.
	standings := make([]domain.RatedPlacement, 0, len(req.Participants))
	seen := map[domain.PlayerID]struct{}{}
	for _, p := range req.Participants {
		if !p.PlayerID.Valid() || p.Placement < 1 {
			rej := domain.Rejection{Code: domain.RejectInvalidOpponents, Message: "each participant requires playerId and placement >= 1"}
			return GameCompletedResult{
				Kind: domain.OutcomeRejected, CommandID: req.CommandID, EventID: req.EventID, Rejection: &rej,
			}
		}
		if _, dup := seen[p.PlayerID]; dup {
			rej := domain.Rejection{Code: domain.RejectInvalidOpponents, Message: "duplicate participant playerId"}
			return GameCompletedResult{
				Kind: domain.OutcomeRejected, CommandID: req.CommandID, EventID: req.EventID, Rejection: &rej,
			}
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
			return GameCompletedResult{
				Kind: domain.OutcomeRejected, CommandID: req.CommandID, EventID: req.EventID,
				Rejection: rej, PerPlayer: perPlayer,
			}
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
		s.processedEvents[req.EventID] = domain.CommandOutcome{
			Kind: domain.OutcomeAccepted, CommandID: req.CommandID, Facts: allFacts,
		}
	}
	if req.GameID.Valid() {
		s.processedGames[req.GameID] = domain.CommandOutcome{
			Kind: domain.OutcomeAccepted, CommandID: req.CommandID, Facts: allFacts,
		}
	}
	return result
}
