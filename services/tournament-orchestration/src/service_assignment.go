package main

import (
	"context"
	"errors"
	"strings"
	"time"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/services/tournament-orchestration/store"
)

// TournamentReadAuthorizer authorizes bracket/standings private-read policy.
type TournamentReadAuthorizer interface {
	AuthorizeTournamentRead(ctx context.Context, tournamentID string, principal *store.ReadPrincipal) error
}

// PlayerAssignmentLoader loads bounded assignment + authorizes assignment reads.
type PlayerAssignmentLoader interface {
	AuthorizePlayerAssignment(ctx context.Context, tournamentID, requestedPlayerID string, principal *store.ReadPrincipal) error
	LoadPlayerAssignment(ctx context.Context, tournamentID, playerID string) (store.PlayerAssignmentView, error)
}

// AuthorizeBracketStandings gates public/private bracket and standings reads.
func (s *Service) AuthorizeBracketStandings(tournamentID string, principal *store.ReadPrincipal) error {
	if s.readAuth != nil {
		return s.readAuth.AuthorizeTournamentRead(context.Background(), tournamentID, principal)
	}
	return s.authorizeMemoryRead(tournamentID, principal)
}

func (s *Service) authorizeMemoryRead(tournamentID string, principal *store.ReadPrincipal) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.repo.Get(domain.TournamentID(tournamentID))
	if !ok {
		return store.ErrTournamentNotFound
	}
	if t.Visibility() == domain.TournamentVisibilityPublic {
		return nil
	}
	if principal == nil || (principal.PlayerID == "" && !principal.OperatorScope) {
		return store.ErrUnauthorizedRead
	}
	if principal.OperatorScope {
		return nil
	}
	if !t.IsRegistered(domain.PlayerID(principal.PlayerID)) {
		return store.ErrForbiddenRead
	}
	return nil
}

// PlayerAssignment returns the bounded assignment resource after authorization.
func (s *Service) PlayerAssignment(tournamentID, playerID string, principal *store.ReadPrincipal) (map[string]any, error) {
	tournamentID = strings.TrimSpace(tournamentID)
	playerID = strings.TrimSpace(playerID)
	if tournamentID == "" || playerID == "" {
		return nil, store.ErrAssignmentNotFound
	}
	if s.assignments != nil {
		if err := s.assignments.AuthorizePlayerAssignment(context.Background(), tournamentID, playerID, principal); err != nil {
			return nil, err
		}
		view, err := s.assignments.LoadPlayerAssignment(context.Background(), tournamentID, playerID)
		if err != nil {
			return nil, err
		}
		return playerAssignmentToMap(view), nil
	}
	return s.memoryPlayerAssignment(tournamentID, playerID, principal)
}

func (s *Service) memoryPlayerAssignment(tournamentID, playerID string, principal *store.ReadPrincipal) (map[string]any, error) {
	if principal == nil || (principal.PlayerID == "" && !principal.OperatorScope) {
		return nil, store.ErrUnauthorizedRead
	}
	if !principal.OperatorScope && principal.PlayerID != playerID {
		return nil, store.ErrForbiddenRead
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.repo.Get(domain.TournamentID(tournamentID))
	if !ok {
		return nil, store.ErrAssignmentNotFound
	}
	if !t.IsRegistered(domain.PlayerID(playerID)) {
		return nil, store.ErrAssignmentNotFound
	}
	ver, generatedAt := s.projectionVersion(tournamentID)
	view := store.PlayerAssignmentView{
		TournamentID:       tournamentID,
		PlayerID:           playerID,
		Visibility:         string(t.Visibility()),
		Phase:              string(t.Phase()),
		RegistrationStatus: "registered",
		CurrentRound:       t.CurrentRound(),
		ProjectionVersion:  ver,
	}
	if !generatedAt.IsZero() {
		ga := generatedAt.UTC()
		view.GeneratedAt = &ga
	}
	// Latest round with this player seeded (capability-bounded aggregate only).
	var best *store.PlayerSlotAssignment
	for _, round := range t.RoundsSnapshot() {
		for _, slot := range round.Slots {
			for seat, pid := range slot.SeededPlayers {
				_ = seat
				if string(pid) != playerID {
					continue
				}
				asg := &store.PlayerSlotAssignment{
					RoundNumber: round.Number,
					SlotID:      string(slot.SlotID),
					SlotStatus:  string(slot.Status),
				}
				if slot.RoomID.Valid() {
					asg.RoomID = string(slot.RoomID)
				}
				if best == nil || asg.RoundNumber > best.RoundNumber {
					best = asg
				}
			}
		}
	}
	view.Assignment = best
	return playerAssignmentToMap(view), nil
}

func playerAssignmentToMap(view store.PlayerAssignmentView) map[string]any {
	out := map[string]any{
		"tournamentId":       view.TournamentID,
		"playerId":           view.PlayerID,
		"visibility":         view.Visibility,
		"phase":              view.Phase,
		"registrationStatus": view.RegistrationStatus,
		"currentRound":       view.CurrentRound,
		"assignment":         nil,
	}
	if view.Assignment != nil {
		asg := map[string]any{
			"roundNumber": view.Assignment.RoundNumber,
			"slotId":      view.Assignment.SlotID,
			"slotStatus":  view.Assignment.SlotStatus,
		}
		if view.Assignment.RoomID != "" {
			asg["roomId"] = view.Assignment.RoomID
		}
		out["assignment"] = asg
	}
	if view.ProjectionVersion != 0 {
		out["projectionVersion"] = view.ProjectionVersion
	}
	if view.GeneratedAt != nil {
		out["generatedAt"] = view.GeneratedAt.UTC().Format(time.RFC3339Nano)
	}
	return out
}

// parseTrustedReadPrincipal reads Gateway-forwarded trusted headers only.
func parseTrustedReadPrincipal(playerID, operatorScope string) *store.ReadPrincipal {
	playerID = strings.TrimSpace(playerID)
	op := strings.TrimSpace(operatorScope)
	operator := op == "1" || strings.EqualFold(op, "true")
	if playerID == "" && !operator {
		return nil
	}
	return &store.ReadPrincipal{PlayerID: playerID, OperatorScope: operator}
}

func mapTournamentReadErr(err error) (status int, code, msg string, ok bool) {
	switch {
	case errors.Is(err, store.ErrTournamentNotFound), errors.Is(err, errBracketNotFound), errors.Is(err, store.ErrAssignmentNotFound):
		return 404, "not_found", "tournament not found", true
	case errors.Is(err, store.ErrUnauthorizedRead):
		return 401, "unauthorized", "authentication required", true
	case errors.Is(err, store.ErrForbiddenRead):
		return 403, "forbidden", "forbidden", true
	case errors.Is(err, store.ErrInvalidBracketCursor), errors.Is(err, store.ErrInvalidBracketPageQuery):
		return 400, "bad_request", "invalid cursor or query", true
	case errors.Is(err, store.ErrMalformedStandings):
		return 503, "unavailable", "standings read unavailable", true
	default:
		return 0, "", "", false
	}
}
