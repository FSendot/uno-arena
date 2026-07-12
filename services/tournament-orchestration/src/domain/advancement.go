package domain

import (
	"errors"
	"sort"
	"strconv"
	"strings"
)

// Sentinel errors for standings validation and non-final advancement.
var (
	ErrInvalidPlayerID       = errors.New("invalid player id")
	ErrDuplicatePlayerID     = errors.New("duplicate player id")
	ErrNegativeMatchWins     = errors.New("negative match wins")
	ErrNegativeCardPoints    = errors.New("negative cumulative card points")
	ErrZeroCompletionTime    = errors.New("zero final-game completion time")
	ErrInsufficientEligible  = errors.New("no eligible players for advancement")
	ErrInvalidFinalStandings = errors.New("final standings must contain 1..10 unique players")
)

// RankStandings orders players for advancement / final placement without mutating
// the input. Order: non-forfeited before forfeited, then match wins descending,
// cumulative card points ascending, final-game completion UTC time ascending,
// then stable PlayerID ascending.
//
// Rejects invalid/duplicate player IDs, negative wins/points, and zero completion times.
func RankStandings(standings []PlayerMatchStanding) ([]PlayerMatchStanding, error) {
	if err := validateStandings(standings); err != nil {
		return nil, err
	}
	out := make([]PlayerMatchStanding, len(standings))
	copy(out, standings)
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Forfeited != b.Forfeited {
			return !a.Forfeited && b.Forfeited
		}
		if a.MatchWins != b.MatchWins {
			return a.MatchWins > b.MatchWins
		}
		if a.CumulativeCardPoints != b.CumulativeCardPoints {
			return a.CumulativeCardPoints < b.CumulativeCardPoints
		}
		aAt, bAt := a.FinalGameCompletedAt.UTC(), b.FinalGameCompletedAt.UTC()
		if !aAt.Equal(bAt) {
			return aAt.Before(bAt)
		}
		return string(a.PlayerID) < string(b.PlayerID)
	})
	return out, nil
}

// TopThree returns one to AdvancersPerMatch non-forfeited players for non-final
// advancement, ordered by RankStandings. Forfeited players never advance.
// One or two are valid when an undersized/forfeit outcome leaves fewer eligible
// players; empty advancement is ErrInsufficientEligible.
func TopThree(standings []PlayerMatchStanding) ([]PlayerID, error) {
	ranked, err := RankStandings(standings)
	if err != nil {
		return nil, err
	}
	eligible := make([]PlayerID, 0, AdvancersPerMatch)
	for _, s := range ranked {
		if s.Forfeited {
			continue
		}
		eligible = append(eligible, s.PlayerID)
		if len(eligible) == AdvancersPerMatch {
			return eligible, nil
		}
	}
	if len(eligible) == 0 {
		return nil, ErrInsufficientEligible
	}
	return eligible, nil
}

// SelectAdvancers returns up to AdvancersPerMatch eligible players for a non-final match.
// Prefer TopThree; this wrapper preserves older call sites and returns nil on error.
func SelectAdvancers(standings []PlayerMatchStanding) []PlayerID {
	ids, err := TopThree(standings)
	if err != nil {
		return nil
	}
	return ids
}

// ChampionFromStandings returns the first-ranked player from a final-room result.
func ChampionFromStandings(standings []PlayerMatchStanding) (PlayerID, bool) {
	ranked, err := RankStandings(standings)
	if err != nil || len(ranked) == 0 {
		return "", false
	}
	return ranked[0].PlayerID, true
}

// ValidateFinalStandings enforces AsyncAPI cardinality: 1..FinalPlayerThreshold unique players.
func ValidateFinalStandings(ids []PlayerID) error {
	if len(ids) < 1 || len(ids) > FinalPlayerThreshold {
		return ErrInvalidFinalStandings
	}
	seen := make(map[PlayerID]struct{}, len(ids))
	for _, id := range ids {
		if !id.Valid() {
			return ErrInvalidFinalStandings
		}
		if _, dup := seen[id]; dup {
			return ErrInvalidFinalStandings
		}
		seen[id] = struct{}{}
	}
	return nil
}

func validateStandings(standings []PlayerMatchStanding) error {
	seen := make(map[PlayerID]struct{}, len(standings))
	for _, s := range standings {
		if !s.PlayerID.Valid() {
			return ErrInvalidPlayerID
		}
		if _, dup := seen[s.PlayerID]; dup {
			return ErrDuplicatePlayerID
		}
		seen[s.PlayerID] = struct{}{}
		if s.MatchWins < 0 {
			return ErrNegativeMatchWins
		}
		if s.CumulativeCardPoints < 0 {
			return ErrNegativeCardPoints
		}
		if s.FinalGameCompletedAt.IsZero() {
			return ErrZeroCompletionTime
		}
	}
	return nil
}

func joinPlayerIDs(ids []PlayerID) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = string(id)
	}
	return strings.Join(parts, ",")
}

func slotIDForIndex(index int) SlotID {
	return SlotID("slot_" + strconv.Itoa(index))
}

func batchIDForIndex(index int) BatchID {
	return BatchID("batch_" + strconv.Itoa(index))
}

func roomIDForSlot(tournamentID TournamentID, roundNumber int, slotID SlotID) RoomID {
	return RoomID("room_" + string(tournamentID) + "_r" + strconv.Itoa(roundNumber) + "_" + string(slotID))
}
