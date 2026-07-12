package domain

import "fmt"

// Tournament advancement award (ADR-0037): fixed +10 per PlayersAdvanced listing.
// roundNumber is retained as advancement_depth audit fact and never multiplies the award.
const TournamentAdvancementAward = 10

// TournamentFinalStandingAwards is the top-heavy final-placement bonus for places 1..10.
// Index 0 = first place. Finals with fewer than ten players use only the leading positions.
var TournamentFinalStandingAwards = []int{100, 70, 50, 35, 25, 20, 15, 10, 5, 0}

// ComputeTournamentAward returns the Ranking-owned non-negative award for a tournament
// performance fact. Callers never supply a delta; Ranking computes from reason + depth/placement.
func ComputeTournamentAward(reason RatingHistoryReason, placement, roundNumber int) (award int, err error) {
	switch reason {
	case ReasonTournamentAdvancement:
		if roundNumber < 1 {
			return 0, fmt.Errorf("tournament advancement requires roundNumber >= 1")
		}
		if placement != 0 {
			return 0, fmt.Errorf("tournament advancement must not set placement")
		}
		return TournamentAdvancementAward, nil
	case ReasonTournamentFinalStanding:
		if placement < 1 || placement > len(TournamentFinalStandingAwards) {
			return 0, fmt.Errorf("tournament final standing placement must be 1..%d", len(TournamentFinalStandingAwards))
		}
		if roundNumber != 0 {
			return 0, fmt.Errorf("tournament final standing must not set roundNumber")
		}
		return TournamentFinalStandingAwards[placement-1], nil
	default:
		return 0, fmt.Errorf("unknown tournament award reason %q", reason)
	}
}
