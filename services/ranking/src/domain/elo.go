package domain

import "math"

// ComputePairwiseEloDeltas calculates multiplayer Elo deltas from placement order.
//
// For every ordered pair (i, j), i's score is 1 if i placed better (lower placement
// number), 0 if worse, and 0.5 on a tie. Expected score uses the classic Elo curve
// with scale 400. Each player's total is the average of pairwise K*(S-E) over
// opponents, then rounded with half-away-from-zero.
//
// Before rounding the deltas sum to ~0 (zero-sum). Integer rounding may leave a
// residual bounded by the number of players.
func ComputePairwiseEloDeltas(participants []RatedPlacement, kFactor int) map[PlayerID]int {
	n := len(participants)
	out := make(map[PlayerID]int, n)
	if n == 0 {
		return out
	}
	if kFactor <= 0 {
		kFactor = DefaultKFactor
	}
	if n == 1 {
		out[participants[0].PlayerID] = 0
		return out
	}

	raw := make([]float64, n)
	for i := 0; i < n; i++ {
		var sum float64
		for j := 0; j < n; j++ {
			if i == j {
				continue
			}
			s := pairwiseScore(participants[i].Placement, participants[j].Placement)
			e := expectedScore(participants[i].Rating, participants[j].Rating)
			sum += float64(kFactor) * (s - e)
		}
		raw[i] = sum / float64(n-1)
	}
	for i := 0; i < n; i++ {
		out[participants[i].PlayerID] = roundHalfAwayFromZero(raw[i])
	}
	return out
}

// ApplyFloor clamps rating to at least floor.
func ApplyFloor(rating, floor int) int {
	if rating < floor {
		return floor
	}
	return rating
}

func pairwiseScore(placeI, placeJ int) float64 {
	if placeI < placeJ {
		return 1
	}
	if placeI > placeJ {
		return 0
	}
	return 0.5
}

func expectedScore(ratingI, ratingJ int) float64 {
	return 1.0 / (1.0 + math.Pow(10, float64(ratingJ-ratingI)/float64(EloScale)))
}

func roundHalfAwayFromZero(x float64) int {
	return int(math.Round(x))
}
