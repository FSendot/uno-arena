package domain

import (
	"math"
	"testing"
)

func TestComputePairwiseEloDeltas_TwoPlayerDeterministic(t *testing.T) {
	players := []RatedPlacement{
		{PlayerID: "a", Rating: 1000, Placement: 1},
		{PlayerID: "b", Rating: 1000, Placement: 2},
	}
	d1 := ComputePairwiseEloDeltas(players, 32)
	d2 := ComputePairwiseEloDeltas(players, 32)
	if d1["a"] != d2["a"] || d1["b"] != d2["b"] {
		t.Fatalf("deltas not deterministic: %#v vs %#v", d1, d2)
	}
	if d1["a"] <= 0 {
		t.Fatalf("winner should gain rating, got %d", d1["a"])
	}
	if d1["b"] >= 0 {
		t.Fatalf("loser should lose rating, got %d", d1["b"])
	}
}

func TestComputePairwiseEloDeltas_MultiplayerZeroSumSubjectToRounding(t *testing.T) {
	players := []RatedPlacement{
		{PlayerID: "p1", Rating: 1200, Placement: 1},
		{PlayerID: "p2", Rating: 1100, Placement: 2},
		{PlayerID: "p3", Rating: 1000, Placement: 3},
		{PlayerID: "p4", Rating: 900, Placement: 4},
	}
	k := 32
	floats := map[PlayerID]float64{}
	for i := 0; i < len(players); i++ {
		for j := i + 1; j < len(players); j++ {
			a, b := players[i], players[j]
			winner, loser := a, b
			if b.Placement < a.Placement {
				winner, loser = b, a
			}
			ew := expectedScore(winner.Rating, loser.Rating)
			el := 1 - ew
			floats[winner.PlayerID] += float64(k) * (1.0 - ew)
			floats[loser.PlayerID] += float64(k) * (0.0 - el)
		}
	}
	var floatSum float64
	for _, v := range floats {
		floatSum += v
	}
	if math.Abs(floatSum) > 1e-9 {
		t.Fatalf("float deltas not zero-sum: %v", floatSum)
	}

	deltas := ComputePairwiseEloDeltas(players, k)
	sum := 0
	for _, d := range deltas {
		sum += d
	}
	if absInt(sum) > len(players) {
		t.Fatalf("integer delta sum %d exceeds rounding bound", sum)
	}
	if deltas["p1"] <= 0 {
		t.Fatalf("first place should gain, got %d", deltas["p1"])
	}
	if deltas["p4"] >= 0 {
		t.Fatalf("last place should lose, got %d", deltas["p4"])
	}
}

func TestApplyFloor(t *testing.T) {
	if got := ApplyFloor(50, 100); got != 100 {
		t.Fatalf("got %d want 100", got)
	}
	if got := ApplyFloor(150, 100); got != 150 {
		t.Fatalf("got %d want 150", got)
	}
}

func absInt(n int) int {
	if n < 0 {
		return -n
	}
	return n
}
