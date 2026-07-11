package domain

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"
)

func baseCompletion(t *testing.T) time.Time {
	t.Helper()
	return time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
}

func advStanding(id PlayerID, wins, points int, at time.Time, forfeited bool) PlayerMatchStanding {
	return PlayerMatchStanding{
		PlayerID:             id,
		MatchWins:            wins,
		CumulativeCardPoints: points,
		FinalGameCompletedAt: at,
		Forfeited:            forfeited,
	}
}

func idsOf(standings []PlayerMatchStanding) []PlayerID {
	out := make([]PlayerID, len(standings))
	for i := range standings {
		out[i] = standings[i].PlayerID
	}
	return out
}

func TestRankStandings_Table(t *testing.T) {
	base := baseCompletion(t)
	tests := []struct {
		name    string
		in      []PlayerMatchStanding
		wantIDs []PlayerID
		wantErr error
	}{
		{
			name: "match_wins_descending",
			in: []PlayerMatchStanding{
				advStanding("c", 0, 0, base, false),
				advStanding("a", 2, 50, base, false),
				advStanding("b", 1, 0, base, false),
			},
			wantIDs: []PlayerID{"a", "b", "c"},
		},
		{
			name: "card_points_ascending_on_equal_wins",
			in: []PlayerMatchStanding{
				advStanding("x", 1, 40, base, false),
				advStanding("y", 1, 10, base, false),
				advStanding("z", 1, 20, base, false),
			},
			wantIDs: []PlayerID{"y", "z", "x"},
		},
		{
			name: "completion_time_ascending_utc",
			in: []PlayerMatchStanding{
				advStanding("late", 1, 5, base.Add(2*time.Minute), false),
				advStanding("early", 1, 5, base, false),
				advStanding("mid", 1, 5, base.Add(time.Minute), false),
			},
			wantIDs: []PlayerID{"early", "mid", "late"},
		},
		{
			name: "completion_time_compares_as_utc",
			in: []PlayerMatchStanding{
				advStanding("later_local", 1, 5, time.Date(2026, 7, 10, 14, 0, 0, 0, time.FixedZone("plus1", 3600)), false), // 13:00 UTC
				advStanding("earlier_utc", 1, 5, time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC), false),
			},
			wantIDs: []PlayerID{"earlier_utc", "later_local"},
		},
		{
			name: "stable_player_id_fallback",
			in: []PlayerMatchStanding{
				advStanding("p3", 1, 5, base, false),
				advStanding("p1", 1, 5, base, false),
				advStanding("p2", 1, 5, base, false),
			},
			wantIDs: []PlayerID{"p1", "p2", "p3"},
		},
		{
			name: "combined_priority",
			in: []PlayerMatchStanding{
				advStanding("d", 2, 30, base.Add(time.Hour), false),
				advStanding("a", 2, 10, base.Add(time.Hour), false),
				advStanding("b", 2, 10, base, false),
				advStanding("c", 1, 0, base, false),
			},
			wantIDs: []PlayerID{"b", "a", "d", "c"},
		},
		{
			name: "forfeited_rank_after_non_forfeited_even_with_better_stats",
			in: []PlayerMatchStanding{
				advStanding("forfeit_leader", 2, 0, base, true),
				advStanding("active_low", 0, 99, base.Add(time.Hour), false),
				advStanding("active_mid", 1, 50, base, false),
			},
			wantIDs: []PlayerID{"active_mid", "active_low", "forfeit_leader"},
		},
		{
			name: "forfeited_tie_break_among_themselves",
			in: []PlayerMatchStanding{
				advStanding("f_late", 1, 5, base.Add(time.Minute), true),
				advStanding("f_early", 1, 5, base, true),
				advStanding("ok", 0, 100, base, false),
			},
			wantIDs: []PlayerID{"ok", "f_early", "f_late"},
		},
		{
			name:    "empty_input",
			in:      nil,
			wantIDs: nil,
		},
		{
			name: "reject_invalid_player_id",
			in: []PlayerMatchStanding{
				advStanding("", 1, 0, base, false),
				advStanding("a", 0, 0, base, false),
			},
			wantErr: ErrInvalidPlayerID,
		},
		{
			name: "reject_duplicate_player_id",
			in: []PlayerMatchStanding{
				advStanding("a", 2, 0, base, false),
				advStanding("b", 1, 0, base, false),
				advStanding("a", 0, 0, base, false),
			},
			wantErr: ErrDuplicatePlayerID,
		},
		{
			name: "reject_negative_match_wins",
			in: []PlayerMatchStanding{
				advStanding("a", -1, 0, base, false),
			},
			wantErr: ErrNegativeMatchWins,
		},
		{
			name: "reject_negative_card_points",
			in: []PlayerMatchStanding{
				advStanding("a", 0, -1, base, false),
			},
			wantErr: ErrNegativeCardPoints,
		},
		{
			name: "reject_zero_completion_time",
			in: []PlayerMatchStanding{
				advStanding("a", 1, 0, time.Time{}, false),
			},
			wantErr: ErrZeroCompletionTime,
		},
		{
			name: "zero_wins_and_points_allowed",
			in: []PlayerMatchStanding{
				advStanding("b", 0, 0, base, false),
				advStanding("a", 0, 0, base, false),
			},
			wantIDs: []PlayerID{"a", "b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orig := cloneStandings(tt.in)
			got, err := RankStandings(tt.in)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err=%v want %v", err, tt.wantErr)
				}
				if got != nil {
					t.Fatalf("got=%v want nil on error", got)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected err: %v", err)
				}
				if !slices.Equal(idsOf(got), tt.wantIDs) {
					t.Fatalf("ids=%v want %v", idsOf(got), tt.wantIDs)
				}
			}
			assertStandingsUnchanged(t, orig, tt.in)
		})
	}
}

func TestTopThree_Table(t *testing.T) {
	base := baseCompletion(t)
	tests := []struct {
		name    string
		in      []PlayerMatchStanding
		want    []PlayerID
		wantErr error
	}{
		{
			name: "exactly_top_three_from_five",
			in: []PlayerMatchStanding{
				advStanding("p1", 2, 5, base, false),
				advStanding("p2", 1, 8, base, false),
				advStanding("p3", 1, 9, base, false),
				advStanding("p4", 0, 1, base, false),
				advStanding("p5", 0, 2, base, false),
			},
			want: []PlayerID{"p1", "p2", "p3"},
		},
		{
			name: "exactly_three_eligible",
			in: []PlayerMatchStanding{
				advStanding("a", 2, 1, base, false),
				advStanding("b", 1, 1, base, false),
				advStanding("c", 0, 1, base, false),
			},
			want: []PlayerID{"a", "b", "c"},
		},
		{
			name: "forfeited_excluded_from_advancement",
			in: []PlayerMatchStanding{
				advStanding("champ", 2, 0, base, false),
				advStanding("second", 1, 10, base, false),
				advStanding("third", 1, 20, base, false),
				advStanding("ghost", 2, 0, base, true),
				advStanding("bench", 0, 0, base, false),
			},
			want: []PlayerID{"champ", "second", "third"},
		},
		{
			name: "forfeited_with_best_stats_still_excluded",
			in: []PlayerMatchStanding{
				advStanding("f1", 2, 0, base, true),
				advStanding("a", 0, 50, base, false),
				advStanding("b", 0, 40, base, false),
				advStanding("c", 0, 30, base, false),
			},
			want: []PlayerID{"c", "b", "a"},
		},
		{
			name: "insufficient_eligible_when_forfeits_reduce_below_three",
			in: []PlayerMatchStanding{
				advStanding("a", 2, 0, base, false),
				advStanding("b", 1, 0, base, false),
				advStanding("f1", 1, 0, base, true),
				advStanding("f2", 0, 0, base, true),
			},
			wantErr: ErrInsufficientEligible,
		},
		{
			name: "insufficient_eligible_two_players",
			in: []PlayerMatchStanding{
				advStanding("a", 2, 0, base, false),
				advStanding("b", 1, 0, base, false),
			},
			wantErr: ErrInsufficientEligible,
		},
		{
			name: "insufficient_eligible_all_forfeited",
			in: []PlayerMatchStanding{
				advStanding("f1", 2, 0, base, true),
				advStanding("f2", 1, 0, base, true),
				advStanding("f3", 0, 0, base, true),
			},
			wantErr: ErrInsufficientEligible,
		},
		{
			name:    "insufficient_eligible_empty",
			in:      nil,
			wantErr: ErrInsufficientEligible,
		},
		{
			name: "propagates_validation_error",
			in: []PlayerMatchStanding{
				advStanding("a", 1, 0, base, false),
				advStanding("a", 0, 0, base, false),
				advStanding("b", 0, 0, base, false),
			},
			wantErr: ErrDuplicatePlayerID,
		},
		{
			name: "more_than_three_eligible_takes_ranked_top",
			in: []PlayerMatchStanding{
				advStanding("d", 0, 10, base, false),
				advStanding("a", 2, 5, base, false),
				advStanding("c", 1, 20, base, false),
				advStanding("b", 1, 5, base, false),
				advStanding("e", 0, 1, base, false),
			},
			want: []PlayerID{"a", "b", "c"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orig := cloneStandings(tt.in)
			got, err := TopThree(tt.in)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err=%v want %v", err, tt.wantErr)
				}
				if got != nil {
					t.Fatalf("got=%v want nil on error", got)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected err: %v", err)
				}
				if !slices.Equal(got, tt.want) {
					t.Fatalf("got=%v want %v", got, tt.want)
				}
				if len(got) != AdvancersPerMatch {
					t.Fatalf("len=%d want %d", len(got), AdvancersPerMatch)
				}
			}
			assertStandingsUnchanged(t, orig, tt.in)
		})
	}
}

func TestRankStandings_PermutationDeterminism(t *testing.T) {
	base := baseCompletion(t)
	canonical := []PlayerMatchStanding{
		advStanding("p4", 0, 20, base.Add(2*time.Minute), false),
		advStanding("p1", 2, 10, base, false),
		advStanding("p5", 0, 5, base, true),
		advStanding("p2", 1, 5, base.Add(time.Minute), false),
		advStanding("p3", 1, 5, base, false),
		advStanding("p6", 2, 0, base, true),
	}
	want, err := RankStandings(canonical)
	if err != nil {
		t.Fatalf("canonical rank: %v", err)
	}
	wantIDs := idsOf(want)

	perms := allPermutations(canonical)
	if len(perms) < 2 {
		t.Fatal("expected multiple permutations")
	}
	for i, perm := range perms {
		got, err := RankStandings(perm)
		if err != nil {
			t.Fatalf("perm %d: %v", i, err)
		}
		if !slices.Equal(idsOf(got), wantIDs) {
			t.Fatalf("perm %d ids=%v want %v", i, idsOf(got), wantIDs)
		}
	}
}

func TestTopThree_PermutationDeterminism(t *testing.T) {
	base := baseCompletion(t)
	canonical := []PlayerMatchStanding{
		advStanding("d", 0, 30, base, false),
		advStanding("a", 2, 10, base.Add(time.Minute), false),
		advStanding("f", 2, 0, base, true),
		advStanding("b", 1, 5, base, false),
		advStanding("c", 1, 8, base, false),
		advStanding("e", 0, 1, base.Add(time.Hour), false),
	}
	want, err := TopThree(canonical)
	if err != nil {
		t.Fatalf("canonical top3: %v", err)
	}

	for i, perm := range allPermutations(canonical) {
		got, err := TopThree(perm)
		if err != nil {
			t.Fatalf("perm %d: %v", i, err)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("perm %d got=%v want %v", i, got, want)
		}
	}
}

func TestRankStandings_DoesNotMutateInputElements(t *testing.T) {
	base := baseCompletion(t)
	in := []PlayerMatchStanding{
		advStanding("b", 1, 10, base, false),
		advStanding("a", 2, 5, base, false),
	}
	in[0].MatchWins = 1
	before := in[0]
	if _, err := RankStandings(in); err != nil {
		t.Fatal(err)
	}
	if in[0] != before {
		t.Fatalf("input element mutated: %+v -> %+v", before, in[0])
	}
}

func TestSelectAdvancers_Compatibility(t *testing.T) {
	base := baseCompletion(t)
	standings := []PlayerMatchStanding{
		advStanding("p1", 2, 5, base, false),
		advStanding("p2", 1, 8, base, false),
		advStanding("p3", 1, 9, base, false),
		advStanding("p4", 0, 1, base, false),
	}
	got := SelectAdvancers(standings)
	want := []PlayerID{"p1", "p2", "p3"}
	if !slices.Equal(got, want) {
		t.Fatalf("got=%v want %v", got, want)
	}
}

func TestChampionFromStandings_Compatibility(t *testing.T) {
	base := baseCompletion(t)
	champ, ok := ChampionFromStandings([]PlayerMatchStanding{
		advStanding("runner", 1, 0, base, false),
		advStanding("champ", 2, 99, base, false),
	})
	if !ok || champ != "champ" {
		t.Fatalf("champion=%s ok=%v", champ, ok)
	}
}

func cloneStandings(in []PlayerMatchStanding) []PlayerMatchStanding {
	if in == nil {
		return nil
	}
	out := make([]PlayerMatchStanding, len(in))
	copy(out, in)
	return out
}

func assertStandingsUnchanged(t *testing.T, want, got []PlayerMatchStanding) {
	t.Helper()
	if want == nil && got == nil {
		return
	}
	if len(want) != len(got) {
		t.Fatalf("input length changed: %d -> %d", len(want), len(got))
	}
	for i := range want {
		if want[i] != got[i] {
			t.Fatalf("input[%d] mutated: %+v -> %+v", i, want[i], got[i])
		}
	}
}

func allPermutations(in []PlayerMatchStanding) [][]PlayerMatchStanding {
	if len(in) == 0 {
		return [][]PlayerMatchStanding{nil}
	}
	var out [][]PlayerMatchStanding
	var generate func(a []PlayerMatchStanding, l, r int)
	generate = func(a []PlayerMatchStanding, l, r int) {
		if l == r {
			cp := make([]PlayerMatchStanding, len(a))
			copy(cp, a)
			out = append(out, cp)
			return
		}
		for i := l; i <= r; i++ {
			a[l], a[i] = a[i], a[l]
			generate(a, l+1, r)
			a[l], a[i] = a[i], a[l]
		}
	}
	buf := make([]PlayerMatchStanding, len(in))
	copy(buf, in)
	generate(buf, 0, len(buf)-1)
	if len(out) == 0 {
		panic(fmt.Sprintf("no permutations for len=%d", len(in)))
	}
	return out
}
