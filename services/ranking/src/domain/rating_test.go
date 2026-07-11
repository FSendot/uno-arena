package domain

import (
	"strconv"
	"testing"
)

func casualCmd(playerID PlayerID, commandID CommandID, eventID EventID, gameID GameID, placement int, others ...RatedPlacement) ApplyCasualEloUpdateCommand {
	parts := make([]RatedPlacement, 0, 1+len(others))
	parts = append(parts, RatedPlacement{PlayerID: playerID, Placement: placement})
	parts = append(parts, others...)
	return ApplyCasualEloUpdateCommand{
		CommandID:     commandID,
		EventID:       eventID,
		PlayerID:      playerID,
		GameID:        gameID,
		RoomID:        "room-1",
		RoomType:      RoomTypeAdHoc,
		Authoritative: true,
		Completed:     true,
		Participants:  parts,
	}
}

func TestNewPlayerRating_ConfigurableFloor(t *testing.T) {
	p := NewPlayerRating("p1", RatingConfig{
		Floor: 100, InitialCasualElo: 1200, InitialTournamentRating: 50, KFactor: 24,
	})
	if p.CasualElo().Value != 1200 || p.TournamentPlacementRating() != 100 || p.Floor() != 100 {
		t.Fatalf("got casual=%d tour=%d floor=%d", p.CasualElo().Value, p.TournamentPlacementRating(), p.Floor())
	}
	snap := p.PublicSnapshot()
	if snap.PlayerID != "p1" || snap.CasualElo != 1200 || snap.TournamentPlacementRating != 100 {
		t.Fatalf("snapshot=%+v", snap)
	}
}

func TestCasualElo_RejectsIneligibleWithoutMutation(t *testing.T) {
	p := NewPlayerRating("alice", DefaultRatingConfig())
	before := p.CasualElo().Value
	bob := RatedPlacement{PlayerID: "bob", Placement: 2, Rating: 1000}

	cases := []struct {
		name string
		cmd  ApplyCasualEloUpdateCommand
		code RejectionCode
	}{
		{
			name: "tournament",
			cmd: ApplyCasualEloUpdateCommand{
				CommandID: "c1", EventID: "e1", PlayerID: "alice", GameID: "g1", RoomID: "r1",
				RoomType: RoomTypeTournament, Authoritative: true, Completed: true,
				Participants: []RatedPlacement{{PlayerID: "alice", Placement: 1}, bob},
			},
			code: RejectTournamentGame,
		},
		{
			name: "abandoned",
			cmd: ApplyCasualEloUpdateCommand{
				CommandID: "c2", EventID: "e2", PlayerID: "alice", GameID: "g2", RoomID: "r1",
				RoomType: RoomTypeAdHoc, IsAbandoned: true, Authoritative: true, Completed: true,
				Participants: []RatedPlacement{{PlayerID: "alice", Placement: 1}, bob},
			},
			code: RejectAbandonedGame,
		},
		{
			name: "not authoritative",
			cmd: ApplyCasualEloUpdateCommand{
				CommandID: "c3", EventID: "e3", PlayerID: "alice", GameID: "g3", RoomID: "r1",
				RoomType: RoomTypeAdHoc, Authoritative: false, Completed: true,
				Participants: []RatedPlacement{{PlayerID: "alice", Placement: 1}, bob},
			},
			code: RejectNotAuthoritative,
		},
		{
			name: "not completed",
			cmd: ApplyCasualEloUpdateCommand{
				CommandID: "c4", EventID: "e4", PlayerID: "alice", GameID: "g4", RoomID: "r1",
				RoomType: RoomTypeAdHoc, Authoritative: true, Completed: false,
				Participants: []RatedPlacement{{PlayerID: "alice", Placement: 1}, bob},
			},
			code: RejectIneligibleGame,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := p.ApplyCasualEloUpdate(tc.cmd)
			if !out.Rejected() || out.Rejection.Code != tc.code {
				t.Fatalf("got %#v want %s", out, tc.code)
			}
			if p.CasualElo().Value != before || len(p.History()) != 0 || len(out.Facts) != 0 {
				t.Fatalf("state mutated or facts emitted on reject")
			}
		})
	}
}

func TestCasualElo_AcceptsAdHocAndEmitsFact(t *testing.T) {
	p := NewPlayerRating("alice", DefaultRatingConfig())
	out := p.ApplyCasualEloUpdate(casualCmd("alice", "c1", "e1", "g1", 1,
		RatedPlacement{PlayerID: "bob", Placement: 2, Rating: 1000},
	))
	if out.Kind != OutcomeAccepted {
		t.Fatalf("%#v", out)
	}
	if p.CasualElo().Value <= 1000 {
		t.Fatalf("winner should gain, got %d", p.CasualElo().Value)
	}
	f := out.Facts[0]
	if f.Name != FactPlayerRatingUpdated {
		t.Fatalf("fact=%s", f.Name)
	}
	if f.Data["previousRating"] != "1000" || f.Data["gameId"] != "g1" || f.Data["delta"] == "" {
		t.Fatalf("fact data=%v", f.Data)
	}
	if p.TournamentPlacementRating() != 0 {
		t.Fatalf("casual must not touch tournament rating")
	}
	h := p.History()
	if len(h) != 1 || h[0].SourceType != SourceCasualElo {
		t.Fatalf("history=%v", h)
	}
	if h[0].RoomID != "room-1" || h[0].EventID != "e1" || h[0].GameID != "g1" {
		t.Fatalf("history ids=%+v", h[0])
	}
}

func TestCasualElo_EnforcesFloor(t *testing.T) {
	p := NewPlayerRating("alice", RatingConfig{
		Floor: 1000, InitialCasualElo: 1000, InitialTournamentRating: 0, KFactor: 32,
	})
	out := p.ApplyCasualEloUpdate(casualCmd("alice", "c1", "e1", "g1", 2,
		RatedPlacement{PlayerID: "bob", Placement: 1, Rating: 1000},
	))
	if out.Kind != OutcomeAccepted {
		t.Fatalf("%#v", out)
	}
	if p.CasualElo().Value != 1000 {
		t.Fatalf("floor not enforced: %d", p.CasualElo().Value)
	}
	if out.Facts[0].Data["delta"] != "0" || out.Facts[0].Data["newRating"] != "1000" {
		t.Fatalf("fact=%v", out.Facts[0].Data)
	}
}

func TestCasualElo_DuplicateByGameAndEventStable(t *testing.T) {
	p := NewPlayerRating("alice", DefaultRatingConfig())
	cmd := casualCmd("alice", "c1", "e1", "g1", 1,
		RatedPlacement{PlayerID: "bob", Placement: 2, Rating: 1000},
	)
	first := p.ApplyCasualEloUpdate(cmd)
	rating := p.CasualElo().Value
	hist := len(p.History())

	if p.ApplyCasualEloUpdate(cmd).Kind != OutcomeDuplicate {
		t.Fatal("same command should duplicate")
	}
	dupKey := p.ApplyCasualEloUpdate(casualCmd("alice", "c2", "e2", "g1", 1,
		RatedPlacement{PlayerID: "bob", Placement: 2, Rating: 1000},
	))
	if dupKey.Kind != OutcomeDuplicate {
		t.Fatalf("game key duplicate: %#v", dupKey)
	}
	dupEvt := p.ApplyCasualEloUpdate(casualCmd("alice", "c3", "e1", "g9", 1,
		RatedPlacement{PlayerID: "bob", Placement: 2, Rating: 1000},
	))
	if dupEvt.Kind != OutcomeDuplicate {
		t.Fatalf("event duplicate: %#v", dupEvt)
	}
	if p.CasualElo().Value != rating || len(p.History()) != hist {
		t.Fatal("duplicate mutated state")
	}
	if first.Facts[0].Data["newRating"] != dupKey.Facts[0].Data["newRating"] {
		t.Fatal("unstable duplicate facts")
	}
}

func TestTournament_SeparatedFromCasual(t *testing.T) {
	p := NewPlayerRating("alice", DefaultRatingConfig())
	casualBefore := p.CasualElo().Value

	out := p.ApplyTournamentPlacementUpdate(ApplyTournamentPlacementUpdateCommand{
		CommandID: "t1", EventID: "te1", PlayerID: "alice",
		TournamentID: "tour1", PlacementEventID: "pe1",
		Placement: 1, Delta: 50, Reason: ReasonTournamentFinalStanding,
	})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("%#v", out)
	}
	if p.CasualElo().Value != casualBefore {
		t.Fatal("tournament mutated casual")
	}
	if p.TournamentPlacementRating() != 50 {
		t.Fatalf("tour=%d", p.TournamentPlacementRating())
	}
	f := out.Facts[0]
	if f.Name != FactTournamentPlacementRatingUpdated {
		t.Fatalf("fact=%s", f.Name)
	}
	if f.Data["previousRating"] != "0" || f.Data["newRating"] != "50" || f.Data["delta"] != "50" {
		t.Fatalf("data=%v", f.Data)
	}
	if f.Data["tournamentId"] != "tour1" || f.Data["placementEventId"] != "pe1" {
		t.Fatalf("source ids=%v", f.Data)
	}
	h := p.History()
	if len(h) != 1 || h[0].EventID != "te1" || h[0].TournamentID != "tour1" || h[0].PlacementEventID != "pe1" {
		t.Fatalf("tournament history=%+v", h)
	}

	_ = p.ApplyCasualEloUpdate(casualCmd("alice", "c1", "ce1", "g1", 1,
		RatedPlacement{PlayerID: "bob", Placement: 2, Rating: 1000},
	))
	if p.TournamentPlacementRating() != 50 {
		t.Fatal("casual mutated tournament")
	}
	if p.CasualElo().Value == casualBefore {
		t.Fatal("casual should have changed")
	}
}

func TestTournament_DuplicateBusinessKey(t *testing.T) {
	p := NewPlayerRating("alice", DefaultRatingConfig())
	first := p.ApplyTournamentPlacementUpdate(ApplyTournamentPlacementUpdateCommand{
		CommandID: "t1", EventID: "te1", PlayerID: "alice",
		TournamentID: "tour1", PlacementEventID: "pe1",
		Placement: 2, Delta: 20, Reason: ReasonTournamentPlacement,
	})
	rating := p.TournamentPlacementRating()
	dup := p.ApplyTournamentPlacementUpdate(ApplyTournamentPlacementUpdateCommand{
		CommandID: "t2", EventID: "te2", PlayerID: "alice",
		TournamentID: "tour1", PlacementEventID: "pe1",
		Placement: 2, Delta: 20, Reason: ReasonTournamentPlacement,
	})
	if dup.Kind != OutcomeDuplicate {
		t.Fatalf("%#v", dup)
	}
	if p.TournamentPlacementRating() != rating || len(p.History()) != 1 {
		t.Fatal("duplicate mutated")
	}
	if first.Facts[0].Data["newRating"] != dup.Facts[0].Data["newRating"] {
		t.Fatal("unstable facts")
	}
}

func TestTournament_DuplicateEventStable(t *testing.T) {
	p := NewPlayerRating("alice", DefaultRatingConfig())
	first := p.ApplyTournamentPlacementUpdate(ApplyTournamentPlacementUpdateCommand{
		CommandID: "t1", EventID: "te1", PlayerID: "alice",
		TournamentID: "tour1", PlacementEventID: "pe1",
		Placement: 1, Delta: 30, Reason: ReasonTournamentPlacement,
	})
	dup := p.ApplyTournamentPlacementUpdate(ApplyTournamentPlacementUpdateCommand{
		CommandID: "t2", EventID: "te1", PlayerID: "alice",
		TournamentID: "tour2", PlacementEventID: "pe2",
		Placement: 1, Delta: 30, Reason: ReasonTournamentPlacement,
	})
	if dup.Kind != OutcomeDuplicate {
		t.Fatalf("%#v", dup)
	}
	if p.TournamentPlacementRating() != 30 || len(p.History()) != 1 {
		t.Fatal("event duplicate mutated")
	}
	if first.Facts[0].Data["newRating"] != dup.Facts[0].Data["newRating"] {
		t.Fatal("unstable facts")
	}
}

func TestTournament_EnforcesFloor(t *testing.T) {
	p := NewPlayerRating("alice", RatingConfig{
		Floor: 100, InitialCasualElo: 1000, InitialTournamentRating: 105, KFactor: 32,
	})
	out := p.ApplyTournamentPlacementUpdate(ApplyTournamentPlacementUpdateCommand{
		CommandID: "t1", EventID: "te1", PlayerID: "alice",
		TournamentID: "tour1", PlacementEventID: "pe1",
		Placement: 8, Delta: -50, Reason: ReasonTournamentPlacement,
	})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("%#v", out)
	}
	if p.TournamentPlacementRating() != 100 {
		t.Fatalf("got %d", p.TournamentPlacementRating())
	}
}

func TestHistoryImmutableCopy(t *testing.T) {
	p := NewPlayerRating("alice", DefaultRatingConfig())
	_ = p.ApplyCasualEloUpdate(casualCmd("alice", "c1", "e1", "g1", 1,
		RatedPlacement{PlayerID: "bob", Placement: 2, Rating: 1000},
	))
	h := p.History()
	h[0].Delta = 9999
	h[0].RoomID = "mutated"
	if p.History()[0].Delta == 9999 || p.History()[0].RoomID == "mutated" {
		t.Fatal("history leaked mutation")
	}
}

func TestCasualElo_UsesPairwiseDeltas(t *testing.T) {
	alice := NewPlayerRating("alice", DefaultRatingConfig())
	bob := NewPlayerRating("bob", DefaultRatingConfig())
	standings := []RatedPlacement{
		{PlayerID: "alice", Rating: 1000, Placement: 1},
		{PlayerID: "bob", Rating: 1000, Placement: 2},
	}
	want := ComputePairwiseEloDeltas(standings, DefaultKFactor)

	_ = alice.ApplyCasualEloUpdate(casualCmd("alice", "ca", "ea", "g1", 1,
		RatedPlacement{PlayerID: "bob", Placement: 2, Rating: 1000},
	))
	_ = bob.ApplyCasualEloUpdate(casualCmd("bob", "cb", "eb", "g1", 2,
		RatedPlacement{PlayerID: "alice", Placement: 1, Rating: 1000},
	))
	if alice.CasualElo().Value != 1000+want["alice"] {
		t.Fatalf("alice=%d want %d", alice.CasualElo().Value, 1000+want["alice"])
	}
	if bob.CasualElo().Value != 1000+want["bob"] {
		t.Fatalf("bob=%d want %d", bob.CasualElo().Value, 1000+want["bob"])
	}
}

func TestCasualElo_FactDeltaParses(t *testing.T) {
	p := NewPlayerRating("alice", DefaultRatingConfig())
	out := p.ApplyCasualEloUpdate(casualCmd("alice", "c1", "e1", "g1", 1,
		RatedPlacement{PlayerID: "bob", Placement: 2, Rating: 1000},
	))
	d, err := strconv.Atoi(out.Facts[0].Data["delta"])
	if err != nil || d != 16 {
		t.Fatalf("delta=%q err=%v", out.Facts[0].Data["delta"], err)
	}
}
