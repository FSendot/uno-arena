package domain

import "testing"

func TestNormalizeTournamentVisibility(t *testing.T) {
	t.Parallel()
	cases := []struct {
		raw     string
		want    TournamentVisibility
		wantErr bool
	}{
		{"", TournamentVisibilityPublic, false},
		{"public", TournamentVisibilityPublic, false},
		{"private", TournamentVisibilityPrivate, false},
		{" public ", TournamentVisibilityPublic, false},
		{"PRIVATE", "", true},
		{"secret", "", true},
		{"unknown", "", true},
	}
	for _, tc := range cases {
		got, err := NormalizeTournamentVisibility(tc.raw)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("raw=%q: expected error", tc.raw)
			}
			continue
		}
		if err != nil {
			t.Fatalf("raw=%q: %v", tc.raw, err)
		}
		if got != tc.want {
			t.Fatalf("raw=%q: got %q want %q", tc.raw, got, tc.want)
		}
	}
}

func TestCreateTournament_VisibilityDefaultAndPrivate(t *testing.T) {
	t.Parallel()
	tr, out := CreateTournament(CreateTournamentCommand{
		CommandID: "c-vis-def", TournamentID: "t-vis-def", Capacity: 4,
	})
	if !out.Accepted() || tr == nil {
		t.Fatalf("create: %+v", out)
	}
	if tr.Visibility() != TournamentVisibilityPublic {
		t.Fatalf("default visibility=%q", tr.Visibility())
	}
	if out.Facts[0].Data["visibility"] != "public" {
		t.Fatalf("fact visibility=%q", out.Facts[0].Data["visibility"])
	}

	trPriv, outPriv := CreateTournament(CreateTournamentCommand{
		CommandID: "c-vis-priv", TournamentID: "t-vis-priv", Capacity: 4,
		Visibility: TournamentVisibilityPrivate,
	})
	if !outPriv.Accepted() || trPriv == nil {
		t.Fatalf("private create: %+v", outPriv)
	}
	if trPriv.Visibility() != TournamentVisibilityPrivate {
		t.Fatalf("private visibility=%q", trPriv.Visibility())
	}
	if outPriv.Facts[0].Data["visibility"] != "private" {
		t.Fatalf("private fact=%q", outPriv.Facts[0].Data["visibility"])
	}
}

func TestCreateTournament_RejectsUnknownVisibility(t *testing.T) {
	t.Parallel()
	_, out := CreateTournament(CreateTournamentCommand{
		CommandID: "c-vis-bad", TournamentID: "t-vis-bad", Capacity: 4,
		Visibility: TournamentVisibility("secret"),
	})
	if out.Accepted() || out.Rejection == nil || out.Rejection.Code != RejectInvalidCommand {
		t.Fatalf("want invalid_command, got %+v", out)
	}
}

func TestDecideCreateTournament_Visibility(t *testing.T) {
	t.Parallel()
	ok := DecideCreateTournament(CreateTournamentCommand{
		CommandID: "d1", TournamentID: "t1", Capacity: 8,
	})
	if ok.Kind != RegistrationCreate || ok.Outcome.Facts[0].Data["visibility"] != "public" {
		t.Fatalf("default decide: %+v", ok)
	}
	priv := DecideCreateTournament(CreateTournamentCommand{
		CommandID: "d2", TournamentID: "t2", Capacity: 8,
		Visibility: TournamentVisibilityPrivate,
	})
	if priv.Kind != RegistrationCreate || priv.Outcome.Facts[0].Data["visibility"] != "private" {
		t.Fatalf("private decide: %+v", priv)
	}
	bad := DecideCreateTournament(CreateTournamentCommand{
		CommandID: "d3", TournamentID: "t3", Capacity: 8,
		Visibility: TournamentVisibility("nope"),
	})
	if bad.Kind != RegistrationReject || bad.Outcome.Rejection.Code != RejectInvalidCommand {
		t.Fatalf("bad decide: %+v", bad)
	}
}

func TestRestoreTournament_VisibilityRoundTrip(t *testing.T) {
	t.Parallel()
	tr, out := CreateTournament(CreateTournamentCommand{
		CommandID: "c-rt", TournamentID: "t-rt", Capacity: 4,
		Visibility: TournamentVisibilityPrivate,
	})
	if !out.Accepted() {
		t.Fatal(out)
	}
	restored := RestoreTournament(RestoreTournamentInput{
		ID:         tr.ID(),
		Phase:      tr.Phase(),
		Capacity:   tr.Capacity(),
		Visibility: tr.Visibility(),
	})
	if restored.Visibility() != TournamentVisibilityPrivate {
		t.Fatalf("restored visibility=%q", restored.Visibility())
	}
	publicRestored := RestoreTournament(RestoreTournamentInput{
		ID: "t-rt2", Phase: PhaseRegistration, Capacity: 2,
	})
	if publicRestored.Visibility() != TournamentVisibilityPublic {
		t.Fatalf("empty restore visibility=%q", publicRestored.Visibility())
	}
}
