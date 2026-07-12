package store

import (
	"os"
	"strings"
	"testing"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/shared/envelope"
)

func TestSeedingCommit_RejectsCommandIDMismatch(t *testing.T) {
	uow := &SeedingUnitOfWork{commandID: "cmd-x", tid: "t-1"}
	err := uow.Commit(SeedingCommitRequest{
		TournamentID: "t-1",
		CommandID:    "cmd-y",
		CommandType:  "SeedRound",
		Outcome:      envelope.Accepted("cmd-y", "SeedRound", nil, nil),
		Decision:     domain.SeedRound1KickoffDecision{Kind: domain.SeedKickoffReject},
	})
	if err == nil {
		t.Fatal("expected commandID mismatch")
	}
	if !strings.Contains(err.Error(), "command") {
		t.Fatalf("error should mention command: %v", err)
	}
	if uow.done {
		t.Fatal("mismatch must leave UoW rollback-able")
	}
}

func TestSeedingCommit_RejectsTournamentIDMismatch(t *testing.T) {
	uow := &SeedingUnitOfWork{commandID: "cmd-x", tid: "t-1"}
	err := uow.Commit(SeedingCommitRequest{
		TournamentID: "t-other",
		CommandID:    "cmd-x",
		CommandType:  "SeedRound",
		Outcome:      envelope.Accepted("cmd-x", "SeedRound", nil, nil),
		Decision:     domain.SeedRound1KickoffDecision{Kind: domain.SeedKickoffReject},
	})
	if err == nil {
		t.Fatal("expected tournamentId mismatch")
	}
	if uow.done {
		t.Fatal("mismatch must leave UoW rollback-able")
	}
}

func TestSeedingCommit_RejectsCommandTypeMismatch(t *testing.T) {
	uow := &SeedingUnitOfWork{commandID: "cmd-x", tid: "t-1"}
	err := uow.Commit(SeedingCommitRequest{
		TournamentID: "t-1",
		CommandID:    "cmd-x",
		CommandType:  "ProvisionRoundMatches",
		Outcome:      envelope.Accepted("cmd-x", "ProvisionRoundMatches", nil, nil),
		Decision:     domain.SeedRound1KickoffDecision{Kind: domain.SeedKickoffReject},
	})
	if err == nil {
		t.Fatal("expected command type mismatch")
	}
	if uow.done {
		t.Fatal("mismatch must leave UoW rollback-able")
	}
}

func TestSeedingCommit_RejectsScheduleOnStandalone(t *testing.T) {
	uow := &SeedingUnitOfWork{commandID: "cmd-x"} // no tid
	err := uow.Commit(SeedingCommitRequest{
		CommandID:   "cmd-x",
		CommandType: "SeedRound",
		Outcome:     envelope.Accepted("cmd-x", "SeedRound", nil, nil),
		Decision: domain.SeedRound1KickoffDecision{
			Kind: domain.SeedKickoffSchedule,
			Plan: domain.Round1SlotPlan{PlayerCount: 1, SlotCount: 1, BaseSize: 1},
		},
	})
	if err == nil {
		t.Fatal("schedule on standalone must fail")
	}
	if uow.done {
		t.Fatal("mismatch must leave UoW rollback-able")
	}
}

func TestSeedingLookupOutcome_RejectsCommandIDMismatch(t *testing.T) {
	uow := &SeedingUnitOfWork{commandID: "cmd-locked"}
	if _, ok := uow.LookupOutcome("cmd-other"); ok {
		t.Fatal("LookupOutcome must return false for commandID != locked")
	}
}

func TestSeedingNoHardcodedRoundOneInClaimProcessFinalize(t *testing.T) {
	b, err := os.ReadFile("seeding.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(b)
	for _, name := range []string{
		"ClaimNextSeedingJob",
		"ProcessSeedingChunk",
		"finalizeSeedingInTx",
		"finalizeSeedingJob",
		"quarantineSeedingTx",
		"cancelSeedingTx",
		"LoadSeedingJob",
	} {
		body := extractStoreFuncBody(t, src, name)
		if strings.Contains(body, "round_number = 1") || strings.Contains(body, "round_number=1") {
			t.Fatalf("%s must not hardcode round_number = 1", name)
		}
	}
}

func extractStoreFuncBody(t *testing.T, src, name string) string {
	t.Helper()
	markers := []string{
		"func (s *TournamentStore) " + name,
		"func (u *RoundMatchUnitOfWork) " + name,
		"func (u *QuarantineResultUnitOfWork) " + name,
		"func (u *LifecycleUnitOfWork) " + name,
		"func " + name,
	}
	start := -1
	for _, m := range markers {
		start = strings.Index(src, m)
		if start >= 0 {
			break
		}
	}
	if start < 0 {
		t.Fatalf("function %s not found", name)
	}
	brace := strings.Index(src[start:], "{")
	if brace < 0 {
		t.Fatalf("no body for %s", name)
	}
	i := start + brace
	depth := 0
	for j := i; j < len(src); j++ {
		switch src[j] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return src[i : j+1]
			}
		}
	}
	t.Fatalf("unbalanced braces for %s", name)
	return ""
}
