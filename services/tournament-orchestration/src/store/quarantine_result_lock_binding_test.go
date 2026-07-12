package store

import (
	"strings"
	"testing"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/shared/envelope"
)

func TestQuarantineResultCommit_RejectsCommandTypeMismatch(t *testing.T) {
	uow := &QuarantineResultUnitOfWork{
		commandID: "cmd-x", tid: "t-1", roomID: "room-1", completionVersion: 3,
		loaded: domain.QuarantineTournamentResultContext{Exists: true, AssignmentResolved: true, RoundNumber: 1, SlotID: "s1"},
	}
	err := uow.Commit(QuarantineResultCommitRequest{
		TournamentID: "t-1", CommandID: "cmd-x", CommandType: "RecordMatchResult",
		Outcome:  envelope.Accepted("cmd-x", "RecordMatchResult", nil, nil),
		Decision: domain.QuarantineTournamentResultDecision{Kind: domain.QuarantineResultReject},
		Command: domain.QuarantineTournamentResultCommand{
			CommandID: "cmd-x", RoomID: "room-1", CompletionVersion: 3,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "command type") {
		t.Fatalf("want command type mismatch, got %v", err)
	}
	if uow.done {
		t.Fatal("mismatch must leave UoW rollback-able")
	}
}

func TestQuarantineResultCommit_RejectsTournamentMismatch(t *testing.T) {
	uow := &QuarantineResultUnitOfWork{
		commandID: "cmd-x", tid: "t-1", roomID: "room-1", completionVersion: 3,
	}
	err := uow.Commit(QuarantineResultCommitRequest{
		TournamentID: "t-other", CommandID: "cmd-x", CommandType: quarantineResultCommandType,
		Outcome:  envelope.Accepted("cmd-x", quarantineResultCommandType, nil, nil),
		Decision: domain.QuarantineTournamentResultDecision{Kind: domain.QuarantineResultReject},
		Command: domain.QuarantineTournamentResultCommand{
			CommandID: "cmd-x", RoomID: "room-1", CompletionVersion: 3,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "tournament") {
		t.Fatalf("want tournament mismatch, got %v", err)
	}
	if uow.done {
		t.Fatal("mismatch must leave UoW rollback-able")
	}
}

func TestQuarantineResultCommit_RejectsRoomMismatch(t *testing.T) {
	uow := &QuarantineResultUnitOfWork{
		commandID: "cmd-x", tid: "t-1", roomID: "room-1", completionVersion: 3,
	}
	err := uow.Commit(QuarantineResultCommitRequest{
		TournamentID: "t-1", CommandID: "cmd-x", CommandType: quarantineResultCommandType,
		Outcome:  envelope.Accepted("cmd-x", quarantineResultCommandType, nil, nil),
		Decision: domain.QuarantineTournamentResultDecision{Kind: domain.QuarantineResultReject},
		Command: domain.QuarantineTournamentResultCommand{
			CommandID: "cmd-x", RoomID: "room-other", CompletionVersion: 3,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "room") {
		t.Fatalf("want room mismatch, got %v", err)
	}
	if uow.done {
		t.Fatal("mismatch must leave UoW rollback-able")
	}
}

func TestQuarantineResultCommit_RejectsCompletionVersionMismatch(t *testing.T) {
	uow := &QuarantineResultUnitOfWork{
		commandID: "cmd-x", tid: "t-1", roomID: "room-1", completionVersion: 3,
	}
	err := uow.Commit(QuarantineResultCommitRequest{
		TournamentID: "t-1", CommandID: "cmd-x", CommandType: quarantineResultCommandType,
		Outcome:  envelope.Accepted("cmd-x", quarantineResultCommandType, nil, nil),
		Decision: domain.QuarantineTournamentResultDecision{Kind: domain.QuarantineResultReject},
		Command: domain.QuarantineTournamentResultCommand{
			CommandID: "cmd-x", RoomID: "room-1", CompletionVersion: 9,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "completionVersion") {
		t.Fatalf("want completionVersion mismatch, got %v", err)
	}
	if uow.done {
		t.Fatal("mismatch must leave UoW rollback-able")
	}
}

func TestQuarantineResultCommit_RejectsCommandIDMismatch(t *testing.T) {
	uow := &QuarantineResultUnitOfWork{
		commandID: "cmd-locked", tid: "t-1", roomID: "room-1", completionVersion: 3,
	}
	err := uow.Commit(QuarantineResultCommitRequest{
		TournamentID: "t-1", CommandID: "cmd-other", CommandType: quarantineResultCommandType,
		Outcome:  envelope.Accepted("cmd-other", quarantineResultCommandType, nil, nil),
		Decision: domain.QuarantineTournamentResultDecision{Kind: domain.QuarantineResultReject},
		Command: domain.QuarantineTournamentResultCommand{
			CommandID: "cmd-other", RoomID: "room-1", CompletionVersion: 3,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "command") {
		t.Fatalf("want commandId mismatch, got %v", err)
	}
	if uow.done {
		t.Fatal("mismatch must leave UoW rollback-able")
	}
}

func TestQuarantineResultCommit_RejectsCmdCommandIDMismatch(t *testing.T) {
	uow := &QuarantineResultUnitOfWork{
		commandID: "cmd-locked", tid: "t-1", roomID: "room-1", completionVersion: 3,
	}
	err := uow.Commit(QuarantineResultCommitRequest{
		TournamentID: "t-1", CommandID: "cmd-locked", CommandType: quarantineResultCommandType,
		Outcome:  envelope.Accepted("cmd-locked", quarantineResultCommandType, nil, nil),
		Decision: domain.QuarantineTournamentResultDecision{Kind: domain.QuarantineResultReject},
		Command: domain.QuarantineTournamentResultCommand{
			CommandID: "cmd-forged", RoomID: "room-1", CompletionVersion: 3,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "command") {
		t.Fatalf("want cmd.CommandID mismatch, got %v", err)
	}
	if uow.done {
		t.Fatal("mismatch must leave UoW rollback-able")
	}
}

func TestQuarantineResultCommit_RejectsSuccessOnStandalone(t *testing.T) {
	uow := &QuarantineResultUnitOfWork{commandID: "cmd-x"} // standalone: no tid/room/version
	err := uow.Commit(QuarantineResultCommitRequest{
		CommandID: "cmd-x", CommandType: quarantineResultCommandType,
		Outcome: envelope.Accepted("cmd-x", quarantineResultCommandType, nil, nil),
		Decision: domain.QuarantineTournamentResultDecision{
			Kind:             domain.QuarantineResultLedgerOnly,
			WriteMatchResult: false,
		},
		Command: domain.QuarantineTournamentResultCommand{
			CommandID: "cmd-x", RoomID: "room-1", CompletionVersion: 1,
		},
	})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "standalone") {
		t.Fatalf("want standalone reject, got %v", err)
	}
	if uow.done {
		t.Fatal("mismatch must leave UoW rollback-able")
	}
}

func TestQuarantineResultCommit_RejectsForgedPersistenceTarget(t *testing.T) {
	uow := &QuarantineResultUnitOfWork{
		commandID: "cmd-x", tid: "t-1", roomID: "room-1", completionVersion: 3,
		loaded: domain.QuarantineTournamentResultContext{
			Exists: true, AssignmentResolved: true, RoundNumber: 1, SlotID: "s1",
		},
	}
	err := uow.Commit(QuarantineResultCommitRequest{
		TournamentID: "t-1", CommandID: "cmd-x", CommandType: quarantineResultCommandType,
		Outcome: envelope.Accepted("cmd-x", quarantineResultCommandType, nil, nil),
		Decision: domain.QuarantineTournamentResultDecision{
			Kind:             domain.QuarantineResultInsertQuarantined,
			WriteMatchResult: true,
			AffectsSlot:      true,
			PersistRound:     99, // forged
			PersistSlot:      "forged-slot",
		},
		Command: domain.QuarantineTournamentResultCommand{
			CommandID: "cmd-x", RoomID: "room-1", CompletionVersion: 3,
		},
	})
	if err == nil || (!strings.Contains(err.Error(), "Persist") && !strings.Contains(err.Error(), "assignment") && !strings.Contains(err.Error(), "persist")) {
		t.Fatalf("want forged persistence reject, got %v", err)
	}
	if uow.done {
		t.Fatal("mismatch must leave UoW rollback-able")
	}
}

func TestQuarantineResultCommit_RejectsInjectedWriteMatchResultOnLedgerOnly(t *testing.T) {
	uow := &QuarantineResultUnitOfWork{
		commandID: "cmd-x", tid: "t-1", roomID: "room-1", completionVersion: 3,
		loaded: domain.QuarantineTournamentResultContext{
			Exists: true, AssignmentResolved: true, RoundNumber: 1, SlotID: "s1",
			PriorDisposition: domain.DispositionRecorded,
		},
	}
	err := uow.Commit(QuarantineResultCommitRequest{
		TournamentID: "t-1", CommandID: "cmd-x", CommandType: quarantineResultCommandType,
		Outcome: envelope.Accepted("cmd-x", quarantineResultCommandType, nil, nil),
		Decision: domain.QuarantineTournamentResultDecision{
			Kind:             domain.QuarantineResultLedgerOnly,
			WriteMatchResult: true, // forged
			AffectsSlot:      true,
			PersistRound:     1,
			PersistSlot:      "s1",
		},
		Command: domain.QuarantineTournamentResultCommand{
			CommandID: "cmd-x", RoomID: "room-1", CompletionVersion: 3,
		},
	})
	if err == nil {
		t.Fatal("want WriteMatchResult forge reject")
	}
	if uow.done {
		t.Fatal("mismatch must leave UoW rollback-able")
	}
}
