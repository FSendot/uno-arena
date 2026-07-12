package store

import (
	"strings"
	"testing"

	"unoarena/shared/envelope"
)

func TestLookupOutcome_RejectsCommandIDMismatch(t *testing.T) {
	uow := &RegistrationUnitOfWork{
		commandID: "cmd-locked",
		op:        RegistrationOpRegister,
	}
	if _, ok := uow.LookupOutcome("cmd-other"); ok {
		t.Fatal("LookupOutcome must return false for commandID != locked commandID")
	}
	if _, ok := uow.LookupOutcome(""); ok {
		t.Fatal("LookupOutcome must return false for empty commandID")
	}
}

func TestCommit_RejectsCommandIDMismatch(t *testing.T) {
	uow := &RegistrationUnitOfWork{
		commandID: "cmd-x",
		op:        RegistrationOpCreate,
	}
	err := uow.Commit(RegistrationCommitRequest{
		Op:        RegistrationOpCreate,
		CommandID: "cmd-y",
		Outcome:   envelope.Accepted("cmd-y", "CreateTournament", nil, nil),
	})
	if err == nil {
		t.Fatal("expected commandID mismatch error")
	}
	if !strings.Contains(err.Error(), "command") {
		t.Fatalf("error should mention command mismatch: %v", err)
	}
	if uow.done {
		t.Fatal("mismatch must leave UoW rollback-able (done=false)")
	}
}

func TestCommit_RejectsOpMismatch(t *testing.T) {
	uow := &RegistrationUnitOfWork{
		commandID: "cmd-x",
		op:        RegistrationOpCreate,
	}
	err := uow.Commit(RegistrationCommitRequest{
		Op:        RegistrationOpClose,
		CommandID: "cmd-x",
		Outcome:   envelope.Accepted("cmd-x", "CloseRegistration", nil, nil),
	})
	if err == nil {
		t.Fatal("expected op mismatch error")
	}
	if !strings.Contains(err.Error(), "op") {
		t.Fatalf("error should mention op mismatch: %v", err)
	}
	if uow.done {
		t.Fatal("mismatch must leave UoW rollback-able (done=false)")
	}
}

func TestFinalizeRegister_RejectsCommandIDMismatch(t *testing.T) {
	uow := &RegistrationUnitOfWork{
		commandID: "cmd-x",
		op:        RegistrationOpRegister,
		reserved:  true,
	}
	err := uow.FinalizeRegister(RegistrationCommitRequest{
		Op:        RegistrationOpRegister,
		CommandID: "cmd-y",
		Outcome:   envelope.Accepted("cmd-y", "RegisterPlayer", nil, nil),
	})
	if err == nil {
		t.Fatal("expected commandID mismatch error")
	}
	if uow.done {
		t.Fatal("mismatch must leave UoW rollback-able so reservation can roll back")
	}
	if _, ok := uow.LookupOutcome("cmd-y"); ok {
		t.Fatal("must not treat mismatched commandID as lookup hit")
	}
}

func TestFinalizeRegister_RejectsOpMismatch(t *testing.T) {
	uow := &RegistrationUnitOfWork{
		commandID: "cmd-x",
		op:        RegistrationOpRegister,
	}
	err := uow.FinalizeRegister(RegistrationCommitRequest{
		Op:        RegistrationOpCreate,
		CommandID: "cmd-x",
		Outcome:   envelope.Accepted("cmd-x", "CreateTournament", nil, nil),
	})
	if err == nil {
		t.Fatal("expected op mismatch / non-register finalize error")
	}
	if uow.done {
		t.Fatal("mismatch must leave UoW rollback-able")
	}
}

func TestFinalizeRegister_RejectsWhenUoWOpNotRegister(t *testing.T) {
	uow := &RegistrationUnitOfWork{
		commandID: "cmd-x",
		op:        RegistrationOpStandalone,
	}
	err := uow.FinalizeRegister(RegistrationCommitRequest{
		Op:        RegistrationOpRegister,
		CommandID: "cmd-x",
		Outcome:   envelope.Accepted("cmd-x", "RegisterPlayer", nil, nil),
	})
	if err == nil {
		t.Fatal("FinalizeRegister must reject when locked op is not register")
	}
	if uow.done {
		t.Fatal("mismatch must leave UoW rollback-able")
	}
}

func TestReserveRegistration_RequiresRegisterOpAndLockedCommandID(t *testing.T) {
	t.Run("wrong op", func(t *testing.T) {
		uow := &RegistrationUnitOfWork{
			commandID: "cmd-x",
			op:        RegistrationOpCreate,
		}
		_, _, err := uow.ReserveRegistration()
		if err == nil {
			t.Fatal("ReserveRegistration must reject non-register op")
		}
		if uow.reserved {
			t.Fatal("must not mark reserved on reject")
		}
	})
	t.Run("empty commandID", func(t *testing.T) {
		uow := &RegistrationUnitOfWork{
			op: RegistrationOpRegister,
		}
		_, _, err := uow.ReserveRegistration()
		if err == nil {
			t.Fatal("ReserveRegistration must reject empty locked commandID")
		}
		if uow.reserved {
			t.Fatal("must not mark reserved on reject")
		}
	})
}

func TestReserveThenFinalizeWrongCommand_LeavesRollbackable(t *testing.T) {
	// Simulates: Begin(cmd-x) → Reserve → Finalize(cmd-y).
	// Without a DB, reserved=true stands in for a successful ReserveRegistration.
	uow := &RegistrationUnitOfWork{
		commandID: "cmd-x",
		op:        RegistrationOpRegister,
		tid:       "t-1",
		playerID:  "p-1",
		reserved:  true,
	}
	err := uow.FinalizeRegister(RegistrationCommitRequest{
		Op:           RegistrationOpRegister,
		TournamentID: "t-1",
		CommandID:    "cmd-y",
		CommandType:  "RegisterPlayer",
		Outcome:      envelope.Accepted("cmd-y", "RegisterPlayer", nil, nil),
	})
	if err == nil {
		t.Fatal("finalize of cmd-y under lock for cmd-x must fail")
	}
	if uow.done {
		t.Fatal("UoW must stay open for Rollback (no commit of reservation/outcome)")
	}
	if _, ok := uow.LookupOutcome("cmd-y"); ok {
		t.Fatal("no outcome lookup for mismatched cmd-y")
	}
	if !uow.reserved {
		t.Fatal("reservation flag must remain so caller Rollback can undo uncommitted mutations")
	}
}
