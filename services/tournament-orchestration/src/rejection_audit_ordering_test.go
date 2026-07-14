package main

import (
	"context"
	"errors"
	"testing"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/shared/audit"
	"unoarena/shared/envelope"
)

type rejectionAuditProbe struct {
	err error
	ctx context.Context
}

func (a *rejectionAuditProbe) RecordRejection(ctx context.Context, _ audit.RejectionRecord) error {
	a.ctx = ctx
	return a.err
}

type auditOrderingSeedingUoW struct{ commits int }

func (*auditOrderingSeedingUoW) Exists() bool { return true }
func (*auditOrderingSeedingUoW) LookupOutcome(string) (envelope.Result, bool) {
	return envelope.Result{}, false
}
func (*auditOrderingSeedingUoW) KickoffContext() domain.SeedRoundKickoffContext {
	return domain.SeedRoundKickoffContext{}
}
func (u *auditOrderingSeedingUoW) Commit(SeedingCommitRequest) error { u.commits++; return nil }
func (*auditOrderingSeedingUoW) Rollback() error                     { return nil }

type auditOrderingProvisioningUoW struct{ commits int }

func (*auditOrderingProvisioningUoW) Exists() bool { return true }
func (*auditOrderingProvisioningUoW) LookupOutcome(string) (envelope.Result, bool) {
	return envelope.Result{}, false
}
func (*auditOrderingProvisioningUoW) KickoffContext() domain.ProvisionKickoffContext {
	return domain.ProvisionKickoffContext{}
}
func (u *auditOrderingProvisioningUoW) Commit(ProvisioningCommitRequest) error {
	u.commits++
	return nil
}
func (*auditOrderingProvisioningUoW) Rollback() error { return nil }

func rejectedCommandOutcome(commandID string) domain.CommandOutcome {
	rejection := domain.Rejection{Code: domain.RejectInvalidCommand}
	return domain.CommandOutcome{
		Kind:      domain.OutcomeRejected,
		CommandID: domain.CommandID(commandID),
		Rejection: &rejection,
	}
}

func TestRejectedOutcomesAuditBeforeEveryCommitFamily(t *testing.T) {
	wantErr := errors.New("audit unavailable")
	auditProbe := &rejectionAuditProbe{err: wantErr}
	svc := NewService(ServiceDeps{Repo: NewMemoryTournamentRepository(), Audit: auditProbe})
	ctx := context.Background()
	req := func(id, typ string) CommandRequest {
		return CommandRequest{CommandID: id, Type: typ, SchemaVersion: envelope.CurrentSchemaVersion}
	}

	t.Run("round match", func(t *testing.T) {
		repo := &stubRoundMatchRepo{}
		uow := &stubRoundMatchUoW{commandID: "audit-rm", repo: repo}
		_, err := svc.commitRoundMatchOutcome(ctx, uow, req("audit-rm", CmdRecordMatchResult), domain.RoundMatchDecision{
			Kind: domain.RoundMatchReject, Outcome: rejectedCommandOutcome("audit-rm"),
		}, domain.RecordMatchResultCommand{}, "t1", "corr", nil)
		if !errors.Is(err, wantErr) || uow.committed != nil {
			t.Fatalf("err=%v committed=%v", err, uow.committed != nil)
		}
	})

	t.Run("registration", func(t *testing.T) {
		uow := &kickoffRegistrationUoW{}
		_, err := svc.commitRegistrationOutcome(ctx, uow, req("audit-reg", CmdCreateTournament), domain.RegistrationDecision{
			Kind: domain.RegistrationReject, Outcome: rejectedCommandOutcome("audit-reg"),
		}, domain.CreateTournamentCommand{}, "t1", "corr", false, nil)
		if !errors.Is(err, wantErr) || uow.committed.CommandID != "" {
			t.Fatalf("err=%v committed=%q", err, uow.committed.CommandID)
		}
	})

	t.Run("seeding", func(t *testing.T) {
		uow := &auditOrderingSeedingUoW{}
		_, err := svc.commitSeedingOutcome(ctx, uow, req("audit-seed", CmdSeedRound), domain.SeedRoundKickoffDecision{
			Kind: domain.SeedKickoffReject, Outcome: rejectedCommandOutcome("audit-seed"),
		}, "t1", "corr")
		if !errors.Is(err, wantErr) || uow.commits != 0 {
			t.Fatalf("err=%v commits=%d", err, uow.commits)
		}
	})

	t.Run("provisioning", func(t *testing.T) {
		uow := &auditOrderingProvisioningUoW{}
		_, err := svc.commitProvisioningOutcome(ctx, uow, req("audit-prov", CmdProvisionRoundMatches), domain.ProvisionKickoffDecision{
			Kind: domain.ProvisionKickoffReject, Outcome: rejectedCommandOutcome("audit-prov"),
		}, "t1", "corr", 1)
		if !errors.Is(err, wantErr) || uow.commits != 0 {
			t.Fatalf("err=%v commits=%d", err, uow.commits)
		}
	})

	t.Run("complete round", func(t *testing.T) {
		repo := &stubCompleteRoundRepo{}
		uow := &stubCompleteRoundUoW{commandID: "audit-round", repo: repo}
		_, err := svc.commitCompleteRoundOutcome(ctx, uow, req("audit-round", CmdCompleteRound), domain.CompleteRoundDecision{
			Kind: domain.CompleteRoundReject, Outcome: rejectedCommandOutcome("audit-round"),
		}, "t1", "corr")
		if !errors.Is(err, wantErr) || repo.commitCalls.Load() != 0 {
			t.Fatalf("err=%v commits=%d", err, repo.commitCalls.Load())
		}
	})

	t.Run("lifecycle", func(t *testing.T) {
		repo := &stubLifecycleRepo{}
		uow := &stubLifecycleUoW{commandID: "audit-life", repo: repo}
		_, err := svc.commitLifecycleComplete(ctx, uow, req("audit-life", CmdCompleteTournament), domain.CompleteTournamentDecision{
			Kind: domain.CompleteTournamentReject, Outcome: rejectedCommandOutcome("audit-life"),
		}, "t1", "corr")
		if !errors.Is(err, wantErr) || repo.commitCalls.Load() != 0 {
			t.Fatalf("err=%v commits=%d", err, repo.commitCalls.Load())
		}
	})

	t.Run("quarantine result", func(t *testing.T) {
		repo := &stubQuarantineResultRepo{}
		uow := &stubQuarantineResultUoW{commandID: "audit-qr", repo: repo}
		_, err := svc.commitQuarantineResult(ctx, uow, req("audit-qr", CmdQuarantineResult), domain.QuarantineTournamentResultDecision{
			Kind: domain.QuarantineResultReject, Outcome: rejectedCommandOutcome("audit-qr"),
		}, domain.QuarantineTournamentResultCommand{}, "t1", "corr")
		if !errors.Is(err, wantErr) || repo.commitCalls.Load() != 0 {
			t.Fatalf("err=%v commits=%d", err, repo.commitCalls.Load())
		}
	})

	t.Run("legacy fallback", func(t *testing.T) {
		repo := NewMemoryTournamentRepository()
		legacy := NewService(ServiceDeps{Repo: repo, Audit: auditProbe})
		_, err := legacy.reject(ctx, req("audit-legacy", "Unknown"), "corr", "t1", "unknown_command_type")
		if !errors.Is(err, wantErr) {
			t.Fatalf("err=%v", err)
		}
		if _, ok := repo.LookupOutcome("audit-legacy"); ok {
			t.Fatal("audit failure installed a prior outcome")
		}
	})
}

func TestSubmitCommandPropagatesContextToRejectionAudit(t *testing.T) {
	type contextKey string
	const key contextKey = "audit-context"
	auditProbe := &rejectionAuditProbe{}
	svc := NewService(ServiceDeps{Repo: NewMemoryTournamentRepository(), Audit: auditProbe})
	ctx := context.WithValue(context.Background(), key, "active")
	_, err := svc.SubmitCommand(ctx, CommandRequest{
		CommandID: "audit-context-command", Type: "Unknown", SchemaVersion: envelope.CurrentSchemaVersion,
		Payload: []byte(`{}`),
	}, "corr")
	if err != nil {
		t.Fatal(err)
	}
	if got := auditProbe.ctx.Value(key); got != "active" {
		t.Fatalf("audit context value=%v", got)
	}
}
