package main

import (
	"context"
	"encoding/json"
	"testing"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/shared/envelope"
)

type kickoffRegistrationRepo struct {
	uow *kickoffRegistrationUoW
}

func (r *kickoffRegistrationRepo) BeginCreateTournament(context.Context, string, string) (RegistrationUnitOfWork, error) {
	return r.uow, nil
}
func (r *kickoffRegistrationRepo) BeginRegisterPlayer(context.Context, string, string, string) (RegistrationUnitOfWork, error) {
	return r.uow, nil
}
func (r *kickoffRegistrationRepo) BeginCloseRegistration(context.Context, string, string) (RegistrationUnitOfWork, error) {
	return r.uow, nil
}
func (r *kickoffRegistrationRepo) BeginStandaloneCommand(context.Context, string) (RegistrationUnitOfWork, error) {
	return r.uow, nil
}
func (*kickoffRegistrationRepo) LookupOutcome(string) (envelope.Result, bool) {
	return envelope.Result{}, false
}

type kickoffRegistrationUoW struct {
	registerCtx domain.RegistrationContext
	closeCtx    domain.CloseRegistrationContext
	autoClosed  bool
	committed   RegistrationCommitRequest
}

func (*kickoffRegistrationUoW) Exists() bool { return true }
func (*kickoffRegistrationUoW) LookupOutcome(string) (envelope.Result, bool) {
	return envelope.Result{}, false
}
func (u *kickoffRegistrationUoW) RegisterContext() domain.RegistrationContext   { return u.registerCtx }
func (u *kickoffRegistrationUoW) CloseContext() domain.CloseRegistrationContext { return u.closeCtx }
func (*kickoffRegistrationUoW) CreateContext() domain.CreateTournamentContext {
	return domain.CreateTournamentContext{}
}
func (u *kickoffRegistrationUoW) ReserveRegistration() (int, bool, error) {
	return 0, u.autoClosed, nil
}
func (u *kickoffRegistrationUoW) FinalizeRegister(req RegistrationCommitRequest) error {
	u.committed = req
	return nil
}
func (u *kickoffRegistrationUoW) Commit(req RegistrationCommitRequest) error {
	u.committed = req
	return nil
}
func (*kickoffRegistrationUoW) Rollback() error { return nil }

func TestCloseRegistrationSchedulesRound1InsideRegistrationCommit(t *testing.T) {
	uow := &kickoffRegistrationUoW{closeCtx: domain.CloseRegistrationContext{
		TournamentID: "t-close-kickoff", Exists: true, Phase: domain.PhaseRegistration,
		Capacity: 8, RegisteredCount: 2,
	}}
	svc := NewService(ServiceDeps{Registrations: &kickoffRegistrationRepo{uow: uow}})
	res, err := svc.SubmitCommand(context.Background(), CommandRequest{
		CommandID: "close-1", Type: CmdCloseRegistration, SchemaVersion: 1,
		Payload: json.RawMessage(`{"tournamentId":"t-close-kickoff"}`),
	}, "corr-close")
	if err != nil || res.Status != envelope.StatusAccepted {
		t.Fatalf("close: err=%v result=%#v", err, res)
	}
	assertRound1Kickoff(t, uow.committed, "t-close-kickoff", 2, "corr-close")
}

func TestCapacityAutoCloseSchedulesRound1InsideRegisterCommit(t *testing.T) {
	uow := &kickoffRegistrationUoW{
		registerCtx: domain.RegistrationContext{
			TournamentID: "t-auto-kickoff", Exists: true, Phase: domain.PhaseRegistration, Capacity: 2,
		},
		autoClosed: true,
	}
	svc := NewService(ServiceDeps{Registrations: &kickoffRegistrationRepo{uow: uow}})
	res, err := svc.SubmitCommand(context.Background(), CommandRequest{
		CommandID: "register-final", Type: CmdRegisterPlayer, SchemaVersion: 1,
		Payload: json.RawMessage(`{"tournamentId":"t-auto-kickoff","playerId":"p2"}`),
	}, "corr-auto")
	if err != nil || res.Status != envelope.StatusAccepted {
		t.Fatalf("register: err=%v result=%#v", err, res)
	}
	assertRound1Kickoff(t, uow.committed, "t-auto-kickoff", 2, "corr-auto")
}

func assertRound1Kickoff(t *testing.T, req RegistrationCommitRequest, tournamentID string, players int, correlationID string) {
	t.Helper()
	if req.Round1Kickoff == nil {
		t.Fatal("registration close committed without round-1 kickoff")
	}
	wantID := domain.SeedRoundCommandID(domain.TournamentID(tournamentID), 1)
	if req.Round1Kickoff.CommandID != wantID {
		t.Fatalf("kickoff commandId=%q want %q", req.Round1Kickoff.CommandID, wantID)
	}
	if req.Round1Kickoff.CorrelationID != correlationID {
		t.Fatalf("kickoff correlationId=%q want %q", req.Round1Kickoff.CorrelationID, correlationID)
	}
	d := req.Round1Kickoff.Decision
	if d.Kind != domain.SeedKickoffSchedule || d.Source != domain.SeedingSourceRegistrations || d.Plan.PlayerCount != players {
		t.Fatalf("unexpected kickoff decision: %#v", d)
	}
}
