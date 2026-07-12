package main

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/shared/envelope"
)

type stubLifecycleRepo struct {
	standaloneCalls atomic.Int32
	completeCalls   atomic.Int32
	cancelCalls     atomic.Int32
	commitCalls     atomic.Int32
	outcomes        map[string]envelope.Result
	completeCtx     domain.CompleteTournamentContext
	cancelCtx       domain.CancelTournamentContext
}

func (s *stubLifecycleRepo) LookupOutcome(commandID string) (envelope.Result, bool) {
	if s.outcomes == nil {
		return envelope.Result{}, false
	}
	out, ok := s.outcomes[commandID]
	return out, ok
}

func (s *stubLifecycleRepo) BeginStandaloneCommand(ctx context.Context, commandID string) (LifecycleUnitOfWork, error) {
	s.standaloneCalls.Add(1)
	return &stubLifecycleUoW{commandID: commandID, repo: s}, nil
}

func (s *stubLifecycleRepo) BeginCompleteTournament(ctx context.Context, tournamentID, commandID string) (LifecycleUnitOfWork, error) {
	s.completeCalls.Add(1)
	loaded := s.completeCtx
	if loaded.TournamentID == "" {
		loaded.TournamentID = domain.TournamentID(tournamentID)
	}
	return &stubLifecycleUoW{commandID: commandID, repo: s, completeCtx: loaded, exists: loaded.Exists}, nil
}

func (s *stubLifecycleRepo) BeginCancelTournament(ctx context.Context, tournamentID, commandID string) (LifecycleUnitOfWork, error) {
	s.cancelCalls.Add(1)
	loaded := s.cancelCtx
	if loaded.TournamentID == "" {
		loaded.TournamentID = domain.TournamentID(tournamentID)
	}
	return &stubLifecycleUoW{commandID: commandID, repo: s, cancelCtx: loaded, exists: loaded.Exists}, nil
}

type stubLifecycleUoW struct {
	commandID   string
	repo        *stubLifecycleRepo
	exists      bool
	completeCtx domain.CompleteTournamentContext
	cancelCtx   domain.CancelTournamentContext
	done        bool
}

func (u *stubLifecycleUoW) Exists() bool { return u.exists }
func (u *stubLifecycleUoW) CompleteContext() domain.CompleteTournamentContext {
	return u.completeCtx
}
func (u *stubLifecycleUoW) CancelContext() domain.CancelTournamentContext { return u.cancelCtx }
func (u *stubLifecycleUoW) LookupOutcome(commandID string) (envelope.Result, bool) {
	if commandID != u.commandID {
		return envelope.Result{}, false
	}
	return u.repo.LookupOutcome(commandID)
}
func (u *stubLifecycleUoW) Rollback() error {
	u.done = true
	return nil
}
func (u *stubLifecycleUoW) Commit(req LifecycleCommitRequest) error {
	u.repo.commitCalls.Add(1)
	if u.repo.outcomes == nil {
		u.repo.outcomes = map[string]envelope.Result{}
	}
	u.repo.outcomes[req.CommandID] = req.Outcome
	u.done = true
	return nil
}

type countingBeginExistingRepo struct {
	TournamentRepository
	beginExistingCalls atomic.Int32
}

func (r *countingBeginExistingRepo) BeginExisting(id domain.TournamentID) (TournamentUnitOfWork, error) {
	r.beginExistingCalls.Add(1)
	return r.TournamentRepository.BeginExisting(id)
}

func TestSubmitCommand_LifecycleDifferential_InvalidPayloadStableRejection(t *testing.T) {
	repo := &countingBeginExistingRepo{TournamentRepository: NewMemoryTournamentRepository()}
	audit := NewMemoryAudit()
	life := &stubLifecycleRepo{}
	svc := NewService(ServiceDeps{Repo: repo, Lifecycle: life, Audit: audit})

	cases := []struct {
		name  string
		cmdID string
		typ   string
		body  json.RawMessage
	}{
		{"empty_complete", "lc-empty", CmdCompleteTournament, json.RawMessage(``)},
		{"null_cancel", "lc-null", CmdCancelTournament, json.RawMessage(`null`)},
		{"array", "lc-arr", CmdCompleteTournament, json.RawMessage(`[]`)},
		{"malformed", "lc-bad", CmdCancelTournament, json.RawMessage(`{not-json`)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := svc.SubmitCommand(context.Background(), CommandRequest{
				CommandID: tc.cmdID, Type: tc.typ, SchemaVersion: envelope.CurrentSchemaVersion,
				Payload: tc.body,
			}, "corr")
			if err != nil {
				t.Fatal(err)
			}
			if res.Status != envelope.StatusRejected || res.Reason != "invalid_payload" {
				t.Fatalf("got %+v", res)
			}
			again, err := svc.SubmitCommand(context.Background(), CommandRequest{
				CommandID: tc.cmdID, Type: tc.typ, SchemaVersion: envelope.CurrentSchemaVersion,
				Payload: tc.body,
			}, "corr")
			if err != nil {
				t.Fatal(err)
			}
			if again.Status != envelope.StatusRejected || again.Reason != "invalid_payload" {
				t.Fatalf("replay %+v", again)
			}
		})
	}
	if repo.beginExistingCalls.Load() != 0 {
		t.Fatalf("legacy BeginExisting called %d times", repo.beginExistingCalls.Load())
	}
	if life.standaloneCalls.Load() < 1 {
		t.Fatal("expected standalone rejects")
	}
}

func TestSubmitCommand_LifecycleDifferential_CompleteSuccessNoLegacy(t *testing.T) {
	repo := &countingBeginExistingRepo{TournamentRepository: NewMemoryTournamentRepository()}
	life := &stubLifecycleRepo{
		completeCtx: domain.CompleteTournamentContext{
			TournamentID: "t1", Exists: true, Phase: domain.PhaseInProgress,
			CurrentRound: 1, RoundFound: true, IsFinal: true, RoundCompleted: true,
			FinalSlotCount: 1, FinalSlotIndex: 0,
			FinalStandings: []domain.PlayerID{"champ", "p1"},
		},
	}
	svc := NewService(ServiceDeps{Repo: repo, Lifecycle: life, Audit: NoopAudit{}})
	payload, _ := json.Marshal(map[string]any{"tournamentId": "t1"})
	res, err := svc.SubmitCommand(context.Background(), CommandRequest{
		CommandID: "ct-1", Type: CmdCompleteTournament, SchemaVersion: envelope.CurrentSchemaVersion,
		Payload: payload,
	}, "ct-1")
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != envelope.StatusAccepted {
		t.Fatalf("got %+v", res)
	}
	if repo.beginExistingCalls.Load() != 0 {
		t.Fatal("CompleteTournament must not call BeginExisting when Lifecycle wired")
	}
	if life.completeCalls.Load() != 1 || life.commitCalls.Load() != 1 {
		t.Fatalf("complete=%d commit=%d", life.completeCalls.Load(), life.commitCalls.Load())
	}
}

func TestSubmitCommand_LifecycleDifferential_CancelSuccessNoLegacy(t *testing.T) {
	repo := &countingBeginExistingRepo{TournamentRepository: NewMemoryTournamentRepository()}
	life := &stubLifecycleRepo{
		cancelCtx: domain.CancelTournamentContext{
			TournamentID: "t1", Exists: true, Phase: domain.PhaseRegistration,
		},
	}
	svc := NewService(ServiceDeps{Repo: repo, Lifecycle: life, Audit: NoopAudit{}})
	payload, _ := json.Marshal(map[string]any{"tournamentId": "t1"})
	res, err := svc.SubmitCommand(context.Background(), CommandRequest{
		CommandID: "cancel-1", Type: CmdCancelTournament, SchemaVersion: envelope.CurrentSchemaVersion,
		Payload: payload,
	}, "cancel-1")
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != envelope.StatusAccepted {
		t.Fatalf("got %+v", res)
	}
	if repo.beginExistingCalls.Load() != 0 {
		t.Fatal("CancelTournament must not call BeginExisting when Lifecycle wired")
	}
	if life.cancelCalls.Load() != 1 {
		t.Fatalf("cancelCalls=%d", life.cancelCalls.Load())
	}
}

func TestSubmitCommand_LifecycleMemoryFallbackStillUsesLegacy(t *testing.T) {
	repo := &countingBeginExistingRepo{TournamentRepository: NewMemoryTournamentRepository()}
	svc := NewService(ServiceDeps{Repo: repo, Audit: NoopAudit{}})
	// Create + cancel via memory aggregate path.
	createPayload, _ := json.Marshal(map[string]any{"tournamentId": "t-mem", "capacity": 4})
	_, err := svc.SubmitCommand(context.Background(), CommandRequest{
		CommandID: "c-mem", Type: CmdCreateTournament, SchemaVersion: envelope.CurrentSchemaVersion,
		Payload: createPayload,
	}, "c-mem")
	if err != nil {
		t.Fatal(err)
	}
	cancelPayload, _ := json.Marshal(map[string]any{"tournamentId": "t-mem"})
	res, err := svc.SubmitCommand(context.Background(), CommandRequest{
		CommandID: "cancel-mem", Type: CmdCancelTournament, SchemaVersion: envelope.CurrentSchemaVersion,
		Payload: cancelPayload,
	}, "cancel-mem")
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != envelope.StatusAccepted {
		t.Fatalf("got %+v", res)
	}
	if repo.beginExistingCalls.Load() < 1 {
		t.Fatal("memory fallback must use BeginExisting for CancelTournament")
	}
}
