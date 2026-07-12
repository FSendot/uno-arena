package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/services/tournament-orchestration/store"
	"unoarena/shared/envelope"
)

type stubCompleteRoundRepo struct {
	standaloneCalls atomic.Int32
	beginCalls      atomic.Int32
	rollbackCalls   atomic.Int32
	commitCalls     atomic.Int32
	outcomes        map[string]envelope.Result
	loaded          domain.CompleteRoundContext
	beginErr        error
}

func (s *stubCompleteRoundRepo) LookupOutcome(commandID string) (envelope.Result, bool) {
	if s.outcomes == nil {
		return envelope.Result{}, false
	}
	out, ok := s.outcomes[commandID]
	return out, ok
}

func (s *stubCompleteRoundRepo) BeginStandaloneCommand(ctx context.Context, commandID string) (CompleteRoundUnitOfWork, error) {
	s.standaloneCalls.Add(1)
	return &stubCompleteRoundUoW{commandID: commandID, repo: s}, nil
}

func (s *stubCompleteRoundRepo) BeginCompleteRound(ctx context.Context, tournamentID string, roundNumber int, commandID string) (CompleteRoundUnitOfWork, error) {
	s.beginCalls.Add(1)
	if s.beginErr != nil {
		return nil, s.beginErr
	}
	loaded := s.loaded
	if loaded.TournamentID == "" {
		loaded.TournamentID = domain.TournamentID(tournamentID)
	}
	if loaded.RoundNumber == 0 {
		loaded.RoundNumber = roundNumber
	}
	return &stubCompleteRoundUoW{commandID: commandID, repo: s, loaded: loaded, exists: loaded.Exists}, nil
}

func (s *stubCompleteRoundRepo) FindReadyRoundCandidate(ctx context.Context) (*store.ReadyRoundCandidate, error) {
	return nil, nil
}

type stubCompleteRoundUoW struct {
	commandID string
	repo      *stubCompleteRoundRepo
	exists    bool
	loaded    domain.CompleteRoundContext
	done      bool
}

func (u *stubCompleteRoundUoW) Exists() bool { return u.exists }
func (u *stubCompleteRoundUoW) Loaded() domain.CompleteRoundContext {
	return u.loaded
}
func (u *stubCompleteRoundUoW) LookupOutcome(commandID string) (envelope.Result, bool) {
	if commandID != u.commandID {
		return envelope.Result{}, false
	}
	return u.repo.LookupOutcome(commandID)
}
func (u *stubCompleteRoundUoW) Rollback() error {
	u.repo.rollbackCalls.Add(1)
	u.done = true
	return nil
}
func (u *stubCompleteRoundUoW) Commit(req CompleteRoundCommitRequest) error {
	u.repo.commitCalls.Add(1)
	if u.repo.outcomes == nil {
		u.repo.outcomes = map[string]envelope.Result{}
	}
	u.repo.outcomes[req.CommandID] = req.Outcome
	u.done = true
	return nil
}

func TestSubmitCommand_CompleteRoundDifferential_InvalidPayloadStableRejection(t *testing.T) {
	repo := NewMemoryTournamentRepository()
	audit := NewMemoryAudit()
	cr := &stubCompleteRoundRepo{}
	svc := NewService(ServiceDeps{Repo: repo, CompleteRounds: cr, Audit: audit})

	cases := []struct {
		name    string
		cmdID   string
		payload json.RawMessage
	}{
		{"malformed", "cr-bad-json", json.RawMessage(`{not-json`)},
		{"array", "cr-array-payload", json.RawMessage(`[]`)},
		{"string", "cr-string-payload", json.RawMessage(`"x"`)},
		{"null", "cr-null-payload", json.RawMessage(`null`)},
		{"empty", "cr-empty-payload", json.RawMessage(``)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			beforeStandalone := cr.standaloneCalls.Load()
			beforeBegin := cr.beginCalls.Load()
			beforeAudit := audit.Len()
			res, err := svc.SubmitCommand(context.Background(), CommandRequest{
				CommandID: tc.cmdID, Type: CmdCompleteRound, SchemaVersion: envelope.CurrentSchemaVersion,
				Payload: tc.payload,
			}, "corr")
			if err != nil {
				t.Fatalf("must not return Go error: %v", err)
			}
			if res.Status != envelope.StatusRejected || res.Reason != "invalid_payload" {
				t.Fatalf("got status=%s reason=%s", res.Status, res.Reason)
			}
			if cr.standaloneCalls.Load() != beforeStandalone+1 {
				t.Fatalf("standaloneCalls delta want 1 got %d", cr.standaloneCalls.Load()-beforeStandalone)
			}
			if cr.beginCalls.Load() != beforeBegin {
				t.Fatalf("BeginCompleteRound must not run; beginCalls=%d", cr.beginCalls.Load())
			}
			if audit.Len() != beforeAudit+1 {
				t.Fatalf("audit delta want 1 got %d", audit.Len()-beforeAudit)
			}
			replay, err := svc.SubmitCommand(context.Background(), CommandRequest{
				CommandID: tc.cmdID, Type: CmdCompleteRound, SchemaVersion: envelope.CurrentSchemaVersion,
				Payload: json.RawMessage(`{"tournamentId":"t1","roundNumber":1}`),
			}, "corr-replay")
			if err != nil {
				t.Fatal(err)
			}
			if replay.Status != res.Status || replay.Reason != res.Reason || replay.CommandID != res.CommandID {
				t.Fatalf("replay mismatch first=%+v replay=%+v", res, replay)
			}
		})
	}

	_, err := svc.SubmitCommand(context.Background(), CommandRequest{
		CommandID: "cr-schema", Type: CmdCompleteRound, SchemaVersion: envelope.CurrentSchemaVersion + 1,
		Payload: json.RawMessage(`{"tournamentId":"t1","roundNumber":1}`),
	}, "corr")
	if err == nil {
		t.Fatal("schema mismatch must return Go error")
	}
}

func TestTryCompleteReadyRound_RejectRollsBackWithoutPersist(t *testing.T) {
	cr := &stubCompleteRoundRepo{
		loaded: domain.CompleteRoundContext{
			TournamentID:  "t-try",
			Exists:        true,
			Phase:         domain.PhaseInProgress,
			RoundNumber:   1,
			RoundFound:    true,
			RoundStatus:   domain.RoundInProgress,
			AssignedCount: 0, // incomplete → reject
		},
	}
	svc := NewService(ServiceDeps{Repo: NewMemoryTournamentRepository(), CompleteRounds: cr, Audit: NewMemoryAudit()})
	res, err := svc.TryCompleteReadyRound(context.Background(), "t-try", 1)
	if !errors.Is(err, ErrCompleteRoundNotReady) {
		t.Fatalf("want ErrCompleteRoundNotReady, got res=%+v err=%v", res, err)
	}
	var nr *CompleteRoundNotReadyError
	if !errors.As(err, &nr) || nr.Reason != string(domain.RejectRoundIncomplete) {
		t.Fatalf("want typed incomplete reason, got %v", err)
	}
	if cr.commitCalls.Load() != 0 {
		t.Fatalf("reject must not commit, commits=%d", cr.commitCalls.Load())
	}
	if cr.beginCalls.Load() != 1 {
		t.Fatalf("beginCalls=%d", cr.beginCalls.Load())
	}
	cmdID := domain.CompleteRoundCommandID("t-try", 1)
	if _, ok := cr.LookupOutcome(cmdID); ok {
		t.Fatal("must not persist command outcome on not-ready")
	}
}

func TestTryCompleteReadyRound_SuccessPersistsOnce(t *testing.T) {
	cr := &stubCompleteRoundRepo{
		loaded: domain.CompleteRoundContext{
			TournamentID:              "t-ok",
			Exists:                    true,
			Phase:                     domain.PhaseInProgress,
			RoundNumber:               1,
			RoundFound:                true,
			RoundStatus:               domain.RoundInProgress,
			AssignedCount:             3,
			ResolvedCount:             3,
			AdvancingCount:             9,
			AdvancementRecordsPlayers:  9,
			NormalizedAdvancingPlayers: 9,
		},
	}
	svc := NewService(ServiceDeps{Repo: NewMemoryTournamentRepository(), CompleteRounds: cr, Audit: NoopAudit{}})
	res, err := svc.TryCompleteReadyRound(context.Background(), "t-ok", 1)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != envelope.StatusAccepted {
		t.Fatalf("want accepted, got %+v", res)
	}
	if cr.commitCalls.Load() != 1 {
		t.Fatalf("commits=%d", cr.commitCalls.Load())
	}
	cmdID := domain.CompleteRoundCommandID("t-ok", 1)
	prior, ok := cr.LookupOutcome(cmdID)
	if !ok || prior.Status != envelope.StatusAccepted {
		t.Fatalf("expected persisted accepted outcome")
	}
	// Second try returns prior without another commit.
	again, err := svc.TryCompleteReadyRound(context.Background(), "t-ok", 1)
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != envelope.StatusAccepted {
		t.Fatalf("replay=%+v", again)
	}
	if cr.beginCalls.Load() != 1 {
		t.Fatalf("prior lookup must short-circuit begin; beginCalls=%d", cr.beginCalls.Load())
	}
}
