package main

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/shared/envelope"
)

type stubQuarantineResultRepo struct {
	beginCalls      atomic.Int32
	standaloneCalls atomic.Int32
	commitCalls     atomic.Int32
	outcomes        map[string]envelope.Result
	loaded          domain.QuarantineTournamentResultContext
}

func (s *stubQuarantineResultRepo) LookupOutcome(commandID string) (envelope.Result, bool) {
	if s.outcomes == nil {
		return envelope.Result{}, false
	}
	out, ok := s.outcomes[commandID]
	return out, ok
}

func (s *stubQuarantineResultRepo) BeginStandaloneCommand(ctx context.Context, commandID string) (QuarantineResultUnitOfWork, error) {
	s.standaloneCalls.Add(1)
	return &stubQuarantineResultUoW{commandID: commandID, repo: s}, nil
}

func (s *stubQuarantineResultRepo) BeginQuarantineTournamentResult(ctx context.Context, tournamentID, roomID string, completionVersion uint64, commandID string) (QuarantineResultUnitOfWork, error) {
	s.beginCalls.Add(1)
	loaded := s.loaded
	if loaded.TournamentID == "" {
		loaded.TournamentID = domain.TournamentID(tournamentID)
	}
	if !loaded.Exists && loaded.TournamentID != "" {
		loaded.Exists = true
	}
	loaded.RoomID = domain.RoomID(roomID)
	loaded.CompletionVersion = domain.CompletionVersion(completionVersion)
	return &stubQuarantineResultUoW{commandID: commandID, repo: s, loaded: loaded, exists: loaded.Exists}, nil
}

type stubQuarantineResultUoW struct {
	commandID string
	repo      *stubQuarantineResultRepo
	exists    bool
	loaded    domain.QuarantineTournamentResultContext
	done      bool
}

func (u *stubQuarantineResultUoW) Exists() bool { return u.exists }
func (u *stubQuarantineResultUoW) Loaded() domain.QuarantineTournamentResultContext {
	return u.loaded
}
func (u *stubQuarantineResultUoW) LookupOutcome(commandID string) (envelope.Result, bool) {
	if commandID != u.commandID {
		return envelope.Result{}, false
	}
	return u.repo.LookupOutcome(commandID)
}
func (u *stubQuarantineResultUoW) Rollback() error {
	u.done = true
	return nil
}
func (u *stubQuarantineResultUoW) Commit(req QuarantineResultCommitRequest) error {
	u.repo.commitCalls.Add(1)
	if u.repo.outcomes == nil {
		u.repo.outcomes = map[string]envelope.Result{}
	}
	u.repo.outcomes[req.CommandID] = req.Outcome
	u.done = true
	return nil
}

func TestSubmitCommand_QuarantineResultDifferential_NoLegacyBeginExisting(t *testing.T) {
	repo := &countingBeginExistingRepo{TournamentRepository: NewMemoryTournamentRepository()}
	qr := &stubQuarantineResultRepo{
		loaded: domain.QuarantineTournamentResultContext{
			TournamentID: "t-qr", Exists: true, RoomID: "room-1",
		},
	}
	svc := NewService(ServiceDeps{Repo: repo, QuarantineResults: qr, Audit: NewMemoryAudit()})
	payload, _ := json.Marshal(map[string]any{
		"tournamentId": "t-qr", "roomId": "room-1", "completionVersion": 2,
		"reason": "operator hold NEVER",
	})
	out, err := svc.SubmitCommand(context.Background(), CommandRequest{
		CommandID: "qr-1", Type: CmdQuarantineResult, SchemaVersion: 1, Payload: payload,
	}, "corr")
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != envelope.StatusAccepted {
		t.Fatalf("%+v", out)
	}
	if repo.beginExistingCalls.Load() != 0 {
		t.Fatalf("legacy BeginExisting called %d times", repo.beginExistingCalls.Load())
	}
	if qr.beginCalls.Load() != 1 || qr.commitCalls.Load() != 1 {
		t.Fatalf("begin=%d commit=%d", qr.beginCalls.Load(), qr.commitCalls.Load())
	}
	body, _ := json.Marshal(out.Payload)
	if strings.Contains(string(body), "operator") || strings.Contains(string(body), "NEVER") {
		t.Fatalf("raw reason leaked into outcome: %s", body)
	}
}

func TestSubmitCommand_QuarantineResult_MemoryFallbackUsesBeginExisting(t *testing.T) {
	repo := &countingBeginExistingRepo{TournamentRepository: NewMemoryTournamentRepository()}
	svc := NewService(ServiceDeps{Repo: repo, Audit: NewMemoryAudit()})
	// Create tournament first via memory path.
	createPayload, _ := json.Marshal(map[string]any{
		"tournamentId": "t-mem-qr", "capacity": 12, "retryBudget": 3, "batchSize": 2,
	})
	if _, err := svc.SubmitCommand(context.Background(), CommandRequest{
		CommandID: "c0", Type: CmdCreateTournament, SchemaVersion: 1, Payload: createPayload,
	}, "corr"); err != nil {
		t.Fatal(err)
	}
	repo.beginExistingCalls.Store(0)
	payload, _ := json.Marshal(map[string]any{
		"tournamentId": "t-mem-qr", "roomId": "room-x", "completionVersion": 1,
	})
	if _, err := svc.SubmitCommand(context.Background(), CommandRequest{
		CommandID: "qr-mem", Type: CmdQuarantineResult, SchemaVersion: 1, Payload: payload,
	}, "corr"); err != nil {
		t.Fatal(err)
	}
	if repo.beginExistingCalls.Load() == 0 {
		t.Fatal("memory fallback must use BeginExisting for QuarantineTournamentResult")
	}
}

func TestSubmitCommand_QuarantineResult_ZeroVersionRejectNoLegacy(t *testing.T) {
	repo := &countingBeginExistingRepo{TournamentRepository: NewMemoryTournamentRepository()}
	qr := &stubQuarantineResultRepo{}
	svc := NewService(ServiceDeps{Repo: repo, QuarantineResults: qr, Audit: NewMemoryAudit()})
	payload, _ := json.Marshal(map[string]any{
		"tournamentId": "t-qr", "roomId": "room-1", "completionVersion": 0,
	})
	out, err := svc.SubmitCommand(context.Background(), CommandRequest{
		CommandID: "qr-0", Type: CmdQuarantineResult, SchemaVersion: 1, Payload: payload,
	}, "corr")
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != envelope.StatusRejected {
		t.Fatalf("%+v", out)
	}
	if repo.beginExistingCalls.Load() != 0 {
		t.Fatal("zero version must not use legacy BeginExisting")
	}
	if qr.standaloneCalls.Load() != 1 {
		t.Fatalf("standalone=%d", qr.standaloneCalls.Load())
	}
}
