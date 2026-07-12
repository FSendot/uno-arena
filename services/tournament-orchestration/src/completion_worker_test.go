package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/services/tournament-orchestration/store"
	"unoarena/shared/envelope"
)

type fakeCompletionCandidates struct {
	calls atomic.Int64
	seq   []*store.ReadyRoundCandidate
	idx   int
	mu    sync.Mutex
}

func (f *fakeCompletionCandidates) FindReadyRoundCandidate(ctx context.Context) (*store.ReadyRoundCandidate, error) {
	f.calls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.idx >= len(f.seq) {
		return nil, nil
	}
	c := f.seq[f.idx]
	f.idx++
	return c, nil
}

type fakeCompletionCommands struct {
	calls     atomic.Int64
	mu        sync.Mutex
	byKey     map[string]int
	results   map[string]envelope.Result
	errs      map[string]error
	lastTID   string
	lastRound int
}

func (f *fakeCompletionCommands) TryCompleteReadyRound(ctx context.Context, tournamentID string, roundNumber int) (envelope.Result, error) {
	f.calls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastTID = tournamentID
	f.lastRound = roundNumber
	key := domain.CompleteRoundCommandID(domain.TournamentID(tournamentID), roundNumber)
	if f.byKey == nil {
		f.byKey = map[string]int{}
	}
	f.byKey[key]++
	if f.errs != nil {
		if err, ok := f.errs[key]; ok {
			return envelope.Result{}, err
		}
	}
	if f.results != nil {
		if r, ok := f.results[key]; ok {
			return r, nil
		}
	}
	return envelope.Accepted(key, CmdCompleteRound, nil, json.RawMessage(`{"facts":[]}`)), nil
}

func TestCompletionWorker_OneCandidatePerTick(t *testing.T) {
	cands := &fakeCompletionCandidates{
		seq: []*store.ReadyRoundCandidate{
			{TournamentID: "t-a", RoundNumber: 1},
			{TournamentID: "t-b", RoundNumber: 1},
		},
	}
	cmds := &fakeCompletionCommands{}
	w := NewCompletionWorker(cands, cmds)
	w.tick()
	if cands.calls.Load() != 1 || cmds.calls.Load() != 1 {
		t.Fatalf("tick candidate=%d try=%d", cands.calls.Load(), cmds.calls.Load())
	}
	w.tick()
	if cands.calls.Load() != 2 || cmds.calls.Load() != 2 {
		t.Fatalf("second tick candidate=%d try=%d", cands.calls.Load(), cmds.calls.Load())
	}
	want := domain.CompleteRoundCommandID("t-a", 1)
	if cmds.byKey[want] != 1 {
		t.Fatalf("deterministic commandId %q count=%d", want, cmds.byKey[want])
	}
	if cmds.lastTID != "t-b" || cmds.lastRound != 1 {
		t.Fatalf("last try tid=%s round=%d", cmds.lastTID, cmds.lastRound)
	}
}

func TestCompletionWorker_TwoReplicasIdempotentSameCommandID(t *testing.T) {
	cand := &store.ReadyRoundCandidate{TournamentID: "t-idem", RoundNumber: 2}
	cands1 := &fakeCompletionCandidates{seq: []*store.ReadyRoundCandidate{cand}}
	cands2 := &fakeCompletionCandidates{seq: []*store.ReadyRoundCandidate{cand}}
	cmds := &fakeCompletionCommands{
		results: map[string]envelope.Result{},
	}
	cmdID := domain.CompleteRoundCommandID("t-idem", 2)
	accepted := envelope.Accepted(cmdID, CmdCompleteRound, nil, json.RawMessage(`{"facts":[{"name":"TournamentRoundCompleted"}]}`))
	cmds.results[cmdID] = accepted

	w1 := NewCompletionWorker(cands1, cmds)
	w2 := NewCompletionWorker(cands2, cmds)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); w1.tick() }()
	go func() { defer wg.Done(); w2.tick() }()
	wg.Wait()
	if cmds.byKey[cmdID] != 2 {
		t.Fatalf("both replicas must invoke same commandId, got %d", cmds.byKey[cmdID])
	}
}

func TestCompletionWorker_RetryableNotReadyDoesNotPanic(t *testing.T) {
	cands := &fakeCompletionCandidates{
		seq: []*store.ReadyRoundCandidate{{TournamentID: "t-nr", RoundNumber: 1}},
	}
	cmdID := domain.CompleteRoundCommandID("t-nr", 1)
	cmds := &fakeCompletionCommands{
		errs: map[string]error{
			cmdID: &CompleteRoundNotReadyError{Reason: string(domain.RejectRoundIncomplete)},
		},
	}
	w := NewCompletionWorker(cands, cmds)
	w.tick()
	if cmds.calls.Load() != 1 {
		t.Fatal("expected try")
	}
}

func TestCompletionWorker_RejectedPriorIsSurfaced(t *testing.T) {
	cands := &fakeCompletionCandidates{
		seq: []*store.ReadyRoundCandidate{{TournamentID: "t-rej", RoundNumber: 1}},
	}
	cmdID := domain.CompleteRoundCommandID("t-rej", 1)
	cmds := &fakeCompletionCommands{
		results: map[string]envelope.Result{
			cmdID: envelope.Rejected(cmdID, CmdCompleteRound, string(domain.RejectRoundIncomplete), nil),
		},
	}
	w := NewCompletionWorker(cands, cmds)
	w.tick() // must not silently ignore; logs completion_rejected_prior
	if cmds.byKey[cmdID] != 1 {
		t.Fatal("expected try")
	}
}

func TestCompletionWorker_StopWaitsInflight(t *testing.T) {
	cands := &fakeCompletionCandidates{
		seq: []*store.ReadyRoundCandidate{{TournamentID: "t1", RoundNumber: 1}},
	}
	cmds := &fakeCompletionCommands{}
	w := NewCompletionWorker(cands, cmds)
	w.pollInterval = 20 * time.Millisecond
	w.Start()
	deadline := time.Now().Add(2 * time.Second)
	for cmds.calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if cmds.calls.Load() == 0 {
		t.Fatal("expected try")
	}
	w.Stop()
	w.Stop()
}

func TestWireCompletionWorkerRequiresDatabaseURL(t *testing.T) {
	t.Setenv("WORKER_ROLE", workerRoleTournamentCompletion)
	t.Setenv("DATABASE_URL", "")
	_, err := wireTournamentRuntime()
	if err == nil {
		t.Fatal("expected error without DATABASE_URL")
	}
}

func TestCompleteRoundNotReadyError_Unwrap(t *testing.T) {
	err := &CompleteRoundNotReadyError{Reason: "quarantined"}
	if !errors.Is(err, ErrCompleteRoundNotReady) {
		t.Fatal("must unwrap to ErrCompleteRoundNotReady")
	}
}
