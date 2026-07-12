package main

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"unoarena/services/tournament-orchestration/store"
	"unoarena/shared/envelope"
)

const (
	defaultCompletionPollInterval  = 500 * time.Millisecond
	workerRoleTournamentCompletion = "tournament-round-completion"
)

// CompletionCandidateStore finds ready-round hints for the completion worker.
type CompletionCandidateStore interface {
	FindReadyRoundCandidate(ctx context.Context) (*store.ReadyRoundCandidate, error)
}

// CompletionCommandRunner invokes the non-poisoning CompleteRound try path.
// Transient reject/not-ready must not persist command_idempotency for the deterministic id.
type CompletionCommandRunner interface {
	TryCompleteReadyRound(ctx context.Context, tournamentID string, roundNumber int) (envelope.Result, error)
}

// CompletionWorker polls one ready-round candidate per tick and completes it
// with deterministic commandId complete:{tournamentId}:r{roundNumber}.
type CompletionWorker struct {
	candidates   CompletionCandidateStore
	commands     CompletionCommandRunner
	pollInterval time.Duration

	stopCh   chan struct{}
	doneCh   chan struct{}
	inflight sync.WaitGroup
	stopped  atomic.Bool
}

// NewCompletionWorker constructs a tournament round-completion worker.
func NewCompletionWorker(candidates CompletionCandidateStore, commands CompletionCommandRunner) *CompletionWorker {
	return &CompletionWorker{
		candidates:   candidates,
		commands:     commands,
		pollInterval: defaultCompletionPollInterval,
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
	}
}

// Start begins the poll/complete loop in a background goroutine.
func (w *CompletionWorker) Start() {
	go w.loop()
}

// Stop stops new polls and waits for in-flight CompleteRound work.
func (w *CompletionWorker) Stop() {
	if w == nil || w.stopped.Swap(true) {
		return
	}
	close(w.stopCh)
	<-w.doneCh
	w.inflight.Wait()
}

func (w *CompletionWorker) loop() {
	defer close(w.doneCh)
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.tick()
		}
	}
}

func (w *CompletionWorker) tick() {
	if w.stopped.Load() {
		return
	}
	cand, err := w.candidates.FindReadyRoundCandidate(context.Background())
	if err != nil {
		log.Printf(`{"level":"warn","service":"tournament-orchestration","event":"completion_candidate_failed","err":%q}`, sanitizeCompletionErr(err))
		return
	}
	if cand == nil {
		return
	}
	if w.stopped.Load() {
		return
	}
	w.inflight.Add(1)
	defer w.inflight.Done()

	res, err := w.commands.TryCompleteReadyRound(context.Background(), cand.TournamentID, cand.RoundNumber)
	if err != nil {
		if errors.Is(err, ErrCompleteRoundNotReady) {
			reason := ""
			var nr *CompleteRoundNotReadyError
			if errors.As(err, &nr) && nr != nil {
				reason = nr.Reason
			}
			log.Printf(`{"level":"info","service":"tournament-orchestration","event":"completion_not_ready","tournamentId":%q,"roundNumber":%d,"reason":%q}`,
				cand.TournamentID, cand.RoundNumber, reason)
			return
		}
		log.Printf(`{"level":"warn","service":"tournament-orchestration","event":"completion_try_failed","tournamentId":%q,"roundNumber":%d,"err":%q}`,
			cand.TournamentID, cand.RoundNumber, sanitizeCompletionErr(err))
		return
	}
	// Prior persisted reject (e.g. manual poison of deterministic id): surface it —
	// do not silently ignore rejected envelopes while the hint keeps reappearing.
	if res.Status == envelope.StatusRejected {
		log.Printf(`{"level":"warn","service":"tournament-orchestration","event":"completion_rejected_prior","tournamentId":%q,"roundNumber":%d,"commandId":%q,"reason":%q}`,
			cand.TournamentID, cand.RoundNumber, res.CommandID, res.Reason)
	}
}

func sanitizeCompletionErr(err error) string {
	if err == nil {
		return ""
	}
	return sanitizeDLQErrorSummary(err.Error())
}
