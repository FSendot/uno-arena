package main

import (
	"context"
	"errors"
	"log/slog"
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
		slog.Warn("completion candidate lookup failed", "event", "completion_candidate_failed", "error", sanitizeCompletionErr(err))
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
			slog.Info("completion candidate not ready", "event", "completion_not_ready",
				"tournamentId", cand.TournamentID, "roundNumber", cand.RoundNumber, "reason", reason)
			return
		}
		slog.Warn("completion attempt failed", "event", "completion_try_failed",
			"tournamentId", cand.TournamentID, "roundNumber", cand.RoundNumber, "error", sanitizeCompletionErr(err))
		return
	}
	// Prior persisted reject (e.g. manual poison of deterministic id): surface it —
	// do not silently ignore rejected envelopes while the hint keeps reappearing.
	if res.Status == envelope.StatusRejected {
		slog.Warn("completion used prior rejection", "event", "completion_rejected_prior",
			"tournamentId", cand.TournamentID, "roundNumber", cand.RoundNumber, "commandId", res.CommandID, "reason", res.Reason)
	}
}

func sanitizeCompletionErr(err error) string {
	if err == nil {
		return ""
	}
	return sanitizeDLQErrorSummary(err.Error())
}
