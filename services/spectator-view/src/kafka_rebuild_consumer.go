package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"unoarena/services/spectator-view/domain"
	"unoarena/services/spectator-view/store"
)

// ProjectionRebuildExecutor performs the ADR-0039 recovery pipeline for one request.
type ProjectionRebuildExecutor interface {
	ExecuteRebuild(ctx context.Context, req ParsedProjectionRebuildRequest) error
}

// atomicRebuildDoneMarker is implemented by executors that persist the declared
// idempotency key atomically with owned-state mutation (no separate MarkDone).
type atomicRebuildDoneMarker interface {
	MarksRebuildDoneAtomically() bool
}

// StoreBackedProjectionRebuildExecutor wires Room snapshot + held DLQ + Redis CAS rebuild.
type StoreBackedProjectionRebuildExecutor struct {
	Room    RoomSpectatorRecoveryClient
	Held    HeldSpectatorRecordSource
	Store   *store.RedisProjectionStore
	Release store.RecoveryRelease
}

// MarksRebuildDoneAtomically reports that success writes the AsyncAPI idempotency
// marker inside the same Redis Lua transaction as the fenced swap + quarantine release.
func (e *StoreBackedProjectionRebuildExecutor) MarksRebuildDoneAtomically() bool {
	return true
}

func (e *StoreBackedProjectionRebuildExecutor) ExecuteRebuild(ctx context.Context, req ParsedProjectionRebuildRequest) error {
	if e == nil || e.Room == nil || e.Held == nil || e.Store == nil {
		return fmt.Errorf("rebuild executor not configured")
	}
	snap, err := e.Room.FetchSpectatorRecoverySnapshot(ctx, req.RoomID, req.FailedCheckpoint, req.RecoveryJobID)
	if err != nil {
		return err
	}
	snapshotEvt := snap.ToSnapshotSanitizedEvent()

	held, err := e.Held.LoadHeldAfterCheckpoint(ctx, HeldSpectatorRecordQuery{
		RoomID:           req.RoomID,
		RecoveryJobID:    req.RecoveryJobID,
		FailedCheckpoint: req.FailedCheckpoint,
		ResumeCheckpoint: snap.ResumeCheckpoint,
	})
	if err != nil {
		return err
	}

	fenceGen, fenceSeq, err := e.Store.CurrentFence(ctx, domain.RoomID(req.RoomID))
	if err != nil {
		return err
	}
	release := e.Release
	if !release.Enabled {
		release = store.RecoveryRelease{
			Enabled: true,
			Note:    store.ReleaseNoteRecoveryContinuityProven,
		}
	}

	res, err := e.Store.RecoveryRebuildFromSnapshotWithIdempotency(
		ctx,
		domain.RoomID(req.RoomID),
		snapshotEvt,
		fenceGen,
		fenceSeq,
		held,
		release,
		store.RecoveryIdempotency{Identity: req.IdempotencyKey()},
	)
	if err != nil {
		return err
	}
	if res.Stale || res.AlreadyDone {
		// Newer live Apply won, or identical job already completed — success/no-op; do not DLQ.
		return nil
	}
	return nil
}

// ProjectionRebuildKafkaConsumer owns spectator.projection.rebuild_requested consumption.
type ProjectionRebuildKafkaConsumer struct {
	source KafkaRecordSource
	dlq    DLQPublisher
	exec   ProjectionRebuildExecutor
	idemp  RebuildIdempotencyStore
	cfg    ProjectionRebuildKafkaConfig
	clock  Clock
	sleep  func(ctx context.Context, d time.Duration) error
}

// Run polls until ctx is cancelled. Source offsets commit only after success or DLQ ack.
func (c *ProjectionRebuildKafkaConsumer) Run(ctx context.Context) error {
	if c == nil || c.source == nil {
		return fmt.Errorf("projection rebuild consumer not configured")
	}
	var pending []ConsumerRecord
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(pending) == 0 {
			recs, err := c.source.Poll(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return err
				}
				if sleepErr := c.doSleep(ctx, c.cfg.RetryBackoff); sleepErr != nil {
					return sleepErr
				}
				continue
			}
			if len(recs) == 0 {
				continue
			}
			pending = recs
		}
		if err := c.ProcessBatch(ctx, pending); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			if sleepErr := c.doSleep(ctx, c.cfg.RetryBackoff); sleepErr != nil {
				return sleepErr
			}
			continue
		}
		pending = nil
	}
}

// ProcessBatch handles rebuild requests serially (room-keyed control plane; low volume).
func (c *ProjectionRebuildKafkaConsumer) ProcessBatch(ctx context.Context, recs []ConsumerRecord) error {
	for _, rec := range recs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := c.processOne(ctx, rec); err != nil {
			return err
		}
	}
	return nil
}

func (c *ProjectionRebuildKafkaConsumer) processOne(ctx context.Context, rec ConsumerRecord) error {
	sourceTopic := firstNonEmpty(rec.Topic, c.cfg.Topic)
	key := strings.TrimSpace(string(rec.Key))

	req, err := ParseSpectatorProjectionRebuildRequested(rec.Value)
	if err != nil {
		return c.terminalToDLQAndCommit(ctx, rec, err, 1, key)
	}
	if key == "" {
		return c.terminalToDLQAndCommit(ctx, rec,
			newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("kafka record key is required")),
			1, req.RoomID)
	}
	if key != req.RoomID {
		return c.terminalToDLQAndCommit(ctx, rec,
			newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("kafka key must equal roomId")),
			1, key)
	}

	idempKey := req.IdempotencyKey()
	if c.idemp != nil {
		done, err := c.idemp.AlreadyDone(ctx, idempKey)
		if err != nil {
			return err
		}
		if done {
			return c.source.Commit(ctx, rec)
		}
	}

	maxAttempts := c.cfg.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	var firstFail, lastFail time.Time
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := c.exec.ExecuteRebuild(ctx, req)
		now := c.now()
		if firstFail.IsZero() {
			firstFail = now
		}
		lastFail = now
		if err == nil {
			if c.idemp != nil {
				if marker, ok := c.exec.(atomicRebuildDoneMarker); !ok || !marker.MarksRebuildDoneAtomically() {
					if markErr := c.idemp.MarkDone(ctx, idempKey); markErr != nil {
						return markErr
					}
				}
			}
			return c.source.Commit(ctx, rec)
		}
		lastErr = err
		if IsTerminalKafkaConsumeError(err) || isTerminalRebuildError(err) {
			return c.failureToDLQAndCommit(ctx, rec, DLQFailureMeta{
				Consumer:        c.cfg.Group,
				SourceTopic:     sourceTopic,
				SourcePartition: rec.Partition,
				SourceOffset:    rec.Offset,
				AttemptCount:    attempt,
				Classification:  ClassifyKafkaConsumeError(err),
				FirstFailureAt:  firstFail,
				LastFailureAt:   lastFail,
				CorrelationID:   req.CorrelationID,
				ErrorSummary:    sanitizeDLQErrorSummary(err.Error()),
			})
		}
		if attempt == maxAttempts {
			break
		}
		if sleepErr := c.doSleep(ctx, c.cfg.RetryBackoff); sleepErr != nil {
			return sleepErr
		}
	}
	return c.failureToDLQAndCommit(ctx, rec, DLQFailureMeta{
		Consumer:        c.cfg.Group,
		SourceTopic:     sourceTopic,
		SourcePartition: rec.Partition,
		SourceOffset:    rec.Offset,
		AttemptCount:    maxAttempts,
		Classification:  ClassifyKafkaConsumeError(lastErr),
		FirstFailureAt:  firstFail,
		LastFailureAt:   lastFail,
		CorrelationID:   req.CorrelationID,
		ErrorSummary:    sanitizeDLQErrorSummary(lastErr.Error()),
	})
}

func isTerminalRebuildError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, domain.ErrHeldContinuityGap) ||
		errors.Is(err, domain.ErrHeldContinuityBound) ||
		errors.Is(err, domain.ErrHeldContinuityInvalid) ||
		errors.Is(err, domain.ErrRecoverySnapshotInvalid) ||
		errors.Is(err, domain.ErrRecoveryApplyFailed) ||
		errors.Is(err, store.ErrInvalidReleaseNote) {
		return true
	}
	// Hard CAS conflict (not stale) is terminal for this job shape.
	if errors.Is(err, store.ErrRecoveryConflict) {
		return true
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "room recovery status 404") ||
		strings.Contains(msg, "room recovery status 400") ||
		strings.Contains(msg, "room recovery status 401") {
		return true
	}
	return false
}

func (c *ProjectionRebuildKafkaConsumer) terminalToDLQAndCommit(ctx context.Context, rec ConsumerRecord, err error, attempts int, aggregateKey string) error {
	_ = aggregateKey
	now := c.now()
	return c.failureToDLQAndCommit(ctx, rec, DLQFailureMeta{
		Consumer:        c.cfg.Group,
		SourceTopic:     firstNonEmpty(rec.Topic, c.cfg.Topic),
		SourcePartition: rec.Partition,
		SourceOffset:    rec.Offset,
		AttemptCount:    attempts,
		Classification:  ClassifyKafkaConsumeError(err),
		FirstFailureAt:  now,
		LastFailureAt:   now,
		CorrelationID:   peekSafeCorrelationID(rec.Value),
		ErrorSummary:    sanitizeDLQErrorSummary(err.Error()),
	})
}

// failureToDLQAndCommit publishes DLQ BEFORE committing the source offset.
func (c *ProjectionRebuildKafkaConsumer) failureToDLQAndCommit(ctx context.Context, rec ConsumerRecord, meta DLQFailureMeta) error {
	if c.dlq == nil {
		return fmt.Errorf("dlq publisher not configured")
	}
	if err := c.dlq.PublishDLQ(ctx, rec, meta); err != nil {
		return fmt.Errorf("dlq publish: %w", err)
	}
	return c.source.Commit(ctx, rec)
}

func (c *ProjectionRebuildKafkaConsumer) now() time.Time {
	if c.clock != nil {
		return c.clock.Now().UTC()
	}
	return time.Now().UTC()
}

func (c *ProjectionRebuildKafkaConsumer) doSleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	if c.sleep != nil {
		return c.sleep(ctx, d)
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
