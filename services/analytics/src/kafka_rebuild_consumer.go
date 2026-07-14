package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel/propagation"

	"unoarena/services/analytics/domain"
	"unoarena/services/analytics/store"
)

// AnalyticsProjectionRebuildExecutor performs one Kafka rebuild-request page.
type AnalyticsProjectionRebuildExecutor interface {
	ExecutePage(ctx context.Context, req ParsedAnalyticsProjectionRebuildRequest) (RebuildPageResult, error)
}

// RebuildPageResult describes follow-up publish / finalize after a durable page.
type RebuildPageResult struct {
	AlreadyDone       bool
	Finalize          bool
	FollowUp          *ParsedAnalyticsProjectionRebuildRequest
	FollowUpEventID   string
	RepublishFollowUp bool
}

// StoreBackedAnalyticsProjectionRebuildExecutor pages producers under ClickHouse fences.
type StoreBackedAnalyticsProjectionRebuildExecutor struct {
	Store      analyticsRebuildStore
	Backfill   AnalyticsBackfillClient
	OwnerToken string
	LeaseTTL   time.Duration
	Clock      Clock
}

// analyticsRebuildStore is the ClickHouse-backed recovery surface used by the rebuild executor.
type analyticsRebuildStore interface {
	LoadRequestIdempotency(ctx context.Context, jobID, topic, pageCursor string) (store.RecoveryRequestIdempotency, bool, error)
	AcquireOrRenewLease(ctx context.Context, jobID, ownerToken string, ttl time.Duration) (store.RecoveryLease, error)
	EnsureRecoveryBuildingGeneration(ctx context.Context, spec store.RecoveryJobSpec) (string, error)
	ValidateLeaseOwnership(ctx context.Context, jobID, ownerToken string) error
	LoadPageCheckpoint(ctx context.Context, jobID, topic, pageCursor string) (store.RecoveryPageCheckpoint, bool, error)
	LoadRecoveryJob(ctx context.Context, jobID string) (store.RecoveryJobState, error)
	ApplyToGeneration(ctx context.Context, genID string, evt domain.UpstreamEvent) (domain.ApplyOutcome, error)
	MarkRecoveryFailed(ctx context.Context, ownerToken, jobID, status, code, summary, quarantineKey string) error
	PersistPageCheckpoint(ctx context.Context, ownerToken string, cp store.RecoveryPageCheckpoint) error
	ReconcileRecoveryJobProgressFromCheckpoint(ctx context.Context, ownerToken string, cp store.RecoveryPageCheckpoint) error
	ActivateRecoveryGeneration(ctx context.Context, ownerToken, jobID string) error
	PersistRequestIdempotency(ctx context.Context, ownerToken string, row store.RecoveryRequestIdempotency) error
}

func (e *StoreBackedAnalyticsProjectionRebuildExecutor) ExecutePage(ctx context.Context, req ParsedAnalyticsProjectionRebuildRequest) (RebuildPageResult, error) {
	if e == nil || e.Store == nil || e.Backfill == nil || e.OwnerToken == "" {
		return RebuildPageResult{}, fmt.Errorf("rebuild executor not configured")
	}
	ttl := e.LeaseTTL
	if ttl <= 0 {
		ttl = store.DefaultRecoveryLeaseTTL
	}
	spec := store.RecoveryJobSpec{
		RecoveryJobID:      req.RecoveryJobID,
		SourceContext:      req.SourceContext,
		SourceTopic:        req.ExpectedSourceTopic,
		FromCheckpoint:     req.FromCheckpoint,
		ToCheckpoint:       req.ToCheckpoint,
		FromOccurredAt:     req.FromOccurredAt,
		ToOccurredAt:       req.ToOccurredAt,
		HasCheckpointRange: req.HasCheckpointRange,
		HasOccurredRange:   req.HasOccurredRange,
	}

	// Crash-safe: completed page → republish deterministic follow-up or finalize, no reapply.
	if idemp, ok, err := e.Store.LoadRequestIdempotency(ctx, req.RecoveryJobID, req.ExpectedSourceTopic, req.PageCursor); err != nil {
		return RebuildPageResult{}, err
	} else if ok {
		switch idemp.Disposition {
		case store.IdempotencyFinalized:
			return RebuildPageResult{AlreadyDone: true, Finalize: true}, nil
		case store.IdempotencyFollowUpPublished:
			follow, err := e.buildFollowUp(req, idemp.FollowUpEventID /*next from checkpoint*/, "")
			if err != nil {
				return RebuildPageResult{}, err
			}
			// Prefer durable next cursor from page checkpoint.
			if cp, ok, err := e.Store.LoadPageCheckpoint(ctx, req.RecoveryJobID, req.ExpectedSourceTopic, req.PageCursor); err != nil {
				return RebuildPageResult{}, err
			} else if ok {
				follow.PageCursor = cp.NextPageCursor
			}
			if follow.PageCursor == "" {
				return RebuildPageResult{AlreadyDone: true, Finalize: true, FollowUpEventID: idemp.FollowUpEventID}, nil
			}
			follow.EventID = idemp.FollowUpEventID
			return RebuildPageResult{
				AlreadyDone:       true,
				FollowUp:          &follow,
				FollowUpEventID:   idemp.FollowUpEventID,
				RepublishFollowUp: true,
			}, nil
		}
	}

	if _, err := e.Store.AcquireOrRenewLease(ctx, req.RecoveryJobID, e.OwnerToken, ttl); err != nil {
		return RebuildPageResult{}, err
	}

	cp, hasCP, err := e.Store.LoadPageCheckpoint(ctx, req.RecoveryJobID, req.ExpectedSourceTopic, req.PageCursor)
	if err != nil {
		return RebuildPageResult{}, err
	}
	hasAppliedCP := hasCP && (cp.Status == store.PageStatusApplied || cp.Status == store.PageStatusFinalized)

	if job, jobErr := e.Store.LoadRecoveryJob(ctx, req.RecoveryJobID); jobErr == nil && job.GenerationID != "" {
		if err := store.ValidateImmutableRecoveryJobSpec(job, spec); err != nil {
			return RebuildPageResult{}, err
		}
		if err := store.AssertRecoveryJobAcceptsPage(job.Status, hasAppliedCP); err != nil {
			return RebuildPageResult{}, err
		}
		if hasAppliedCP {
			if err := e.Store.ReconcileRecoveryJobProgressFromCheckpoint(ctx, e.OwnerToken, cp); err != nil {
				return RebuildPageResult{}, err
			}
			return e.afterPageApplied(ctx, req, job.GenerationID, cp)
		}
	} else if jobErr != nil && !strings.Contains(jobErr.Error(), "not found") {
		return RebuildPageResult{}, jobErr
	}

	genID, err := e.Store.EnsureRecoveryBuildingGeneration(ctx, spec)
	if err != nil {
		return RebuildPageResult{}, err
	}
	if err := e.Store.ValidateLeaseOwnership(ctx, req.RecoveryJobID, e.OwnerToken); err != nil {
		return RebuildPageResult{}, err
	}

	// Idempotent page already applied (checkpoint without request idempotency yet).
	if hasAppliedCP {
		if err := e.Store.ReconcileRecoveryJobProgressFromCheckpoint(ctx, e.OwnerToken, cp); err != nil {
			return RebuildPageResult{}, err
		}
		return e.afterPageApplied(ctx, req, genID, cp)
	}

	page, events, err := e.Backfill.FetchPage(ctx, req)
	if err != nil {
		return RebuildPageResult{}, err
	}
	if err := e.Store.ValidateLeaseOwnership(ctx, req.RecoveryJobID, e.OwnerToken); err != nil {
		return RebuildPageResult{}, err
	}

	var accepted, quarantined uint32
	for _, evt := range events {
		out, err := e.Store.ApplyToGeneration(ctx, genID, evt)
		if err != nil {
			return RebuildPageResult{}, err
		}
		switch {
		case out.Kind == domain.OutcomeQuarantined || (out.Kind == domain.OutcomeDuplicate && !out.Accepted()):
			quarantined++
		case out.Accepted(), out.Kind == domain.OutcomeIgnored:
			accepted++
		}
	}

	pageIndex, err := e.nextPageIndex(ctx, req)
	if err != nil {
		return RebuildPageResult{}, err
	}
	cp = store.RecoveryPageCheckpoint{
		RecoveryJobID:  req.RecoveryJobID,
		SourceTopic:    req.ExpectedSourceTopic,
		PageCursor:     req.PageCursor,
		PageIndex:      pageIndex,
		NextPageCursor: page.NextCursor,
		// Coverage markers are only what the producer returned — never substitute request bounds.
		FromCheckpoint:   page.FromCheckpoint,
		ToCheckpoint:     page.ToCheckpoint,
		RecordsApplied:   accepted,
		QuarantinedCount: quarantined,
		Status:           store.PageStatusApplied,
		GenerationID:     genID,
	}
	if quarantined > 0 {
		_ = e.Store.MarkRecoveryFailed(ctx, e.OwnerToken, req.RecoveryJobID, store.RecoveryStatusQuarantined,
			"quarantined_record", "page contained quarantined analytics records", req.IdempotencyKey())
		if err := e.Store.PersistPageCheckpoint(ctx, e.OwnerToken, cp); err != nil {
			return RebuildPageResult{}, err
		}
		return RebuildPageResult{}, fmt.Errorf("%w: quarantined=%d", store.ErrRecoveryQuarantined, quarantined)
	}
	if err := e.Store.PersistPageCheckpoint(ctx, e.OwnerToken, cp); err != nil {
		return RebuildPageResult{}, err
	}
	// Idempotent progress write — also covers crash after checkpoint before progress update.
	if err := e.Store.ReconcileRecoveryJobProgressFromCheckpoint(ctx, e.OwnerToken, cp); err != nil {
		return RebuildPageResult{}, err
	}

	return e.afterPageApplied(ctx, req, genID, cp)
}

func (e *StoreBackedAnalyticsProjectionRebuildExecutor) afterPageApplied(
	ctx context.Context,
	req ParsedAnalyticsProjectionRebuildRequest,
	genID string,
	cp store.RecoveryPageCheckpoint,
) (RebuildPageResult, error) {
	if err := e.Store.ValidateLeaseOwnership(ctx, req.RecoveryJobID, e.OwnerToken); err != nil {
		return RebuildPageResult{}, err
	}
	if cp.NextPageCursor == "" {
		if err := e.Store.ActivateRecoveryGeneration(ctx, e.OwnerToken, req.RecoveryJobID); err != nil {
			return RebuildPageResult{}, err
		}
		if err := e.Store.PersistRequestIdempotency(ctx, e.OwnerToken, store.RecoveryRequestIdempotency{
			RecoveryJobID: req.RecoveryJobID,
			SourceTopic:   req.ExpectedSourceTopic,
			PageCursor:    req.PageCursor,
			Disposition:   store.IdempotencyFinalized,
			GenerationID:  genID,
		}); err != nil {
			return RebuildPageResult{}, err
		}
		return RebuildPageResult{Finalize: true}, nil
	}

	followEventID := store.DeterministicFollowUpEventID(req.RecoveryJobID, req.ExpectedSourceTopic, req.PageCursor)
	follow, err := e.buildFollowUp(req, followEventID, cp.NextPageCursor)
	if err != nil {
		return RebuildPageResult{}, err
	}
	if err := e.Store.PersistRequestIdempotency(ctx, e.OwnerToken, store.RecoveryRequestIdempotency{
		RecoveryJobID:   req.RecoveryJobID,
		SourceTopic:     req.ExpectedSourceTopic,
		PageCursor:      req.PageCursor,
		Disposition:     store.IdempotencyFollowUpPublished,
		FollowUpEventID: followEventID,
		GenerationID:    genID,
	}); err != nil {
		return RebuildPageResult{}, err
	}
	return RebuildPageResult{
		FollowUp:        &follow,
		FollowUpEventID: followEventID,
	}, nil
}

func (e *StoreBackedAnalyticsProjectionRebuildExecutor) buildFollowUp(
	req ParsedAnalyticsProjectionRebuildRequest,
	eventID, nextCursor string,
) (ParsedAnalyticsProjectionRebuildRequest, error) {
	now := time.Now().UTC()
	if e.Clock != nil {
		now = e.Clock.Now().UTC()
	}
	out := req
	out.EventID = eventID
	out.CausationID = req.EventID
	out.OccurredAt = now
	out.PageCursor = nextCursor
	out.Attempt = 1
	if out.EventID == "" {
		out.EventID = store.DeterministicFollowUpEventID(req.RecoveryJobID, req.ExpectedSourceTopic, req.PageCursor)
	}
	return out, nil
}

func (e *StoreBackedAnalyticsProjectionRebuildExecutor) nextPageIndex(ctx context.Context, req ParsedAnalyticsProjectionRebuildRequest) (uint32, error) {
	if req.PageCursor == "" {
		return 0, nil
	}
	job, err := e.Store.LoadRecoveryJob(ctx, req.RecoveryJobID)
	if err != nil {
		return 0, err
	}
	return job.PagesCompleted, nil
}

// AnalyticsProjectionRebuildKafkaConsumer owns analytics.projection.rebuild_requested.
type AnalyticsProjectionRebuildKafkaConsumer struct {
	source KafkaRecordSource
	dlq    DLQPublisher
	follow RebuildFollowUpPublisher
	exec   AnalyticsProjectionRebuildExecutor
	cfg    ProjectionRebuildKafkaConfig
	clock  Clock
	sleep  func(ctx context.Context, d time.Duration) error
}

// Run polls until ctx is cancelled. Source offsets commit only after success or DLQ ack.
func (c *AnalyticsProjectionRebuildKafkaConsumer) Run(ctx context.Context) error {
	if c == nil || c.source == nil {
		return fmt.Errorf("analytics projection rebuild consumer not configured")
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

// ProcessBatch handles rebuild requests serially.
func (c *AnalyticsProjectionRebuildKafkaConsumer) ProcessBatch(ctx context.Context, recs []ConsumerRecord) error {
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

func (c *AnalyticsProjectionRebuildKafkaConsumer) processOne(ctx context.Context, rec ConsumerRecord) error {
	if rec.Context != nil {
		ctx = rec.Context
	} else {
		ctx = processPropagator().Extract(ctx, propagation.MapCarrier(rec.Headers))
	}
	ctx, span := processTracerProvider().Tracer("unoarena/services/analytics").Start(ctx, "analytics.projection-rebuild.process")
	defer span.End()
	sourceTopic := firstNonEmpty(rec.Topic, c.cfg.Topic)
	key := strings.TrimSpace(string(rec.Key))

	req, err := ParseAnalyticsProjectionRebuildRequested(rec.Value)
	if err != nil {
		return c.terminalToDLQAndCommit(ctx, rec, err, 1)
	}
	if key == "" {
		return c.terminalToDLQAndCommit(ctx, rec,
			newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("kafka record key is required")), 1)
	}
	if key != req.RecoveryJobID {
		return c.terminalToDLQAndCommit(ctx, rec,
			newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("kafka key must equal recoveryJobId")), 1)
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
		res, err := c.exec.ExecutePage(ctx, req)
		now := c.now()
		if firstFail.IsZero() {
			firstFail = now
		}
		lastFail = now
		if err == nil {
			if res.FollowUp != nil {
				if c.follow == nil {
					return fmt.Errorf("follow-up publisher not configured")
				}
				payload, encErr := EncodeAnalyticsProjectionRebuildRequested(*res.FollowUp)
				if encErr != nil {
					return encErr
				}
				if pubErr := c.follow.PublishRebuildRequest(ctx, res.FollowUp.RecoveryJobID, payload); pubErr != nil {
					return pubErr
				}
			}
			return c.source.Commit(ctx, rec)
		}
		lastErr = err
		if IsTerminalKafkaConsumeError(err) || isTerminalAnalyticsRebuildError(err) {
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

func isTerminalAnalyticsRebuildError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, store.ErrRecoveryQuarantined) ||
		errors.Is(err, store.ErrRecoveryContinuityGap) ||
		errors.Is(err, store.ErrRecoveryActivationFenceFailed) ||
		errors.Is(err, store.ErrRecoveryJobSpecMismatch) ||
		errors.Is(err, store.ErrRecoveryJobClosed) {
		return true
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "backfill auth failed") ||
		strings.Contains(msg, "backfill bad request") ||
		strings.Contains(msg, "backfill recoveryjobid mismatch") ||
		strings.Contains(msg, "backfill sourcetopic mismatch") ||
		strings.Contains(msg, "backfill schemaversion") ||
		strings.Contains(msg, "backfill nextcursor") {
		return true
	}
	return false
}

func (c *AnalyticsProjectionRebuildKafkaConsumer) terminalToDLQAndCommit(ctx context.Context, rec ConsumerRecord, err error, attempts int) error {
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
func (c *AnalyticsProjectionRebuildKafkaConsumer) failureToDLQAndCommit(ctx context.Context, rec ConsumerRecord, meta DLQFailureMeta) error {
	if c.dlq == nil {
		return fmt.Errorf("dlq publisher not configured")
	}
	if err := c.dlq.PublishDLQ(ctx, rec, meta); err != nil {
		return fmt.Errorf("dlq publish: %w", err)
	}
	return c.source.Commit(ctx, rec)
}

func (c *AnalyticsProjectionRebuildKafkaConsumer) now() time.Time {
	if c.clock != nil {
		return c.clock.Now().UTC()
	}
	return time.Now().UTC()
}

func (c *AnalyticsProjectionRebuildKafkaConsumer) doSleep(ctx context.Context, d time.Duration) error {
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
