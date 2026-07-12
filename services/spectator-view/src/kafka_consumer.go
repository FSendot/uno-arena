package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"unoarena/services/spectator-view/domain"
	"unoarena/services/spectator-view/store"
)

// ConsumerRecord is a narrow Kafka record view for Spectator unit tests.
type ConsumerRecord struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	// Headers are optional (DLQ operational metadata). Live safe-events apply ignores them.
	Headers map[string]string
}

// DLQFailureMeta is sanitized operational metadata published with a DLQ record.
type DLQFailureMeta struct {
	Consumer        string
	SourceTopic     string
	SourcePartition int32
	SourceOffset    int64
	AttemptCount    int
	Classification  string
	FirstFailureAt  time.Time
	LastFailureAt   time.Time
	CorrelationID   string
	ErrorSummary    string
}

// AggregateQuarantineRecord is Spectator-owned Kafka aggregate quarantine state.
type AggregateQuarantineRecord struct {
	ConsumerGroup   string
	SourceTopic     string
	AggregateKey    string
	Classification  string
	Reason          string
	SourcePartition int32
	SourceOffset    int64
	EventID         string
	CorrelationID   string
}

// AggregateQuarantineStore persists ADR-0017 ordered-consumer quarantine state.
type AggregateQuarantineStore interface {
	IsQuarantined(ctx context.Context, consumerGroup, sourceTopic, aggregateKey string) (bool, error)
	Quarantine(ctx context.Context, rec AggregateQuarantineRecord) error
}

// SpectatorSafeIngester is the domain seam invoked after successful parse.
type SpectatorSafeIngester interface {
	Apply(ctx context.Context, roomID domain.RoomID, events []domain.SpectatorSafeEvent) (domain.ApplyOutcome, error)
}

// KafkaRecordSource polls and manually commits source offsets.
type KafkaRecordSource interface {
	Poll(ctx context.Context) ([]ConsumerRecord, error)
	Commit(ctx context.Context, rec ConsumerRecord) error
	Close() error
}

// DLQPublisher publishes the original envelope plus failure metadata.
type DLQPublisher interface {
	PublishDLQ(ctx context.Context, original ConsumerRecord, meta DLQFailureMeta) error
}

// Clock is injectable for deterministic tests.
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

type kafkaConsumeError struct {
	class    string
	terminal bool
	err      error
}

func (e *kafkaConsumeError) Error() string {
	if e == nil || e.err == nil {
		return "kafka consume error"
	}
	return e.err.Error()
}

func (e *kafkaConsumeError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func newTerminalKafkaError(class string, err error) error {
	return &kafkaConsumeError{class: class, terminal: true, err: err}
}

// IsTerminalKafkaConsumeError reports schema/payload failures that skip retries.
func IsTerminalKafkaConsumeError(err error) bool {
	var ke *kafkaConsumeError
	if errors.As(err, &ke) {
		return ke.terminal
	}
	return false
}

// ClassifyKafkaConsumeError maps an error to a DLQ/ops classification.
func ClassifyKafkaConsumeError(err error) string {
	if err == nil {
		return KafkaFailureApplication
	}
	var ke *kafkaConsumeError
	if errors.As(err, &ke) && ke.class != "" {
		return ke.class
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "timeout"),
		strings.Contains(msg, "temporar"),
		strings.Contains(msg, "connection"),
		strings.Contains(msg, "unavailable"),
		strings.Contains(msg, "redis"),
		strings.Contains(msg, "cas conflict"):
		return KafkaFailureDependency
	default:
		return KafkaFailureApplication
	}
}

func sanitizeDLQErrorSummary(msg string) string {
	msg = strings.TrimSpace(msg)
	msg = strings.ReplaceAll(msg, "\n", " ")
	msg = strings.ReplaceAll(msg, "\r", " ")
	lower := strings.ToLower(msg)
	for _, secret := range []string{"password=", "pwd=", "secret=", "credential=", "token="} {
		if i := strings.Index(lower, secret); i >= 0 {
			msg = msg[:i] + secret + "[redacted]"
			break
		}
	}
	if len(msg) > maxDLQErrorSummaryLen {
		msg = msg[:maxDLQErrorSummaryLen]
	}
	return msg
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if s := strings.TrimSpace(v); s != "" {
			return s
		}
	}
	return ""
}

// SpectatorSafeKafkaConsumer owns room.spectator-safe.events consumption.
type SpectatorSafeKafkaConsumer struct {
	source     KafkaRecordSource
	dlq        DLQPublisher
	handler    SpectatorSafeIngester
	quarantine AggregateQuarantineStore
	cfg        SpectatorSafeKafkaConfig
	clock      Clock
	sleep      func(ctx context.Context, d time.Duration) error
}

// Run polls until ctx is cancelled. Failed batches are retained and retried with
// bounded backoff so fetch-position advance cannot silently drop in-memory records.
func (c *SpectatorSafeKafkaConsumer) Run(ctx context.Context) error {
	if c == nil || c.source == nil {
		return fmt.Errorf("kafka consumer not configured")
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

// ProcessBatch applies records with bounded partition concurrency. Offsets within
// each partition remain strictly serial; unrelated partitions may progress in parallel.
func (c *SpectatorSafeKafkaConsumer) ProcessBatch(ctx context.Context, recs []ConsumerRecord) error {
	if len(recs) == 0 {
		return nil
	}
	byPartition := map[int32][]ConsumerRecord{}
	var partitions []int32
	for _, rec := range recs {
		if _, ok := byPartition[rec.Partition]; !ok {
			partitions = append(partitions, rec.Partition)
		}
		byPartition[rec.Partition] = append(byPartition[rec.Partition], rec)
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })

	maxWorkers := c.cfg.MaxPartitionWorkers
	if maxWorkers < 1 {
		maxWorkers = defaultSpectatorSafePartitionWorkers
	}
	if maxWorkers > len(partitions) {
		maxWorkers = len(partitions)
	}

	batchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, maxWorkers)
	errCh := make(chan error, len(partitions))
	var wg sync.WaitGroup

	for _, p := range partitions {
		group := byPartition[p]
		sort.Slice(group, func(i, j int) bool { return group[i].Offset < group[j].Offset })
		wg.Add(1)
		go func(group []ConsumerRecord) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-batchCtx.Done():
				errCh <- batchCtx.Err()
				return
			}
			defer func() { <-sem }()

			for _, rec := range group {
				if err := batchCtx.Err(); err != nil {
					errCh <- err
					return
				}
				if err := c.processOne(batchCtx, rec); err != nil {
					errCh <- err
					cancel()
					return
				}
			}
		}(group)
	}
	wg.Wait()
	close(errCh)

	var firstNonCtx, firstCtx error
	for err := range errCh {
		if err == nil {
			continue
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			if firstCtx == nil {
				firstCtx = err
			}
			continue
		}
		if firstNonCtx == nil {
			firstNonCtx = err
		}
	}
	if firstNonCtx != nil {
		return firstNonCtx
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return firstCtx
}

func (c *SpectatorSafeKafkaConsumer) processOne(ctx context.Context, rec ConsumerRecord) error {
	key := strings.TrimSpace(string(rec.Key))
	sourceTopic := firstNonEmpty(rec.Topic, c.cfg.Topic)

	if key != "" {
		if quarantined, err := c.isAggregateQuarantined(ctx, sourceTopic, key); err != nil {
			return err
		} else if quarantined {
			now := c.now()
			return c.publishDLQAndCommit(ctx, rec, DLQFailureMeta{
				Consumer:        c.cfg.Group,
				SourceTopic:     sourceTopic,
				SourcePartition: rec.Partition,
				SourceOffset:    rec.Offset,
				AttemptCount:    1,
				Classification:  KafkaFailureAggregateQuarantined,
				FirstFailureAt:  now,
				LastFailureAt:   now,
				CorrelationID:   peekSafeCorrelationID(rec.Value),
				ErrorSummary:    "aggregate_quarantined",
			})
		}
	}

	parsed, evt, err := ParseSpectatorSafeRecord(rec.Value)
	if err != nil {
		aggregateKey := key
		if aggregateKey == "" {
			aggregateKey = peekSafeRoomID(rec.Value)
		}
		return c.terminalToDLQAndCommit(ctx, rec, err, 1, aggregateKey)
	}

	if key == "" {
		return c.terminalToDLQAndCommit(ctx, rec,
			newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("kafka record key is required")),
			1, parsed.RoomID)
	}
	if key != parsed.RoomID {
		return c.terminalToDLQAndCommit(ctx, rec,
			newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("kafka key must equal roomId")),
			1, key)
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
		out, err := c.handler.Apply(ctx, domain.RoomID(parsed.RoomID), []domain.SpectatorSafeEvent{evt})
		now := c.now()
		if firstFail.IsZero() {
			firstFail = now
		}
		lastFail = now
		if err == nil {
			switch out.Kind {
			case domain.OutcomeAccepted, domain.OutcomeDuplicate, domain.OutcomeIgnored:
				return c.source.Commit(ctx, rec)
			case domain.OutcomeQuarantined, domain.OutcomeDropped:
				class := classificationFromDomainOutcome(out)
				return c.domainQuarantineToDLQAndCommit(ctx, rec, DLQFailureMeta{
					Consumer:        c.cfg.Group,
					SourceTopic:     sourceTopic,
					SourcePartition: rec.Partition,
					SourceOffset:    rec.Offset,
					AttemptCount:    attempt,
					Classification:  class,
					FirstFailureAt:  firstFail,
					LastFailureAt:   lastFail,
					CorrelationID:   parsed.CorrelationID,
					ErrorSummary:    sanitizeDLQErrorSummary(domainOutcomeSummary(out)),
				}, parsed.RoomID, parsed.EventID, false)
			default:
				lastErr = fmt.Errorf("unexpected outcome kind %q", out.Kind)
			}
		} else {
			lastErr = err
			if IsTerminalKafkaConsumeError(err) {
				return c.failureToDLQAndCommit(ctx, rec, DLQFailureMeta{
					Consumer:        c.cfg.Group,
					SourceTopic:     sourceTopic,
					SourcePartition: rec.Partition,
					SourceOffset:    rec.Offset,
					AttemptCount:    attempt,
					Classification:  ClassifyKafkaConsumeError(err),
					FirstFailureAt:  firstFail,
					LastFailureAt:   lastFail,
					CorrelationID:   parsed.CorrelationID,
					ErrorSummary:    sanitizeDLQErrorSummary(err.Error()),
				}, parsed.RoomID, parsed.EventID)
			}
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
		CorrelationID:   parsed.CorrelationID,
		ErrorSummary:    sanitizeDLQErrorSummary(lastErr.Error()),
	}, parsed.RoomID, parsed.EventID)
}

func classificationFromDomainOutcome(out domain.ApplyOutcome) string {
	switch store.ClassificationForDomainOutcome(out) {
	case store.QuarantineClassPrivacy:
		return KafkaFailurePrivacyViolation
	case store.QuarantineClassSchemaInvalid:
		return KafkaFailureSchemaInvalid
	case store.QuarantineClassPayloadInvalid:
		return KafkaFailurePayloadInvalid
	case store.QuarantineClassDependency:
		return KafkaFailureDependency
	default:
		return KafkaFailureApplication
	}
}

func domainOutcomeSummary(out domain.ApplyOutcome) string {
	if out.Rejection != nil {
		if out.Rejection.Message != "" {
			return out.Rejection.Message
		}
		return string(out.Rejection.Code)
	}
	return string(out.Kind)
}

func (c *SpectatorSafeKafkaConsumer) isAggregateQuarantined(ctx context.Context, sourceTopic, aggregateKey string) (bool, error) {
	if c.quarantine == nil || aggregateKey == "" {
		return false, nil
	}
	return c.quarantine.IsQuarantined(ctx, c.cfg.Group, sourceTopic, aggregateKey)
}

func (c *SpectatorSafeKafkaConsumer) terminalToDLQAndCommit(ctx context.Context, rec ConsumerRecord, err error, attempts int, aggregateKey string) error {
	now := c.now()
	eventID := peekSafeEventID(rec.Value)
	corr := peekSafeCorrelationID(rec.Value)
	return c.failureToDLQAndCommit(ctx, rec, DLQFailureMeta{
		Consumer:        c.cfg.Group,
		SourceTopic:     firstNonEmpty(rec.Topic, c.cfg.Topic),
		SourcePartition: rec.Partition,
		SourceOffset:    rec.Offset,
		AttemptCount:    attempts,
		Classification:  ClassifyKafkaConsumeError(err),
		FirstFailureAt:  now,
		LastFailureAt:   now,
		CorrelationID:   corr,
		ErrorSummary:    sanitizeDLQErrorSummary(err.Error()),
	}, aggregateKey, eventID)
}

// domainQuarantineToDLQAndCommit publishes DLQ after Apply already atomically marked quarantine.
// persist=false skips a second quarantine write (Apply Lua already committed the marker).
func (c *SpectatorSafeKafkaConsumer) domainQuarantineToDLQAndCommit(ctx context.Context, rec ConsumerRecord, meta DLQFailureMeta, aggregateKey, eventID string, persist bool) error {
	if persist && aggregateKey != "" {
		if err := c.persistQuarantine(ctx, rec, meta, aggregateKey, eventID); err != nil {
			return err
		}
	}
	return c.publishDLQAndCommit(ctx, rec, meta)
}

func (c *SpectatorSafeKafkaConsumer) failureToDLQAndCommit(ctx context.Context, rec ConsumerRecord, meta DLQFailureMeta, aggregateKey, eventID string) error {
	if aggregateKey != "" {
		if err := c.persistQuarantine(ctx, rec, meta, aggregateKey, eventID); err != nil {
			return err
		}
	}
	return c.publishDLQAndCommit(ctx, rec, meta)
}

func (c *SpectatorSafeKafkaConsumer) persistQuarantine(ctx context.Context, rec ConsumerRecord, meta DLQFailureMeta, aggregateKey, eventID string) error {
	if c.quarantine == nil {
		return nil
	}
	return c.quarantine.Quarantine(ctx, AggregateQuarantineRecord{
		ConsumerGroup:   c.cfg.Group,
		SourceTopic:     firstNonEmpty(rec.Topic, c.cfg.Topic),
		AggregateKey:    aggregateKey,
		Classification:  meta.Classification,
		Reason:          sanitizeDLQErrorSummary(meta.ErrorSummary),
		SourcePartition: rec.Partition,
		SourceOffset:    rec.Offset,
		EventID:         eventID,
		CorrelationID:   meta.CorrelationID,
	})
}

func (c *SpectatorSafeKafkaConsumer) publishDLQAndCommit(ctx context.Context, rec ConsumerRecord, meta DLQFailureMeta) error {
	if c.dlq == nil {
		return fmt.Errorf("dlq publisher not configured")
	}
	if err := c.dlq.PublishDLQ(ctx, rec, meta); err != nil {
		return fmt.Errorf("dlq publish: %w", err)
	}
	return c.source.Commit(ctx, rec)
}

func (c *SpectatorSafeKafkaConsumer) now() time.Time {
	if c.clock != nil {
		return c.clock.Now().UTC()
	}
	return time.Now().UTC()
}

func (c *SpectatorSafeKafkaConsumer) doSleep(ctx context.Context, d time.Duration) error {
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
