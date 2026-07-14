package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/propagation"

	"unoarena/services/analytics/domain"
)

// ConsumerRecord is a narrow Kafka record view for Analytics unit tests.
type ConsumerRecord struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string]string
	Context   context.Context
}

// topicPartition identifies a Kafka topic-partition for serial offset processing.
type topicPartition struct {
	Topic     string
	Partition int32
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

// AnalyticsIngester is the domain seam invoked after successful parse/map.
type AnalyticsIngester interface {
	Apply(ctx context.Context, evt domain.UpstreamEvent) (domain.ApplyOutcome, error)
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
		strings.Contains(msg, "clickhouse"),
		strings.Contains(msg, "database"):
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

// AnalyticsKafkaConsumer owns multi-topic durable Analytics Kafka ingestion.
// There is no aggregate quarantine store — terminal/exhausted failures go to DLQ then commit.
type AnalyticsKafkaConsumer struct {
	source  KafkaRecordSource
	dlq     DLQPublisher
	handler AnalyticsIngester
	cfg     AnalyticsKafkaConfig
	clock   Clock
	sleep   func(ctx context.Context, d time.Duration) error
}

// Run polls until ctx is cancelled. Failed batches are retained and retried with
// bounded backoff so fetch-position advance cannot silently drop in-memory records.
func (c *AnalyticsKafkaConsumer) Run(ctx context.Context) error {
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

// ProcessBatch applies records with bounded topic-partition concurrency. Offsets within
// each (topic, partition) remain strictly serial; unrelated topic-partitions may progress
// in parallel. Grouping uses Topic+Partition because Kafka partitions are per-topic.
func (c *AnalyticsKafkaConsumer) ProcessBatch(ctx context.Context, recs []ConsumerRecord) error {
	if len(recs) == 0 {
		return nil
	}
	byTP := map[topicPartition][]ConsumerRecord{}
	var keys []topicPartition
	for _, rec := range recs {
		tp := topicPartition{Topic: rec.Topic, Partition: rec.Partition}
		if _, ok := byTP[tp]; !ok {
			keys = append(keys, tp)
		}
		byTP[tp] = append(byTP[tp], rec)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Topic != keys[j].Topic {
			return keys[i].Topic < keys[j].Topic
		}
		return keys[i].Partition < keys[j].Partition
	})

	maxWorkers := c.cfg.MaxPartitionWorkers
	if maxWorkers < 1 {
		maxWorkers = defaultAnalyticsPartitionWorkers
	}
	if maxWorkers > len(keys) {
		maxWorkers = len(keys)
	}

	batchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, maxWorkers)
	errCh := make(chan error, len(keys))
	var wg sync.WaitGroup

	for _, tp := range keys {
		group := byTP[tp]
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

func (c *AnalyticsKafkaConsumer) processOne(ctx context.Context, rec ConsumerRecord) error {
	if rec.Context != nil {
		ctx = rec.Context
	} else {
		ctx = processPropagator().Extract(ctx, propagation.MapCarrier(rec.Headers))
	}
	ctx, span := processTracerProvider().Tracer("unoarena/services/analytics").Start(ctx, "analytics.kafka.process")
	defer span.End()
	sourceTopic := strings.TrimSpace(rec.Topic)
	evt, err := ParseAnalyticsRecord(rec)
	if err != nil {
		return c.terminalToDLQAndCommit(ctx, rec, err, 1)
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
		out, err := c.handler.Apply(ctx, evt)
		now := c.now()
		if firstFail.IsZero() {
			firstFail = now
		}
		lastFail = now
		if err == nil {
			switch out.Kind {
			case domain.OutcomeAccepted, domain.OutcomeDuplicate, domain.OutcomeQuarantined, domain.OutcomeIgnored:
				return c.source.Commit(ctx, rec)
			default:
				lastErr = fmt.Errorf("unexpected outcome kind %q", out.Kind)
			}
		} else {
			lastErr = err
			if IsTerminalKafkaConsumeError(err) {
				return c.publishDLQAndCommit(ctx, rec, DLQFailureMeta{
					Consumer:        c.cfg.Group,
					SourceTopic:     sourceTopic,
					SourcePartition: rec.Partition,
					SourceOffset:    rec.Offset,
					AttemptCount:    attempt,
					Classification:  ClassifyKafkaConsumeError(err),
					FirstFailureAt:  firstFail,
					LastFailureAt:   lastFail,
					CorrelationID:   evt.CorrelationID,
					ErrorSummary:    sanitizeDLQErrorSummary(err.Error()),
				})
			}
		}
		if attempt == maxAttempts {
			break
		}
		if sleepErr := c.doSleep(ctx, c.cfg.RetryBackoff); sleepErr != nil {
			return sleepErr
		}
	}
	return c.publishDLQAndCommit(ctx, rec, DLQFailureMeta{
		Consumer:        c.cfg.Group,
		SourceTopic:     sourceTopic,
		SourcePartition: rec.Partition,
		SourceOffset:    rec.Offset,
		AttemptCount:    maxAttempts,
		Classification:  ClassifyKafkaConsumeError(lastErr),
		FirstFailureAt:  firstFail,
		LastFailureAt:   lastFail,
		CorrelationID:   evt.CorrelationID,
		ErrorSummary:    sanitizeDLQErrorSummary(lastErr.Error()),
	})
}

func (c *AnalyticsKafkaConsumer) terminalToDLQAndCommit(ctx context.Context, rec ConsumerRecord, err error, attempts int) error {
	now := c.now()
	return c.publishDLQAndCommit(ctx, rec, DLQFailureMeta{
		Consumer:        c.cfg.Group,
		SourceTopic:     strings.TrimSpace(rec.Topic),
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

func (c *AnalyticsKafkaConsumer) publishDLQAndCommit(ctx context.Context, rec ConsumerRecord, meta DLQFailureMeta) error {
	if c.dlq == nil {
		return fmt.Errorf("dlq publisher not configured")
	}
	if err := c.dlq.PublishDLQ(ctx, rec, meta); err != nil {
		return fmt.Errorf("dlq publish: %w", err)
	}
	return c.source.Commit(ctx, rec)
}

func (c *AnalyticsKafkaConsumer) now() time.Time {
	if c.clock != nil {
		return c.clock.Now().UTC()
	}
	return time.Now().UTC()
}

func (c *AnalyticsKafkaConsumer) doSleep(ctx context.Context, d time.Duration) error {
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
