package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"unoarena/services/ranking/domain"
	"unoarena/services/ranking/store"
)

// ConsumerRecord is a narrow Kafka record view for Ranking unit tests.
type ConsumerRecord struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
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

// AggregateQuarantineRecord is Ranking-owned Kafka aggregate quarantine state.
// It stores sanitized operational fields only — never raw private payload or secrets.
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

// GameCompletedIngester is the domain seam invoked after successful GameCompleted parse.
type GameCompletedIngester interface {
	ApplyCasualGameCompleted(ctx context.Context, req GameCompletedRequest) (GameCompletedResult, error)
}

// TournamentPerformanceIngester is the domain seam for tournament performance facts.
type TournamentPerformanceIngester interface {
	ApplyTournamentPerformance(ctx context.Context, req TournamentPerformanceRequest) (TournamentPerformanceResult, error)
}

// RankingKafkaHandler combines casual + tournament ingest seams plus Redis CDC apply.
type RankingKafkaHandler interface {
	GameCompletedIngester
	TournamentPerformanceIngester
	PlayerRatingUpdatedApplier
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
	if store.IsTournamentPerformanceConflict(err) {
		return KafkaFailureBusinessKeyConflict
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
		strings.Contains(msg, "database"),
		strings.Contains(msg, "postgres"),
		strings.Contains(msg, "redis"):
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

// GameCompletedKafkaConsumer owns Ranking Kafka consumption (casual + tournament topics).
type GameCompletedKafkaConsumer struct {
	source     KafkaRecordSource
	dlq        DLQPublisher
	handler    RankingKafkaHandler
	quarantine AggregateQuarantineStore
	cfg        RankingKafkaConfig
	clock      Clock
	sleep      func(ctx context.Context, d time.Duration) error
}

// topicPartition identifies a Kafka topic-partition for serial offset processing.
type topicPartition struct {
	Topic     string
	Partition int32
}

// Run polls until ctx is cancelled. Failed batches are retained and retried with
// bounded backoff so fetch-position advance cannot silently drop in-memory records.
// Run returns only on context cancellation (or misconfiguration before the loop).
func (c *GameCompletedKafkaConsumer) Run(ctx context.Context) error {
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
// in parallel.
func (c *GameCompletedKafkaConsumer) ProcessBatch(ctx context.Context, recs []ConsumerRecord) error {
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
		maxWorkers = defaultRankingPartitionWorkers
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

func (c *GameCompletedKafkaConsumer) processOne(ctx context.Context, rec ConsumerRecord) error {
	sourceTopic := firstNonEmpty(rec.Topic, c.cfg.Topic, DefaultGameCompletedTopic)
	switch c.cfg.classifyTopic(sourceTopic) {
	case topicKindPlayersAdvanced:
		return c.processPlayersAdvanced(ctx, rec)
	case topicKindTournamentCompleted:
		return c.processTournamentCompleted(ctx, rec)
	case topicKindPlayerRatingUpdated:
		return c.processPlayerRatingUpdated(ctx, rec)
	default:
		return c.processGameCompleted(ctx, rec)
	}
}

func (c *GameCompletedKafkaConsumer) processPlayerRatingUpdated(ctx context.Context, rec ConsumerRecord) error {
	sourceTopic := firstNonEmpty(rec.Topic, DefaultPlayerRatingUpdatedTopic)
	key := strings.TrimSpace(string(rec.Key))
	evt, err := ParsePlayerRatingUpdatedRecord(rec.Value)
	if err != nil {
		aggregateKey := key
		if aggregateKey == "" {
			aggregateKey = string(evt.PlayerID)
		}
		return c.terminalToDLQAndCommit(ctx, rec, err, 1, aggregateKey, false)
	}
	if key == "" {
		return c.terminalToDLQAndCommit(ctx, rec,
			newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("kafka record key is required")),
			1, string(evt.PlayerID), false)
	}
	if key != string(evt.PlayerID) {
		return c.terminalToDLQAndCommit(ctx, rec,
			newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("kafka key must equal playerId")),
			1, key, false)
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
		err := c.handler.ApplyPlayerRatingUpdated(ctx, evt)
		if err == nil {
			return c.source.Commit(ctx, rec)
		}
		now := c.now()
		if firstFail.IsZero() {
			firstFail = now
		}
		lastFail = now
		lastErr = err
		if IsTerminalKafkaConsumeError(err) {
			break
		}
		if attempt < maxAttempts {
			if sleepErr := c.doSleep(ctx, c.cfg.RetryBackoff); sleepErr != nil {
				return sleepErr
			}
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
		CorrelationID:   firstNonEmpty(evt.CorrelationID, peekSafeCorrelationID(rec.Value)),
		ErrorSummary:    sanitizeDLQErrorSummary(lastErr.Error()),
	})
}

func (c *GameCompletedKafkaConsumer) processGameCompleted(ctx context.Context, rec ConsumerRecord) error {
	key := strings.TrimSpace(string(rec.Key))
	sourceTopic := firstNonEmpty(rec.Topic, c.cfg.Topic, DefaultGameCompletedTopic)

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

	evt, err := ParseGameCompletedRecord(rec.Value)
	if err != nil {
		aggregateKey := key
		if aggregateKey == "" {
			aggregateKey = peekSafeRoomID(rec.Value)
		}
		return c.terminalToDLQAndCommit(ctx, rec, err, 1, aggregateKey, true)
	}

	if key == "" {
		return c.terminalToDLQAndCommit(ctx, rec,
			newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("kafka record key is required")),
			1, evt.RoomID, true)
	}
	if key != evt.RoomID {
		return c.terminalToDLQAndCommit(ctx, rec,
			newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("kafka key must equal roomId")),
			1, key, true)
	}

	req := MapGameCompletedToRequest(evt)
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
		result, err := c.handler.ApplyCasualGameCompleted(ctx, req)
		now := c.now()
		if firstFail.IsZero() {
			firstFail = now
		}
		lastFail = now
		if err == nil {
			switch result.Kind {
			case domain.OutcomeAccepted, domain.OutcomeDuplicate, domain.OutcomeRejected:
				return c.source.Commit(ctx, rec)
			default:
				lastErr = fmt.Errorf("unexpected outcome kind %q", result.Kind)
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
					CorrelationID:   evt.CorrelationID,
					ErrorSummary:    sanitizeDLQErrorSummary(err.Error()),
				}, evt.RoomID, evt.EventID, true)
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
		CorrelationID:   evt.CorrelationID,
		ErrorSummary:    sanitizeDLQErrorSummary(lastErr.Error()),
	}, evt.RoomID, evt.EventID, true)
}

func (c *GameCompletedKafkaConsumer) processPlayersAdvanced(ctx context.Context, rec ConsumerRecord) error {
	key := strings.TrimSpace(string(rec.Key))
	sourceTopic := firstNonEmpty(rec.Topic, DefaultPlayersAdvancedTopic)
	evt, err := ParsePlayersAdvancedRecord(rec.Value)
	if err != nil {
		return c.terminalToDLQAndCommit(ctx, rec, err, 1, key, false)
	}
	if key == "" {
		return c.terminalToDLQAndCommit(ctx, rec,
			newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("kafka record key is required")),
			1, evt.TournamentID, false)
	}
	if key != evt.TournamentID {
		return c.terminalToDLQAndCommit(ctx, rec,
			newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("kafka key must equal tournamentId")),
			1, key, false)
	}
	req := MapPlayersAdvancedToRequest(evt)
	req.SourceTopic = sourceTopic
	return c.applyTournamentPerformance(ctx, rec, req, evt.CorrelationID, evt.TournamentID, evt.EventID)
}

func (c *GameCompletedKafkaConsumer) processTournamentCompleted(ctx context.Context, rec ConsumerRecord) error {
	key := strings.TrimSpace(string(rec.Key))
	sourceTopic := firstNonEmpty(rec.Topic, DefaultTournamentCompletedTopic)
	evt, err := ParseTournamentCompletedRecord(rec.Value)
	if err != nil {
		return c.terminalToDLQAndCommit(ctx, rec, err, 1, key, false)
	}
	if key == "" {
		return c.terminalToDLQAndCommit(ctx, rec,
			newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("kafka record key is required")),
			1, evt.TournamentID, false)
	}
	if key != evt.TournamentID {
		return c.terminalToDLQAndCommit(ctx, rec,
			newTerminalKafkaError(KafkaFailureSchemaInvalid, fmt.Errorf("kafka key must equal tournamentId")),
			1, key, false)
	}
	req := MapTournamentCompletedToRequest(evt)
	req.SourceTopic = sourceTopic
	return c.applyTournamentPerformance(ctx, rec, req, evt.CorrelationID, evt.TournamentID, evt.EventID)
}

func (c *GameCompletedKafkaConsumer) applyTournamentPerformance(ctx context.Context, rec ConsumerRecord, req TournamentPerformanceRequest, correlationID, tournamentID, eventID string) error {
	sourceTopic := firstNonEmpty(rec.Topic, req.SourceTopic)
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
		result, err := c.handler.ApplyTournamentPerformance(ctx, req)
		now := c.now()
		if firstFail.IsZero() {
			firstFail = now
		}
		lastFail = now
		if err == nil {
			switch result.Kind {
			case domain.OutcomeAccepted, domain.OutcomeDuplicate, domain.OutcomeRejected:
				return c.source.Commit(ctx, rec)
			default:
				lastErr = fmt.Errorf("unexpected outcome kind %q", result.Kind)
			}
		} else {
			lastErr = err
			if store.IsTournamentPerformanceConflict(err) || IsTerminalKafkaConsumeError(err) {
				return c.failureToDLQAndCommit(ctx, rec, DLQFailureMeta{
					Consumer:        c.cfg.Group,
					SourceTopic:     sourceTopic,
					SourcePartition: rec.Partition,
					SourceOffset:    rec.Offset,
					AttemptCount:    attempt,
					Classification:  ClassifyKafkaConsumeError(err),
					FirstFailureAt:  firstFail,
					LastFailureAt:   lastFail,
					CorrelationID:   correlationID,
					ErrorSummary:    sanitizeDLQErrorSummary(err.Error()),
				}, "", eventID, false)
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
		CorrelationID:   correlationID,
		ErrorSummary:    sanitizeDLQErrorSummary(lastErr.Error()),
	}, "", eventID, false)
}

func (c *GameCompletedKafkaConsumer) isAggregateQuarantined(ctx context.Context, sourceTopic, aggregateKey string) (bool, error) {
	if c.quarantine == nil || aggregateKey == "" {
		return false, nil
	}
	return c.quarantine.IsQuarantined(ctx, c.cfg.Group, sourceTopic, aggregateKey)
}

func (c *GameCompletedKafkaConsumer) terminalToDLQAndCommit(ctx context.Context, rec ConsumerRecord, err error, attempts int, aggregateKey string, quarantine bool) error {
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
	}, aggregateKey, eventID, quarantine)
}

func (c *GameCompletedKafkaConsumer) failureToDLQAndCommit(ctx context.Context, rec ConsumerRecord, meta DLQFailureMeta, aggregateKey, eventID string, quarantine bool) error {
	if quarantine && aggregateKey != "" {
		if err := c.persistQuarantine(ctx, rec, meta, aggregateKey, eventID); err != nil {
			return err
		}
	}
	return c.publishDLQAndCommit(ctx, rec, meta)
}

func (c *GameCompletedKafkaConsumer) persistQuarantine(ctx context.Context, rec ConsumerRecord, meta DLQFailureMeta, aggregateKey, eventID string) error {
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

func (c *GameCompletedKafkaConsumer) publishDLQAndCommit(ctx context.Context, rec ConsumerRecord, meta DLQFailureMeta) error {
	if c.dlq == nil {
		return fmt.Errorf("dlq publisher not configured")
	}
	if err := c.dlq.PublishDLQ(ctx, rec, meta); err != nil {
		return fmt.Errorf("dlq publish: %w", err)
	}
	return c.source.Commit(ctx, rec)
}

func (c *GameCompletedKafkaConsumer) now() time.Time {
	if c.clock != nil {
		return c.clock.Now().UTC()
	}
	return time.Now().UTC()
}

func (c *GameCompletedKafkaConsumer) doSleep(ctx context.Context, d time.Duration) error {
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
