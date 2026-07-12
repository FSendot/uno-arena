package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// franz-go v1.21.5 Kafka client for Analytics durable ingestion.

const (
	dlqHeaderConsumer        = "x-uno-dlq-consumer"
	dlqHeaderSourceTopic     = "x-uno-dlq-source-topic"
	dlqHeaderSourcePartition = "x-uno-dlq-source-partition"
	dlqHeaderSourceOffset    = "x-uno-dlq-source-offset"
	dlqHeaderAttemptCount    = "x-uno-dlq-attempt-count"
	dlqHeaderClassification  = "x-uno-dlq-classification"
	dlqHeaderFirstFailureAt  = "x-uno-dlq-first-failure-at"
	dlqHeaderLastFailureAt   = "x-uno-dlq-last-failure-at"
	dlqHeaderCorrelationID   = "x-uno-dlq-correlation-id"
	dlqHeaderErrorSummary    = "x-uno-dlq-error-summary"
)

// franzAnalyticsClient wraps a single kgo client for multi-topic consume + per-topic DLQ produce.
type franzAnalyticsClient struct {
	cl  *kgo.Client
	cfg AnalyticsKafkaConfig
}

func newFranzAnalyticsClient(cfg AnalyticsKafkaConfig) (*franzAnalyticsClient, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka brokers required")
	}
	if cfg.Group == "" || len(cfg.Topics) == 0 {
		return nil, fmt.Errorf("kafka group/topics required")
	}
	for _, t := range cfg.Topics {
		if cfg.DLQTopic(t) == "" {
			return nil, fmt.Errorf("kafka dlq required for topic %q", t)
		}
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.DisableAutoCommit(),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return &franzAnalyticsClient{cl: cl, cfg: cfg}, nil
}

func (c *franzAnalyticsClient) Poll(ctx context.Context) ([]ConsumerRecord, error) {
	fetches := c.cl.PollFetches(ctx)
	if errs := fetches.Errors(); len(errs) > 0 {
		for _, fe := range errs {
			if fe.Err != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				return nil, fe.Err
			}
		}
	}
	var out []ConsumerRecord
	fetches.EachRecord(func(r *kgo.Record) {
		out = append(out, consumerRecordFromKgo(r))
	})
	return out, nil
}

func (c *franzAnalyticsClient) Commit(ctx context.Context, rec ConsumerRecord) error {
	kr := &kgo.Record{
		Topic:     rec.Topic,
		Partition: rec.Partition,
		Offset:    rec.Offset,
		Key:       rec.Key,
		Value:     rec.Value,
	}
	if err := c.cl.CommitRecords(ctx, kr); err != nil {
		return fmt.Errorf("commit source offset: %w", err)
	}
	return nil
}

func (c *franzAnalyticsClient) PublishDLQ(ctx context.Context, original ConsumerRecord, meta DLQFailureMeta) error {
	source := firstNonEmpty(meta.SourceTopic, original.Topic)
	dlqTopic := c.cfg.DLQTopic(source)
	if dlqTopic == "" {
		return fmt.Errorf("dlq topic unresolved for source %q", source)
	}
	rec := &kgo.Record{
		Topic:   dlqTopic,
		Key:     append([]byte(nil), original.Key...),
		Value:   append([]byte(nil), original.Value...),
		Headers: dlqHeaders(meta),
	}
	results := c.cl.ProduceSync(ctx, rec)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("dlq produce: %w", err)
	}
	return nil
}

func (c *franzAnalyticsClient) Close() error {
	c.cl.Close()
	return nil
}

func consumerRecordFromKgo(r *kgo.Record) ConsumerRecord {
	return ConsumerRecord{
		Topic:     r.Topic,
		Partition: r.Partition,
		Offset:    r.Offset,
		Key:       append([]byte(nil), r.Key...),
		Value:     append([]byte(nil), r.Value...),
	}
}

func dlqHeaders(meta DLQFailureMeta) []kgo.RecordHeader {
	headers := []kgo.RecordHeader{
		{Key: dlqHeaderConsumer, Value: []byte(meta.Consumer)},
		{Key: dlqHeaderSourceTopic, Value: []byte(meta.SourceTopic)},
		{Key: dlqHeaderSourcePartition, Value: []byte(strconv.FormatInt(int64(meta.SourcePartition), 10))},
		{Key: dlqHeaderSourceOffset, Value: []byte(strconv.FormatInt(meta.SourceOffset, 10))},
		{Key: dlqHeaderAttemptCount, Value: []byte(strconv.Itoa(meta.AttemptCount))},
		{Key: dlqHeaderClassification, Value: []byte(meta.Classification)},
		{Key: dlqHeaderFirstFailureAt, Value: []byte(meta.FirstFailureAt.UTC().Format(time.RFC3339Nano))},
		{Key: dlqHeaderLastFailureAt, Value: []byte(meta.LastFailureAt.UTC().Format(time.RFC3339Nano))},
		{Key: dlqHeaderErrorSummary, Value: []byte(meta.ErrorSummary)},
	}
	if meta.CorrelationID != "" {
		headers = append(headers, kgo.RecordHeader{Key: dlqHeaderCorrelationID, Value: []byte(meta.CorrelationID)})
	}
	return headers
}
