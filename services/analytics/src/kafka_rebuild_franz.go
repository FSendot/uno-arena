package main

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// RebuildFollowUpPublisher publishes AnalyticsProjectionRebuildRequested follow-ups.
type RebuildFollowUpPublisher interface {
	PublishRebuildRequest(ctx context.Context, key string, value []byte) error
}

// franzAnalyticsRebuildClient consumes rebuild requests, publishes DLQ + follow-ups.
type franzAnalyticsRebuildClient struct {
	inner *franzAnalyticsClient
	cfg   ProjectionRebuildKafkaConfig
}

func newFranzAnalyticsRebuildClient(cfg ProjectionRebuildKafkaConfig) (*franzAnalyticsRebuildClient, error) {
	inner, err := newFranzAnalyticsClient(cfg.ToAnalyticsKafkaConfig())
	if err != nil {
		return nil, err
	}
	return &franzAnalyticsRebuildClient{inner: inner, cfg: cfg}, nil
}

func (c *franzAnalyticsRebuildClient) Poll(ctx context.Context) ([]ConsumerRecord, error) {
	return c.inner.Poll(ctx)
}

func (c *franzAnalyticsRebuildClient) Commit(ctx context.Context, rec ConsumerRecord) error {
	return c.inner.Commit(ctx, rec)
}

func (c *franzAnalyticsRebuildClient) PublishDLQ(ctx context.Context, original ConsumerRecord, meta DLQFailureMeta) error {
	return c.inner.PublishDLQ(ctx, original, meta)
}

func (c *franzAnalyticsRebuildClient) PublishRebuildRequest(ctx context.Context, key string, value []byte) error {
	if c == nil || c.inner == nil || c.inner.cl == nil {
		return fmt.Errorf("rebuild kafka client not configured")
	}
	rec := &kgo.Record{
		Topic: c.cfg.Topic,
		Key:   []byte(key),
		Value: append([]byte(nil), value...),
	}
	results := c.inner.cl.ProduceSync(ctx, rec)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("rebuild follow-up produce: %w", err)
	}
	return nil
}

func (c *franzAnalyticsRebuildClient) Close() error {
	if c == nil || c.inner == nil {
		return nil
	}
	return c.inner.Close()
}
