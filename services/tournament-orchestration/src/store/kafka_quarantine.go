package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// KafkaAggregateQuarantine is a sanitized ADR-0017 quarantine row.
type KafkaAggregateQuarantine struct {
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

// IsKafkaAggregateQuarantined reports whether an active quarantine holds the aggregate key.
func (s *TournamentStore) IsKafkaAggregateQuarantined(ctx context.Context, consumerGroup, sourceTopic, aggregateKey string) (bool, error) {
	if s == nil || s.pool == nil {
		return false, fmt.Errorf("nil store")
	}
	consumerGroup = strings.TrimSpace(consumerGroup)
	sourceTopic = strings.TrimSpace(sourceTopic)
	aggregateKey = strings.TrimSpace(aggregateKey)
	if consumerGroup == "" || sourceTopic == "" || aggregateKey == "" {
		return false, fmt.Errorf("quarantine identity required")
	}
	var active bool
	err := s.pool.QueryRow(ctx, `
		SELECT active FROM kafka_consumer_quarantine
		WHERE consumer_group = $1 AND source_topic = $2 AND aggregate_key = $3
	`, consumerGroup, sourceTopic, aggregateKey).Scan(&active)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, wrapUnavailable(err)
	}
	return active, nil
}

// QuarantineKafkaAggregate upserts an active quarantine row for the aggregate key.
func (s *TournamentStore) QuarantineKafkaAggregate(ctx context.Context, rec KafkaAggregateQuarantine) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("nil store")
	}
	rec.ConsumerGroup = strings.TrimSpace(rec.ConsumerGroup)
	rec.SourceTopic = strings.TrimSpace(rec.SourceTopic)
	rec.AggregateKey = strings.TrimSpace(rec.AggregateKey)
	rec.Classification = strings.TrimSpace(rec.Classification)
	rec.Reason = strings.TrimSpace(rec.Reason)
	if rec.ConsumerGroup == "" || rec.SourceTopic == "" || rec.AggregateKey == "" {
		return fmt.Errorf("quarantine identity required")
	}
	if rec.Classification == "" {
		rec.Classification = "application_error"
	}
	if rec.Reason == "" {
		rec.Reason = "quarantined"
	}
	now := time.Now().UTC()
	_, err := s.pool.Exec(ctx, `
		INSERT INTO kafka_consumer_quarantine (
			consumer_group, source_topic, aggregate_key, classification, reason,
			source_partition, source_offset, event_id, correlation_id,
			active, quarantined_at, updated_at, released_at, release_note
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, NULLIF($8, ''), NULLIF($9, ''),
			true, $10, $10, NULL, NULL
		)
		ON CONFLICT (consumer_group, source_topic, aggregate_key) DO UPDATE SET
			classification = EXCLUDED.classification,
			reason = EXCLUDED.reason,
			source_partition = EXCLUDED.source_partition,
			source_offset = EXCLUDED.source_offset,
			event_id = EXCLUDED.event_id,
			correlation_id = EXCLUDED.correlation_id,
			active = true,
			updated_at = EXCLUDED.updated_at,
			released_at = NULL,
			release_note = NULL
	`, rec.ConsumerGroup, rec.SourceTopic, rec.AggregateKey, rec.Classification, rec.Reason,
		rec.SourcePartition, rec.SourceOffset, rec.EventID, rec.CorrelationID, now)
	if err != nil {
		return wrapUnavailable(err)
	}
	return nil
}
