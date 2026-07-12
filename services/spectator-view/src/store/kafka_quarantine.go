package store

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"unoarena/services/spectator-view/domain"
)

// Kafka quarantine classification strings (ADR-0017 operational metadata).
const (
	QuarantineClassSchemaInvalid  = "schema_invalid"
	QuarantineClassPayloadInvalid = "payload_invalid"
	QuarantineClassPrivacy        = "privacy_violation"
	QuarantineClassDependency     = "dependency_error"
	QuarantineClassApplication    = "application_error"
)

// KafkaAggregateQuarantine is a sanitized ADR-0017 quarantine record.
// Never store secrets or raw private payload.
type KafkaAggregateQuarantine struct {
	ConsumerGroup   string
	SourceTopic     string
	AggregateKey    string // roomId
	Classification  string
	Reason          string
	SourcePartition int32
	SourceOffset    int64
	EventID         string
	CorrelationID   string
	QuarantinedAt   time.Time
}

// ClassificationForDomainOutcome maps a durable domain outcome to a quarantine class.
func ClassificationForDomainOutcome(out domain.ApplyOutcome) string {
	if out.Rejection != nil {
		switch out.Rejection.Code {
		case domain.RejectForbiddenField, domain.RejectDisallowedField:
			return QuarantineClassPrivacy
		case domain.RejectInvalidSchema, domain.RejectUnknownEventType, domain.RejectInvalidIdentity:
			return QuarantineClassSchemaInvalid
		case domain.RejectOutOfOrderSequence, domain.RejectStaleSequence, domain.RejectRoomMismatch:
			return QuarantineClassApplication
		}
	}
	switch out.Kind {
	case domain.OutcomeDropped:
		return QuarantineClassPrivacy
	case domain.OutcomeQuarantined:
		return QuarantineClassApplication
	default:
		return QuarantineClassApplication
	}
}

// IsKafkaAggregateQuarantined reports whether an active quarantine holds the room.
func (s *RedisProjectionStore) IsKafkaAggregateQuarantined(ctx context.Context, consumerGroup, sourceTopic, aggregateKey string) (bool, error) {
	if s == nil || s.rdb == nil {
		return false, fmt.Errorf("nil store")
	}
	consumerGroup = strings.TrimSpace(consumerGroup)
	sourceTopic = strings.TrimSpace(sourceTopic)
	aggregateKey = strings.TrimSpace(aggregateKey)
	if consumerGroup == "" || sourceTopic == "" || aggregateKey == "" {
		return false, fmt.Errorf("quarantine identity required")
	}
	roomID := domain.RoomID(aggregateKey)
	if err := ValidateRoomID(roomID); err != nil {
		return false, err
	}
	fields, err := s.rdb.HGetAll(ctx, s.keys.KafkaQuarantine(roomID)).Result()
	if err != nil {
		return false, err
	}
	if len(fields) == 0 {
		return false, nil
	}
	if fields["active"] != "1" {
		return false, nil
	}
	if fields["consumer_group"] != consumerGroup || fields["source_topic"] != sourceTopic {
		return false, nil
	}
	return true, nil
}

// QuarantineKafkaAggregate atomically upserts an active quarantine for the room.
// Used for terminal-parse quarantine before DLQ; schema retains release fields for later audit.
func (s *RedisProjectionStore) QuarantineKafkaAggregate(ctx context.Context, rec KafkaAggregateQuarantine) error {
	if s == nil || s.rdb == nil {
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
		rec.Classification = QuarantineClassApplication
	}
	if rec.Reason == "" {
		rec.Reason = "quarantined"
	}
	roomID := domain.RoomID(rec.AggregateKey)
	if err := ValidateRoomID(roomID); err != nil {
		// Terminal parse may peek a roomId that cannot form Redis keys; skip persist.
		return nil
	}
	ts := rec.QuarantinedAt.UTC()
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	_, err := kafkaQuarantineScript.Run(ctx, s.rdb, []string{s.keys.KafkaQuarantine(roomID)},
		rec.ConsumerGroup,
		rec.SourceTopic,
		rec.Classification,
		rec.Reason,
		rec.EventID,
		rec.CorrelationID,
		strconv.FormatInt(int64(rec.SourcePartition), 10),
		strconv.FormatInt(rec.SourceOffset, 10),
		ts.Format(time.RFC3339Nano),
	).Text()
	return err
}

func sanitizeQuarantineReason(msg string) string {
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
	const maxLen = 256
	if len(msg) > maxLen {
		msg = msg[:maxLen]
	}
	return msg
}
