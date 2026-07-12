package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultSessionInvalidationTTL covers source retention (24h) + DLQ (30d).
// 31d is an acceptable production bound (ADR-0029 / ADR-0032).
const DefaultSessionInvalidationTTL = 31 * 24 * time.Hour

// SessionInvalidationApplyKind is the Redis Lua outcome for Apply.
type SessionInvalidationApplyKind string

const (
	SessionInvalidationAccepted  SessionInvalidationApplyKind = "accepted"
	SessionInvalidationRestored  SessionInvalidationApplyKind = "restored"
	SessionInvalidationDuplicate SessionInvalidationApplyKind = "duplicate"
	SessionInvalidationConflict  SessionInvalidationApplyKind = "conflict"
)

// SessionInvalidationRecord is the durable Gateway invalidation state.
// Redis is early-rejection / stream-close state only; Identity remains authoritative.
type SessionInvalidationRecord struct {
	EventID       string
	SessionID     string
	PlayerID      string
	Reason        string
	CorrelationID string
	OccurredAt    time.Time
}

// SessionInvalidationNotify is the Pub/Sub wake payload (not authoritative).
type SessionInvalidationNotify struct {
	EventID   string `json:"eventId"`
	SessionID string `json:"sessionId"`
	PlayerID  string `json:"playerId,omitempty"`
}

// SessionInvalidationStore persists Gateway-owned session invalidation on Redis DB6.
type SessionInvalidationStore struct {
	rdb  redis.UniversalClient
	keys SessionInvalidationKeySpace
	ttl  time.Duration
}

// NewSessionInvalidationStore constructs a store on the Gateway rate-limit Redis (DB6).
func NewSessionInvalidationStore(rdb redis.UniversalClient, ttl time.Duration) *SessionInvalidationStore {
	if ttl <= 0 {
		ttl = DefaultSessionInvalidationTTL
	}
	return &SessionInvalidationStore{
		rdb:  rdb,
		keys: NewSessionInvalidationKeySpace(""),
		ttl:  ttl,
	}
}

// WithKeyPrefix overrides the key namespace (tests).
func (s *SessionInvalidationStore) WithKeyPrefix(prefix string) *SessionInvalidationStore {
	if s == nil {
		return nil
	}
	out := *s
	out.keys = NewSessionInvalidationKeySpace(prefix)
	return &out
}

// LoadScripts warms Lua scripts (optional; EvalSHA falls back automatically).
func (s *SessionInvalidationStore) LoadScripts(ctx context.Context) error {
	if s == nil || s.rdb == nil {
		return fmt.Errorf("nil store")
	}
	if err := sessionInvalidationApplyScript.Load(ctx, s.rdb).Err(); err != nil {
		return err
	}
	return kafkaSIQuarantineScript.Load(ctx, s.rdb).Err()
}

// Apply atomically records invalidation + control binding and notifies replicas on first accept.
func (s *SessionInvalidationStore) Apply(ctx context.Context, rec SessionInvalidationRecord) (SessionInvalidationApplyKind, error) {
	if s == nil || s.rdb == nil {
		return "", fmt.Errorf("nil store")
	}
	rec.EventID = strings.TrimSpace(rec.EventID)
	rec.SessionID = strings.TrimSpace(rec.SessionID)
	rec.PlayerID = strings.TrimSpace(rec.PlayerID)
	rec.Reason = strings.TrimSpace(rec.Reason)
	rec.CorrelationID = strings.TrimSpace(rec.CorrelationID)
	if rec.EventID == "" || rec.SessionID == "" || rec.PlayerID == "" || rec.Reason == "" {
		return "", fmt.Errorf("eventId, sessionId, playerId, and reason are required")
	}
	if err := ValidateSessionInvalidationID(rec.SessionID); err != nil {
		return "", fmt.Errorf("sessionId: %w", err)
	}
	if err := ValidateSessionInvalidationID(rec.EventID); err != nil {
		return "", fmt.Errorf("eventId: %w", err)
	}
	if err := ValidateSessionInvalidationID(rec.PlayerID); err != nil {
		return "", fmt.Errorf("playerId: %w", err)
	}
	occurred := rec.OccurredAt.UTC()
	if occurred.IsZero() {
		occurred = time.Now().UTC()
	}
	notify, err := json.Marshal(SessionInvalidationNotify{
		EventID:   rec.EventID,
		SessionID: rec.SessionID,
		PlayerID:  rec.PlayerID,
	})
	if err != nil {
		return "", err
	}
	ttlSec := int(s.ttl.Seconds())
	if ttlSec < 1 {
		ttlSec = int(DefaultSessionInvalidationTTL.Seconds())
	}
	out, err := sessionInvalidationApplyScript.Run(ctx, s.rdb,
		[]string{s.keys.Session(rec.SessionID), s.keys.Event(rec.EventID)},
		rec.EventID,
		rec.SessionID,
		rec.PlayerID,
		rec.Reason,
		rec.CorrelationID,
		occurred.Format(time.RFC3339Nano),
		ttlSec,
		s.keys.NotifyChannel(),
		string(notify),
	).Text()
	if err != nil {
		return "", err
	}
	switch SessionInvalidationApplyKind(out) {
	case SessionInvalidationAccepted, SessionInvalidationRestored,
		SessionInvalidationDuplicate, SessionInvalidationConflict:
		return SessionInvalidationApplyKind(out), nil
	default:
		return "", fmt.Errorf("unexpected apply outcome %q", out)
	}
}

// IsInvalidated reports whether durable Redis holds an active invalidation for sessionID.
// Empty sessionID is never invalidated (anonymous spectator path).
func (s *SessionInvalidationStore) IsInvalidated(ctx context.Context, sessionID string) (bool, error) {
	if s == nil || s.rdb == nil {
		return false, fmt.Errorf("nil store")
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return false, nil
	}
	if err := ValidateSessionInvalidationID(sessionID); err != nil {
		return false, err
	}
	fields, err := s.rdb.HGetAll(ctx, s.keys.Session(sessionID)).Result()
	if err != nil {
		return false, err
	}
	if len(fields) == 0 {
		return false, nil
	}
	if fields["active"] == "0" {
		return false, nil
	}
	return true, nil
}

// NotifyChannel returns the Pub/Sub channel name.
func (s *SessionInvalidationStore) NotifyChannel() string {
	if s == nil {
		return SessionInvalidationNotifyChannel
	}
	return s.keys.NotifyChannel()
}

// Kafka quarantine classification strings (ADR-0017).
const (
	QuarantineClassSchemaInvalid  = "schema_invalid"
	QuarantineClassPayloadInvalid = "payload_invalid"
	QuarantineClassDependency     = "dependency_error"
	QuarantineClassApplication    = "application_error"
)

// KafkaAggregateQuarantine is a sanitized ADR-0017 quarantine record.
type KafkaAggregateQuarantine struct {
	ConsumerGroup   string
	SourceTopic     string
	AggregateKey    string // playerId partition key
	Classification  string
	Reason          string
	SourcePartition int32
	SourceOffset    int64
	EventID         string
	CorrelationID   string
	QuarantinedAt   time.Time
}

// IsKafkaAggregateQuarantined reports whether an active quarantine holds the player aggregate.
func (s *SessionInvalidationStore) IsKafkaAggregateQuarantined(ctx context.Context, consumerGroup, sourceTopic, aggregateKey string) (bool, error) {
	if s == nil || s.rdb == nil {
		return false, fmt.Errorf("nil store")
	}
	consumerGroup = strings.TrimSpace(consumerGroup)
	sourceTopic = strings.TrimSpace(sourceTopic)
	aggregateKey = strings.TrimSpace(aggregateKey)
	if consumerGroup == "" || sourceTopic == "" || aggregateKey == "" {
		return false, fmt.Errorf("quarantine identity required")
	}
	if err := ValidateSessionInvalidationID(aggregateKey); err != nil {
		return false, err
	}
	fields, err := s.rdb.HGetAll(ctx, s.keys.KafkaQuarantine(aggregateKey)).Result()
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

// QuarantineKafkaAggregate atomically upserts an active quarantine for the player key.
// Terminal parse with an invalid aggregate key skips persist (return nil).
func (s *SessionInvalidationStore) QuarantineKafkaAggregate(ctx context.Context, rec KafkaAggregateQuarantine) error {
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
	if err := ValidateSessionInvalidationID(rec.AggregateKey); err != nil {
		return nil
	}
	ts := rec.QuarantinedAt.UTC()
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	_, err := kafkaSIQuarantineScript.Run(ctx, s.rdb, []string{s.keys.KafkaQuarantine(rec.AggregateKey)},
		rec.ConsumerGroup,
		rec.SourceTopic,
		rec.Classification,
		sanitizeQuarantineReason(rec.Reason),
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
