package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	AnalyticsBackfillDefaultLimit = 100
	AnalyticsBackfillMaxLimit     = 1000

	TopicMatchAssigned       = "tournament.match.assigned"
	TopicMatchResultRecorded = "tournament.match.result_recorded"
	TopicPlayersAdvanced     = "tournament.players.advanced"
	TopicRoundCompleted      = "tournament.round.completed"
	TopicTournamentCompleted = "tournament.completed"
)

// Tournament analytics-backfill topic allowlist (ADR-0039 / AsyncAPI).
var tournamentAnalyticsBackfillTopics = map[string]string{
	TopicMatchAssigned:       "TournamentMatchAssigned",
	TopicMatchResultRecorded: "TournamentMatchResultRecorded",
	TopicPlayersAdvanced:     "PlayersAdvanced",
	TopicRoundCompleted:      "TournamentRoundCompleted",
	TopicTournamentCompleted: "TournamentCompleted",
}

var analyticsBackfillForbiddenFields = map[string]struct{}{
	"hand": {}, "hands": {}, "cards": {}, "privatehand": {}, "drawncards": {},
	"drawcards": {}, "cardidentity": {}, "drawncardids": {}, "drawidentity": {},
	"deck": {}, "deckorder": {}, "hiddendeck": {}, "remainingdeck": {},
	"seed": {}, "deckseed": {}, "session": {}, "sessionid": {}, "sessiontoken": {},
	"token": {}, "accesstoken": {}, "refreshtoken": {}, "password": {}, "secret": {},
	"privatepayload": {}, "opponenthands": {}, "opponenthand": {}, "playerhand": {},
	"playeremail": {}, "email": {}, "dealmaterial": {}, "integrity": {},
	"gameintegrity": {}, "playerfeed": {},
}

// ErrAnalyticsBackfillBadRequest is a client validation failure.
var ErrAnalyticsBackfillBadRequest = errors.New("analytics backfill bad request")

// ErrAnalyticsBackfillUnavailable is returned when no durable/memory reader is wired.
var ErrAnalyticsBackfillUnavailable = errors.New("analytics backfill unavailable")

// ErrAnalyticsBackfillCorrupt is returned when a stored envelope fails closed validation.
var ErrAnalyticsBackfillCorrupt = errors.New("analytics backfill corrupt payload")

// AnalyticsBackfillRow is one immutable Tournament outbox row for Analytics recovery.
type AnalyticsBackfillRow struct {
	OutboxID      int64
	Topic         string
	EventType     string
	SchemaVersion int
	Payload       json.RawMessage
	OccurredAt    *time.Time // physical created_at used for occurredAt range coverage
}

// AnalyticsBackfillQuery is a bounded keyset page over outbox_events.
type AnalyticsBackfillQuery struct {
	Topic          string
	AfterOutboxID  int64 // exclusive; 0 = start
	Limit          int
	FromOutboxID   *int64
	ToOutboxID     *int64
	FromOccurredAt *time.Time
	ToOccurredAt   *time.Time
}

// AnalyticsBackfillReader reads owned append-only outbox rows without mutation.
type AnalyticsBackfillReader interface {
	List(ctx context.Context, q AnalyticsBackfillQuery) ([]AnalyticsBackfillRow, error)
}

// AnalyticsBackfillRequest is the strict JSON body for POST .../analytics-backfill.
type AnalyticsBackfillRequest struct {
	RecoveryJobID  string `json:"recoveryJobId"`
	SourceTopic    string `json:"sourceTopic"`
	SchemaVersion  int    `json:"schemaVersion"`
	Cursor         string `json:"cursor"`
	Limit          int    `json:"limit"`
	FromCheckpoint string `json:"fromCheckpoint"`
	ToCheckpoint   string `json:"toCheckpoint"`
	FromOccurredAt string `json:"fromOccurredAt"`
	ToOccurredAt   string `json:"toOccurredAt"`
}

// AnalyticsBackfillResponse is the operator recovery page.
type AnalyticsBackfillResponse struct {
	Records        []json.RawMessage `json:"records"`
	NextCursor     string            `json:"nextCursor,omitempty"`
	FromCheckpoint string            `json:"fromCheckpoint,omitempty"`
	ToCheckpoint   string            `json:"toCheckpoint,omitempty"`
	FromOccurredAt string            `json:"fromOccurredAt,omitempty"`
	ToOccurredAt   string            `json:"toOccurredAt,omitempty"`
	RecoveryJobID  string            `json:"recoveryJobId"`
	SourceTopic    string            `json:"sourceTopic"`
	SchemaVersion  int               `json:"schemaVersion"`
}

// MemoryAnalyticsBackfillStore is a bounded in-memory adapter for capability/tests.
type MemoryAnalyticsBackfillStore struct {
	mu   sync.Mutex
	rows []AnalyticsBackfillRow
	next int64
}

// NewMemoryAnalyticsBackfillStore constructs an empty in-memory backfill reader.
func NewMemoryAnalyticsBackfillStore() *MemoryAnalyticsBackfillStore {
	return &MemoryAnalyticsBackfillStore{}
}

// Append adds a canonical envelope for tests. outbox IDs are assigned monotonically.
func (m *MemoryAnalyticsBackfillStore) Append(topic, eventType string, payload json.RawMessage, occurredAt *time.Time) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.next++
	id := m.next
	m.rows = append(m.rows, AnalyticsBackfillRow{
		OutboxID: id, Topic: topic, EventType: eventType, SchemaVersion: 1,
		Payload: payload, OccurredAt: occurredAt,
	})
	return id
}

// AppendCorrupt adds a row with explicit schema_version / event_type for fail-closed tests.
func (m *MemoryAnalyticsBackfillStore) AppendCorrupt(topic, eventType string, schemaVersion int, payload json.RawMessage, occurredAt *time.Time) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.next++
	id := m.next
	m.rows = append(m.rows, AnalyticsBackfillRow{
		OutboxID: id, Topic: topic, EventType: eventType, SchemaVersion: schemaVersion,
		Payload: payload, OccurredAt: occurredAt,
	})
	return id
}

// List implements AnalyticsBackfillReader with keyset semantics (no OFFSET).
func (m *MemoryAnalyticsBackfillStore) List(_ context.Context, q AnalyticsBackfillQuery) ([]AnalyticsBackfillRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	candidates := make([]AnalyticsBackfillRow, 0, len(m.rows))
	for _, row := range m.rows {
		if row.Topic != q.Topic || row.OutboxID <= q.AfterOutboxID {
			continue
		}
		if q.FromOutboxID != nil && row.OutboxID < *q.FromOutboxID {
			continue
		}
		if q.ToOutboxID != nil && row.OutboxID > *q.ToOutboxID {
			continue
		}
		if q.FromOccurredAt != nil {
			if row.OccurredAt == nil || row.OccurredAt.Before(*q.FromOccurredAt) {
				continue
			}
		}
		if q.ToOccurredAt != nil {
			if row.OccurredAt == nil || row.OccurredAt.After(*q.ToOccurredAt) {
				continue
			}
		}
		candidates = append(candidates, row)
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].OutboxID < candidates[j].OutboxID
	})
	if len(candidates) > q.Limit {
		candidates = candidates[:q.Limit]
	}
	return candidates, nil
}

// Count returns stored row count (immutability tests).
func (m *MemoryAnalyticsBackfillStore) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.rows)
}

// SetAnalyticsBackfillReader wires the recovery reader (durable or memory).
func (s *Service) SetAnalyticsBackfillReader(r AnalyticsBackfillReader) {
	s.analyticsBackfill = r
}

// AnalyticsBackfill executes a bounded, read-only page from the owned outbox.
func (s *Service) AnalyticsBackfill(ctx context.Context, req AnalyticsBackfillRequest) (AnalyticsBackfillResponse, error) {
	parsed, err := parseAnalyticsBackfillRequest(req)
	if err != nil {
		return AnalyticsBackfillResponse{}, err
	}
	if s.analyticsBackfill == nil {
		return AnalyticsBackfillResponse{}, ErrAnalyticsBackfillUnavailable
	}

	afterID := int64(0)
	if parsed.cursorRaw != "" {
		cur, err := DecodeAnalyticsBackfillCursor(parsed.cursorRaw)
		if err != nil {
			return AnalyticsBackfillResponse{}, fmt.Errorf("%w: cursor", ErrAnalyticsBackfillBadRequest)
		}
		if cur.SourceTopic != parsed.topic || cur.RecoveryJobID != parsed.jobID ||
			cur.FromCheckpoint != parsed.fromCP || cur.ToCheckpoint != parsed.toCP ||
			cur.FromOccurredAt != parsed.fromAt || cur.ToOccurredAt != parsed.toAt {
			return AnalyticsBackfillResponse{}, fmt.Errorf("%w: cursor query binding mismatch", ErrAnalyticsBackfillBadRequest)
		}
		afterID = cur.OutboxID
	}

	// Fetch one lookahead row so an exactly-full final page is still reported
	// as complete rather than issuing a spurious continuation cursor.
	rows, err := s.analyticsBackfill.List(ctx, AnalyticsBackfillQuery{
		Topic:          parsed.topic,
		AfterOutboxID:  afterID,
		Limit:          parsed.limit + 1,
		FromOutboxID:   parsed.fromOID,
		ToOutboxID:     parsed.toOID,
		FromOccurredAt: parsed.fromTime,
		ToOccurredAt:   parsed.toTime,
	})
	if err != nil {
		return AnalyticsBackfillResponse{}, err
	}
	hasMore := len(rows) > parsed.limit
	if hasMore {
		rows = rows[:parsed.limit]
	}

	wantType := tournamentAnalyticsBackfillTopics[parsed.topic]
	records := make([]json.RawMessage, 0, len(rows))
	var pageFrom, pageTo string
	var pageFromAt, pageToAt string
	for _, row := range rows {
		if row.Topic != parsed.topic {
			return AnalyticsBackfillResponse{}, fmt.Errorf("%w: topic mismatch", ErrAnalyticsBackfillCorrupt)
		}
		if strings.TrimSpace(row.EventType) != "" && row.EventType != wantType {
			return AnalyticsBackfillResponse{}, fmt.Errorf("%w: event_type column", ErrAnalyticsBackfillCorrupt)
		}
		if row.SchemaVersion != 0 && row.SchemaVersion != 1 {
			return AnalyticsBackfillResponse{}, fmt.Errorf("%w: schema_version column", ErrAnalyticsBackfillCorrupt)
		}
		if err := validateAnalyticsBackfillEnvelope(row.Payload, parsed.topic, wantType); err != nil {
			return AnalyticsBackfillResponse{}, fmt.Errorf("%w: %v", ErrAnalyticsBackfillCorrupt, err)
		}
		records = append(records, append(json.RawMessage(nil), row.Payload...))
		cp := strconv.FormatInt(row.OutboxID, 10)
		if pageFrom == "" {
			pageFrom = cp
		}
		pageTo = cp
		if row.OccurredAt != nil {
			at := row.OccurredAt.UTC().Format(time.RFC3339Nano)
			if pageFromAt == "" {
				pageFromAt = at
			}
			pageToAt = at
		}
	}

	resp := AnalyticsBackfillResponse{
		Records:        records,
		FromCheckpoint: pageFrom,
		ToCheckpoint:   pageTo,
		FromOccurredAt: pageFromAt,
		ToOccurredAt:   pageToAt,
		RecoveryJobID:  parsed.jobID,
		SourceTopic:    parsed.topic,
		SchemaVersion:  1,
	}
	if hasMore {
		last := rows[len(rows)-1]
		next, err := EncodeAnalyticsBackfillCursor(AnalyticsBackfillCursor{
			OutboxID: last.OutboxID, SourceTopic: parsed.topic, RecoveryJobID: parsed.jobID,
			FromCheckpoint: parsed.fromCP, ToCheckpoint: parsed.toCP,
			FromOccurredAt: parsed.fromAt, ToOccurredAt: parsed.toAt,
		})
		if err != nil {
			return AnalyticsBackfillResponse{}, err
		}
		resp.NextCursor = next
	}
	return resp, nil
}

type parsedAnalyticsBackfill struct {
	jobID, topic, cursorRaw    string
	fromCP, toCP, fromAt, toAt string
	limit                      int
	fromOID, toOID             *int64
	fromTime, toTime           *time.Time
}

func parseAnalyticsBackfillRequest(req AnalyticsBackfillRequest) (parsedAnalyticsBackfill, error) {
	jobID := strings.TrimSpace(req.RecoveryJobID)
	topic := strings.TrimSpace(req.SourceTopic)
	if jobID == "" {
		return parsedAnalyticsBackfill{}, fmt.Errorf("%w: recoveryJobId required", ErrAnalyticsBackfillBadRequest)
	}
	if req.SchemaVersion != 1 {
		return parsedAnalyticsBackfill{}, fmt.Errorf("%w: schemaVersion must be 1", ErrAnalyticsBackfillBadRequest)
	}
	if _, ok := tournamentAnalyticsBackfillTopics[topic]; !ok {
		return parsedAnalyticsBackfill{}, fmt.Errorf("%w: sourceTopic not allowlisted", ErrAnalyticsBackfillBadRequest)
	}

	limit := req.Limit
	if limit == 0 {
		limit = AnalyticsBackfillDefaultLimit
	}
	if limit < 1 || limit > AnalyticsBackfillMaxLimit {
		return parsedAnalyticsBackfill{}, fmt.Errorf("%w: limit must be 1..%d", ErrAnalyticsBackfillBadRequest, AnalyticsBackfillMaxLimit)
	}

	fromCP := strings.TrimSpace(req.FromCheckpoint)
	toCP := strings.TrimSpace(req.ToCheckpoint)
	fromAt := strings.TrimSpace(req.FromOccurredAt)
	toAt := strings.TrimSpace(req.ToOccurredAt)

	hasCP := fromCP != "" || toCP != ""
	hasAt := fromAt != "" || toAt != ""
	if !hasCP && !hasAt {
		return parsedAnalyticsBackfill{}, fmt.Errorf("%w: bounded range required", ErrAnalyticsBackfillBadRequest)
	}
	if (fromCP == "") != (toCP == "") {
		return parsedAnalyticsBackfill{}, fmt.Errorf("%w: fromCheckpoint/toCheckpoint must both be set", ErrAnalyticsBackfillBadRequest)
	}
	if (fromAt == "") != (toAt == "") {
		return parsedAnalyticsBackfill{}, fmt.Errorf("%w: fromOccurredAt/toOccurredAt must both be set", ErrAnalyticsBackfillBadRequest)
	}

	out := parsedAnalyticsBackfill{
		jobID: jobID, topic: topic, cursorRaw: strings.TrimSpace(req.Cursor),
		fromCP: fromCP, toCP: toCP, fromAt: fromAt, toAt: toAt, limit: limit,
	}
	if fromCP != "" {
		fromOID, err := strconv.ParseInt(fromCP, 10, 64)
		if err != nil || fromOID < 1 {
			return parsedAnalyticsBackfill{}, fmt.Errorf("%w: fromCheckpoint", ErrAnalyticsBackfillBadRequest)
		}
		toOID, err := strconv.ParseInt(toCP, 10, 64)
		if err != nil || toOID < 1 {
			return parsedAnalyticsBackfill{}, fmt.Errorf("%w: toCheckpoint", ErrAnalyticsBackfillBadRequest)
		}
		if fromOID > toOID {
			return parsedAnalyticsBackfill{}, fmt.Errorf("%w: inverted checkpoint range", ErrAnalyticsBackfillBadRequest)
		}
		out.fromOID, out.toOID = &fromOID, &toOID
	}
	if fromAt != "" {
		fromTime, err := time.Parse(time.RFC3339, fromAt)
		if err != nil {
			return parsedAnalyticsBackfill{}, fmt.Errorf("%w: fromOccurredAt", ErrAnalyticsBackfillBadRequest)
		}
		toTime, err := time.Parse(time.RFC3339, toAt)
		if err != nil {
			return parsedAnalyticsBackfill{}, fmt.Errorf("%w: toOccurredAt", ErrAnalyticsBackfillBadRequest)
		}
		if fromTime.After(toTime) {
			return parsedAnalyticsBackfill{}, fmt.Errorf("%w: inverted occurredAt range", ErrAnalyticsBackfillBadRequest)
		}
		out.fromTime, out.toTime = &fromTime, &toTime
	}
	return out, nil
}

func validateAnalyticsBackfillEnvelope(raw json.RawMessage, topic, wantEventType string) error {
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return fmt.Errorf("json: %w", err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("trailing json")
	}
	if err := rejectForbiddenAnalyticsFields(m); err != nil {
		return err
	}
	sv, ok := m["schemaVersion"]
	if !ok {
		return fmt.Errorf("schemaVersion missing")
	}
	switch v := sv.(type) {
	case json.Number:
		n, err := v.Int64()
		if err != nil || n != 1 {
			return fmt.Errorf("schemaVersion")
		}
	case float64:
		if int(v) != 1 {
			return fmt.Errorf("schemaVersion")
		}
	default:
		return fmt.Errorf("schemaVersion type")
	}
	et, _ := m["eventType"].(string)
	if et != wantEventType {
		return fmt.Errorf("eventType want %s got %s", wantEventType, et)
	}
	for _, k := range []string{"eventId", "correlationId", "occurredAt"} {
		s, _ := m[k].(string)
		if strings.TrimSpace(s) == "" {
			return fmt.Errorf("%s required", k)
		}
	}
	if strings.TrimSpace(asString(m["tournamentId"])) == "" {
		return fmt.Errorf("tournamentId required")
	}
	switch topic {
	case TopicMatchAssigned:
		if _, ok := m["roundNumber"]; !ok {
			return fmt.Errorf("roundNumber required")
		}
		if strings.TrimSpace(asString(m["slotId"])) == "" {
			return fmt.Errorf("slotId required")
		}
		if strings.TrimSpace(asString(m["roomId"])) == "" {
			return fmt.Errorf("roomId required")
		}
	case TopicMatchResultRecorded:
		if strings.TrimSpace(asString(m["roomId"])) == "" {
			return fmt.Errorf("roomId required")
		}
		if _, ok := m["completionVersion"]; !ok {
			return fmt.Errorf("completionVersion required")
		}
	case TopicPlayersAdvanced:
		if _, ok := m["roundNumber"]; !ok {
			return fmt.Errorf("roundNumber required")
		}
		if strings.TrimSpace(asString(m["sourceSlotId"])) == "" {
			return fmt.Errorf("sourceSlotId required")
		}
		ids, ok := m["advancingPlayerIds"].([]any)
		if !ok || len(ids) < 1 || len(ids) > 3 {
			return fmt.Errorf("advancingPlayerIds")
		}
	case TopicRoundCompleted:
		if _, ok := m["roundNumber"]; !ok {
			return fmt.Errorf("roundNumber required")
		}
	case TopicTournamentCompleted:
		standings, ok := m["finalStandings"].([]any)
		if !ok || len(standings) < 1 || len(standings) > 10 {
			return fmt.Errorf("finalStandings")
		}
	}
	return nil
}

func rejectForbiddenAnalyticsFields(v any) error {
	switch t := v.(type) {
	case map[string]any:
		for k, child := range t {
			norm := strings.ToLower(strings.ReplaceAll(k, "_", ""))
			if _, bad := analyticsBackfillForbiddenFields[norm]; bad {
				return fmt.Errorf("forbidden field %s", k)
			}
			if err := rejectForbiddenAnalyticsFields(child); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range t {
			if err := rejectForbiddenAnalyticsFields(child); err != nil {
				return err
			}
		}
	}
	return nil
}

func asString(v any) string {
	s, _ := v.(string)
	return s
}
