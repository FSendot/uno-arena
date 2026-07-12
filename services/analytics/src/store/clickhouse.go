package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"unoarena/services/analytics/domain"
)

const (
	dispositionApplied     = "applied"
	dispositionQuarantined = "quarantined"
	dispositionIgnored     = "ignored"
	genStatusBuilding      = "building"
	genStatusComplete      = "complete"
	initialGenerationID    = "gen_initial"
)

// clickHouseSurface is the Exec/Query surface AnalyticsStore uses.
// *Client is the production implementation; tests may inject a blocking stand-in.
type clickHouseSurface interface {
	Exec(ctx context.Context, query string, params map[string]string) error
	Query(ctx context.Context, query string, params ...map[string]string) ([][]string, error)
	QueryCell(ctx context.Context, query string, params map[string]string) (string, error)
}

// AnalyticsStore is the durable ClickHouse projection adapter (stdlib HTTP only).
// ClickHouse is non-transactional: projection rows are written before processed_events.
//
// Live Apply dual-writes to the active completed generation and every durable
// initializing/building recovery generation (ADR-0039). Process-local RWMutex is
// an optimization for ad-hoc Rebuild tests only — never the multi-replica fence.
// Snapshot/ProjectionVersion observe the active completed generation only.
type AnalyticsStore struct {
	client       clickHouseSurface
	httpClient   *Client // non-nil when constructed via NewAnalyticsStore
	mu           sync.RWMutex
	initMu       sync.Mutex
	adHocRebuild bool // tests/capability only; production durable path stays fail-closed
}

// NewAnalyticsStore wraps a ClickHouse HTTP client.
func NewAnalyticsStore(c *Client) *AnalyticsStore {
	return &AnalyticsStore{client: c, httpClient: c}
}

// Client exposes the underlying HTTP client (tests/ops).
func (s *AnalyticsStore) Client() *Client { return s.httpClient }

// Ready verifies schema and ensures an active completed generation exists.
func (s *AnalyticsStore) Ready(ctx context.Context) error {
	if s.httpClient == nil {
		return fmt.Errorf("analytics ready: HTTP ClickHouse client required")
	}
	if err := VerifySchema(ctx, s.httpClient); err != nil {
		return err
	}
	_, err := s.ensureActiveGeneration(ctx)
	return err
}

// Apply validates/sanitizes via domain policy, then durably writes sanitized rows
// to the active generation and every valid initializing/building recovery generation.
// A building write failure fails the whole Apply so the Kafka source offset is not committed.
func (s *AnalyticsStore) Apply(ctx context.Context, evt domain.UpstreamEvent) (domain.ApplyOutcome, error) {
	// Bootstrap without mu — initMu only — so Apply never lock-upgrades RLock→Lock.
	if _, err := s.ensureActiveGeneration(ctx); err != nil {
		return domain.ApplyOutcome{}, err
	}
	// Shared lock is optional serialization vs ad-hoc Rebuild tests only.
	s.mu.RLock()
	defer s.mu.RUnlock()

	gens, err := s.ListWriteGenerations(ctx)
	if err != nil {
		return domain.ApplyOutcome{}, err
	}
	if len(gens) == 0 {
		return domain.ApplyOutcome{}, fmt.Errorf("no write generations")
	}
	var activeOut domain.ApplyOutcome
	for i, genID := range gens {
		out, err := s.applyToGeneration(ctx, genID, evt)
		if err != nil {
			return domain.ApplyOutcome{}, err
		}
		if i == 0 {
			activeOut = out
		}
	}
	return activeOut, nil
}

// ApplyToGeneration applies one event to a specific generation (recovery worker page apply).
func (s *AnalyticsStore) ApplyToGeneration(ctx context.Context, genID string, evt domain.UpstreamEvent) (domain.ApplyOutcome, error) {
	if strings.TrimSpace(genID) == "" {
		return domain.ApplyOutcome{}, fmt.Errorf("generation id required")
	}
	return s.applyToGeneration(ctx, genID, evt)
}

// Rebuild builds a new generation invisibly, then activates it only after completion.
// Fail-closed outside explicit ad-hoc enablement (tests); production uses the coordinator.
func (s *AnalyticsStore) Rebuild(ctx context.Context, events []domain.UpstreamEvent) ([]domain.ApplyOutcome, error) {
	if !s.adHocRebuild {
		return nil, ErrAdHocRebuildDisabled
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	newGen, err := newGenerationID()
	if err != nil {
		return nil, err
	}
	if err := s.insertGeneration(ctx, newGen, genStatusBuilding, 0, false); err != nil {
		return nil, err
	}

	outs := make([]domain.ApplyOutcome, 0, len(events))
	var accepted uint64
	for _, evt := range events {
		out, err := s.applyToGeneration(ctx, newGen, evt)
		if err != nil {
			// Failed rebuild: leave building generation inactive; prior active remains visible.
			return nil, err
		}
		outs = append(outs, out)
		if out.Kind == domain.OutcomeAccepted {
			accepted++
		}
	}

	if err := s.insertGeneration(ctx, newGen, genStatusComplete, accepted, true); err != nil {
		return nil, err
	}
	if err := s.switchActiveGeneration(ctx, newGen); err != nil {
		return nil, err
	}
	return outs, nil
}

// Snapshot reads only the latest completed active generation.
func (s *AnalyticsStore) Snapshot(ctx context.Context) (domain.AnalyticsSnapshot, error) {
	s.mu.RLock()
	genID, err := s.activeCompletedGeneration(ctx)
	s.mu.RUnlock()
	if err != nil {
		return domain.AnalyticsSnapshot{}, err
	}
	version, err := s.projectionVersionFor(ctx, genID)
	if err != nil {
		return domain.AnalyticsSnapshot{}, err
	}
	gameplay, err := s.loadGameplay(ctx, genID)
	if err != nil {
		return domain.AnalyticsSnapshot{}, err
	}
	tournaments, err := s.loadTournaments(ctx, genID)
	if err != nil {
		return domain.AnalyticsSnapshot{}, err
	}
	ratings, err := s.loadRatings(ctx, genID)
	if err != nil {
		return domain.AnalyticsSnapshot{}, err
	}
	return domain.AnalyticsSnapshot{
		Authoritative:     false,
		ProjectionVersion: version,
		GameplayMetrics:   gameplay,
		TournamentStats:   tournaments,
		RatingStats:       ratings,
	}, nil
}

// SnapshotJSON encodes Snapshot.
func (s *AnalyticsStore) SnapshotJSON(ctx context.Context) ([]byte, error) {
	snap, err := s.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	return json.Marshal(snap)
}

// ProjectionVersion returns accepted-event count for the active generation.
func (s *AnalyticsStore) ProjectionVersion(ctx context.Context) (domain.ProjectionVersion, error) {
	s.mu.RLock()
	genID, err := s.activeCompletedGeneration(ctx)
	s.mu.RUnlock()
	if err != nil {
		return 0, err
	}
	return s.projectionVersionFor(ctx, genID)
}

func (s *AnalyticsStore) applyToGeneration(ctx context.Context, genID string, evt domain.UpstreamEvent) (domain.ApplyOutcome, error) {
	// HTTP/rebuild bridges may omit adapter keys; Kafka always supplies them.
	domain.EnsureIngressIdentity(&evt)
	idem := domain.EffectiveIdempotencyKey(evt)
	topic := string(evt.Source)
	fp := strings.TrimSpace(evt.PayloadFingerprint)

	// Lookup by ADR-0029 contract key (generation_id, topic, idempotency_key).
	if prior, priorFP, priorEventID, ok, err := s.loadProcessed(ctx, genID, topic, idem); err != nil {
		return domain.ApplyOutcome{}, err
	} else if ok {
		if priorFP == fp {
			return duplicateFromStored(prior), nil
		}
		// First-wins: conflicting fingerprint quarantines without writing a second marker
		// or mutating projection rows. Record the conflict durably before returning.
		out := conflictWithoutWrite(evt)
		if err := s.recordIngestionConflict(ctx, genID, evt, priorEventID, priorFP, out); err != nil {
			return domain.ApplyOutcome{}, err
		}
		return out, nil
	}

	// Validate + sanitize through domain projection before any durable write.
	tmp := domain.NewPublicAnalyticsProjection()
	out := tmp.Apply(evt)

	outcomeJSON, err := marshalDurableOutcome(out)
	if err != nil {
		return domain.ApplyOutcome{}, err
	}

	switch out.Kind {
	case domain.OutcomeAccepted:
		// Projection rows first, processed marker last (ClickHouse crash-window).
		// Redelivery replaces ReplacingMergeTree logical rows keyed by source_topic+idempotency_key.
		snap := tmp.Snapshot()
		if err := s.insertProjectionRows(ctx, genID, evt, snap); err != nil {
			return domain.ApplyOutcome{}, err
		}
		if err := s.insertProcessed(ctx, genID, evt, dispositionApplied, outcomeJSON); err != nil {
			return domain.ApplyOutcome{}, err
		}
	case domain.OutcomeQuarantined:
		// Zero projection rows; marker last (and only durable write).
		if err := s.insertProcessed(ctx, genID, evt, dispositionQuarantined, outcomeJSON); err != nil {
			return domain.ApplyOutcome{}, err
		}
	case domain.OutcomeIgnored:
		if err := s.insertProcessed(ctx, genID, evt, dispositionIgnored, outcomeJSON); err != nil {
			return domain.ApplyOutcome{}, err
		}
	default:
		// Unexpected for first apply; still persist marker for stability if EventID valid.
		if evt.EventID.Valid() {
			if err := s.insertProcessed(ctx, genID, evt, string(out.Kind), outcomeJSON); err != nil {
				return domain.ApplyOutcome{}, err
			}
		}
	}

	// Concurrent same contract key: re-read FINAL so callers see one logical disposition.
	if stored, storedFP, storedEventID, ok, err := s.loadProcessed(ctx, genID, topic, idem); err != nil {
		return domain.ApplyOutcome{}, err
	} else if ok {
		if storedFP != fp {
			conflictOut := conflictWithoutWrite(evt)
			if err := s.recordIngestionConflict(ctx, genID, evt, storedEventID, storedFP, conflictOut); err != nil {
				return domain.ApplyOutcome{}, err
			}
			return conflictOut, nil
		}
		if stored.Kind == out.Kind {
			return out, nil
		}
		return duplicateFromStored(stored), nil
	}
	return out, nil
}

func conflictWithoutWrite(evt domain.UpstreamEvent) domain.ApplyOutcome {
	rej := domain.Rejection{
		Code:    domain.RejectPayloadConflict,
		Message: "idempotency key collides with a different immutable payload fingerprint",
	}
	return domain.ApplyOutcome{
		Kind:      domain.OutcomeQuarantined,
		EventID:   evt.EventID,
		Rejection: &rej,
		Facts: []domain.Fact{{
			Name: domain.FactProjectionEventQuarantined,
			Data: map[string]string{
				"eventId":       string(evt.EventID),
				"eventType":     string(evt.EventType),
				"code":          string(rej.Code),
				"reason":        rej.Message,
				"authoritative": "false",
			},
		}},
	}
}

func (s *AnalyticsStore) insertProjectionRows(ctx context.Context, genID string, evt domain.UpstreamEvent, snap domain.AnalyticsSnapshot) error {
	occurred := evt.OccurredAt.UTC()
	if occurred.IsZero() {
		occurred = time.Now().UTC()
	}
	occurredStr := occurred.Format("2006-01-02 15:04:05.000")

	idem := domain.EffectiveIdempotencyKey(evt)
	sourceTopic := string(evt.Source)
	for _, m := range snap.GameplayMetrics {
		if string(m.EventID) != string(evt.EventID) {
			continue
		}
		q := `INSERT INTO gameplay_metrics (
			generation_id, source_topic, event_id, idempotency_key, schema_version, correlation_id,
			room_id, game_id, tournament_id, visibility, metric_type,
			public_card_rank, public_card_color, public_card_count_total, room_sequence,
			public_player_id, display_name, occurred_at
		) VALUES (
			{gen:String}, {topic:String}, {eid:String}, {ikey:String}, {sv:UInt16}, {cid:String},
			{room:String}, {game:String}, {tid:String}, {vis:String}, {mt:String},
			{rank:String}, {color:String}, {pct:UInt16}, {seq:UInt64},
			{pid:String}, {dn:String}, {oa:String}
		)`
		params := map[string]string{
			"gen":   genID,
			"topic": sourceTopic,
			"eid":   string(m.EventID),
			"ikey":  idem,
			"sv":    fmt.Sprintf("%d", evt.SchemaVersion),
			"cid":   m.CorrelationID,
			"room":  string(m.RoomID),
			"game":  string(m.GameID),
			"tid":   string(m.TournamentID),
			"vis":   string(m.Visibility),
			"mt":    m.MetricType,
			"rank":  m.PublicCardRank,
			"color": m.PublicCardColor,
			"pct":   fmt.Sprintf("%d", m.PublicCardCountTotal),
			"seq":   fmt.Sprintf("%d", m.RoomSequence),
			"pid":   string(m.PublicPlayerID),
			"dn":    m.DisplayName,
			"oa":    occurredStr,
		}
		if err := s.client.Exec(ctx, q, params); err != nil {
			return fmt.Errorf("insert gameplay_metrics: %w", err)
		}
	}

	for _, t := range snap.TournamentStats {
		if string(t.EventID) != string(evt.EventID) {
			continue
		}
		payloadJSON, err := marshalSortedStringMap(t.PublicPayload)
		if err != nil {
			return err
		}
		q := `INSERT INTO tournament_statistics (
			generation_id, source_topic, event_id, idempotency_key, schema_version, correlation_id,
			tournament_id, round_number, slot_id, event_type, phase,
			registered_count, advancing_player_count, public_payload_json, occurred_at
		) VALUES (
			{gen:String}, {topic:String}, {eid:String}, {ikey:String}, {sv:UInt16}, {cid:String},
			{tid:String}, {rn:Int32}, {slot:String}, {et:String}, {phase:String},
			{rc:UInt32}, {ac:UInt16}, {pp:String}, {oa:String}
		)`
		params := map[string]string{
			"gen":   genID,
			"topic": sourceTopic,
			"eid":   string(t.EventID),
			"ikey":  idem,
			"sv":    fmt.Sprintf("%d", evt.SchemaVersion),
			"cid":   t.CorrelationID,
			"tid":   string(t.TournamentID),
			"rn":    fmt.Sprintf("%d", t.RoundNumber),
			"slot":  t.SlotID,
			"et":    t.EventType,
			"phase": t.Phase,
			"rc":    fmt.Sprintf("%d", t.RegisteredCount),
			"ac":    fmt.Sprintf("%d", t.AdvancingPlayerCount),
			"pp":    payloadJSON,
			"oa":    occurredStr,
		}
		if err := s.client.Exec(ctx, q, params); err != nil {
			return fmt.Errorf("insert tournament_statistics: %w", err)
		}
	}

	for _, r := range snap.RatingStats {
		if string(r.EventID) != string(evt.EventID) {
			continue
		}
		q := `INSERT INTO rating_statistics (
			generation_id, source_topic, event_id, idempotency_key, schema_version, correlation_id,
			player_id, source_type, previous_rating, new_rating, board_type, snapshot_id, occurred_at
		) VALUES (
			{gen:String}, {topic:String}, {eid:String}, {ikey:String}, {sv:UInt16}, {cid:String},
			{pid:String}, {st:String}, {prev:Int32}, {next:Int32}, {bt:String}, {sid:String}, {oa:String}
		)`
		params := map[string]string{
			"gen":   genID,
			"topic": sourceTopic,
			"eid":   string(r.EventID),
			"ikey":  idem,
			"sv":    fmt.Sprintf("%d", evt.SchemaVersion),
			"cid":   r.CorrelationID,
			"pid":   string(r.PlayerID),
			"st":    r.SourceType,
			"prev":  fmt.Sprintf("%d", r.PreviousRating),
			"next":  fmt.Sprintf("%d", r.NewRating),
			"bt":    r.BoardType,
			"sid":   string(r.SnapshotID),
			"oa":    occurredStr,
		}
		if err := s.client.Exec(ctx, q, params); err != nil {
			return fmt.Errorf("insert rating_statistics: %w", err)
		}
	}
	return nil
}

func (s *AnalyticsStore) insertProcessed(ctx context.Context, genID string, evt domain.UpstreamEvent, disposition, outcomeJSON string) error {
	q := `INSERT INTO processed_events (
		generation_id, topic, idempotency_key, event_id, payload_fingerprint, disposition, outcome_json
	) VALUES (
		{gen:String}, {topic:String}, {ikey:String}, {eid:String}, {fp:String}, {disp:String}, {oj:String}
	)`
	return s.client.Exec(ctx, q, map[string]string{
		"gen":   genID,
		"topic": string(evt.Source),
		"ikey":  domain.EffectiveIdempotencyKey(evt),
		"eid":   string(evt.EventID),
		"fp":    strings.TrimSpace(evt.PayloadFingerprint),
		"disp":  disposition,
		"oj":    outcomeJSON,
	})
}

func (s *AnalyticsStore) recordIngestionConflict(ctx context.Context, genID string, evt domain.UpstreamEvent, originalEventID, firstMarkerFP string, out domain.ApplyOutcome) error {
	outcomeJSON, err := marshalDurableOutcome(out)
	if err != nil {
		return err
	}
	q := `INSERT INTO ingestion_conflicts (
		generation_id, topic, idempotency_key, conflicting_fingerprint,
		original_event_id, seen_event_id, first_marker_fingerprint, outcome_json
	) VALUES (
		{gen:String}, {topic:String}, {ikey:String}, {cfp:String},
		{oid:String}, {sid:String}, {ffp:String}, {oj:String}
	)`
	if err := s.client.Exec(ctx, q, map[string]string{
		"gen":   genID,
		"topic": string(evt.Source),
		"ikey":  domain.EffectiveIdempotencyKey(evt),
		"cfp":   strings.TrimSpace(evt.PayloadFingerprint),
		"oid":   originalEventID,
		"sid":   string(evt.EventID),
		"ffp":   firstMarkerFP,
		"oj":    outcomeJSON,
	}); err != nil {
		return fmt.Errorf("insert ingestion_conflicts: %w", err)
	}
	return nil
}

func (s *AnalyticsStore) loadProcessed(ctx context.Context, genID, topic, idempotencyKey string) (domain.ApplyOutcome, string, string, bool, error) {
	rows, err := s.client.Query(ctx,
		`SELECT outcome_json, payload_fingerprint, event_id FROM processed_events FINAL
		 WHERE generation_id = {gen:String} AND topic = {topic:String} AND idempotency_key = {ikey:String}
		 LIMIT 1`,
		map[string]string{"gen": genID, "topic": topic, "ikey": idempotencyKey},
	)
	if err != nil {
		return domain.ApplyOutcome{}, "", "", false, err
	}
	if len(rows) == 0 || len(rows[0]) == 0 || rows[0][0] == "" {
		return domain.ApplyOutcome{}, "", "", false, nil
	}
	out, err := unmarshalDurableOutcome(rows[0][0])
	if err != nil {
		return domain.ApplyOutcome{}, "", "", false, err
	}
	fp := ""
	if len(rows[0]) > 1 {
		fp = rows[0][1]
	}
	eid := ""
	if len(rows[0]) > 2 {
		eid = rows[0][2]
	}
	return out, fp, eid, true, nil
}

func (s *AnalyticsStore) ensureActiveGeneration(ctx context.Context) (string, error) {
	if gen, err := s.activeCompletedGeneration(ctx); err == nil {
		return gen, nil
	} else if err != nil {
		// Incomplete/corrupt active pointer must fail closed (do not auto-heal).
		rows, qerr := s.client.Query(ctx, `SELECT generation_id FROM active_generation FINAL WHERE singleton = 1 LIMIT 1`, nil)
		if qerr != nil {
			return "", qerr
		}
		if len(rows) > 0 && len(rows[0]) > 0 && rows[0][0] != "" {
			return "", err
		}
	}

	// Separate from admission mu: callers may already hold RLock, and bootstrap
	// must never attempt a lock upgrade.
	s.initMu.Lock()
	defer s.initMu.Unlock()
	if gen, err := s.activeCompletedGeneration(ctx); err == nil {
		return gen, nil
	}
	if err := s.insertGeneration(ctx, initialGenerationID, genStatusComplete, 0, true); err != nil {
		return "", err
	}
	if err := s.switchActiveGeneration(ctx, initialGenerationID); err != nil {
		return "", err
	}
	return initialGenerationID, nil
}

func (s *AnalyticsStore) activeCompletedGeneration(ctx context.Context) (string, error) {
	// Subqueries avoid ambiguous FINAL+JOIN forms across ClickHouse versions.
	rows, err := s.client.Query(ctx, `
		SELECT generation_id FROM active_generation FINAL WHERE singleton = 1 LIMIT 1`, nil)
	if err != nil {
		return "", err
	}
	if len(rows) == 0 || len(rows[0]) == 0 || rows[0][0] == "" {
		return "", fmt.Errorf("no active completed generation")
	}
	genID := rows[0][0]
	status, err := s.client.QueryCell(ctx,
		`SELECT status FROM projection_generations FINAL WHERE generation_id = {id:String} LIMIT 1`,
		map[string]string{"id": genID},
	)
	if err != nil {
		return "", err
	}
	if status != genStatusComplete {
		return "", fmt.Errorf("active generation %q is not complete (status=%s)", genID, status)
	}
	return genID, nil
}

func (s *AnalyticsStore) insertGeneration(ctx context.Context, id, status string, accepted uint64, complete bool) error {
	completedAt := "1970-01-01 00:00:00.000"
	if complete {
		completedAt = time.Now().UTC().Format("2006-01-02 15:04:05.000")
	}
	q := `INSERT INTO projection_generations (
		generation_id, status, accepted_count, completed_at
	) VALUES (
		{id:String}, {st:String}, {ac:UInt64}, {ca:String}
	)`
	return s.client.Exec(ctx, q, map[string]string{
		"id": id,
		"st": status,
		"ac": fmt.Sprintf("%d", accepted),
		"ca": completedAt,
	})
}

func (s *AnalyticsStore) switchActiveGeneration(ctx context.Context, id string) error {
	q := `INSERT INTO active_generation (singleton, generation_id) VALUES (1, {id:String})`
	return s.client.Exec(ctx, q, map[string]string{"id": id})
}

func (s *AnalyticsStore) projectionVersionFor(ctx context.Context, genID string) (domain.ProjectionVersion, error) {
	cell, err := s.client.QueryCell(ctx,
		`SELECT count() FROM processed_events FINAL
		 WHERE generation_id = {gen:String} AND disposition = {d:String}`,
		map[string]string{"gen": genID, "d": dispositionApplied},
	)
	if err != nil {
		return 0, err
	}
	var n uint64
	if _, err := fmt.Sscanf(cell, "%d", &n); err != nil {
		return 0, fmt.Errorf("parse projection version: %w", err)
	}
	return domain.ProjectionVersion(n), nil
}

func (s *AnalyticsStore) loadGameplay(ctx context.Context, genID string) ([]domain.GameplayMetric, error) {
	rows, err := s.client.Query(ctx, `
		SELECT event_id, correlation_id, room_id, game_id, tournament_id, visibility, metric_type,
		       public_card_rank, public_card_color, public_card_count_total, room_sequence,
		       public_player_id, display_name
		FROM gameplay_metrics FINAL
		WHERE generation_id = {gen:String}
		ORDER BY event_id`, map[string]string{"gen": genID})
	if err != nil {
		return nil, err
	}
	out := make([]domain.GameplayMetric, 0, len(rows))
	for _, r := range rows {
		if len(r) < 13 {
			continue
		}
		var pct uint16
		var seq uint64
		_, _ = fmt.Sscanf(r[9], "%d", &pct)
		_, _ = fmt.Sscanf(r[10], "%d", &seq)
		out = append(out, domain.GameplayMetric{
			EventID: domain.EventID(r[0]), CorrelationID: r[1],
			RoomID: domain.RoomID(r[2]), GameID: domain.GameID(r[3]),
			TournamentID: domain.TournamentID(r[4]), Visibility: domain.Visibility(r[5]),
			MetricType: r[6], PublicCardRank: r[7], PublicCardColor: r[8],
			PublicCardCountTotal: pct, RoomSequence: seq,
			PublicPlayerID: domain.PlayerID(r[11]), DisplayName: r[12],
		})
	}
	return out, nil
}

func (s *AnalyticsStore) loadTournaments(ctx context.Context, genID string) ([]domain.TournamentStatistic, error) {
	rows, err := s.client.Query(ctx, `
		SELECT event_id, correlation_id, tournament_id, round_number, slot_id, event_type, phase,
		       registered_count, advancing_player_count, public_payload_json
		FROM tournament_statistics FINAL
		WHERE generation_id = {gen:String}
		ORDER BY tournament_id, event_id`, map[string]string{"gen": genID})
	if err != nil {
		return nil, err
	}
	out := make([]domain.TournamentStatistic, 0, len(rows))
	for _, r := range rows {
		if len(r) < 10 {
			continue
		}
		var rn int32
		var rc uint32
		var ac uint16
		_, _ = fmt.Sscanf(r[3], "%d", &rn)
		_, _ = fmt.Sscanf(r[7], "%d", &rc)
		_, _ = fmt.Sscanf(r[8], "%d", &ac)
		payload := map[string]string{}
		if r[9] != "" {
			_ = json.Unmarshal([]byte(r[9]), &payload)
		}
		out = append(out, domain.TournamentStatistic{
			EventID: domain.EventID(r[0]), CorrelationID: r[1],
			TournamentID: domain.TournamentID(r[2]), RoundNumber: rn,
			SlotID: r[4], EventType: r[5], Phase: r[6],
			RegisteredCount: rc, AdvancingPlayerCount: ac, PublicPayload: payload,
		})
	}
	return out, nil
}

func (s *AnalyticsStore) loadRatings(ctx context.Context, genID string) ([]domain.PlayerPublicStatistic, error) {
	rows, err := s.client.Query(ctx, `
		SELECT event_id, correlation_id, player_id, source_type, previous_rating, new_rating, board_type, snapshot_id
		FROM rating_statistics FINAL
		WHERE generation_id = {gen:String}
		ORDER BY event_id, snapshot_id, player_id`, map[string]string{"gen": genID})
	if err != nil {
		return nil, err
	}
	out := make([]domain.PlayerPublicStatistic, 0, len(rows))
	for _, r := range rows {
		if len(r) < 8 {
			continue
		}
		var prev, next int32
		_, _ = fmt.Sscanf(r[4], "%d", &prev)
		_, _ = fmt.Sscanf(r[5], "%d", &next)
		out = append(out, domain.PlayerPublicStatistic{
			EventID: domain.EventID(r[0]), CorrelationID: r[1],
			PlayerID: domain.PlayerID(r[2]), SourceType: r[3],
			PreviousRating: prev, NewRating: next,
			BoardType: r[6], SnapshotID: domain.SnapshotID(r[7]),
		})
	}
	return out, nil
}

// durableOutcome is the byte-stable persisted ApplyOutcome shape.
type durableOutcome struct {
	Kind      string            `json:"kind"`
	EventID   string            `json:"eventId"`
	Rejection *durableRejection `json:"rejection,omitempty"`
	Facts     []durableFact     `json:"facts"`
}

type durableRejection struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type durableFact struct {
	Name string            `json:"name"`
	Data map[string]string `json:"data"`
}

func marshalDurableOutcome(out domain.ApplyOutcome) (string, error) {
	dto := durableOutcome{
		Kind:    string(out.Kind),
		EventID: string(out.EventID),
		Facts:   make([]durableFact, 0, len(out.Facts)),
	}
	if out.Rejection != nil {
		dto.Rejection = &durableRejection{Code: string(out.Rejection.Code), Message: out.Rejection.Message}
	}
	for _, f := range out.Facts {
		dto.Facts = append(dto.Facts, durableFact{Name: string(f.Name), Data: sortedCopyMap(f.Data)})
	}
	// Sort facts by name then stable JSON marshal (maps already key-sorted by encoding/json).
	sort.SliceStable(dto.Facts, func(i, j int) bool { return dto.Facts[i].Name < dto.Facts[j].Name })
	b, err := json.Marshal(dto)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func unmarshalDurableOutcome(raw string) (domain.ApplyOutcome, error) {
	var dto durableOutcome
	if err := json.Unmarshal([]byte(raw), &dto); err != nil {
		return domain.ApplyOutcome{}, err
	}
	out := domain.ApplyOutcome{
		Kind:    domain.OutcomeKind(dto.Kind),
		EventID: domain.EventID(dto.EventID),
	}
	if dto.Rejection != nil {
		out.Rejection = &domain.Rejection{Code: domain.RejectionCode(dto.Rejection.Code), Message: dto.Rejection.Message}
	}
	for _, f := range dto.Facts {
		out.Facts = append(out.Facts, domain.Fact{Name: domain.FactName(f.Name), Data: f.Data})
	}
	return out, nil
}

func duplicateFromStored(prior domain.ApplyOutcome) domain.ApplyOutcome {
	dup := prior
	dup.Kind = domain.OutcomeDuplicate
	if prior.Rejection != nil {
		r := *prior.Rejection
		dup.Rejection = &r
	}
	if len(prior.Facts) > 0 {
		facts := make([]domain.Fact, len(prior.Facts))
		copy(facts, prior.Facts)
		dup.Facts = facts
	}
	return dup
}

func sortedCopyMap(in map[string]string) map[string]string {
	if in == nil {
		return map[string]string{}
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func marshalSortedStringMap(in map[string]string) (string, error) {
	if in == nil {
		in = map[string]string{}
	}
	b, err := json.Marshal(in)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func newGenerationID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "gen_" + hex.EncodeToString(b[:]), nil
}

// TransformMigrationForDatabase rewrites production migration SQL to target a safe test DB name.
// Only replaces the analytics database identifier; refuses forbidden names.
func TransformMigrationForDatabase(sql, database string) (string, error) {
	if err := ValidateSafeTestDatabase(database); err != nil {
		return "", err
	}
	out := strings.ReplaceAll(sql, "__UNOARENA_ANALYTICS_DB__", database)
	out = strings.ReplaceAll(out, "CREATE DATABASE IF NOT EXISTS analytics", "CREATE DATABASE IF NOT EXISTS "+database)
	out = strings.ReplaceAll(out, "analytics.", database+".")
	for _, line := range strings.Split(out, "\n") {
		trim := strings.TrimSpace(line)
		if trim == "" || strings.HasPrefix(trim, "--") {
			continue
		}
		if strings.Contains(trim, "analytics.") ||
			strings.Contains(trim, "DATABASE analytics") ||
			strings.Contains(trim, "EXISTS analytics") ||
			strings.Contains(trim, "INTO analytics") {
			return "", fmt.Errorf("migration transform left residual analytics identifier: %s", truncate(trim, 120))
		}
	}
	return out, nil
}

// SafeTestDatabasePrefix is required for integration harness databases.
const SafeTestDatabasePrefix = "unoarena_analytics_test_"

var forbiddenTestDatabases = map[string]struct{}{
	"analytics": {},
	"default":   {},
	"system":    {},
}

// ValidateSafeTestDatabase enforces unoarena_analytics_test_ + lowercase hex suffix.
func ValidateSafeTestDatabase(name string) error {
	folded := strings.ToLower(strings.TrimSpace(name))
	if folded == "" {
		return fmt.Errorf("analytics integration: refuse empty database name")
	}
	if _, banned := forbiddenTestDatabases[folded]; banned {
		return fmt.Errorf("analytics integration: refuse database %q", name)
	}
	if !strings.HasPrefix(folded, SafeTestDatabasePrefix) {
		return fmt.Errorf("analytics integration: refuse database %q; require prefix %s", name, SafeTestDatabasePrefix)
	}
	suffix := folded[len(SafeTestDatabasePrefix):]
	if suffix == "" {
		return fmt.Errorf("analytics integration: refuse database %q; empty suffix", name)
	}
	for _, r := range suffix {
		if (r >= 'a' && r <= 'f') || (r >= '0' && r <= '9') {
			continue
		}
		return fmt.Errorf("analytics integration: refuse database %q; suffix must be lowercase hex", name)
	}
	if folded != name {
		return fmt.Errorf("analytics integration: refuse database %q; must be lowercase", name)
	}
	return nil
}

// ApplyMigrationStatements splits and executes migration statements against the client database.
func ApplyMigrationStatements(ctx context.Context, c *Client, migrationSQL string) error {
	stmts := splitSQLStatements(migrationSQL)
	if len(stmts) == 0 {
		return fmt.Errorf("no migration statements")
	}
	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if err := c.Exec(ctx, stmt, nil); err != nil {
			return fmt.Errorf("migration statement: %w\n%s", err, truncate(stmt, 200))
		}
	}
	return nil
}

func splitSQLStatements(sql string) []string {
	var out []string
	var buf bytes.Buffer
	lines := strings.Split(sql, "\n")
	for _, line := range lines {
		trim := strings.TrimSpace(line)
		if strings.HasPrefix(trim, "--") {
			continue
		}
		buf.WriteString(line)
		buf.WriteByte('\n')
		if strings.HasSuffix(trim, ";") {
			stmt := strings.TrimSpace(buf.String())
			stmt = strings.TrimSuffix(stmt, ";")
			stmt = strings.TrimSpace(stmt)
			if stmt != "" {
				out = append(out, stmt)
			}
			buf.Reset()
		}
	}
	if rest := strings.TrimSpace(buf.String()); rest != "" {
		out = append(out, rest)
	}
	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
