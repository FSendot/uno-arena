package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	genStatusInitializing = "initializing"

	RecoveryStatusInitializing = "initializing"
	RecoveryStatusBuilding     = "building"
	RecoveryStatusComplete     = "complete"
	RecoveryStatusFailed       = "failed"
	RecoveryStatusQuarantined  = "quarantined"

	PageStatusApplied   = "applied"
	PageStatusFinalized = "finalized"

	IdempotencyFollowUpPublished = "follow_up_published"
	IdempotencyFinalized         = "finalized"

	DefaultRecoveryLeaseTTL = 60 * time.Second
)

// ErrAdHocRebuildDisabled is returned when HTTP/ad-hoc Rebuild is fail-closed.
var ErrAdHocRebuildDisabled = errors.New("ad-hoc rebuild disabled; use analytics projection rebuild coordinator")

// ErrRecoveryLeaseLost indicates another worker holds the durable lease.
var ErrRecoveryLeaseLost = errors.New("recovery lease lost")

// ErrRecoveryNotOwned is returned when ownership validation fails before a fenced write.
var ErrRecoveryNotOwned = errors.New("recovery job not owned by this worker")

// ErrRecoveryContinuityGap blocks activation when page links are discontinuous.
var ErrRecoveryContinuityGap = errors.New("recovery page continuity gap")

// ErrRecoveryQuarantined blocks activation when a page applied quarantined records.
var ErrRecoveryQuarantined = errors.New("recovery page contains quarantined records")

// ErrRecoveryActivationFenceFailed is returned when active pointer readback disagrees.
var ErrRecoveryActivationFenceFailed = errors.New("recovery activation fence failed")

// ErrRecoveryJobSpecMismatch is returned when an existing job's immutable identity differs.
var ErrRecoveryJobSpecMismatch = errors.New("recovery job immutable spec mismatch")

// ErrRecoveryJobClosed blocks reopening completed/failed/quarantined jobs for new pages.
var ErrRecoveryJobClosed = errors.New("recovery job is closed")

// RecoveryJobSpec is the durable recovery job registration payload.
type RecoveryJobSpec struct {
	RecoveryJobID      string
	SourceContext      string
	SourceTopic        string
	FromCheckpoint     string
	ToCheckpoint       string
	FromOccurredAt     time.Time
	ToOccurredAt       time.Time
	HasCheckpointRange bool
	HasOccurredRange   bool
}

// RecoveryJobState is the FINAL-visible recovery job row.
type RecoveryJobState struct {
	RecoveryJobID    string
	SourceContext    string
	SourceTopic      string
	GenerationID     string
	Status           string
	FromCheckpoint   string
	ToCheckpoint     string
	FromOccurredAt   time.Time
	ToOccurredAt     time.Time
	LastPageCursor   string
	NextPageCursor   string
	PagesCompleted   uint32
	AcceptedCount    uint64
	FailureCode      string
	FailureSummary   string
	QuarantineKey    string
	HasCheckpointRng bool
	HasOccurredRng   bool
}

// RecoveryPageCheckpoint is one durable page progress row.
type RecoveryPageCheckpoint struct {
	RecoveryJobID    string
	SourceTopic      string
	PageCursor       string
	PageIndex        uint32
	NextPageCursor   string
	FromCheckpoint   string
	ToCheckpoint     string
	RecordsApplied   uint32
	QuarantinedCount uint32
	Status           string
	GenerationID     string
}

// RecoveryRequestIdempotency is the declared (job, topic, pageCursor) fence.
type RecoveryRequestIdempotency struct {
	RecoveryJobID   string
	SourceTopic     string
	PageCursor      string
	Disposition     string
	FollowUpEventID string
	GenerationID    string
}

// RecoveryLease is the FINAL-visible lease row.
type RecoveryLease struct {
	RecoveryJobID string
	OwnerToken    string
	LeaseEpoch    uint64
	ExpiresAt     time.Time
}

// AllowAdHocRebuild enables the legacy mutex Rebuild path (tests/capability only).
func (s *AnalyticsStore) AllowAdHocRebuild(enabled bool) {
	if s == nil {
		return
	}
	s.adHocRebuild = enabled
}

// NewOwnerToken returns a random lease owner token for this worker process.
func NewOwnerToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "own_" + hex.EncodeToString(b[:]), nil
}

// RecoveryGenerationID returns the deterministic building generation for a job.
func RecoveryGenerationID(recoveryJobID string) string {
	h := sha256.Sum256([]byte(strings.TrimSpace(recoveryJobID)))
	return "gen_rcv_" + hex.EncodeToString(h[:12])
}

// DeterministicFollowUpEventID is stable across crash redelivery of a completed page.
func DeterministicFollowUpEventID(recoveryJobID, sourceTopic, pageCursor string) string {
	sum := sha256.Sum256([]byte(recoveryJobID + "|" + sourceTopic + "|" + pageCursor + "|followup"))
	return "evt_rcv_" + hex.EncodeToString(sum[:16])
}

// SelectLeaseWinner picks highest lease_epoch, then lexicographically smallest owner_token.
// Contenders must already be the latest version per owner (FINAL per ORDER BY job, owner).
func SelectLeaseWinner(contenders []RecoveryLease) (RecoveryLease, bool) {
	if len(contenders) == 0 {
		return RecoveryLease{}, false
	}
	best := contenders[0]
	for i := 1; i < len(contenders); i++ {
		c := contenders[i]
		if c.LeaseEpoch > best.LeaseEpoch {
			best = c
			continue
		}
		if c.LeaseEpoch == best.LeaseEpoch && c.OwnerToken < best.OwnerToken {
			best = c
		}
	}
	return best, true
}

// NextLeaseEpoch returns the next epoch for acquire/renew. Renewals always advance.
func NextLeaseEpoch(cur RecoveryLease, ok bool, ownerToken string, now time.Time) (uint64, error) {
	if !ok {
		return 1, nil
	}
	if cur.ExpiresAt.After(now) && cur.OwnerToken != ownerToken {
		return 0, ErrRecoveryLeaseLost
	}
	return cur.LeaseEpoch + 1, nil
}

// ReconcileJobProgressFromCheckpoint advances job counters from a durable page checkpoint
// exactly once per page_index (idempotent on redelivery).
func ReconcileJobProgressFromCheckpoint(job RecoveryJobState, cp RecoveryPageCheckpoint) (RecoveryJobState, bool) {
	need := false
	wantPages := cp.PageIndex + 1
	if job.PagesCompleted < wantPages {
		job.AcceptedCount += uint64(cp.RecordsApplied)
		job.PagesCompleted = wantPages
		need = true
	}
	if job.LastPageCursor != cp.PageCursor {
		job.LastPageCursor = cp.PageCursor
		need = true
	}
	if job.NextPageCursor != cp.NextPageCursor {
		job.NextPageCursor = cp.NextPageCursor
		need = true
	}
	if job.Status == RecoveryStatusInitializing {
		job.Status = RecoveryStatusBuilding
		need = true
	}
	return job, need
}

// ValidateImmutableRecoveryJobSpec requires exact source/range identity on every page/takeover.
func ValidateImmutableRecoveryJobSpec(existing RecoveryJobState, spec RecoveryJobSpec) error {
	if existing.SourceContext != spec.SourceContext ||
		existing.SourceTopic != spec.SourceTopic ||
		existing.FromCheckpoint != spec.FromCheckpoint ||
		existing.ToCheckpoint != spec.ToCheckpoint ||
		existing.HasCheckpointRng != spec.HasCheckpointRange ||
		existing.HasOccurredRng != spec.HasOccurredRange {
		return fmt.Errorf("%w: job %q", ErrRecoveryJobSpecMismatch, spec.RecoveryJobID)
	}
	if existing.HasOccurredRng {
		if !existing.FromOccurredAt.Equal(spec.FromOccurredAt) || !existing.ToOccurredAt.Equal(spec.ToOccurredAt) {
			return fmt.Errorf("%w: occurred range job %q", ErrRecoveryJobSpecMismatch, spec.RecoveryJobID)
		}
	}
	return nil
}

// AssertRecoveryJobAcceptsPage rejects closed jobs for unseen page cursors.
// Completed jobs may continue only when a durable checkpoint for this page already exists
// (crash recovery through finalize); failed/quarantined never reopen.
func AssertRecoveryJobAcceptsPage(status string, hasPageCheckpoint bool) error {
	switch status {
	case RecoveryStatusInitializing, RecoveryStatusBuilding, "":
		return nil
	case RecoveryStatusComplete:
		if hasPageCheckpoint {
			return nil
		}
		return fmt.Errorf("%w: status %s", ErrRecoveryJobClosed, status)
	case RecoveryStatusFailed, RecoveryStatusQuarantined:
		return fmt.Errorf("%w: status %s", ErrRecoveryJobClosed, status)
	default:
		return fmt.Errorf("%w: status %s", ErrRecoveryJobClosed, status)
	}
}

// ListWriteGenerations returns active completed plus every initializing/building
// generation registered on an active recovery job. Ad-hoc Rebuild gens are excluded.
func (s *AnalyticsStore) ListWriteGenerations(ctx context.Context) ([]string, error) {
	active, err := s.activeCompletedGeneration(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := s.client.Query(ctx, `
		SELECT generation_id FROM recovery_jobs FINAL
		WHERE status IN ({a:String}, {b:String})
		ORDER BY generation_id`,
		map[string]string{"a": RecoveryStatusInitializing, "b": RecoveryStatusBuilding},
	)
	if err != nil {
		return nil, err
	}
	out := []string{active}
	seen := map[string]struct{}{active: {}}
	for _, r := range rows {
		if len(r) == 0 || r[0] == "" {
			continue
		}
		id := r[0]
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out, nil
}

// RegisterBuildingRecoveryJob inserts initializing job + generation before clone.
func (s *AnalyticsStore) RegisterBuildingRecoveryJob(ctx context.Context, spec RecoveryJobSpec) (string, error) {
	genID := RecoveryGenerationID(spec.RecoveryJobID)
	if err := s.upsertRecoveryJob(ctx, RecoveryJobState{
		RecoveryJobID:    spec.RecoveryJobID,
		SourceContext:    spec.SourceContext,
		SourceTopic:      spec.SourceTopic,
		GenerationID:     genID,
		Status:           RecoveryStatusInitializing,
		FromCheckpoint:   spec.FromCheckpoint,
		ToCheckpoint:     spec.ToCheckpoint,
		HasCheckpointRng: spec.HasCheckpointRange,
		HasOccurredRng:   spec.HasOccurredRange,
	}, spec.FromOccurredAt, spec.ToOccurredAt); err != nil {
		return "", err
	}
	if err := s.insertGeneration(ctx, genID, genStatusInitializing, 0, false); err != nil {
		return "", err
	}
	return genID, nil
}

// CloneActiveGenerationInto copies projection rows + processed markers server-side.
// Preserves original ingested_at/processed_at so concurrent newer dual-writes win.
func (s *AnalyticsStore) CloneActiveGenerationInto(ctx context.Context, newGenID string) error {
	active, err := s.activeCompletedGeneration(ctx)
	if err != nil {
		return err
	}
	if active == newGenID {
		return fmt.Errorf("refusing to clone generation onto itself")
	}
	clones := []string{
		`INSERT INTO gameplay_metrics (
			generation_id, source_topic, event_id, idempotency_key, schema_version, correlation_id,
			room_id, game_id, tournament_id, visibility, metric_type,
			public_card_rank, public_card_color, public_card_count_total, room_sequence,
			public_player_id, display_name, occurred_at, ingested_at
		) SELECT
			{new:String}, source_topic, event_id, idempotency_key, schema_version, correlation_id,
			room_id, game_id, tournament_id, visibility, metric_type,
			public_card_rank, public_card_color, public_card_count_total, room_sequence,
			public_player_id, display_name, occurred_at, ingested_at
		FROM gameplay_metrics FINAL WHERE generation_id = {old:String}`,
		`INSERT INTO tournament_statistics (
			generation_id, source_topic, event_id, idempotency_key, schema_version, correlation_id,
			tournament_id, round_number, slot_id, event_type, phase,
			registered_count, advancing_player_count, public_payload_json, occurred_at, ingested_at
		) SELECT
			{new:String}, source_topic, event_id, idempotency_key, schema_version, correlation_id,
			tournament_id, round_number, slot_id, event_type, phase,
			registered_count, advancing_player_count, public_payload_json, occurred_at, ingested_at
		FROM tournament_statistics FINAL WHERE generation_id = {old:String}`,
		`INSERT INTO rating_statistics (
			generation_id, source_topic, event_id, idempotency_key, schema_version, correlation_id,
			player_id, source_type, previous_rating, new_rating, board_type, snapshot_id, occurred_at, ingested_at
		) SELECT
			{new:String}, source_topic, event_id, idempotency_key, schema_version, correlation_id,
			player_id, source_type, previous_rating, new_rating, board_type, snapshot_id, occurred_at, ingested_at
		FROM rating_statistics FINAL WHERE generation_id = {old:String}`,
		`INSERT INTO processed_events (
			generation_id, topic, idempotency_key, event_id, payload_fingerprint, disposition, outcome_json, processed_at
		) SELECT
			{new:String}, topic, idempotency_key, event_id, payload_fingerprint, disposition, outcome_json, processed_at
		FROM processed_events FINAL WHERE generation_id = {old:String}`,
	}
	params := map[string]string{"new": newGenID, "old": active}
	for _, q := range clones {
		if err := s.client.Exec(ctx, q, params); err != nil {
			return fmt.Errorf("clone generation: %w", err)
		}
	}
	return nil
}

// MarkRecoveryGenerationBuilding flips generation + job to building after clone.
func (s *AnalyticsStore) MarkRecoveryGenerationBuilding(ctx context.Context, jobID, genID string) error {
	if err := s.insertGeneration(ctx, genID, genStatusBuilding, 0, false); err != nil {
		return err
	}
	st, err := s.LoadRecoveryJob(ctx, jobID)
	if err != nil {
		return err
	}
	st.Status = RecoveryStatusBuilding
	st.GenerationID = genID
	return s.upsertRecoveryJob(ctx, st, st.FromOccurredAt, st.ToOccurredAt)
}

// EnsureRecoveryBuildingGeneration registers, clones, and marks building idempotently.
func (s *AnalyticsStore) EnsureRecoveryBuildingGeneration(ctx context.Context, spec RecoveryJobSpec) (string, error) {
	genID := RecoveryGenerationID(spec.RecoveryJobID)
	existing, err := s.LoadRecoveryJob(ctx, spec.RecoveryJobID)
	if err == nil && existing.GenerationID != "" {
		if err := ValidateImmutableRecoveryJobSpec(existing, spec); err != nil {
			return "", err
		}
		switch existing.Status {
		case RecoveryStatusBuilding:
			return existing.GenerationID, nil
		case RecoveryStatusComplete, RecoveryStatusFailed, RecoveryStatusQuarantined:
			return "", fmt.Errorf("%w: status %s", ErrRecoveryJobClosed, existing.Status)
		case RecoveryStatusInitializing:
			// Crash during/after clone: re-clone is safe (older timestamps lose to dual-writes).
			if err := s.CloneActiveGenerationInto(ctx, existing.GenerationID); err != nil {
				return "", err
			}
			if err := s.MarkRecoveryGenerationBuilding(ctx, spec.RecoveryJobID, existing.GenerationID); err != nil {
				return "", err
			}
			return existing.GenerationID, nil
		default:
			return "", fmt.Errorf("%w: status %s", ErrRecoveryJobClosed, existing.Status)
		}
	}
	if _, err := s.RegisterBuildingRecoveryJob(ctx, spec); err != nil {
		return "", err
	}
	if err := s.CloneActiveGenerationInto(ctx, genID); err != nil {
		return "", err
	}
	if err := s.MarkRecoveryGenerationBuilding(ctx, spec.RecoveryJobID, genID); err != nil {
		return "", err
	}
	return genID, nil
}

// AcquireOrRenewLease takes over an expired lease or renews ownership with readback fence.
// Every acquire/renew advances lease_epoch; winner is SelectLeaseWinner after insert.
func (s *AnalyticsStore) AcquireOrRenewLease(ctx context.Context, jobID, ownerToken string, ttl time.Duration) (RecoveryLease, error) {
	if ttl <= 0 {
		ttl = DefaultRecoveryLeaseTTL
	}
	now := time.Now().UTC()
	cur, ok, err := s.loadLease(ctx, jobID)
	if err != nil {
		return RecoveryLease{}, err
	}
	epoch, err := NextLeaseEpoch(cur, ok, ownerToken, now)
	if err != nil {
		return RecoveryLease{}, err
	}
	expires := now.Add(ttl)
	if err := s.insertLease(ctx, jobID, ownerToken, epoch, expires); err != nil {
		return RecoveryLease{}, err
	}
	got, ok, err := s.loadLease(ctx, jobID)
	if err != nil {
		return RecoveryLease{}, err
	}
	if !ok || got.OwnerToken != ownerToken || got.LeaseEpoch != epoch {
		return RecoveryLease{}, ErrRecoveryLeaseLost
	}
	return got, nil
}

// ValidateLeaseOwnership confirms FINAL lease still belongs to ownerToken and is unexpired.
func (s *AnalyticsStore) ValidateLeaseOwnership(ctx context.Context, jobID, ownerToken string) error {
	got, ok, err := s.loadLease(ctx, jobID)
	if err != nil {
		return err
	}
	if !ok || got.OwnerToken != ownerToken {
		return ErrRecoveryNotOwned
	}
	if !got.ExpiresAt.After(time.Now().UTC()) {
		return ErrRecoveryLeaseLost
	}
	return nil
}

// LoadRecoveryJob returns the FINAL recovery job or err if missing.
func (s *AnalyticsStore) LoadRecoveryJob(ctx context.Context, jobID string) (RecoveryJobState, error) {
	rows, err := s.client.Query(ctx, `
		SELECT recovery_job_id, source_context, source_topic, generation_id, status,
		       from_checkpoint, to_checkpoint, last_page_cursor, next_page_cursor,
		       pages_completed, accepted_count, failure_code, failure_summary, quarantine_key,
		       has_checkpoint_range, has_occurred_range, from_occurred_at, to_occurred_at
		FROM recovery_jobs FINAL WHERE recovery_job_id = {id:String} LIMIT 1`,
		map[string]string{"id": jobID},
	)
	if err != nil {
		return RecoveryJobState{}, err
	}
	if len(rows) == 0 || len(rows[0]) < 18 {
		return RecoveryJobState{}, fmt.Errorf("recovery job %q not found", jobID)
	}
	r := rows[0]
	var pages uint32
	var accepted uint64
	var hasCP, hasOA uint8
	_, _ = fmt.Sscanf(r[9], "%d", &pages)
	_, _ = fmt.Sscanf(r[10], "%d", &accepted)
	_, _ = fmt.Sscanf(r[14], "%d", &hasCP)
	_, _ = fmt.Sscanf(r[15], "%d", &hasOA)
	fromOA, _ := parseCHDateTime(r[16])
	toOA, _ := parseCHDateTime(r[17])
	return RecoveryJobState{
		RecoveryJobID:    r[0],
		SourceContext:    r[1],
		SourceTopic:      r[2],
		GenerationID:     r[3],
		Status:           r[4],
		FromCheckpoint:   r[5],
		ToCheckpoint:     r[6],
		LastPageCursor:   r[7],
		NextPageCursor:   r[8],
		PagesCompleted:   pages,
		AcceptedCount:    accepted,
		FailureCode:      r[11],
		FailureSummary:   r[12],
		QuarantineKey:    r[13],
		HasCheckpointRng: hasCP == 1,
		HasOccurredRng:   hasOA == 1,
		FromOccurredAt:   fromOA,
		ToOccurredAt:     toOA,
	}, nil
}

// LoadPageCheckpoint returns FINAL page progress when present.
func (s *AnalyticsStore) LoadPageCheckpoint(ctx context.Context, jobID, topic, pageCursor string) (RecoveryPageCheckpoint, bool, error) {
	rows, err := s.client.Query(ctx, `
		SELECT recovery_job_id, source_topic, page_cursor, page_index, next_page_cursor,
		       from_checkpoint, to_checkpoint, records_applied, quarantined_count, status, generation_id
		FROM recovery_page_checkpoints FINAL
		WHERE recovery_job_id = {job:String} AND source_topic = {topic:String} AND page_cursor = {cur:String}
		LIMIT 1`,
		map[string]string{"job": jobID, "topic": topic, "cur": pageCursor},
	)
	if err != nil {
		return RecoveryPageCheckpoint{}, false, err
	}
	if len(rows) == 0 || len(rows[0]) < 11 {
		return RecoveryPageCheckpoint{}, false, nil
	}
	r := rows[0]
	var idx, applied, quar uint32
	_, _ = fmt.Sscanf(r[3], "%d", &idx)
	_, _ = fmt.Sscanf(r[7], "%d", &applied)
	_, _ = fmt.Sscanf(r[8], "%d", &quar)
	return RecoveryPageCheckpoint{
		RecoveryJobID: r[0], SourceTopic: r[1], PageCursor: r[2],
		PageIndex: idx, NextPageCursor: r[4],
		FromCheckpoint: r[5], ToCheckpoint: r[6],
		RecordsApplied: applied, QuarantinedCount: quar,
		Status: r[9], GenerationID: r[10],
	}, true, nil
}

// PersistPageCheckpoint writes page completion under current lease ownership.
func (s *AnalyticsStore) PersistPageCheckpoint(ctx context.Context, ownerToken string, cp RecoveryPageCheckpoint) error {
	if err := s.ValidateLeaseOwnership(ctx, cp.RecoveryJobID, ownerToken); err != nil {
		return err
	}
	q := `INSERT INTO recovery_page_checkpoints (
		recovery_job_id, source_topic, page_cursor, page_index, next_page_cursor,
		from_checkpoint, to_checkpoint, records_applied, quarantined_count, status, generation_id
	) VALUES (
		{job:String}, {topic:String}, {cur:String}, {idx:UInt32}, {next:String},
		{from:String}, {to:String}, {ra:UInt32}, {qc:UInt32}, {st:String}, {gen:String}
	)`
	return s.client.Exec(ctx, q, map[string]string{
		"job": cp.RecoveryJobID, "topic": cp.SourceTopic, "cur": cp.PageCursor,
		"idx": fmt.Sprintf("%d", cp.PageIndex), "next": cp.NextPageCursor,
		"from": cp.FromCheckpoint, "to": cp.ToCheckpoint,
		"ra": fmt.Sprintf("%d", cp.RecordsApplied), "qc": fmt.Sprintf("%d", cp.QuarantinedCount),
		"st": cp.Status, "gen": cp.GenerationID,
	})
}

// PersistRequestIdempotency records declared page completion / follow-up / finalize.
func (s *AnalyticsStore) PersistRequestIdempotency(ctx context.Context, ownerToken string, row RecoveryRequestIdempotency) error {
	if err := s.ValidateLeaseOwnership(ctx, row.RecoveryJobID, ownerToken); err != nil {
		return err
	}
	q := `INSERT INTO recovery_request_idempotency (
		recovery_job_id, source_topic, page_cursor, disposition, follow_up_event_id, generation_id
	) VALUES (
		{job:String}, {topic:String}, {cur:String}, {disp:String}, {fe:String}, {gen:String}
	)`
	return s.client.Exec(ctx, q, map[string]string{
		"job": row.RecoveryJobID, "topic": row.SourceTopic, "cur": row.PageCursor,
		"disp": row.Disposition, "fe": row.FollowUpEventID, "gen": row.GenerationID,
	})
}

// LoadRequestIdempotency returns FINAL idempotency row when present.
func (s *AnalyticsStore) LoadRequestIdempotency(ctx context.Context, jobID, topic, pageCursor string) (RecoveryRequestIdempotency, bool, error) {
	rows, err := s.client.Query(ctx, `
		SELECT recovery_job_id, source_topic, page_cursor, disposition, follow_up_event_id, generation_id
		FROM recovery_request_idempotency FINAL
		WHERE recovery_job_id = {job:String} AND source_topic = {topic:String} AND page_cursor = {cur:String}
		LIMIT 1`,
		map[string]string{"job": jobID, "topic": topic, "cur": pageCursor},
	)
	if err != nil {
		return RecoveryRequestIdempotency{}, false, err
	}
	if len(rows) == 0 || len(rows[0]) < 6 {
		return RecoveryRequestIdempotency{}, false, nil
	}
	r := rows[0]
	return RecoveryRequestIdempotency{
		RecoveryJobID: r[0], SourceTopic: r[1], PageCursor: r[2],
		Disposition: r[3], FollowUpEventID: r[4], GenerationID: r[5],
	}, true, nil
}

// UpdateRecoveryJobProgress updates cursor/page counters after a successful page.
func (s *AnalyticsStore) UpdateRecoveryJobProgress(ctx context.Context, ownerToken string, st RecoveryJobState) error {
	if err := s.ValidateLeaseOwnership(ctx, st.RecoveryJobID, ownerToken); err != nil {
		return err
	}
	return s.upsertRecoveryJob(ctx, st, st.FromOccurredAt, st.ToOccurredAt)
}

// ReconcileRecoveryJobProgressFromCheckpoint idempotently advances job progress from a
// durable page checkpoint (crash between PersistPageCheckpoint and progress update).
func (s *AnalyticsStore) ReconcileRecoveryJobProgressFromCheckpoint(ctx context.Context, ownerToken string, cp RecoveryPageCheckpoint) error {
	if err := s.ValidateLeaseOwnership(ctx, cp.RecoveryJobID, ownerToken); err != nil {
		return err
	}
	job, err := s.LoadRecoveryJob(ctx, cp.RecoveryJobID)
	if err != nil {
		return err
	}
	updated, need := ReconcileJobProgressFromCheckpoint(job, cp)
	if !need {
		return nil
	}
	return s.upsertRecoveryJob(ctx, updated, updated.FromOccurredAt, updated.ToOccurredAt)
}

// MarkRecoveryFailed persists sanitized failure/quarantine metadata.
func (s *AnalyticsStore) MarkRecoveryFailed(ctx context.Context, ownerToken, jobID, status, code, summary, quarantineKey string) error {
	if ownerToken != "" {
		if err := s.ValidateLeaseOwnership(ctx, jobID, ownerToken); err != nil {
			// A stale worker must not poison the current owner's recovery job.
			return err
		}
	}
	st, err := s.LoadRecoveryJob(ctx, jobID)
	if err != nil {
		st = RecoveryJobState{RecoveryJobID: jobID, GenerationID: RecoveryGenerationID(jobID)}
	}
	st.Status = status
	st.FailureCode = code
	st.FailureSummary = truncateStr(summary, 256)
	st.QuarantineKey = quarantineKey
	return s.upsertRecoveryJob(ctx, st, st.FromOccurredAt, st.ToOccurredAt)
}

// ActivateRecoveryGeneration verifies continuity + lease, marks complete, switches active with readback.
func (s *AnalyticsStore) ActivateRecoveryGeneration(ctx context.Context, ownerToken, jobID string) error {
	if err := s.ValidateLeaseOwnership(ctx, jobID, ownerToken); err != nil {
		return err
	}
	job, err := s.LoadRecoveryJob(ctx, jobID)
	if err != nil {
		return err
	}
	if job.Status == RecoveryStatusComplete {
		// Idempotent finalize: ensure active pointer matches.
		active, aerr := s.activeCompletedGeneration(ctx)
		if aerr == nil && active == job.GenerationID {
			return nil
		}
	}
	if job.Status == RecoveryStatusQuarantined || job.Status == RecoveryStatusFailed {
		return fmt.Errorf("%w: job status %s", ErrRecoveryQuarantined, job.Status)
	}
	pages, err := s.listPageCheckpoints(ctx, jobID, job.SourceTopic)
	if err != nil {
		return err
	}
	if err := verifyPageContinuity(pages); err != nil {
		return err
	}
	if err := verifyPageCheckpointsMatchJob(job, pages); err != nil {
		return err
	}
	for _, p := range pages {
		if p.QuarantinedCount > 0 || (p.Status != PageStatusApplied && p.Status != PageStatusFinalized) {
			return fmt.Errorf("%w: page %q", ErrRecoveryQuarantined, p.PageCursor)
		}
	}
	// Progress must be reconciled before claiming complete.
	wantPages := uint32(len(pages))
	if job.PagesCompleted < wantPages {
		return fmt.Errorf("%w: pages_completed=%d want>=%d", ErrRecoveryContinuityGap, job.PagesCompleted, wantPages)
	}
	if err := s.ValidateLeaseOwnership(ctx, jobID, ownerToken); err != nil {
		return err
	}
	version, err := s.projectionVersionFor(ctx, job.GenerationID)
	if err != nil {
		return err
	}
	if err := s.ValidateLeaseOwnership(ctx, jobID, ownerToken); err != nil {
		return err
	}
	if err := s.insertGeneration(ctx, job.GenerationID, genStatusComplete, uint64(version), true); err != nil {
		return err
	}
	if err := s.ValidateLeaseOwnership(ctx, jobID, ownerToken); err != nil {
		return err
	}
	if err := s.switchActiveGeneration(ctx, job.GenerationID); err != nil {
		return err
	}
	if err := s.ValidateLeaseOwnership(ctx, jobID, ownerToken); err != nil {
		return err
	}
	active, err := s.activeCompletedGeneration(ctx)
	if err != nil || active != job.GenerationID {
		return ErrRecoveryActivationFenceFailed
	}
	if err := s.ValidateLeaseOwnership(ctx, jobID, ownerToken); err != nil {
		return err
	}
	job.Status = RecoveryStatusComplete
	job.AcceptedCount = uint64(version)
	if err := s.upsertRecoveryJob(ctx, job, job.FromOccurredAt, job.ToOccurredAt); err != nil {
		return err
	}
	return nil
}

// VerifyPageContinuityForTest exposes page-link validation for unit tests.
func VerifyPageContinuityForTest(pages []RecoveryPageCheckpoint) error {
	return verifyPageContinuity(pages)
}

func verifyPageContinuity(pages []RecoveryPageCheckpoint) error {
	if len(pages) == 0 {
		return fmt.Errorf("%w: no pages", ErrRecoveryContinuityGap)
	}
	// Expect page_index contiguous from 0 and next_cursor chain.
	byIdx := map[uint32]RecoveryPageCheckpoint{}
	var maxIdx uint32
	for _, p := range pages {
		byIdx[p.PageIndex] = p
		if p.PageIndex > maxIdx {
			maxIdx = p.PageIndex
		}
	}
	if uint32(len(pages)) != maxIdx+1 {
		return fmt.Errorf("%w: page index hole", ErrRecoveryContinuityGap)
	}
	var prevNext string
	for i := uint32(0); i <= maxIdx; i++ {
		p, ok := byIdx[i]
		if !ok {
			return fmt.Errorf("%w: missing page_index %d", ErrRecoveryContinuityGap, i)
		}
		if i == 0 {
			if p.PageCursor != "" {
				return fmt.Errorf("%w: first page cursor must be empty", ErrRecoveryContinuityGap)
			}
		} else if p.PageCursor != prevNext {
			return fmt.Errorf("%w: page %d cursor mismatch", ErrRecoveryContinuityGap, i)
		}
		prevNext = p.NextPageCursor
	}
	last := byIdx[maxIdx]
	if last.NextPageCursor != "" {
		return fmt.Errorf("%w: final page still has nextCursor", ErrRecoveryContinuityGap)
	}
	return nil
}

func verifyPageCheckpointsMatchJob(job RecoveryJobState, pages []RecoveryPageCheckpoint) error {
	for _, p := range pages {
		if p.RecoveryJobID != job.RecoveryJobID {
			return fmt.Errorf("%w: checkpoint job mismatch", ErrRecoveryContinuityGap)
		}
		if p.SourceTopic != job.SourceTopic {
			return fmt.Errorf("%w: checkpoint topic mismatch", ErrRecoveryContinuityGap)
		}
		if p.GenerationID != job.GenerationID {
			return fmt.Errorf("%w: checkpoint generation mismatch", ErrRecoveryContinuityGap)
		}
	}
	return nil
}

func (s *AnalyticsStore) listPageCheckpoints(ctx context.Context, jobID, topic string) ([]RecoveryPageCheckpoint, error) {
	rows, err := s.client.Query(ctx, `
		SELECT recovery_job_id, source_topic, page_cursor, page_index, next_page_cursor,
		       from_checkpoint, to_checkpoint, records_applied, quarantined_count, status, generation_id
		FROM recovery_page_checkpoints FINAL
		WHERE recovery_job_id = {job:String} AND source_topic = {topic:String}
		ORDER BY page_index`,
		map[string]string{"job": jobID, "topic": topic},
	)
	if err != nil {
		return nil, err
	}
	out := make([]RecoveryPageCheckpoint, 0, len(rows))
	for _, r := range rows {
		if len(r) < 11 {
			continue
		}
		var idx, applied, quar uint32
		_, _ = fmt.Sscanf(r[3], "%d", &idx)
		_, _ = fmt.Sscanf(r[7], "%d", &applied)
		_, _ = fmt.Sscanf(r[8], "%d", &quar)
		out = append(out, RecoveryPageCheckpoint{
			RecoveryJobID: r[0], SourceTopic: r[1], PageCursor: r[2],
			PageIndex: idx, NextPageCursor: r[4],
			FromCheckpoint: r[5], ToCheckpoint: r[6],
			RecordsApplied: applied, QuarantinedCount: quar,
			Status: r[9], GenerationID: r[10],
		})
	}
	return out, nil
}

func (s *AnalyticsStore) upsertRecoveryJob(ctx context.Context, st RecoveryJobState, fromOA, toOA time.Time) error {
	hasCP, hasOA := "0", "0"
	if st.HasCheckpointRng {
		hasCP = "1"
	}
	if st.HasOccurredRng {
		hasOA = "1"
	}
	fromOAStr := "1970-01-01 00:00:00.000"
	toOAStr := "1970-01-01 00:00:00.000"
	if !fromOA.IsZero() {
		fromOAStr = fromOA.UTC().Format("2006-01-02 15:04:05.000")
	}
	if !toOA.IsZero() {
		toOAStr = toOA.UTC().Format("2006-01-02 15:04:05.000")
	}
	q := `INSERT INTO recovery_jobs (
		recovery_job_id, source_context, source_topic, generation_id, status,
		from_checkpoint, to_checkpoint, from_occurred_at, to_occurred_at,
		has_checkpoint_range, has_occurred_range,
		last_page_cursor, next_page_cursor, pages_completed, accepted_count,
		failure_code, failure_summary, quarantine_key
	) VALUES (
		{job:String}, {ctx:String}, {topic:String}, {gen:String}, {st:String},
		{from:String}, {to:String}, {foa:String}, {toa:String},
		{hcp:UInt8}, {hoa:UInt8},
		{lpc:String}, {npc:String}, {pc:UInt32}, {ac:UInt64},
		{fc:String}, {fs:String}, {qk:String}
	)`
	return s.client.Exec(ctx, q, map[string]string{
		"job": st.RecoveryJobID, "ctx": st.SourceContext, "topic": st.SourceTopic,
		"gen": st.GenerationID, "st": st.Status,
		"from": st.FromCheckpoint, "to": st.ToCheckpoint,
		"foa": fromOAStr, "toa": toOAStr, "hcp": hasCP, "hoa": hasOA,
		"lpc": st.LastPageCursor, "npc": st.NextPageCursor,
		"pc": fmt.Sprintf("%d", st.PagesCompleted), "ac": fmt.Sprintf("%d", st.AcceptedCount),
		"fc": st.FailureCode, "fs": st.FailureSummary, "qk": st.QuarantineKey,
	})
}

func (s *AnalyticsStore) insertLease(ctx context.Context, jobID, owner string, epoch uint64, expires time.Time) error {
	q := `INSERT INTO recovery_leases (recovery_job_id, owner_token, lease_epoch, expires_at)
	      VALUES ({job:String}, {own:String}, {ep:UInt64}, {ex:String})`
	return s.client.Exec(ctx, q, map[string]string{
		"job": jobID, "own": owner, "ep": fmt.Sprintf("%d", epoch),
		"ex": expires.UTC().Format("2006-01-02 15:04:05.000"),
	})
}

func (s *AnalyticsStore) loadLease(ctx context.Context, jobID string) (RecoveryLease, bool, error) {
	rows, err := s.client.Query(ctx, `
		SELECT recovery_job_id, owner_token, lease_epoch, expires_at
		FROM recovery_leases FINAL WHERE recovery_job_id = {job:String}`,
		map[string]string{"job": jobID},
	)
	if err != nil {
		return RecoveryLease{}, false, err
	}
	contenders := make([]RecoveryLease, 0, len(rows))
	for _, r := range rows {
		if len(r) < 4 || r[0] == "" {
			continue
		}
		var epoch uint64
		_, _ = fmt.Sscanf(r[2], "%d", &epoch)
		expires, err := parseCHDateTime(r[3])
		if err != nil {
			return RecoveryLease{}, false, fmt.Errorf("parse lease expires_at: %w", err)
		}
		contenders = append(contenders, RecoveryLease{
			RecoveryJobID: r[0],
			OwnerToken:    r[1],
			LeaseEpoch:    epoch,
			ExpiresAt:     expires.UTC(),
		})
	}
	got, ok := SelectLeaseWinner(contenders)
	return got, ok, nil
}

func parseCHDateTime(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "1970-01-01 00:00:00.000" {
		return time.Time{}, nil
	}
	if t, err := time.ParseInLocation("2006-01-02 15:04:05.000", raw, time.UTC); err == nil {
		return t.UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t.UTC(), nil
	}
	return time.Parse(time.RFC3339, raw)
}

func truncateStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
