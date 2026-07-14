package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"unoarena/platform/telemetry"
	"unoarena/services/room-gameplay/app"
)

// BeginReconciliationIntent encodes repair material and inserts a pending intent.
// Existing pending intents are never silently updated (ErrReconciliationPending).
// Cancelled may restart; done fails closed (ErrReconciliationDone).
func (s *SessionStore) BeginReconciliationIntent(
	ctx context.Context,
	commandID, roomID string,
	expectedRevision int64,
	giPayload json.RawMessage,
	commit app.DurableAcceptedCommit,
) error {
	if s.FailNextBeginIntent != nil {
		err := s.FailNextBeginIntent
		s.FailNextBeginIntent = nil
		return err
	}
	blob, err := EncodeReconciliationRepairBlob(giPayload, commit)
	if err != nil {
		return err
	}
	return s.beginReconciliationIntentRaw(ctx, app.ReconciliationIntent{
		CommandID: commandID, RoomID: roomID, ExpectedRevision: expectedRevision, Payload: blob,
	})
}

func (s *SessionStore) beginReconciliationIntentRaw(ctx context.Context, intent app.ReconciliationIntent) error {
	if intent.CommandID == "" || intent.RoomID == "" {
		return fmt.Errorf("reconciliation intent requires commandId and roomId")
	}
	tx, err := s.intentPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return wrapUnavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	payload := intent.Payload
	if len(payload) == 0 {
		payload = []byte("{}")
	}

	var existingStatus string
	var existingRoom string
	err = tx.QueryRow(ctx, `
		SELECT status, room_id FROM pending_integrity_reconciliations WHERE command_id = $1 FOR UPDATE
	`, intent.CommandID).Scan(&existingStatus, &existingRoom)
	switch {
	case err == nil:
		if existingRoom != intent.RoomID {
			return fmt.Errorf("reconciliation intent command %s room mismatch %s vs %s", intent.CommandID, existingRoom, intent.RoomID)
		}
		switch existingStatus {
		case "pending":
			// Never silently update then allow a second GI Append.
			return app.ErrReconciliationPending
		case "cancelled":
			_, err = tx.Exec(ctx, `
				UPDATE pending_integrity_reconciliations
				SET expected_revision = $2, payload = $3, status = 'pending',
				    log_offset = NULL, revision = NULL, completed_at = NULL,
				    lease_owner = NULL, lease_until = NULL, attempts = 0,
				    next_attempt_at = clock_timestamp() + interval '30 seconds'
				WHERE command_id = $1 AND status = 'cancelled'
			`, intent.CommandID, intent.ExpectedRevision, payload)
		case "done":
			// Durable idempotency must have short-circuited; otherwise fail closed.
			return app.ErrReconciliationDone
		default:
			return fmt.Errorf("reconciliation intent unexpected status %q", existingStatus)
		}
		if err != nil {
			return wrapUnavailable(err)
		}
	case errors.Is(err, pgx.ErrNoRows):
		_, err = tx.Exec(ctx, `
			INSERT INTO pending_integrity_reconciliations
				(command_id, room_id, expected_revision, payload, status, next_attempt_at)
			VALUES ($1, $2, $3, $4, 'pending', clock_timestamp() + interval '30 seconds')
		`, intent.CommandID, intent.RoomID, intent.ExpectedRevision, payload)
		if err != nil {
			return wrapUnavailable(err)
		}
	default:
		return wrapUnavailable(err)
	}
	return wrapUnavailable(tx.Commit(ctx))
}

// FinalizeReconciliationIntent records log_offset/revision after confirmed GI append.
func (s *SessionStore) FinalizeReconciliationIntent(ctx context.Context, commandID string, logOffset, revision int64) error {
	if s.FailNextFinalizeIntent != nil {
		err := s.FailNextFinalizeIntent
		s.FailNextFinalizeIntent = nil
		return err
	}
	tx, err := s.intentPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return wrapUnavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, `
		UPDATE pending_integrity_reconciliations
		SET log_offset = $2, revision = $3
		WHERE command_id = $1 AND status = 'pending'
	`, commandID, logOffset, revision)
	if err != nil {
		return wrapUnavailable(err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("reconciliation intent %s not pending for finalize", commandID)
	}
	return wrapUnavailable(tx.Commit(ctx))
}

// CancelReconciliationIntent marks a definitive pre-write append failure.
func (s *SessionStore) CancelReconciliationIntent(ctx context.Context, commandID string) error {
	_, err := s.intentPool.Exec(ctx, `
		UPDATE pending_integrity_reconciliations
		SET status = 'cancelled', completed_at = clock_timestamp()
		WHERE command_id = $1 AND status = 'pending'
	`, commandID)
	return wrapUnavailable(err)
}

// EncodeReconciliationRepairBlob serializes the full repair material for an intent.
func EncodeReconciliationRepairBlob(
	giPayload json.RawMessage,
	commit app.DurableAcceptedCommit,
) (json.RawMessage, error) {
	snap, err := encodeSessionSnapshot(commit.Session)
	if err != nil {
		return nil, fmt.Errorf("session snapshot: %w", err)
	}
	outcomeBody, err := encodeOutcomeBody(commit.Outcome)
	if err != nil {
		return nil, fmt.Errorf("outcome: %w", err)
	}
	return json.Marshal(app.ReconciliationRepairBlob{
		GIPayload:          giPayload,
		SessionSnapshot:    snap,
		Outbox:             commit.Outbox,
		CommandType:        commit.CommandType,
		Outcome:            outcomeBody,
		PlayerID:           commit.PlayerID,
		PlayerSessionID:    commit.PlayerSessionID,
		BindPlayerSession:  commit.BindPlayerSession,
		IntegrityRevision:  commit.IntegrityRevision,
		StreamSeqHighWater: commit.StreamSeqHighWater,
		SetStreamSeq:       commit.SetStreamSeq,
		CreatePath:         commit.CreatePath,
		ProvisionKey:       commit.ProvisionKey,
		ProvisionRoomID:    commit.ProvisionRoomID,
		ReservationID:      commit.ReservationID,
		ReservationRoomID:  commit.ReservationRoomID,
		ReservationGameID:  commit.ReservationGameID,
		ReservationAction:  commit.ReservationAction,
	})
}

// ListPendingReconciliationMarkers returns pending repair intents oldest-first.
func (s *SessionStore) ListPendingReconciliationMarkers(ctx context.Context, limit int) ([]app.ReconciliationMarker, error) {
	if limit <= 0 {
		limit = 32
	}
	rows, err := s.pool.Query(ctx, `
		SELECT command_id, room_id, expected_revision, log_offset, revision, payload
		FROM pending_integrity_reconciliations
		WHERE status = 'pending'
		ORDER BY created_at ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, wrapUnavailable(err)
	}
	defer rows.Close()
	var out []app.ReconciliationMarker
	for rows.Next() {
		var m app.ReconciliationMarker
		var logOffset, revision *int64
		var payload []byte
		if err := rows.Scan(&m.CommandID, &m.RoomID, &m.ExpectedRevision, &logOffset, &revision, &payload); err != nil {
			return nil, wrapUnavailable(err)
		}
		m.Payload = payload
		if logOffset != nil {
			m.LogOffset = *logOffset
			m.HasLogOffset = true
		}
		if revision != nil {
			m.Revision = *revision
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// ClaimPendingReconciliationMarkers leases a bounded repair batch. Candidate
// rows are locked with SKIP LOCKED so replicas never block one another.
func (s *SessionStore) ClaimPendingReconciliationMarkers(ctx context.Context, owner string, limit int, lease time.Duration) ([]app.ReconciliationMarker, error) {
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return nil, fmt.Errorf("reconciliation claim owner required")
	}
	if limit <= 0 {
		limit = 32
	}
	if limit > 128 {
		limit = 128
	}
	if lease <= 0 {
		lease = time.Minute
	}
	rows, err := s.pool.Query(ctx, `
		WITH candidates AS (
			SELECT command_id
			FROM pending_integrity_reconciliations
			WHERE status = 'pending'
			  AND next_attempt_at <= clock_timestamp()
			  AND (lease_until IS NULL OR lease_until <= clock_timestamp())
			ORDER BY next_attempt_at ASC, created_at ASC, command_id ASC
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		UPDATE pending_integrity_reconciliations AS pending
		SET lease_owner = $2,
		    lease_until = clock_timestamp() + ($3 * interval '1 second'),
		    attempts = pending.attempts + 1
		FROM candidates
		WHERE pending.command_id = candidates.command_id
		RETURNING pending.command_id, pending.room_id, pending.expected_revision,
		          pending.log_offset, pending.revision, pending.payload, pending.attempts
	`, limit, owner, lease.Seconds())
	if err != nil {
		return nil, wrapUnavailable(err)
	}
	defer rows.Close()
	var out []app.ReconciliationMarker
	for rows.Next() {
		var marker app.ReconciliationMarker
		var logOffset, revision *int64
		var payload []byte
		if err := rows.Scan(&marker.CommandID, &marker.RoomID, &marker.ExpectedRevision, &logOffset, &revision, &payload, &marker.Attempts); err != nil {
			return nil, wrapUnavailable(err)
		}
		marker.Payload = payload
		if logOffset != nil {
			marker.LogOffset = *logOffset
			marker.HasLogOffset = true
		}
		if revision != nil {
			marker.Revision = *revision
		}
		out = append(out, marker)
	}
	return out, wrapUnavailable(rows.Err())
}

// ReconcileClaimedMarker verifies the worker lease before applying one repair.
func (s *SessionStore) ReconcileClaimedMarker(ctx context.Context, integrity app.GameIntegrity, deals app.DealSource, marker app.ReconciliationMarker, owner string) error {
	if err := s.requireLiveReconciliationClaim(ctx, marker.CommandID, owner); err != nil {
		return err
	}
	return s.reconcileOneClaimed(ctx, integrity, deals, marker, owner)
}

// RenewReconciliationClaim extends only the caller's still-live marker lease.
// A false result means the worker must stop using the marker immediately.
func (s *SessionStore) RenewReconciliationClaim(ctx context.Context, commandID, owner string, lease time.Duration) (bool, error) {
	if lease <= 0 {
		lease = time.Minute
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE pending_integrity_reconciliations
		SET lease_until = clock_timestamp() + ($3 * interval '1 second')
		WHERE command_id = $1 AND status = 'pending'
		  AND lease_owner = $2 AND lease_until > clock_timestamp()
	`, commandID, owner, lease.Seconds())
	if err != nil {
		return false, wrapUnavailable(err)
	}
	return tag.RowsAffected() == 1, nil
}

// ReleaseReconciliationClaim returns one failed marker after bounded backoff.
func (s *SessionStore) ReleaseReconciliationClaim(ctx context.Context, commandID, owner string, retryDelay time.Duration) error {
	if retryDelay < 0 {
		retryDelay = 0
	}
	if retryDelay > 30*time.Second {
		retryDelay = 30 * time.Second
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE pending_integrity_reconciliations
		SET lease_owner = NULL, lease_until = NULL,
		    next_attempt_at = clock_timestamp() + ($3 * interval '1 second')
		WHERE command_id = $1 AND status = 'pending'
		  AND lease_owner = $2 AND lease_until > clock_timestamp()
	`, commandID, owner, retryDelay.Seconds())
	return wrapUnavailable(err)
}

// TimerIndexRebuildGeneration returns the database-owned completion generation
// that fences one worker process from completions observed after it started.
func (s *SessionStore) TimerIndexRebuildGeneration(ctx context.Context) (int64, error) {
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO room_maintenance_leases (lease_name)
		VALUES ('timer-index-rebuild')
		ON CONFLICT (lease_name) DO NOTHING
	`); err != nil {
		return 0, wrapUnavailable(err)
	}
	var generation int64
	err := s.pool.QueryRow(ctx, `
		SELECT completion_generation
		FROM room_maintenance_leases
		WHERE lease_name = 'timer-index-rebuild'
	`).Scan(&generation)
	return generation, wrapUnavailable(err)
}

// ClaimTimerIndexRebuild acquires the Room-context timer rebuild lease only
// while the database-owned completion generation still matches this worker.
func (s *SessionStore) ClaimTimerIndexRebuild(ctx context.Context, owner string, generation int64, lease time.Duration) (bool, error) {
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return false, fmt.Errorf("timer rebuild owner required")
	}
	if generation < 0 {
		return false, fmt.Errorf("timer rebuild generation must not be negative")
	}
	if lease <= 0 {
		lease = time.Minute
	}
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO room_maintenance_leases (lease_name)
		VALUES ('timer-index-rebuild')
		ON CONFLICT (lease_name) DO NOTHING
	`); err != nil {
		return false, wrapUnavailable(err)
	}
	var claimed bool
	err := s.pool.QueryRow(ctx, `
		WITH claimed AS (
			UPDATE room_maintenance_leases
			SET lease_owner = $1,
			    lease_until = now() + ($3 * interval '1 second'),
			    updated_at = now()
			WHERE lease_name = 'timer-index-rebuild'
			  AND (lease_until IS NULL OR lease_until <= now())
			  AND completion_generation = $2
			RETURNING 1
		)
		SELECT EXISTS(SELECT 1 FROM claimed)
	`, owner, generation, lease.Seconds()).Scan(&claimed)
	return claimed, wrapUnavailable(err)
}

// TimerIndexRebuildCompletedAfter reports whether a sibling timer replica
// advanced the guarded rebuild beyond this worker's initial generation.
func (s *SessionStore) TimerIndexRebuildCompletedAfter(ctx context.Context, generation int64) (bool, error) {
	var completed bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM room_maintenance_leases
			WHERE lease_name = 'timer-index-rebuild' AND completion_generation > $1
		)
	`, generation).Scan(&completed)
	return completed, wrapUnavailable(err)
}

// RenewTimerIndexRebuild extends only a live lease owned by this replica.
func (s *SessionStore) RenewTimerIndexRebuild(ctx context.Context, owner string, lease time.Duration) (bool, error) {
	if lease <= 0 {
		lease = time.Minute
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE room_maintenance_leases
		SET lease_until = now() + ($2 * interval '1 second'), updated_at = now()
		WHERE lease_name = 'timer-index-rebuild'
		  AND lease_owner = $1 AND lease_until > now()
	`, owner, lease.Seconds())
	if err != nil {
		return false, wrapUnavailable(err)
	}
	return tag.RowsAffected() == 1, nil
}

// CompleteTimerIndexRebuild records completion and releases the caller's lease.
func (s *SessionStore) CompleteTimerIndexRebuild(ctx context.Context, owner string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE room_maintenance_leases
		SET lease_owner = NULL,
		    lease_until = NULL,
		    completed_at = now(),
		    completion_generation = completion_generation + 1,
		    updated_at = now()
		WHERE lease_name = 'timer-index-rebuild' AND lease_owner = $1 AND lease_until > now()
	`, owner)
	if err != nil {
		return wrapUnavailable(err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("timer rebuild lease not owned by %s", owner)
	}
	return nil
}

// IsProcessedReconciliationOffset reports whether (room_id, log_offset) was already applied.
func (s *SessionStore) IsProcessedReconciliationOffset(ctx context.Context, roomID string, logOffset int64) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM processed_reconciliation_offsets
			WHERE room_id = $1 AND log_offset = $2
		)
	`, roomID, logOffset).Scan(&exists)
	return exists, wrapUnavailable(err)
}

// MarkReconciliationDone marks a pending intent done (no-op path).
func (s *SessionStore) MarkReconciliationDone(ctx context.Context, commandID string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE pending_integrity_reconciliations
		SET status = 'done', completed_at = clock_timestamp()
		WHERE command_id = $1 AND status = 'pending'
	`, commandID)
	return wrapUnavailable(err)
}

// ReconcilePending processes up to limit pending intents idempotently.
// deals may be nil when no reservation recovery is needed.
func (s *SessionStore) ReconcilePending(ctx context.Context, integrity app.GameIntegrity, deals app.DealSource, limit int) (int, error) {
	markers, err := s.ListPendingReconciliationMarkers(ctx, limit)
	if err != nil {
		return 0, err
	}
	for i, m := range markers {
		if err := s.reconcileOne(ctx, integrity, deals, m); err != nil {
			return i, err
		}
	}
	return len(markers), nil
}

func (s *SessionStore) reconcileOne(ctx context.Context, integrity app.GameIntegrity, deals app.DealSource, m app.ReconciliationMarker) error {
	return s.reconcileOneClaimed(ctx, integrity, deals, m, "")
}

// reconcileOneClaimed performs one repair. When owner is non-empty, every
// durable marker transition is fenced by that owner and a still-live lease.
func (s *SessionStore) reconcileOneClaimed(ctx context.Context, integrity app.GameIntegrity, deals app.DealSource, m app.ReconciliationMarker, owner string) error {
	var blob app.ReconciliationRepairBlob
	if err := json.Unmarshal(m.Payload, &blob); err != nil {
		return fmt.Errorf("repair blob: %w", err)
	}

	if !m.HasLogOffset {
		replay, err := integrity.Replay(ctx, m.RoomID, m.ExpectedRevision)
		if err != nil {
			return fmt.Errorf("gi replay room=%s expectedRev=%d: %w", m.RoomID, m.ExpectedRevision, err)
		}
		var found *app.ReplayEntry
		for i := range replay.Entries {
			e := &replay.Entries[i]
			if e.EventID == m.CommandID {
				found = e
				break
			}
		}
		if found == nil {
			// A strongly consistent replay proving that GI has not advanced closes
			// the uncertain-response gap: re-append the exact persisted intent with
			// its deterministic event ID. Any advanced revision remains ambiguous
			// and must stay fail-closed.
			if replay.Revision != m.ExpectedRevision {
				return fmt.Errorf("gi replay fail-closed: no matching event and revision advanced command=%s room=%s expected=%d got=%d (leave pending)",
					m.CommandID, m.RoomID, m.ExpectedRevision, replay.Revision)
			}
			gameID, err := reconciliationGameID(blob)
			if err != nil {
				return err
			}
			appendRes, err := integrity.Append(ctx, app.AppendRequest{
				RoomID:           m.RoomID,
				GameID:           gameID,
				EventID:          m.CommandID,
				ExpectedRevision: m.ExpectedRevision,
				EventType:        blob.CommandType,
				Payload:          blob.GIPayload,
			})
			if err != nil {
				return fmt.Errorf("gi deterministic reappend command=%s room=%s: %w", m.CommandID, m.RoomID, err)
			}
			if err := s.finalizeReconciliationIntentClaimed(ctx, m.CommandID, appendRes.LogOffset, appendRes.Revision, owner); err != nil {
				return err
			}
			m.LogOffset = appendRes.LogOffset
			m.Revision = appendRes.Revision
			m.HasLogOffset = true
		} else {
			if err := assertReplayIdentity(blob, *found); err != nil {
				return err
			}
			rev, err := resolveRevision(m, *found, replay)
			if err != nil {
				return err
			}
			if err := s.finalizeReconciliationIntentClaimed(ctx, m.CommandID, found.Offset, rev, owner); err != nil {
				return err
			}
			m.LogOffset = found.Offset
			m.Revision = rev
			m.HasLogOffset = true
		}
	}

	processed, err := s.IsProcessedReconciliationOffset(ctx, m.RoomID, m.LogOffset)
	if err != nil {
		return err
	}
	if processed {
		return s.markReconciliationDoneClaimed(ctx, m.CommandID, owner)
	}

	replay, err := integrity.Replay(ctx, m.RoomID, m.LogOffset)
	if err != nil {
		return fmt.Errorf("gi replay room=%s offset=%d: %w", m.RoomID, m.LogOffset, err)
	}
	var found *app.ReplayEntry
	for i := range replay.Entries {
		e := &replay.Entries[i]
		if e.Offset != m.LogOffset {
			continue
		}
		// Exact EventID == commandId required; blank or any-event fallbacks removed.
		if e.EventID != m.CommandID {
			return fmt.Errorf("gi replay fail-closed: eventId mismatch at offset=%d want=%s got=%q (leave pending)",
				m.LogOffset, m.CommandID, e.EventID)
		}
		found = e
		break
	}
	if found == nil {
		return fmt.Errorf("gi replay fail-closed: missing entry room=%s offset=%d", m.RoomID, m.LogOffset)
	}
	if err := assertReplayIdentity(blob, *found); err != nil {
		return err
	}
	rev, err := resolveRevision(m, *found, replay)
	if err != nil {
		return err
	}
	m.Revision = rev

	// Reservation action before local snapshot/outbox repair.
	if blob.ReservationID != "" && blob.ReservationAction != "" {
		if owner != "" {
			if err := s.requireLiveReconciliationClaim(ctx, m.CommandID, owner); err != nil {
				return err
			}
		}
		if deals == nil {
			return fmt.Errorf("reservation recovery requires DealSource (leave pending)")
		}
		actionReq := app.DurableAcceptedCommit{
			ReservationID:     blob.ReservationID,
			ReservationRoomID: blob.ReservationRoomID,
			ReservationGameID: blob.ReservationGameID,
			ReservationAction: blob.ReservationAction,
		}
		if actionReq.ReservationRoomID == "" {
			actionReq.ReservationRoomID = m.RoomID
		}
		if err := performReservationAction(ctx, deals, actionReq); err != nil {
			return fmt.Errorf("reservation %s failed (leave pending): %w", blob.ReservationAction, err)
		}
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return wrapUnavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var integrityOffset int64
	err = tx.QueryRow(ctx, `
		SELECT integrity_log_offset FROM rooms WHERE room_id = $1 FOR UPDATE
	`, m.RoomID).Scan(&integrityOffset)
	roomExists := err == nil
	if err != nil && err != pgx.ErrNoRows {
		return wrapUnavailable(err)
	}
	if roomExists {
		if integrityOffset >= m.Revision {
			if err := markReconciliationDoneTx(ctx, tx, m.CommandID, owner); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO processed_reconciliation_offsets (room_id, log_offset, notes)
				VALUES ($1, $2, 'already_advanced')
				ON CONFLICT DO NOTHING
			`, m.RoomID, m.LogOffset); err != nil {
				return wrapUnavailable(err)
			}
			return wrapUnavailable(tx.Commit(ctx))
		}
	}

	sess, err := decodeSessionSnapshot(blob.SessionSnapshot)
	if err != nil {
		return fmt.Errorf("decode session: %w", err)
	}
	outcome, err := decodeOutcomeBody(blob.Outcome)
	if err != nil {
		return fmt.Errorf("decode outcome: %w", err)
	}
	outbox := blob.Outbox
	outbox.LogOffset = m.LogOffset
	commit := app.DurableAcceptedCommit{
		Session:              sess,
		Outbox:               outbox,
		PlayerID:             blob.PlayerID,
		PlayerSessionID:      blob.PlayerSessionID,
		BindPlayerSession:    blob.BindPlayerSession,
		IntegrityRevision:    blob.IntegrityRevision,
		SetIntegrityRevision: true,
		StreamSeqHighWater:   blob.StreamSeqHighWater,
		SetStreamSeq:         blob.SetStreamSeq,
		LogOffset:            m.LogOffset,
		CommandID:            m.CommandID,
		CommandType:          blob.CommandType,
		Outcome:              outcome,
		CreatePath:           blob.CreatePath && !roomExists,
		ProvisionKey:         blob.ProvisionKey,
		ProvisionRoomID:      blob.ProvisionRoomID,
	}
	if commit.IntegrityRevision == 0 {
		commit.IntegrityRevision = m.Revision
	}
	if err := persistSessionTx(ctx, tx, sess, app.CommitRequest{
		Session:              sess,
		Outbox:               outbox,
		ProvisionKey:         blob.ProvisionKey,
		ProvisionRoomID:      blob.ProvisionRoomID,
		PlayerSessionID:      blob.PlayerSessionID,
		PlayerID:             blob.PlayerID,
		BindPlayerSession:    blob.BindPlayerSession,
		IntegrityRevision:    commit.IntegrityRevision,
		SetIntegrityRevision: true,
		StreamSeqHighWater:   blob.StreamSeqHighWater,
		SetStreamSeq:         blob.SetStreamSeq,
	}, commit.CreatePath); err != nil {
		return wrapUnavailable(err)
	}
	if err := upsertIdempotency(ctx, tx, m.RoomID, m.CommandID, blob.CommandType, blob.PlayerID, outcome); err != nil {
		return wrapUnavailable(err)
	}

	reconcilePayload, _ := json.Marshal(map[string]any{
		"roomId":    m.RoomID,
		"logOffset": m.LogOffset,
		"revision":  m.Revision,
		"commandId": m.CommandID,
	})
	eventID := fmt.Sprintf("reconcile-%s-%d", m.RoomID, m.LogOffset)
	traceparent, tracestate := telemetry.TraceContextHeaders(ctx)
	_, err = tx.Exec(ctx, `
		INSERT INTO integration_outbox_events (
			event_id, event_type, topic, partition_key, schema_version, room_id,
			integrity_log_offset, payload, traceparent, tracestate, occurred_at
		) VALUES ($1,$2,$3,$4,1,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (event_id) DO NOTHING
	`, eventID, app.EventRoomStateReconciled, app.TopicRoomStateReconciled, m.RoomID,
		m.RoomID, m.LogOffset, reconcilePayload, nullIfEmpty(traceparent), nullIfEmpty(tracestate), time.Now().UTC())
	if err != nil {
		return wrapUnavailable(err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO processed_reconciliation_offsets (room_id, log_offset, notes)
		VALUES ($1, $2, 'repaired')
		ON CONFLICT DO NOTHING
	`, m.RoomID, m.LogOffset); err != nil {
		return wrapUnavailable(err)
	}
	if err := markReconciliationDoneTx(ctx, tx, m.CommandID, owner); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return wrapUnavailable(err)
	}
	app.RecordCommittedGameCompletion(ctx, outbox)
	if s.Timers != nil && sess != nil {
		_ = scheduleTimersFromSession(ctx, s.Timers, sess)
	}
	return nil
}

func (s *SessionStore) requireLiveReconciliationClaim(ctx context.Context, commandID, owner string) error {
	var owned bool
	if err := s.pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM pending_integrity_reconciliations
			WHERE command_id = $1 AND status = 'pending'
			  AND lease_owner = $2 AND lease_until > clock_timestamp()
		)
	`, commandID, owner).Scan(&owned); err != nil {
		return wrapUnavailable(err)
	}
	if !owned {
		return fmt.Errorf("reconciliation claim lost command=%s owner=%s", commandID, owner)
	}
	return nil
}

func (s *SessionStore) finalizeReconciliationIntentClaimed(ctx context.Context, commandID string, logOffset, revision int64, owner string) error {
	if owner == "" {
		return s.FinalizeReconciliationIntent(ctx, commandID, logOffset, revision)
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE pending_integrity_reconciliations
		SET log_offset = $2, revision = $3
		WHERE command_id = $1 AND status = 'pending'
		  AND lease_owner = $4 AND lease_until > clock_timestamp()
	`, commandID, logOffset, revision, owner)
	if err != nil {
		return wrapUnavailable(err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("reconciliation claim lost command=%s owner=%s", commandID, owner)
	}
	return nil
}

func (s *SessionStore) markReconciliationDoneClaimed(ctx context.Context, commandID, owner string) error {
	if owner == "" {
		return s.MarkReconciliationDone(ctx, commandID)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return wrapUnavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := markReconciliationDoneTx(ctx, tx, commandID, owner); err != nil {
		return err
	}
	return wrapUnavailable(tx.Commit(ctx))
}

func markReconciliationDoneTx(ctx context.Context, tx pgx.Tx, commandID, owner string) error {
	query := `
		UPDATE pending_integrity_reconciliations
		SET status = 'done', completed_at = clock_timestamp()
		WHERE command_id = $1 AND status = 'pending'
	`
	args := []any{commandID}
	if owner != "" {
		query += " AND lease_owner = $2 AND lease_until > clock_timestamp()"
		args = append(args, owner)
	}
	tag, err := tx.Exec(ctx, query, args...)
	if err != nil {
		return wrapUnavailable(err)
	}
	if owner != "" && tag.RowsAffected() != 1 {
		return fmt.Errorf("reconciliation claim lost command=%s owner=%s", commandID, owner)
	}
	return nil
}

func reconciliationGameID(blob app.ReconciliationRepairBlob) (string, error) {
	if blob.ReservationGameID != "" {
		return blob.ReservationGameID, nil
	}
	sess, err := decodeSessionSnapshot(blob.SessionSnapshot)
	if err != nil {
		return "", fmt.Errorf("decode reconciliation game identity: %w", err)
	}
	if sess == nil {
		return "", fmt.Errorf("decode reconciliation game identity: missing session")
	}
	return string(sess.GameID()), nil
}

func assertReplayIdentity(blob app.ReconciliationRepairBlob, e app.ReplayEntry) error {
	if blob.CommandType != "" && e.EventType != "" && blob.CommandType != e.EventType {
		return fmt.Errorf("gi replay fail-closed: eventType mismatch want=%s got=%s (leave pending)",
			blob.CommandType, e.EventType)
	}
	if len(blob.GIPayload) == 0 || len(e.Payload) == 0 {
		return nil
	}
	ok, err := semanticJSONEqual(blob.GIPayload, e.Payload)
	if err != nil {
		return fmt.Errorf("gi replay fail-closed: payload identity: %w (leave pending)", err)
	}
	if !ok {
		return fmt.Errorf("gi replay fail-closed: payload identity mismatch (leave pending)")
	}
	return nil
}

// semanticJSONEqual compares JSON by decoded value identity (whitespace/key-order independent).
// Invalid JSON on either side is rejected.
func semanticJSONEqual(a, b []byte) (bool, error) {
	var va, vb any
	if err := json.Unmarshal(a, &va); err != nil {
		return false, fmt.Errorf("invalid intent JSON: %w", err)
	}
	if err := json.Unmarshal(b, &vb); err != nil {
		return false, fmt.Errorf("invalid replay JSON: %w", err)
	}
	ca, err := json.Marshal(va)
	if err != nil {
		return false, err
	}
	cb, err := json.Marshal(vb)
	if err != nil {
		return false, err
	}
	return bytes.Equal(ca, cb), nil
}

func resolveRevision(m app.ReconciliationMarker, e app.ReplayEntry, replay app.ReplayResult) (int64, error) {
	implied := e.Offset + 1
	if m.Revision != 0 && m.Revision != implied {
		return 0, fmt.Errorf("gi replay fail-closed: revision contradiction stored=%d offset+1=%d", m.Revision, implied)
	}
	if replay.Revision != 0 && replay.Revision != implied {
		// Replay tip must agree with exact entry offset+1 when repairing a single offset.
		// If tip is ahead (more entries returned), only accept when stored revision matches implied.
		if m.HasLogOffset || len(replay.Entries) == 1 {
			if replay.Revision < implied {
				return 0, fmt.Errorf("gi replay fail-closed: revision contradiction replay=%d offset+1=%d", replay.Revision, implied)
			}
			// Tip ahead of this entry is OK when more entries follow; use implied for this command.
		}
	}
	return implied, nil
}

func performReservationAction(ctx context.Context, deals app.DealSource, req app.DurableAcceptedCommit) error {
	if deals == nil || req.ReservationID == "" {
		return nil
	}
	switch req.ReservationAction {
	case app.ReservationActionConfirm:
		return deals.ConfirmAt(ctx, req.ReservationRoomID, req.ReservationGameID, req.ReservationID)
	case app.ReservationActionCancel:
		return deals.CancelAt(ctx, req.ReservationRoomID, req.ReservationGameID, req.ReservationID)
	case "":
		return nil
	default:
		return fmt.Errorf("unknown reservation action %q", req.ReservationAction)
	}
}
