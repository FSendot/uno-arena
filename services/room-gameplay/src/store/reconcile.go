package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

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
				    log_offset = NULL, revision = NULL, completed_at = NULL
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
				(command_id, room_id, expected_revision, payload, status)
			VALUES ($1, $2, $3, $4, 'pending')
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
		SET status = 'cancelled', completed_at = now()
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
		SET status = 'done', completed_at = now()
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
			return fmt.Errorf("gi replay fail-closed: no matching event command=%s room=%s (leave pending)", m.CommandID, m.RoomID)
		}
		if err := assertReplayIdentity(blob, *found); err != nil {
			return err
		}
		rev, err := resolveRevision(m, *found, replay)
		if err != nil {
			return err
		}
		if err := s.FinalizeReconciliationIntent(ctx, m.CommandID, found.Offset, rev); err != nil {
			return err
		}
		m.LogOffset = found.Offset
		m.Revision = rev
		m.HasLogOffset = true
	}

	processed, err := s.IsProcessedReconciliationOffset(ctx, m.RoomID, m.LogOffset)
	if err != nil {
		return err
	}
	if processed {
		return s.MarkReconciliationDone(ctx, m.CommandID)
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
			if _, err := tx.Exec(ctx, `
				UPDATE pending_integrity_reconciliations
				SET status = 'done', completed_at = now()
				WHERE command_id = $1 AND status = 'pending'
			`, m.CommandID); err != nil {
				return wrapUnavailable(err)
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
	_, err = tx.Exec(ctx, `
		INSERT INTO integration_outbox_events (
			event_id, event_type, topic, partition_key, schema_version, room_id,
			integrity_log_offset, payload, occurred_at
		) VALUES ($1,$2,$3,$4,1,$5,$6,$7,$8)
		ON CONFLICT (event_id) DO NOTHING
	`, eventID, app.EventRoomStateReconciled, app.TopicRoomStateReconciled, m.RoomID,
		m.RoomID, m.LogOffset, reconcilePayload, time.Now().UTC())
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
	if _, err := tx.Exec(ctx, `
		UPDATE pending_integrity_reconciliations
		SET status = 'done', completed_at = now()
		WHERE command_id = $1 AND status = 'pending'
	`, m.CommandID); err != nil {
		return wrapUnavailable(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return wrapUnavailable(err)
	}
	if s.Timers != nil && sess != nil {
		_ = scheduleTimersFromSession(ctx, s.Timers, sess)
	}
	return nil
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
