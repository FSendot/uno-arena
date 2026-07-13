package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ErrRuntimeReadinessMismatch identifies a valid readiness event that does not
// belong to Tournament's authoritative assigned match.
var ErrRuntimeReadinessMismatch = errors.New("room runtime readiness assignment mismatch")

// RoomRuntimeReady is the durable Tournament-side form of RoomRuntimeReady.
type RoomRuntimeReady struct {
	EventID      string
	RoomID       string
	TournamentID string
	RoundNumber  int
	SlotID       string
	Generation   int64
	OccurredAt   time.Time
}

// ApplyRoomRuntimeReady atomically deduplicates the event and opens assignment
// visibility. Later observations never revoke or replace the first readiness.
func (s *TournamentStore) ApplyRoomRuntimeReady(ctx context.Context, evt RoomRuntimeReady) (bool, error) {
	if s == nil || s.pool == nil {
		return false, fmt.Errorf("nil store")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, wrapUnavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var roomID string
	var readyEventID *string
	var slotIndex int
	err = tx.QueryRow(ctx, `
		SELECT am.room_id, am.runtime_ready_event_id, bs.slot_index
		FROM assigned_matches am
		JOIN bracket_slots bs
		  ON bs.tournament_id = am.tournament_id
		 AND bs.round_number = am.round_number
		 AND bs.slot_id = am.slot_id
		WHERE am.tournament_id = $1 AND am.round_number = $2 AND am.slot_id = $3
		FOR UPDATE
	`, evt.TournamentID, evt.RoundNumber, evt.SlotID).Scan(&roomID, &readyEventID, &slotIndex)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && roomID != evt.RoomID) {
		return false, ErrRuntimeReadinessMismatch
	}
	if err != nil {
		return false, wrapUnavailable(err)
	}
	if readyEventID != nil {
		if err := tx.Commit(ctx); err != nil {
			return false, wrapUnavailable(err)
		}
		return false, nil
	}

	tag, err := tx.Exec(ctx, `
		UPDATE assigned_matches
		SET runtime_ready_event_id = $4,
			runtime_ready_generation = $5,
			runtime_ready_at = $6
		WHERE tournament_id = $1 AND round_number = $2 AND slot_id = $3
		  AND runtime_ready_event_id IS NULL
	`, evt.TournamentID, evt.RoundNumber, evt.SlotID, evt.EventID, evt.Generation, evt.OccurredAt.UTC())
	if err != nil {
		return false, wrapUnavailable(err)
	}
	if tag.RowsAffected() != 1 {
		return false, fmt.Errorf("%w: concurrent readiness update", ErrRuntimeReadinessMismatch)
	}
	if err := bumpProjectionShardTx(ctx, tx, evt.TournamentID, slotIndex, evt.OccurredAt.UTC()); err != nil {
		return false, wrapUnavailable(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, wrapUnavailable(err)
	}
	return true, nil
}
