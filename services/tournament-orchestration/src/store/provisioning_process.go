package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/shared/envelope"
)

// PreparedSlotWork is one immutable RoomProvisionRequest snapshot from prepare.
type PreparedSlotWork struct {
	SlotID    string
	SlotIndex int
	RoomID    string
	BatchID   string
	PlayerIDs []string
}

// PrepareProvisioningBatchResult is the post-prepare snapshot (Room calls happen outside TX).
type PrepareProvisioningBatchResult struct {
	AlreadyComplete bool
	Rejected        bool
	RejectReason    string
	Quarantined     bool
	PriorOutcome    *envelope.Result
	Slots           []PreparedSlotWork
	TournamentID    string
	RoundNumber     int
	BatchID         string
	RetryAttempt    int
	LeaseOwner      string
	LeaseVersion    int64
	CommandID       string
	CorrelationID   string
	NewAssignments  int
}

// PrepareProvisioningBatchInput identifies a leased batch attempt for differential prepare.
type PrepareProvisioningBatchInput struct {
	CommandID     string
	CorrelationID string
	TournamentID  string
	RoundNumber   int
	BatchID       string
	SlotFrom      string
	SlotTo        string
	SlotSize      int
	RetryAttempt  int
	LeaseOwner    string
	LeaseVersion  int64
}

// FinalizeProvisioningBatchInput carries success/failure finalize identity.
type FinalizeProvisioningBatchInput struct {
	CommandID       string
	CorrelationID   string
	TournamentID    string
	RoundNumber     int
	BatchID         string
	SlotFrom        string
	SlotTo          string
	SlotSize        int
	RetryAttempt    int
	LeaseOwner      string
	LeaseVersion    int64
	LastError       string // sanitized; success leaves empty
	RetryBudget     int
	ForceQuarantine bool // room assignment conflict / mismatch — skip retry budget
	NextRetryWork   map[string]any
}

// PrepareProvisioningBatch runs the differential prepare TX and returns Room call snapshots.
// Lock order: shared rewrite barrier → attempt command lock → leased batch FOR UPDATE → round → bounded slots.
func (s *TournamentStore) PrepareProvisioningBatch(ctx context.Context, in PrepareProvisioningBatchInput) (*PrepareProvisioningBatchResult, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("nil store")
	}
	if err := validatePrepareInput(in); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, wrapUnavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := acquireRewriteBarrierShared(ctx, tx, in.TournamentID); err != nil {
		return nil, wrapUnavailable(err)
	}
	if err := AcquireCommandLock(ctx, tx, in.CommandID); err != nil {
		return nil, wrapUnavailable(err)
	}

	var priorBody []byte
	err = tx.QueryRow(ctx, `
		SELECT outcome_body FROM command_idempotency WHERE command_id = $1
	`, in.CommandID).Scan(&priorBody)
	if err == nil {
		var prior envelope.Result
		_ = json.Unmarshal(priorBody, &prior)
		if err := tx.Commit(ctx); err != nil {
			return nil, wrapUnavailable(err)
		}
		return &PrepareProvisioningBatchResult{PriorOutcome: &prior, CommandID: in.CommandID}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, wrapUnavailable(err)
	}

	var phase string
	err = tx.QueryRow(ctx, `
		SELECT phase FROM tournaments WHERE tournament_id = $1
	`, in.TournamentID).Scan(&phase)
	if errors.Is(err, pgx.ErrNoRows) {
		return rejectPrepare(ctx, tx, in, "tournament_not_found")
	}
	if err != nil {
		return nil, wrapUnavailable(err)
	}
	if phase == "completed" || phase == "cancelled" {
		return rejectPrepare(ctx, tx, in, string(domain.RejectAlreadyTerminal))
	}

	var (
		batchStatus, slotFrom, slotTo, leaseOwner string
		retryAttempt                              int
		leaseVersion                              int64
	)
	err = tx.QueryRow(ctx, `
		SELECT status, retry_attempt, slot_id_from, slot_id_to,
		       COALESCE(lease_owner, ''), lease_version
		FROM provisioning_batches
		WHERE tournament_id = $1 AND round_number = $2 AND batch_id = $3
		FOR UPDATE
	`, in.TournamentID, in.RoundNumber, in.BatchID).Scan(
		&batchStatus, &retryAttempt, &slotFrom, &slotTo, &leaseOwner, &leaseVersion,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return rejectPrepare(ctx, tx, in, string(domain.RejectBatchNotFound))
	}
	if err != nil {
		return nil, wrapUnavailable(err)
	}
	if batchStatus == string(domain.BatchCancelled) {
		return rejectPrepare(ctx, tx, in, string(domain.RejectBatchCancelled))
	}
	// Terminal completed/quarantined without a prior command outcome: stale workers must
	// get ErrProvisioningFence (no command_idempotency poison). Genuine idempotency is
	// only via the prior-outcome check above.
	if batchStatus == string(domain.BatchCompleted) || batchStatus == string(domain.BatchQuarantined) {
		return nil, ErrProvisioningFence
	}
	// Fence: status must be in_progress with exact owner+version+retry. Stale same-owner
	// old version must NOT poison command_idempotency.
	if batchStatus != string(domain.BatchInProgress) ||
		leaseOwner != in.LeaseOwner ||
		leaseVersion != in.LeaseVersion ||
		retryAttempt != in.RetryAttempt {
		return nil, ErrProvisioningFence
	}
	if slotFrom != in.SlotFrom || slotTo != in.SlotTo {
		return rejectPrepare(ctx, tx, in, "slot range does not match batch sharding")
	}
	fromIdx, err := parseSlotIndex(slotFrom)
	if err != nil {
		return nil, err
	}
	toIdx, err := parseSlotIndex(slotTo)
	if err != nil {
		return nil, err
	}
	slotSize := toIdx - fromIdx + 1
	if slotSize != in.SlotSize || slotSize <= 0 || slotSize > domain.MaxProvisioningBatchSize {
		return rejectPrepare(ctx, tx, in, "slotSize does not match batch sharding")
	}

	var roundStatus string
	err = tx.QueryRow(ctx, `
		SELECT status FROM tournament_rounds
		WHERE tournament_id = $1 AND round_number = $2
		FOR UPDATE
	`, in.TournamentID, in.RoundNumber).Scan(&roundStatus)
	if err != nil {
		return nil, wrapUnavailable(err)
	}
	if roundStatus != string(domain.RoundProvisioning) {
		return rejectPrepare(ctx, tx, in, string(domain.RejectRoundNotReady))
	}

	// Savepoint isolates bulk mutations so conflict quarantine never commits
	// partial assigned_matches/slots/outbox, and so caught PG 23505 can recover
	// the transaction (aborted until ROLLBACK TO SAVEPOINT).
	if _, err := tx.Exec(ctx, `SAVEPOINT prepare_bulk`); err != nil {
		return nil, wrapUnavailable(err)
	}
	prepared, newAssigned, conflictReason, err := prepareBulkAssignments(ctx, tx, in, fromIdx, toIdx, slotSize, now)
	if err != nil {
		return nil, err
	}
	if conflictReason != "" {
		if _, rbErr := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT prepare_bulk`); rbErr != nil {
			return nil, wrapUnavailable(rbErr)
		}
		if _, relErr := tx.Exec(ctx, `RELEASE SAVEPOINT prepare_bulk`); relErr != nil {
			return nil, wrapUnavailable(relErr)
		}
		switch conflictReason {
		case string(domain.RejectConflictingAssignment), "outbox payload conflict":
			return quarantinePrepareConflict(ctx, tx, in, conflictReason, now)
		default:
			return rejectPrepare(ctx, tx, in, conflictReason)
		}
	}
	if _, relErr := tx.Exec(ctx, `RELEASE SAVEPOINT prepare_bulk`); relErr != nil {
		return nil, wrapUnavailable(relErr)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, wrapUnavailable(err)
	}
	return &PrepareProvisioningBatchResult{
		Slots:          prepared,
		TournamentID:   in.TournamentID,
		RoundNumber:    in.RoundNumber,
		BatchID:        in.BatchID,
		RetryAttempt:   in.RetryAttempt,
		LeaseOwner:     in.LeaseOwner,
		LeaseVersion:   in.LeaseVersion,
		CommandID:      in.CommandID,
		CorrelationID:  in.CorrelationID,
		NewAssignments: newAssigned,
	}, nil
}

// prepareBulkAssignments performs set-based assigned_matches / slot / outbox / shard ops.
// Returns conflictReason when immutable assignment/outbox/room uniqueness conflicts.
func prepareBulkAssignments(
	ctx context.Context, tx pgx.Tx, in PrepareProvisioningBatchInput,
	fromIdx, toIdx, slotSize int, now time.Time,
) ([]PreparedSlotWork, int, string, error) {
	rows, err := tx.Query(ctx, `
		SELECT s.slot_id, s.slot_index, s.status, s.seeded_player_ids,
		       a.room_id, a.provisioning_batch_id
		FROM bracket_slots s
		LEFT JOIN assigned_matches a
		  ON a.tournament_id = s.tournament_id
		 AND a.round_number = s.round_number
		 AND a.slot_id = s.slot_id
		WHERE s.tournament_id = $1 AND s.round_number = $2
		  AND s.slot_index >= $3 AND s.slot_index <= $4
		ORDER BY s.slot_index ASC
		LIMIT $5
	`, in.TournamentID, in.RoundNumber, fromIdx, toIdx, domain.MaxProvisioningBatchSize)
	if err != nil {
		return nil, 0, "", wrapUnavailable(err)
	}
	type slotRow struct {
		slotID, status string
		slotIndex      int
		players        []string
		roomID         *string
		batchID        *string
	}
	slots := make([]slotRow, 0, slotSize)
	for rows.Next() {
		var r slotRow
		if err := rows.Scan(&r.slotID, &r.slotIndex, &r.status, &r.players, &r.roomID, &r.batchID); err != nil {
			rows.Close()
			return nil, 0, "", wrapUnavailable(err)
		}
		slots = append(slots, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, 0, "", wrapUnavailable(err)
	}
	if len(slots) != slotSize {
		return nil, 0, "bounded slot range count mismatch", nil
	}

	type wantRow struct {
		SlotID    string   `json:"slot_id"`
		SlotIndex int      `json:"slot_index"`
		RoomID    string   `json:"room_id"`
		BatchID   string   `json:"batch_id"`
		Players   []string `json:"players"`
		Status    string   `json:"status"`
		EventID   string   `json:"event_id"`
	}
	wants := make([]wantRow, 0, len(slots))
	prepared := make([]PreparedSlotWork, 0, len(slots))
	for _, sl := range slots {
		wantRoom := string(domain.RoomIDForSlot(domain.TournamentID(in.TournamentID), in.RoundNumber, domain.SlotID(sl.slotID)))
		wantBatch := in.BatchID
		if sl.roomID != nil {
			if *sl.roomID != wantRoom {
				return nil, 0, string(domain.RejectConflictingAssignment), nil
			}
			if sl.batchID != nil && *sl.batchID != wantBatch {
				return nil, 0, string(domain.RejectConflictingAssignment), nil
			}
		}
		if sl.status == string(domain.SlotQuarantined) || sl.status == string(domain.SlotCancelled) {
			return nil, 0, string(domain.RejectQuarantined), nil
		}
		eventID := domain.MatchAssignedEventID(domain.TournamentID(in.TournamentID), in.RoundNumber, domain.SlotID(sl.slotID))
		players := append([]string(nil), sl.players...)
		wants = append(wants, wantRow{
			SlotID: sl.slotID, SlotIndex: sl.slotIndex, RoomID: wantRoom, BatchID: wantBatch,
			Players: players, Status: sl.status, EventID: eventID,
		})
		prepared = append(prepared, PreparedSlotWork{
			SlotID: sl.slotID, SlotIndex: sl.slotIndex, RoomID: wantRoom, BatchID: wantBatch, PlayerIDs: players,
		})
	}

	wantJSON, err := json.Marshal(wants)
	if err != nil {
		return nil, 0, "", err
	}

	// Detect global room_id collisions for slots we still need to insert.
	var roomCollision int
	err = tx.QueryRow(ctx, `
		SELECT count(*)::int
		FROM jsonb_to_recordset($1::jsonb) AS w(slot_id text, room_id text)
		INNER JOIN assigned_matches a ON a.room_id = w.room_id
		WHERE NOT EXISTS (
			SELECT 1 FROM assigned_matches e
			WHERE e.tournament_id = $2 AND e.round_number = $3 AND e.slot_id = w.slot_id
		)
	`, wantJSON, in.TournamentID, in.RoundNumber).Scan(&roomCollision)
	if err != nil {
		return nil, 0, "", wrapUnavailable(err)
	}
	if roomCollision > 0 {
		return nil, 0, string(domain.RejectConflictingAssignment), nil
	}

	// Bulk insert missing assignments (one round-trip).
	tag, err := tx.Exec(ctx, `
		INSERT INTO assigned_matches (
			tournament_id, round_number, slot_id, room_id, assigned_at, provisioning_batch_id
		)
		SELECT $2, $3, w.slot_id, w.room_id, $4, w.batch_id
		FROM jsonb_to_recordset($1::jsonb) AS w(
			slot_id text, slot_index int, room_id text, batch_id text
		)
		WHERE NOT EXISTS (
			SELECT 1 FROM assigned_matches a
			WHERE a.tournament_id = $2 AND a.round_number = $3 AND a.slot_id = w.slot_id
		)
	`, wantJSON, in.TournamentID, in.RoundNumber, now)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return nil, 0, string(domain.RejectConflictingAssignment), nil
		}
		return nil, 0, "", wrapUnavailable(err)
	}
	newAssigned := int(tag.RowsAffected())

	// Re-validate existing rows match immutable want (covers concurrent insert races).
	var mismatch int
	err = tx.QueryRow(ctx, `
		SELECT count(*)::int
		FROM jsonb_to_recordset($1::jsonb) AS w(slot_id text, room_id text, batch_id text)
		INNER JOIN assigned_matches a
		  ON a.tournament_id = $2 AND a.round_number = $3 AND a.slot_id = w.slot_id
		WHERE a.room_id <> w.room_id
		   OR COALESCE(a.provisioning_batch_id, '') <> w.batch_id
	`, wantJSON, in.TournamentID, in.RoundNumber).Scan(&mismatch)
	if err != nil {
		return nil, 0, "", wrapUnavailable(err)
	}
	if mismatch > 0 {
		return nil, 0, string(domain.RejectConflictingAssignment), nil
	}

	// Bulk pending → assigned.
	_, err = tx.Exec(ctx, `
		UPDATE bracket_slots s
		SET status = 'assigned', updated_at = $4
		FROM jsonb_to_recordset($1::jsonb) AS w(slot_id text, status text)
		WHERE s.tournament_id = $2 AND s.round_number = $3
		  AND s.slot_id = w.slot_id
		  AND s.status = 'pending'
		  AND w.status = 'pending'
	`, wantJSON, in.TournamentID, in.RoundNumber, now)
	if err != nil {
		return nil, 0, "", wrapUnavailable(err)
	}

	// Bulk outbox insert; verify immutable fields on conflict.
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events (
			event_id, event_type, tournament_id, topic, partition_key, schema_version, payload, created_at
		)
		SELECT
			w.event_id,
			$5::text,
			$2,
			'tournament.match.assigned',
			$2,
			1,
			jsonb_build_object(
				'schemaVersion', 1,
				'eventId', w.event_id,
				'eventType', $5::text,
				'correlationId', COALESCE(NULLIF($6::text, ''), $7::text),
				'causationId', $7::text,
				'occurredAt', to_char($4::timestamptz AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
				'tournamentId', $2::text,
				'roundNumber', $3::int,
				'slotId', w.slot_id,
				'roomId', w.room_id,
				'batchId', w.batch_id
			),
			$4
		FROM jsonb_to_recordset($1::jsonb) AS w(
			slot_id text, room_id text, batch_id text, event_id text
		)
		ON CONFLICT (event_id) DO NOTHING
	`, wantJSON, in.TournamentID, in.RoundNumber, now,
		string(domain.FactTournamentMatchAssigned), in.CorrelationID, in.CommandID)
	if err != nil {
		return nil, 0, "", wrapUnavailable(err)
	}

	// Verify existing outbox rows preserve canonical columns + payload business identity.
	// Do not compare/reconstruct occurredAt; preserve the original accepted row.
	existRows, err := tx.Query(ctx, `
		SELECT o.event_id, o.event_type, o.tournament_id, o.topic, o.partition_key, o.schema_version, o.payload
		FROM jsonb_to_recordset($1::jsonb) AS w(event_id text, slot_id text, room_id text, batch_id text)
		INNER JOIN outbox_events o ON o.event_id = w.event_id
	`, wantJSON)
	if err != nil {
		return nil, 0, "", wrapUnavailable(err)
	}
	defer existRows.Close()
	wantByEvent := make(map[string]wantRow, len(wants))
	for _, w := range wants {
		wantByEvent[w.EventID] = w
	}
	for existRows.Next() {
		var row outboxImmutableRow
		if err := existRows.Scan(
			&row.EventID, &row.EventType, &row.TournamentID, &row.Topic, &row.PartitionKey, &row.SchemaVersion, &row.Payload,
		); err != nil {
			return nil, 0, "", wrapUnavailable(err)
		}
		w := wantByEvent[row.EventID]
		if !outboxImmutableFieldsMatch(row, w.EventID, string(domain.FactTournamentMatchAssigned),
			in.TournamentID, in.RoundNumber, w.SlotID, w.RoomID, w.BatchID) {
			return nil, 0, "outbox payload conflict", nil
		}
	}
	if err := existRows.Err(); err != nil {
		return nil, 0, "", wrapUnavailable(err)
	}

	// Progress shard increments only for newly inserted assignment rows.
	if newAssigned > 0 {
		_, err = tx.Exec(ctx, `
			WITH inserted AS (
				SELECT w.slot_index
				FROM jsonb_to_recordset($1::jsonb) AS w(slot_id text, slot_index int, room_id text)
				INNER JOIN assigned_matches a
				  ON a.tournament_id = $2 AND a.round_number = $3 AND a.slot_id = w.slot_id
				 AND a.room_id = w.room_id AND a.assigned_at = $4
			),
			incs AS (
				SELECT (slot_index % 64) AS shard_id, count(*)::int AS inc
				FROM inserted
				GROUP BY 1
			)
			INSERT INTO round_progress_shards (tournament_id, round_number, shard_id, assigned_count, resolved_count, quarantined_count)
			SELECT $2, $3, i.shard_id, i.inc, 0, 0
			FROM incs i
			ON CONFLICT (tournament_id, round_number, shard_id) DO UPDATE
			SET assigned_count = round_progress_shards.assigned_count + EXCLUDED.assigned_count
		`, wantJSON, in.TournamentID, in.RoundNumber, now)
		if err != nil {
			return nil, 0, "", wrapUnavailable(err)
		}
		if err := bumpProjectionVersionTx(ctx, tx, in.TournamentID, now); err != nil {
			return nil, 0, "", wrapUnavailable(err)
		}
		tag, err := tx.Exec(ctx, `
			UPDATE provisioning_batches
			SET prepared_at = COALESCE(prepared_at, $4), updated_at = $4
			WHERE tournament_id = $1 AND round_number = $2 AND batch_id = $3
			  AND status = 'in_progress'
			  AND lease_owner = $5
			  AND lease_version = $6
			  AND retry_attempt = $7
		`, in.TournamentID, in.RoundNumber, in.BatchID, now, in.LeaseOwner, in.LeaseVersion, in.RetryAttempt)
		if err != nil {
			return nil, 0, "", wrapUnavailable(err)
		}
		if tag.RowsAffected() != 1 {
			return nil, 0, "", ErrProvisioningFence
		}
	}

	return prepared, newAssigned, "", nil
}

func quarantinePrepareConflict(ctx context.Context, tx pgx.Tx, in PrepareProvisioningBatchInput, reason string, now time.Time) (*PrepareProvisioningBatchResult, error) {
	sanitized := sanitizeProvisioningError(reason)
	if sanitized == "" {
		sanitized = string(domain.RejectConflictingAssignment)
	}
	tag, err := tx.Exec(ctx, `
		UPDATE provisioning_batches
		SET status = 'quarantined',
		    quarantine_reason = COALESCE(quarantine_reason, $4),
		    last_error = $4,
		    lease_owner = NULL,
		    lease_expires_at = NULL,
		    lease_heartbeat_at = NULL,
		    updated_at = $5
		WHERE tournament_id = $1 AND round_number = $2 AND batch_id = $3
		  AND status = 'in_progress'
		  AND lease_owner = $6
		  AND lease_version = $7
		  AND retry_attempt = $8
	`, in.TournamentID, in.RoundNumber, in.BatchID, sanitized, now, in.LeaseOwner, in.LeaseVersion, in.RetryAttempt)
	if err != nil {
		return nil, wrapUnavailable(err)
	}
	if tag.RowsAffected() != 1 {
		return nil, ErrProvisioningFence
	}
	roundTag, err := tx.Exec(ctx, `
		UPDATE tournament_rounds
		SET status = 'blocked'
		WHERE tournament_id = $1 AND round_number = $2 AND status = 'provisioning'
	`, in.TournamentID, in.RoundNumber)
	if err != nil {
		return nil, wrapUnavailable(err)
	}
	if roundTag.RowsAffected() == 1 {
		if err := bumpProjectionVersionTx(ctx, tx, in.TournamentID, now); err != nil {
			return nil, wrapUnavailable(err)
		}
	}
	payload, _ := json.Marshal(map[string]any{"facts": []map[string]any{{
		"name": string(domain.FactTournamentProvisioningBatchQuarantined),
		"data": map[string]string{
			"tournamentId": in.TournamentID,
			"roundNumber":  strconv.Itoa(in.RoundNumber),
			"batchId":      in.BatchID,
			"reason":       sanitized,
		},
	}}})
	res := envelope.Accepted(in.CommandID, provisionAttemptCmdType, nil, payload)
	if err := insertCommandOutcomeWithTournament(ctx, tx, in.CommandID, in.TournamentID, provisionAttemptCmdType, res); err != nil {
		return nil, wrapUnavailable(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, wrapUnavailable(err)
	}
	return &PrepareProvisioningBatchResult{
		Quarantined:  true,
		RejectReason: sanitized,
		CommandID:    in.CommandID,
		TournamentID: in.TournamentID,
		RoundNumber:  in.RoundNumber,
		BatchID:      in.BatchID,
	}, nil
}

func rejectPrepare(ctx context.Context, tx pgx.Tx, in PrepareProvisioningBatchInput, reason string) (*PrepareProvisioningBatchResult, error) {
	res := envelope.Rejected(in.CommandID, provisionAttemptCmdType, reason, nil)
	if err := insertCommandOutcomeWithTournament(ctx, tx, in.CommandID, in.TournamentID, provisionAttemptCmdType, res); err != nil {
		return nil, wrapUnavailable(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, wrapUnavailable(err)
	}
	return &PrepareProvisioningBatchResult{Rejected: true, RejectReason: reason, CommandID: in.CommandID}, nil
}

func validatePrepareInput(in PrepareProvisioningBatchInput) error {
	if strings.TrimSpace(in.CommandID) == "" {
		return fmt.Errorf("commandId is required")
	}
	if strings.TrimSpace(in.TournamentID) == "" {
		return fmt.Errorf("tournamentId is required")
	}
	if in.RoundNumber < 1 {
		return fmt.Errorf("roundNumber is required")
	}
	if strings.TrimSpace(in.BatchID) == "" {
		return fmt.Errorf("batchId is required")
	}
	if strings.TrimSpace(in.LeaseOwner) == "" {
		return fmt.Errorf("leaseOwner is required")
	}
	if in.LeaseVersion < 1 {
		return fmt.Errorf("leaseVersion is required")
	}
	if strings.TrimSpace(in.SlotFrom) == "" || strings.TrimSpace(in.SlotTo) == "" {
		return fmt.Errorf("explicit slotFrom and slotTo are required")
	}
	if in.SlotSize <= 0 || in.SlotSize > domain.MaxProvisioningBatchSize {
		return fmt.Errorf("slotSize exceeds max provisioning batch size %d", domain.MaxProvisioningBatchSize)
	}
	return nil
}

func matchAssignedPayload(eventID, correlationID, causationID, tid string, roundNumber int, slotID, roomID, batchID string, at time.Time) map[string]any {
	return map[string]any{
		"schemaVersion": 1,
		"eventId":       eventID,
		"eventType":     string(domain.FactTournamentMatchAssigned),
		"correlationId": firstNonEmptyStr(correlationID, causationID),
		"causationId":   causationID,
		"occurredAt":    at.UTC().Format(time.RFC3339),
		"tournamentId":  tid,
		"roundNumber":   roundNumber,
		"slotId":        slotID,
		"roomId":        roomID,
		"batchId":       batchID,
	}
}

func firstNonEmptyStr(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

// outboxImmutableRow is the canonical outbox_events columns checked on conflict.
type outboxImmutableRow struct {
	EventID       string
	EventType     string
	TournamentID  *string
	Topic         string
	PartitionKey  string
	SchemaVersion int
	Payload       []byte
}

// outboxImmutableFieldsMatch verifies canonical row columns and payload schema/business
// identity. occurredAt is intentionally not compared so the original accepted row is preserved.
func outboxImmutableFieldsMatch(row outboxImmutableRow, eventID, eventType, tid string, roundNumber int, slotID, roomID, batchID string) bool {
	if row.EventID != eventID {
		return false
	}
	if row.EventType != eventType {
		return false
	}
	if row.TournamentID == nil || *row.TournamentID != tid {
		return false
	}
	if row.Topic != "tournament.match.assigned" {
		return false
	}
	if row.PartitionKey != tid {
		return false
	}
	if row.SchemaVersion != 1 {
		return false
	}
	var m map[string]any
	if json.Unmarshal(row.Payload, &m) != nil {
		return false
	}
	if intFieldAny(m["schemaVersion"]) != 1 {
		return false
	}
	if strField(m, "eventId") != eventID {
		return false
	}
	if strField(m, "eventType") != eventType {
		return false
	}
	if strField(m, "tournamentId") != tid {
		return false
	}
	if intFieldAny(m["roundNumber"]) != roundNumber {
		return false
	}
	if strField(m, "slotId") != slotID {
		return false
	}
	if strField(m, "roomId") != roomID {
		return false
	}
	if strField(m, "batchId") != batchID {
		return false
	}
	return true
}

func strField(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}

func intFieldAny(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	default:
		return 0
	}
}

// HeartbeatProvisioningLease renews lease_expires_at for the owning fenced worker.
// Exact fence: status=in_progress + owner + lease_version + retry_attempt.
func (s *TournamentStore) HeartbeatProvisioningLease(ctx context.Context, tid string, roundNumber int, batchID, owner string, leaseVersion int64, retryAttempt int, now time.Time, ttl time.Duration) (bool, error) {
	if s == nil || s.pool == nil {
		return false, fmt.Errorf("nil store")
	}
	if owner == "" {
		return false, fmt.Errorf("lease owner required")
	}
	if leaseVersion < 1 {
		return false, fmt.Errorf("lease version required")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	if ttl <= 0 {
		ttl = DefaultProvisioningLease
	}
	expires := now.Add(ttl)
	tag, err := s.pool.Exec(ctx, `
		UPDATE provisioning_batches
		SET lease_expires_at = $5,
		    lease_heartbeat_at = $4,
		    updated_at = $4
		WHERE tournament_id = $1 AND round_number = $2 AND batch_id = $3
		  AND status = 'in_progress'
		  AND lease_owner = $6
		  AND lease_version = $7
		  AND retry_attempt = $8
	`, tid, roundNumber, batchID, now, expires, owner, leaseVersion, retryAttempt)
	if err != nil {
		return false, wrapUnavailable(err)
	}
	return tag.RowsAffected() == 1, nil
}

// FinalizeProvisioningBatchSuccess marks batch completed and optionally transitions the round.
func (s *TournamentStore) FinalizeProvisioningBatchSuccess(ctx context.Context, in FinalizeProvisioningBatchInput) (envelope.Result, error) {
	if s == nil || s.pool == nil {
		return envelope.Result{}, fmt.Errorf("nil store")
	}
	now := time.Now().UTC()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return envelope.Result{}, wrapUnavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := acquireRewriteBarrierShared(ctx, tx, in.TournamentID); err != nil {
		return envelope.Result{}, wrapUnavailable(err)
	}
	if err := AcquireCommandLock(ctx, tx, in.CommandID); err != nil {
		return envelope.Result{}, wrapUnavailable(err)
	}

	var priorBody []byte
	err = tx.QueryRow(ctx, `
		SELECT outcome_body FROM command_idempotency WHERE command_id = $1
	`, in.CommandID).Scan(&priorBody)
	if err == nil {
		var prior envelope.Result
		_ = json.Unmarshal(priorBody, &prior)
		if err := tx.Commit(ctx); err != nil {
			return envelope.Result{}, wrapUnavailable(err)
		}
		return prior, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return envelope.Result{}, wrapUnavailable(err)
	}

	var batchStatus, leaseOwner, slotFrom, slotTo string
	var retryAttempt int
	var leaseVersion int64
	err = tx.QueryRow(ctx, `
		SELECT status, retry_attempt, COALESCE(lease_owner, ''), lease_version,
		       slot_id_from, slot_id_to
		FROM provisioning_batches
		WHERE tournament_id = $1 AND round_number = $2 AND batch_id = $3
		FOR UPDATE
	`, in.TournamentID, in.RoundNumber, in.BatchID).Scan(
		&batchStatus, &retryAttempt, &leaseOwner, &leaseVersion, &slotFrom, &slotTo,
	)
	if err != nil {
		return envelope.Result{}, wrapUnavailable(err)
	}
	// Stale worker after another claimant completed: fence, do not insert a new outcome.
	if batchStatus == string(domain.BatchCompleted) || batchStatus == string(domain.BatchQuarantined) {
		return envelope.Result{}, ErrProvisioningFence
	}
	if batchStatus != string(domain.BatchInProgress) ||
		leaseOwner != in.LeaseOwner ||
		leaseVersion != in.LeaseVersion ||
		retryAttempt != in.RetryAttempt {
		return envelope.Result{}, ErrProvisioningFence
	}
	if slotFrom != in.SlotFrom || slotTo != in.SlotTo {
		return envelope.Result{}, fmt.Errorf("slot range mismatch")
	}

	var assignedCount int
	err = tx.QueryRow(ctx, `
		SELECT count(*)::int FROM assigned_matches
		WHERE tournament_id = $1 AND round_number = $2 AND provisioning_batch_id = $3
	`, in.TournamentID, in.RoundNumber, in.BatchID).Scan(&assignedCount)
	if err != nil {
		return envelope.Result{}, wrapUnavailable(err)
	}
	if assignedCount != in.SlotSize {
		return envelope.Result{}, fmt.Errorf("assignment count mismatch: have %d want %d", assignedCount, in.SlotSize)
	}

	tag, err := tx.Exec(ctx, `
		UPDATE provisioning_batches
		SET status = 'completed',
		    last_error = NULL,
		    lease_owner = NULL,
		    lease_expires_at = NULL,
		    lease_heartbeat_at = NULL,
		    completed_at = $4,
		    updated_at = $4
		WHERE tournament_id = $1 AND round_number = $2 AND batch_id = $3
		  AND status = 'in_progress'
		  AND lease_owner = $5
		  AND lease_version = $6
		  AND retry_attempt = $7
	`, in.TournamentID, in.RoundNumber, in.BatchID, now, in.LeaseOwner, in.LeaseVersion, in.RetryAttempt)
	if err != nil {
		return envelope.Result{}, wrapUnavailable(err)
	}
	if tag.RowsAffected() != 1 {
		return envelope.Result{}, ErrProvisioningFence
	}

	var incomplete int
	err = tx.QueryRow(ctx, `
		SELECT count(*)::int FROM provisioning_batches
		WHERE tournament_id = $1 AND round_number = $2 AND status <> 'completed'
	`, in.TournamentID, in.RoundNumber).Scan(&incomplete)
	if err != nil {
		return envelope.Result{}, wrapUnavailable(err)
	}

	facts := []map[string]any{{
		"name": string(domain.FactTournamentProvisioningBatchCompleted),
		"data": map[string]string{
			"tournamentId": in.TournamentID,
			"roundNumber":  strconv.Itoa(in.RoundNumber),
			"batchId":      in.BatchID,
		},
	}}
	if incomplete == 0 {
		roundTag, err := tx.Exec(ctx, `
			UPDATE tournament_rounds
			SET status = 'in_progress'
			WHERE tournament_id = $1 AND round_number = $2 AND status = 'provisioning'
		`, in.TournamentID, in.RoundNumber)
		if err != nil {
			return envelope.Result{}, wrapUnavailable(err)
		}
		if roundTag.RowsAffected() == 1 {
			facts[0]["data"].(map[string]string)[domain.FactDataPublicBracketVisible] = "true"
			if err := bumpProjectionVersionTx(ctx, tx, in.TournamentID, now); err != nil {
				return envelope.Result{}, wrapUnavailable(err)
			}
		}
	}

	payload, _ := json.Marshal(map[string]any{"facts": facts})
	res := envelope.Accepted(in.CommandID, provisionAttemptCmdType, nil, payload)
	if err := insertCommandOutcomeWithTournament(ctx, tx, in.CommandID, in.TournamentID, provisionAttemptCmdType, res); err != nil {
		return envelope.Result{}, wrapUnavailable(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return envelope.Result{}, wrapUnavailable(err)
	}
	return res, nil
}

// FinalizeProvisioningBatchFailure stores sanitized error, retries or quarantines.
func (s *TournamentStore) FinalizeProvisioningBatchFailure(ctx context.Context, in FinalizeProvisioningBatchInput) (envelope.Result, error) {
	if s == nil || s.pool == nil {
		return envelope.Result{}, fmt.Errorf("nil store")
	}
	now := time.Now().UTC()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return envelope.Result{}, wrapUnavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := acquireRewriteBarrierShared(ctx, tx, in.TournamentID); err != nil {
		return envelope.Result{}, wrapUnavailable(err)
	}
	if err := AcquireCommandLock(ctx, tx, in.CommandID); err != nil {
		return envelope.Result{}, wrapUnavailable(err)
	}

	var priorBody []byte
	err = tx.QueryRow(ctx, `
		SELECT outcome_body FROM command_idempotency WHERE command_id = $1
	`, in.CommandID).Scan(&priorBody)
	if err == nil {
		var prior envelope.Result
		_ = json.Unmarshal(priorBody, &prior)
		if err := tx.Commit(ctx); err != nil {
			return envelope.Result{}, wrapUnavailable(err)
		}
		return prior, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return envelope.Result{}, wrapUnavailable(err)
	}

	var batchStatus, leaseOwner string
	var retryAttempt int
	var leaseVersion int64
	err = tx.QueryRow(ctx, `
		SELECT status, retry_attempt, COALESCE(lease_owner, ''), lease_version
		FROM provisioning_batches
		WHERE tournament_id = $1 AND round_number = $2 AND batch_id = $3
		FOR UPDATE
	`, in.TournamentID, in.RoundNumber, in.BatchID).Scan(&batchStatus, &retryAttempt, &leaseOwner, &leaseVersion)
	if err != nil {
		return envelope.Result{}, wrapUnavailable(err)
	}
	// Stale worker after another claimant completed/quarantined: fence, no outcome poison.
	if batchStatus == string(domain.BatchCompleted) || batchStatus == string(domain.BatchQuarantined) {
		return envelope.Result{}, ErrProvisioningFence
	}
	if batchStatus != string(domain.BatchInProgress) ||
		leaseOwner != in.LeaseOwner ||
		leaseVersion != in.LeaseVersion ||
		retryAttempt != in.RetryAttempt {
		return envelope.Result{}, ErrProvisioningFence
	}

	sanitized := sanitizeProvisioningError(in.LastError)
	nextAttempt := retryAttempt + 1
	budget := in.RetryBudget
	if budget <= 0 {
		budget = domain.DefaultRetryBudget
	}

	var facts []map[string]any
	payloadExtra := map[string]any{}

	quarantine := in.ForceQuarantine || nextAttempt > budget
	if !quarantine {
		tag, err := tx.Exec(ctx, `
			UPDATE provisioning_batches
			SET status = 'retried',
			    retry_attempt = $4,
			    last_error = $5,
			    lease_owner = NULL,
			    lease_expires_at = NULL,
			    lease_heartbeat_at = NULL,
			    updated_at = $6
			WHERE tournament_id = $1 AND round_number = $2 AND batch_id = $3
			  AND status = 'in_progress'
			  AND lease_owner = $7
			  AND lease_version = $8
			  AND retry_attempt = $9
		`, in.TournamentID, in.RoundNumber, in.BatchID, nextAttempt, sanitized, now,
			in.LeaseOwner, in.LeaseVersion, in.RetryAttempt)
		if err != nil {
			return envelope.Result{}, wrapUnavailable(err)
		}
		if tag.RowsAffected() != 1 {
			return envelope.Result{}, ErrProvisioningFence
		}
		facts = []map[string]any{{
			"name": string(domain.FactTournamentProvisioningBatchRetried),
			"data": map[string]string{
				"tournamentId": in.TournamentID,
				"roundNumber":  strconv.Itoa(in.RoundNumber),
				"batchId":      in.BatchID,
				"retryAttempt": strconv.Itoa(nextAttempt),
			},
		}}
		if in.NextRetryWork != nil {
			payloadExtra["nextRetryWork"] = in.NextRetryWork
		}
	} else {
		tag, err := tx.Exec(ctx, `
			UPDATE provisioning_batches
			SET status = 'quarantined',
			    retry_attempt = $4,
			    last_error = $5,
			    quarantine_reason = COALESCE(quarantine_reason, $5),
			    lease_owner = NULL,
			    lease_expires_at = NULL,
			    lease_heartbeat_at = NULL,
			    updated_at = $6
			WHERE tournament_id = $1 AND round_number = $2 AND batch_id = $3
			  AND status = 'in_progress'
			  AND lease_owner = $7
			  AND lease_version = $8
			  AND retry_attempt = $9
		`, in.TournamentID, in.RoundNumber, in.BatchID, nextAttempt, sanitized, now,
			in.LeaseOwner, in.LeaseVersion, in.RetryAttempt)
		if err != nil {
			return envelope.Result{}, wrapUnavailable(err)
		}
		if tag.RowsAffected() != 1 {
			return envelope.Result{}, ErrProvisioningFence
		}
		roundTag, err := tx.Exec(ctx, `
			UPDATE tournament_rounds
			SET status = 'blocked'
			WHERE tournament_id = $1 AND round_number = $2 AND status = 'provisioning'
		`, in.TournamentID, in.RoundNumber)
		if err != nil {
			return envelope.Result{}, wrapUnavailable(err)
		}
		if roundTag.RowsAffected() == 1 {
			if err := bumpProjectionVersionTx(ctx, tx, in.TournamentID, now); err != nil {
				return envelope.Result{}, wrapUnavailable(err)
			}
		}
		facts = []map[string]any{{
			"name": string(domain.FactTournamentProvisioningBatchQuarantined),
			"data": map[string]string{
				"tournamentId": in.TournamentID,
				"roundNumber":  strconv.Itoa(in.RoundNumber),
				"batchId":      in.BatchID,
				"reason":       sanitized,
			},
		}}
	}

	payloadMap := map[string]any{"facts": facts}
	for k, v := range payloadExtra {
		payloadMap[k] = v
	}
	payload, _ := json.Marshal(payloadMap)
	res := envelope.Accepted(in.CommandID, provisionAttemptCmdType, nil, payload)
	if err := insertCommandOutcomeWithTournament(ctx, tx, in.CommandID, in.TournamentID, provisionAttemptCmdType, res); err != nil {
		return envelope.Result{}, wrapUnavailable(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return envelope.Result{}, wrapUnavailable(err)
	}
	return res, nil
}

// ManualRetryProvisioningBatch is the differential path for RetryTournamentProvisioningBatch.
func (s *TournamentStore) ManualRetryProvisioningBatch(ctx context.Context, commandID, tid string, roundNumber int, batchID string, retryAttempt int, correlationID string) (envelope.Result, error) {
	if strings.TrimSpace(commandID) == "" || strings.TrimSpace(tid) == "" ||
		roundNumber < 1 || strings.TrimSpace(batchID) == "" {
		return envelope.Result{}, fmt.Errorf("manual retry requires commandId, tournamentId, roundNumber, batchId")
	}
	now := time.Now().UTC()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return envelope.Result{}, wrapUnavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := acquireRewriteBarrierShared(ctx, tx, tid); err != nil {
		return envelope.Result{}, wrapUnavailable(err)
	}
	if err := AcquireCommandLock(ctx, tx, commandID); err != nil {
		return envelope.Result{}, wrapUnavailable(err)
	}
	var priorBody []byte
	err = tx.QueryRow(ctx, `SELECT outcome_body FROM command_idempotency WHERE command_id = $1`, commandID).Scan(&priorBody)
	if err == nil {
		var prior envelope.Result
		_ = json.Unmarshal(priorBody, &prior)
		if err := tx.Commit(ctx); err != nil {
			return envelope.Result{}, wrapUnavailable(err)
		}
		return prior, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return envelope.Result{}, wrapUnavailable(err)
	}

	var status string
	var curAttempt int
	var rulesRaw []byte
	err = tx.QueryRow(ctx, `
		SELECT b.status, b.retry_attempt, t.rules
		FROM provisioning_batches b
		INNER JOIN tournaments t ON t.tournament_id = b.tournament_id
		WHERE b.tournament_id = $1 AND b.round_number = $2 AND b.batch_id = $3
		FOR UPDATE OF b
	`, tid, roundNumber, batchID).Scan(&status, &curAttempt, &rulesRaw)
	if errors.Is(err, pgx.ErrNoRows) {
		res := envelope.Rejected(commandID, "RetryTournamentProvisioningBatch", string(domain.RejectBatchNotFound), nil)
		if err := insertCommandOutcomeWithTournament(ctx, tx, commandID, tid, "RetryTournamentProvisioningBatch", res); err != nil {
			return envelope.Result{}, wrapUnavailable(err)
		}
		if err := tx.Commit(ctx); err != nil {
			return envelope.Result{}, wrapUnavailable(err)
		}
		return res, nil
	}
	if err != nil {
		return envelope.Result{}, wrapUnavailable(err)
	}
	var rules tournamentRules
	_ = json.Unmarshal(rulesRaw, &rules)
	budget := rules.RetryBudget
	if budget <= 0 {
		budget = domain.DefaultRetryBudget
	}

	// Already quarantined: exact factless no-op (no mutation / fact / projection bump).
	if status == string(domain.BatchQuarantined) {
		res := envelope.Accepted(commandID, "RetryTournamentProvisioningBatch", nil, json.RawMessage(`{"facts":[]}`))
		if err := insertCommandOutcomeWithTournament(ctx, tx, commandID, tid, "RetryTournamentProvisioningBatch", res); err != nil {
			return envelope.Result{}, wrapUnavailable(err)
		}
		if err := tx.Commit(ctx); err != nil {
			return envelope.Result{}, wrapUnavailable(err)
		}
		return res, nil
	}

	// Strict sequencing: explicit retryAttempt may be current+1; status=retried +
	// explicit == current is an exact no-op; lower/skip reject; zero keeps auto-next.
	if retryAttempt > 0 && status == string(domain.BatchRetried) && retryAttempt == curAttempt {
		res := envelope.Accepted(commandID, "RetryTournamentProvisioningBatch", nil, json.RawMessage(`{"facts":[]}`))
		if err := insertCommandOutcomeWithTournament(ctx, tx, commandID, tid, "RetryTournamentProvisioningBatch", res); err != nil {
			return envelope.Result{}, wrapUnavailable(err)
		}
		if err := tx.Commit(ctx); err != nil {
			return envelope.Result{}, wrapUnavailable(err)
		}
		return res, nil
	}

	nextAttempt := curAttempt + 1
	if retryAttempt > 0 && retryAttempt != nextAttempt {
		res := envelope.Rejected(commandID, "RetryTournamentProvisioningBatch", string(domain.RejectInvalidCommand), nil)
		if err := insertCommandOutcomeWithTournament(ctx, tx, commandID, tid, "RetryTournamentProvisioningBatch", res); err != nil {
			return envelope.Result{}, wrapUnavailable(err)
		}
		if err := tx.Commit(ctx); err != nil {
			return envelope.Result{}, wrapUnavailable(err)
		}
		return res, nil
	}

	if nextAttempt > budget {
		tag, err := tx.Exec(ctx, `
			UPDATE provisioning_batches
			SET status = 'quarantined',
			    quarantine_reason = COALESCE(quarantine_reason, 'retry_budget_exhausted'),
			    lease_owner = NULL, lease_expires_at = NULL, lease_heartbeat_at = NULL,
			    updated_at = $4
			WHERE tournament_id = $1 AND round_number = $2 AND batch_id = $3
			  AND status <> 'quarantined'
		`, tid, roundNumber, batchID, now)
		if err != nil {
			return envelope.Result{}, wrapUnavailable(err)
		}
		if tag.RowsAffected() != 1 {
			res := envelope.Rejected(commandID, "RetryTournamentProvisioningBatch", string(domain.RejectUnexpectedBatchStatus), nil)
			if err := insertCommandOutcomeWithTournament(ctx, tx, commandID, tid, "RetryTournamentProvisioningBatch", res); err != nil {
				return envelope.Result{}, wrapUnavailable(err)
			}
			if err := tx.Commit(ctx); err != nil {
				return envelope.Result{}, wrapUnavailable(err)
			}
			return res, nil
		}
		// Projection bumps only when the round actually transitions to blocked —
		// parity with ManualQuarantine / domain: any nonterminal actionable status.
		roundTag, err := tx.Exec(ctx, `
			UPDATE tournament_rounds SET status = 'blocked'
			WHERE tournament_id = $1 AND round_number = $2 AND status IN ('provisioning', 'in_progress', 'seeded')
		`, tid, roundNumber)
		if err != nil {
			return envelope.Result{}, wrapUnavailable(err)
		}
		if roundTag.RowsAffected() == 1 {
			if err := bumpProjectionVersionTx(ctx, tx, tid, now); err != nil {
				return envelope.Result{}, wrapUnavailable(err)
			}
		}
		payload, _ := json.Marshal(map[string]any{"facts": []map[string]any{{
			"name": string(domain.FactTournamentProvisioningBatchQuarantined),
			"data": map[string]string{
				"tournamentId": tid,
				"roundNumber":  strconv.Itoa(roundNumber),
				"batchId":      batchID,
				"reason":       "retry_budget_exhausted",
				"retryAttempt": strconv.Itoa(nextAttempt),
			},
		}}})
		res := envelope.Accepted(commandID, "RetryTournamentProvisioningBatch", nil, payload)
		if err := insertCommandOutcomeWithTournament(ctx, tx, commandID, tid, "RetryTournamentProvisioningBatch", res); err != nil {
			return envelope.Result{}, wrapUnavailable(err)
		}
		if err := tx.Commit(ctx); err != nil {
			return envelope.Result{}, wrapUnavailable(err)
		}
		return res, nil
	}
	tag, err := tx.Exec(ctx, `
		UPDATE provisioning_batches
		SET status = 'retried', retry_attempt = $4,
		    lease_owner = NULL, lease_expires_at = NULL, lease_heartbeat_at = NULL,
		    updated_at = $5
		WHERE tournament_id = $1 AND round_number = $2 AND batch_id = $3
		  AND status IN ('pending', 'retried', 'in_progress')
	`, tid, roundNumber, batchID, nextAttempt, now)
	if err != nil {
		return envelope.Result{}, wrapUnavailable(err)
	}
	if tag.RowsAffected() != 1 {
		res := envelope.Rejected(commandID, "RetryTournamentProvisioningBatch", string(domain.RejectUnexpectedBatchStatus), nil)
		if err := insertCommandOutcomeWithTournament(ctx, tx, commandID, tid, "RetryTournamentProvisioningBatch", res); err != nil {
			return envelope.Result{}, wrapUnavailable(err)
		}
		if err := tx.Commit(ctx); err != nil {
			return envelope.Result{}, wrapUnavailable(err)
		}
		return res, nil
	}
	// Retry never bumps projection: batch status is BracketPage-hidden.
	payload, _ := json.Marshal(map[string]any{"facts": []map[string]any{{
		"name": string(domain.FactTournamentProvisioningBatchRetried),
		"data": map[string]string{
			"tournamentId": tid,
			"roundNumber":  strconv.Itoa(roundNumber),
			"batchId":      batchID,
			"retryAttempt": strconv.Itoa(nextAttempt),
		},
	}}})
	res := envelope.Accepted(commandID, "RetryTournamentProvisioningBatch", nil, payload)
	if err := insertCommandOutcomeWithTournament(ctx, tx, commandID, tid, "RetryTournamentProvisioningBatch", res); err != nil {
		return envelope.Result{}, wrapUnavailable(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return envelope.Result{}, wrapUnavailable(err)
	}
	return res, nil
}

// ManualQuarantineProvisioningBatch is the differential path for QuarantineTournamentProvisioningBatch.
func (s *TournamentStore) ManualQuarantineProvisioningBatch(ctx context.Context, commandID, tid string, roundNumber int, batchID, reason, correlationID string) (envelope.Result, error) {
	if strings.TrimSpace(commandID) == "" || strings.TrimSpace(tid) == "" ||
		roundNumber < 1 || strings.TrimSpace(batchID) == "" {
		return envelope.Result{}, fmt.Errorf("manual quarantine requires commandId, tournamentId, roundNumber, batchId")
	}
	now := time.Now().UTC()
	sanitized := sanitizeProvisioningError(reason)
	if sanitized == "" {
		sanitized = "quarantined"
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return envelope.Result{}, wrapUnavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := acquireRewriteBarrierShared(ctx, tx, tid); err != nil {
		return envelope.Result{}, wrapUnavailable(err)
	}
	if err := AcquireCommandLock(ctx, tx, commandID); err != nil {
		return envelope.Result{}, wrapUnavailable(err)
	}
	var priorBody []byte
	err = tx.QueryRow(ctx, `SELECT outcome_body FROM command_idempotency WHERE command_id = $1`, commandID).Scan(&priorBody)
	if err == nil {
		var prior envelope.Result
		_ = json.Unmarshal(priorBody, &prior)
		if err := tx.Commit(ctx); err != nil {
			return envelope.Result{}, wrapUnavailable(err)
		}
		return prior, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return envelope.Result{}, wrapUnavailable(err)
	}
	var status string
	err = tx.QueryRow(ctx, `
		SELECT status FROM provisioning_batches
		WHERE tournament_id = $1 AND round_number = $2 AND batch_id = $3
		FOR UPDATE
	`, tid, roundNumber, batchID).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		res := envelope.Rejected(commandID, "QuarantineTournamentProvisioningBatch", string(domain.RejectBatchNotFound), nil)
		if err := insertCommandOutcomeWithTournament(ctx, tx, commandID, tid, "QuarantineTournamentProvisioningBatch", res); err != nil {
			return envelope.Result{}, wrapUnavailable(err)
		}
		if err := tx.Commit(ctx); err != nil {
			return envelope.Result{}, wrapUnavailable(err)
		}
		return res, nil
	}
	if err != nil {
		return envelope.Result{}, wrapUnavailable(err)
	}
	if status == string(domain.BatchQuarantined) {
		// Exact no-op — do not bump projection.
		res := envelope.Accepted(commandID, "QuarantineTournamentProvisioningBatch", nil, json.RawMessage(`{"facts":[]}`))
		if err := insertCommandOutcomeWithTournament(ctx, tx, commandID, tid, "QuarantineTournamentProvisioningBatch", res); err != nil {
			return envelope.Result{}, wrapUnavailable(err)
		}
		if err := tx.Commit(ctx); err != nil {
			return envelope.Result{}, wrapUnavailable(err)
		}
		return res, nil
	}
	tag, err := tx.Exec(ctx, `
		UPDATE provisioning_batches
		SET status = 'quarantined',
		    quarantine_reason = COALESCE(quarantine_reason, $4),
		    lease_owner = NULL, lease_expires_at = NULL, lease_heartbeat_at = NULL,
		    updated_at = $5
		WHERE tournament_id = $1 AND round_number = $2 AND batch_id = $3
		  AND status <> 'quarantined'
	`, tid, roundNumber, batchID, sanitized, now)
	if err != nil {
		return envelope.Result{}, wrapUnavailable(err)
	}
	if tag.RowsAffected() != 1 {
		res := envelope.Rejected(commandID, "QuarantineTournamentProvisioningBatch", string(domain.RejectUnexpectedBatchStatus), nil)
		if err := insertCommandOutcomeWithTournament(ctx, tx, commandID, tid, "QuarantineTournamentProvisioningBatch", res); err != nil {
			return envelope.Result{}, wrapUnavailable(err)
		}
		if err := tx.Commit(ctx); err != nil {
			return envelope.Result{}, wrapUnavailable(err)
		}
		return res, nil
	}
	// Projection bumps only when the round actually transitions to blocked —
	// not merely because a batch row changed while the round was already blocked.
	roundTag, err := tx.Exec(ctx, `
		UPDATE tournament_rounds SET status = 'blocked'
		WHERE tournament_id = $1 AND round_number = $2 AND status IN ('provisioning', 'in_progress', 'seeded')
	`, tid, roundNumber)
	if err != nil {
		return envelope.Result{}, wrapUnavailable(err)
	}
	if roundTag.RowsAffected() == 1 {
		if err := bumpProjectionVersionTx(ctx, tx, tid, now); err != nil {
			return envelope.Result{}, wrapUnavailable(err)
		}
	}
	payload, _ := json.Marshal(map[string]any{"facts": []map[string]any{{
		"name": string(domain.FactTournamentProvisioningBatchQuarantined),
		"data": map[string]string{
			"tournamentId": tid,
			"roundNumber":  strconv.Itoa(roundNumber),
			"batchId":      batchID,
			"reason":       sanitized,
		},
	}}})
	res := envelope.Accepted(commandID, "QuarantineTournamentProvisioningBatch", nil, payload)
	if err := insertCommandOutcomeWithTournament(ctx, tx, commandID, tid, "QuarantineTournamentProvisioningBatch", res); err != nil {
		return envelope.Result{}, wrapUnavailable(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return envelope.Result{}, wrapUnavailable(err)
	}
	return res, nil
}

// sanitizeProvisioningError keeps only a short class/code; never raw HTTP bodies or secrets.
func sanitizeProvisioningError(raw string) string {
	return domain.SanitizeProvisioningReason(raw)
}

// LoadRetryBudget reads tournaments.rules.retryBudget for failure finalize.
func (s *TournamentStore) LoadRetryBudget(ctx context.Context, tournamentID string) (int, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("nil store")
	}
	var rulesRaw []byte
	err := s.pool.QueryRow(ctx, `SELECT rules FROM tournaments WHERE tournament_id = $1`, tournamentID).Scan(&rulesRaw)
	if err != nil {
		return 0, wrapUnavailable(err)
	}
	var rules tournamentRules
	_ = json.Unmarshal(rulesRaw, &rules)
	if rules.RetryBudget <= 0 {
		return domain.DefaultRetryBudget, nil
	}
	return rules.RetryBudget, nil
}
