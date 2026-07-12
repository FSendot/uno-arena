package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/shared/envelope"
)

// Differential MatchCompleted / HTTP RecordMatchResult lock order (all paths must match):
//  1. Shared rewrite barrier (tournament:rewrite:{id})
//  2. Global command lock (AcquireCommandLock) when commandID nonempty
//  3. Resolve assignment identity by room_id (bounded reads under barrier; no row locks yet)
//  4. round_progress_shards FOR UPDATE (one shard: slot_index % 64) — only when assignment resolved
//  5. bracket_slots FOR UPDATE — only when assignment resolved
//  6. advancement_records point row when present (Begin load; still under slot)
//  7. Business-key advisory lock for (roomId, completionVersion) — AttachPriorResult
//  8. match_results point row FOR UPDATE (if present) / insert path
//  9. tournament_rounds FOR UPDATE only on rare block transition (never on successful record)
// 10. bracket_projection_shards FOR UPDATE (one shard) when ProjectionChanged
//
// Known room after Begin+Attach: slot → business → exact result/ledger.
// Unknown room AttachPriorResult: business → exact result/ledger (no slot).
// Standalone invalid/outcome-only: command lock only (no barrier/tournament).
//
// Never: tournaments FOR UPDATE, whole-tournament hydrate, full-aggregate rewrite, or subtree DELETE.
// Never lock a claimed slot for an unknown room (would mutate another player's slot).
// Never: business lock before slot/shard (deadlocks QuarantineTournamentResult).

// RoundMatchCommitRequest is the bounded differential commit unit.
type RoundMatchCommitRequest struct {
	TournamentID      string
	CommandID         string
	CommandType       string
	Outcome           envelope.Result
	Events            []OutboxEvent
	Decision          domain.RoundMatchDecision
	Command           domain.RecordMatchResultCommand
	MatchResultSource *MatchResultSource
	ProjectionChanged bool
}

// RoundMatchUnitOfWork holds one READ COMMITTED tx for bounded MatchCompleted apply.
type RoundMatchUnitOfWork struct {
	store     *TournamentStore
	ctx       context.Context
	tx        pgx.Tx
	tid       string
	roomID    string
	commandID string
	loaded    domain.RoundMatchContext
	exists    bool // tournament present
	done      bool
}

// BeginStandaloneRoundMatchCommand locks only the global command id for invalid/outcome-only rejects.
func (s *TournamentStore) BeginStandaloneRoundMatchCommand(ctx context.Context, commandID string) (*RoundMatchUnitOfWork, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("nil store")
	}
	if commandID == "" {
		return nil, fmt.Errorf("commandId required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, wrapUnavailable(err)
	}
	uow := &RoundMatchUnitOfWork{store: s, ctx: ctx, tx: tx, commandID: commandID}
	if err := AcquireCommandLock(ctx, tx, commandID); err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	return uow, nil
}

// BeginRoundMatch starts a differential UoW for one room's MatchCompleted / HTTP RecordMatchResult.
// Metadata round/slot from the event are hints only; assignment is resolved from assigned_matches.
// Unknown rooms never lock a claimed slot.
func (s *TournamentStore) BeginRoundMatch(ctx context.Context, tournamentID, roomID string, hintRound int, hintSlot string, commandID string) (*RoundMatchUnitOfWork, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("nil store")
	}
	if tournamentID == "" || roomID == "" {
		return nil, fmt.Errorf("tournamentId and roomId required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, wrapUnavailable(err)
	}
	uow := &RoundMatchUnitOfWork{store: s, ctx: ctx, tx: tx, tid: tournamentID, roomID: roomID, commandID: commandID}
	if err := acquireRewriteBarrierShared(ctx, tx, tournamentID); err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	if commandID != "" {
		if err := AcquireCommandLock(ctx, tx, commandID); err != nil {
			_ = tx.Rollback(ctx)
			return nil, wrapUnavailable(err)
		}
	}

	var phase string
	err = tx.QueryRow(ctx, `SELECT phase FROM tournaments WHERE tournament_id = $1`, tournamentID).Scan(&phase)
	if errors.Is(err, pgx.ErrNoRows) {
		uow.exists = false
		uow.loaded = domain.RoundMatchContext{TournamentID: domain.TournamentID(tournamentID)}
		return uow, nil
	}
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}

	var (
		roundNumber  int
		slotID       string
		slotIndex    int
		slotStatus   string
		roomAssigned string
		isFinal      bool
		roundStatus  string
	)
	err = tx.QueryRow(ctx, `
		SELECT a.round_number, a.slot_id, s.slot_index, s.status, a.room_id, r.is_final, r.status
		FROM assigned_matches a
		JOIN bracket_slots s
		  ON s.tournament_id = a.tournament_id AND s.round_number = a.round_number AND s.slot_id = a.slot_id
		JOIN tournament_rounds r
		  ON r.tournament_id = a.tournament_id AND r.round_number = a.round_number
		WHERE a.tournament_id = $1 AND a.room_id = $2
	`, tournamentID, roomID).Scan(&roundNumber, &slotID, &slotIndex, &slotStatus, &roomAssigned, &isFinal, &roundStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		// Unknown room: keep hints for quarantine metadata only. Do NOT lock claimed slots.
		uow.exists = true
		uow.loaded = domain.RoundMatchContext{
			TournamentID: domain.TournamentID(tournamentID),
			Phase:        domain.TournamentPhase(phase),
			RoundNumber:  hintRound,
		}
		if hintRound >= 1 {
			var rf bool
			var rs string
			err2 := tx.QueryRow(ctx, `
				SELECT is_final, status FROM tournament_rounds
				WHERE tournament_id = $1 AND round_number = $2
			`, tournamentID, hintRound).Scan(&rf, &rs)
			if err2 == nil {
				uow.loaded.RoundFound = true
				uow.loaded.IsFinal = rf
				uow.loaded.RoundStatus = domain.RoundStatus(rs)
			}
			if hintSlot != "" {
				var sIdx int
				var sStatus string
				var assignedRoom *string
				err3 := tx.QueryRow(ctx, `
					SELECT s.slot_index, s.status, a.room_id
					FROM bracket_slots s
					LEFT JOIN assigned_matches a
					  ON a.tournament_id = s.tournament_id
					 AND a.round_number = s.round_number
					 AND a.slot_id = s.slot_id
					WHERE s.tournament_id = $1 AND s.round_number = $2 AND s.slot_id = $3
				`, tournamentID, hintRound, hintSlot).Scan(&sIdx, &sStatus, &assignedRoom)
				if err3 == nil {
					uow.loaded.SlotFound = true
					slotState := domain.RoundMatchSlotState{
						SlotID:             domain.SlotID(hintSlot),
						SlotIndex:          sIdx,
						Status:             domain.SlotStatus(sStatus),
						AssignmentResolved: false, // claimed room is not the assignee
					}
					if assignedRoom != nil {
						slotState.RoomID = domain.RoomID(*assignedRoom)
					}
					uow.loaded.Slot = slotState
				}
			}
		}
		return uow, nil
	}
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}

	if err := uow.lockShardAndSlot(slotIndex, roundNumber, slotID); err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}

	// Re-read slot under lock.
	err = tx.QueryRow(ctx, `
		SELECT status, slot_index FROM bracket_slots
		WHERE tournament_id = $1 AND round_number = $2 AND slot_id = $3
	`, tournamentID, roundNumber, slotID).Scan(&slotStatus, &slotIndex)
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}

	ctxLoaded := domain.RoundMatchContext{
		TournamentID: domain.TournamentID(tournamentID),
		Phase:        domain.TournamentPhase(phase),
		RoundNumber:  roundNumber,
		IsFinal:      isFinal,
		RoundStatus:  domain.RoundStatus(roundStatus),
		RoundFound:   true,
		SlotFound:    true,
		Slot: domain.RoundMatchSlotState{
			SlotID:             domain.SlotID(slotID),
			RoomID:             domain.RoomID(roomAssigned),
			SlotIndex:          slotIndex,
			Status:             domain.SlotStatus(slotStatus),
			AssignmentResolved: true,
		},
	}

	// Load advancement / recorded standings if slot already terminal.
	var advIDs []string
	var srcRoom string
	var srcVer int64
	err = tx.QueryRow(ctx, `
		SELECT advancing_player_ids, source_room_id, source_completion_version
		FROM advancement_records
		WHERE tournament_id = $1 AND round_number = $2 AND slot_id = $3
		FOR UPDATE
	`, tournamentID, roundNumber, slotID).Scan(&advIDs, &srcRoom, &srcVer)
	if err == nil {
		ctxLoaded.Slot.HasResult = true
		ctxLoaded.Slot.CompletionVersion = domain.CompletionVersion(srcVer)
		ctxLoaded.Slot.Advancing = make([]domain.PlayerID, len(advIDs))
		for i, p := range advIDs {
			ctxLoaded.Slot.Advancing[i] = domain.PlayerID(p)
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	} else if domain.SlotStatus(slotStatus) == domain.SlotResultRecorded || domain.SlotStatus(slotStatus) == domain.SlotAdvanced {
		ctxLoaded.Slot.HasResult = true
	}

	// Latest quarantine reason for this resolved slot (ledger preferred over match_results).
	var qReason *string
	_ = tx.QueryRow(ctx, `
		SELECT reason FROM match_result_quarantines
		WHERE tournament_id = $1 AND resolved_round_number = $2 AND resolved_slot_id = $3
		  AND affects_slot = true
		ORDER BY created_at DESC
		LIMIT 1
	`, tournamentID, roundNumber, slotID).Scan(&qReason)
	if qReason == nil {
		_ = tx.QueryRow(ctx, `
			SELECT quarantine_reason FROM match_results
			WHERE tournament_id = $1 AND round_number = $2 AND slot_id = $3
			  AND quarantine_reason IS NOT NULL
			ORDER BY processed_at DESC
			LIMIT 1
		`, tournamentID, roundNumber, slotID).Scan(&qReason)
	}
	if qReason != nil {
		ctxLoaded.Slot.QuarantineReason = *qReason
	}

	uow.exists = true
	uow.loaded = ctxLoaded
	return uow, nil
}

func (u *RoundMatchUnitOfWork) lockShardAndSlot(slotIndex, roundNumber int, slotID string) error {
	shardID := domain.ProgressShardID(slotIndex)
	// Ensure shard row exists (legacy may have rebuilt; race-safe upsert of zeros then lock).
	if _, err := u.tx.Exec(u.ctx, `
		INSERT INTO round_progress_shards (
			tournament_id, round_number, shard_id, assigned_count, resolved_count, quarantined_count
		) VALUES ($1, $2, $3, 0, 0, 0)
		ON CONFLICT DO NOTHING
	`, u.tid, roundNumber, shardID); err != nil {
		return err
	}
	var sid int
	if err := u.tx.QueryRow(u.ctx, `
		SELECT shard_id FROM round_progress_shards
		WHERE tournament_id = $1 AND round_number = $2 AND shard_id = $3
		FOR UPDATE
	`, u.tid, roundNumber, shardID).Scan(&sid); err != nil {
		return err
	}
	var locked string
	return u.tx.QueryRow(u.ctx, `
		SELECT slot_id FROM bracket_slots
		WHERE tournament_id = $1 AND round_number = $2 AND slot_id = $3
		FOR UPDATE
	`, u.tid, roundNumber, slotID).Scan(&locked)
}

// Loaded returns the bounded decision context.
func (u *RoundMatchUnitOfWork) Loaded() domain.RoundMatchContext { return u.loaded }

// Exists reports whether the tournament was found.
func (u *RoundMatchUnitOfWork) Exists() bool { return u.exists }

// AttachPriorResult acquires the (room, version) business-key advisory lock, then loads
// the match_results row and exact business-key quarantine ledger under that lock.
// Exact ledger quarantine takes precedence even when match_results already has
// disposition=recorded: DecideRecordMatchResult then sees a quarantined prior
// (factless / held) and must never overwrite the recorded row.
//
// Lock order relative to BeginRoundMatch: known room already holds shard/slot, so
// Attach adds business → exact result/ledger. insertQuarantineLedger may re-acquire
// the same advisory (pg_advisory_xact_lock is reentrant in one tx); never reverse.
func (u *RoundMatchUnitOfWork) AttachPriorResult(roomID string, completionVersion uint64) error {
	if u == nil || u.done {
		return fmt.Errorf("unit of work finished")
	}
	if _, err := u.tx.Exec(u.ctx, quarantineResultBizLockSQL,
		quarantineResultBusinessLockKey(roomID, completionVersion)); err != nil {
		return wrapUnavailable(err)
	}
	var disp, fp string
	var src *string
	var ranked []byte
	hasResult := false
	err := u.tx.QueryRow(u.ctx, `
		SELECT disposition, ranked_result, source_event_id
		FROM match_results
		WHERE room_id = $1 AND completion_version = $2
		FOR UPDATE
	`, roomID, int64(completionVersion)).Scan(&disp, &ranked, &src)
	switch {
	case err == nil:
		hasResult = true
		var payload map[string]any
		_ = json.Unmarshal(ranked, &payload)
		if fingerprint, ok := payload["fingerprint"].(string); ok {
			fp = fingerprint
		}
		srcID := ""
		if src != nil {
			srcID = *src
		}
		u.loaded.PriorResult = &domain.RoundMatchPriorResult{
			Disposition:   domain.ResultDisposition(disp),
			Fingerprint:   fp,
			SourceEventID: srcID,
		}
		if disp == string(domain.DispositionRecorded) {
			u.loaded.Slot.HasResult = true
			u.loaded.Slot.ResultFingerprint = fp
			u.loaded.Slot.CompletionVersion = domain.CompletionVersion(completionVersion)
		}
	case errors.Is(err, pgx.ErrNoRows):
		// No match_results row yet — still check ledger below.
	default:
		return wrapUnavailable(err)
	}

	// Exact business-key ledger always, regardless of match_results presence.
	var ledgerTID string
	var qFP, qSrc *string
	qerr := u.tx.QueryRow(u.ctx, `
		SELECT tournament_id, fingerprint, source_event_id
		FROM match_result_quarantines
		WHERE claimed_room_id = $1 AND completion_version = $2
		FOR UPDATE
	`, roomID, int64(completionVersion)).Scan(&ledgerTID, &qFP, &qSrc)
	switch {
	case qerr == nil:
		if ledgerTID != u.tid {
			return fmt.Errorf("%w: ledger tournament_id=%q want %q", ErrImmutableLedgerDrift, ledgerTID, u.tid)
		}
		ledgerFP := ""
		if qFP != nil {
			ledgerFP = *qFP
		}
		ledgerSrc := ""
		if qSrc != nil {
			ledgerSrc = *qSrc
		}
		// Quarantine-held / factless for DecideRecordMatchResult; recorded row stays untouched.
		u.loaded.PriorResult = &domain.RoundMatchPriorResult{
			Disposition:   domain.DispositionQuarantined,
			Fingerprint:   ledgerFP,
			SourceEventID: ledgerSrc,
		}
	case errors.Is(qerr, pgx.ErrNoRows):
		if !hasResult {
			return nil
		}
	default:
		return wrapUnavailable(qerr)
	}
	return nil
}

// AttachPriorEventOutcome looks up command_idempotency for ingest:{eventId}.
func (u *RoundMatchUnitOfWork) AttachPriorEventOutcome(eventID string) error {
	if eventID == "" {
		return nil
	}
	commandID := "ingest:" + eventID
	var body []byte
	err := u.tx.QueryRow(u.ctx, `
		SELECT outcome_body FROM command_idempotency WHERE command_id = $1
	`, commandID).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return wrapUnavailable(err)
	}
	var res envelope.Result
	if err := json.Unmarshal(body, &res); err != nil {
		return err
	}
	out := domain.CommandOutcome{
		CommandID: domain.CommandID(res.CommandID),
		Kind:      domain.OutcomeAccepted,
	}
	if res.Status == envelope.StatusRejected {
		out.Kind = domain.OutcomeRejected
	}
	u.loaded.PriorEvent = &out
	return nil
}

// LookupOutcome reads command_idempotency under the held transaction.
// Only the commandID locked at Begin may be queried; any other id returns false.
func (u *RoundMatchUnitOfWork) LookupOutcome(commandID string) (envelope.Result, bool) {
	if u == nil || u.done || commandID == "" || commandID != u.commandID {
		return envelope.Result{}, false
	}
	var body []byte
	err := u.tx.QueryRow(u.ctx, `
		SELECT outcome_body FROM command_idempotency WHERE command_id = $1
	`, commandID).Scan(&body)
	if err != nil {
		return envelope.Result{}, false
	}
	var out envelope.Result
	if err := json.Unmarshal(body, &out); err != nil {
		return envelope.Result{}, false
	}
	return out, true
}

func (u *RoundMatchUnitOfWork) finishWithPrior(body []byte) error {
	var prior envelope.Result
	if err := json.Unmarshal(body, &prior); err != nil {
		u.done = true
		_ = u.tx.Rollback(u.ctx)
		return wrapUnavailable(err)
	}
	u.done = true
	_ = u.tx.Rollback(u.ctx)
	return &PriorCommandOutcome{Outcome: prior}
}

// Commit applies the bounded differential mutation + outcome + outbox. Never calls full-aggregate rewrite.
func (u *RoundMatchUnitOfWork) Commit(req RoundMatchCommitRequest) error {
	if u == nil || u.done {
		return fmt.Errorf("unit of work already finished")
	}
	if req.CommandID == "" {
		return fmt.Errorf("commandId required for commit")
	}
	if u.commandID != "" && req.CommandID != u.commandID {
		return fmt.Errorf("commandId mismatch: locked %q got %q", u.commandID, req.CommandID)
	}

	var existingBody []byte
	err := u.tx.QueryRow(u.ctx, `
		SELECT outcome_body FROM command_idempotency WHERE command_id = $1 FOR UPDATE
	`, req.CommandID).Scan(&existingBody)
	if err == nil {
		return u.finishWithPrior(existingBody)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return wrapUnavailable(err)
	}

	if err := u.applyDecision(req); err != nil {
		return wrapUnavailable(err)
	}
	if req.ProjectionChanged {
		shardID := domain.ProgressShardID(u.loaded.Slot.SlotIndex)
		if req.Decision.PersistSlot.Valid() && u.loaded.Slot.AssignmentResolved {
			shardID = domain.ProgressShardID(u.loaded.Slot.SlotIndex)
		}
		if err := bumpProjectionShardTx(u.ctx, u.tx, u.tid, shardID, time.Now().UTC()); err != nil {
			return wrapUnavailable(err)
		}
	}
	tid := u.tid
	if tid == "" {
		tid = req.TournamentID
	}
	if err := insertCommandOutcomeWithTournament(u.ctx, u.tx, req.CommandID, tid, req.CommandType, req.Outcome); err != nil {
		return wrapUnavailable(err)
	}
	if err := insertOutboxEvents(u.ctx, u.tx, req.Events); err != nil {
		return wrapUnavailable(err)
	}

	if u.store.FailNextCommits > 0 {
		u.store.FailNextCommits--
		u.done = true
		_ = u.tx.Rollback(u.ctx)
		return fmt.Errorf("injected commit failure")
	}

	u.done = true
	if err := u.tx.Commit(u.ctx); err != nil {
		return wrapUnavailable(err)
	}
	return nil
}

func (u *RoundMatchUnitOfWork) applyDecision(req RoundMatchCommitRequest) error {
	d := req.Decision
	cmd := req.Command
	now := time.Now().UTC()

	// Differential persistence always targets locked resolved identity when available.
	rn := u.loaded.RoundNumber
	slotID := string(u.loaded.Slot.SlotID)
	if d.PersistRound >= 1 {
		rn = d.PersistRound
	}
	if d.PersistSlot.Valid() {
		slotID = string(d.PersistSlot)
	}
	shardID := domain.ProgressShardID(u.loaded.Slot.SlotIndex)

	switch d.Kind {
	case domain.RoundMatchReject, domain.RoundMatchDuplicateEvent, domain.RoundMatchExactDuplicate, domain.RoundMatchQuarantineHeld:
		// command_idempotency only. No match_results / quarantine / slot / shard / projection mutation.
		return nil

	case domain.RoundMatchRecord:
		ranked, err := json.Marshal(standingsPayload(d.Standings, d.Fingerprint))
		if err != nil {
			return err
		}
		srcEvent := ""
		if req.MatchResultSource != nil {
			srcEvent = req.MatchResultSource.EventID
		}
		if srcEvent == "" && cmd.EventID.Valid() {
			srcEvent = string(cmd.EventID)
		}
		var sourceEvent any
		if srcEvent != "" {
			sourceEvent = srcEvent
		}
		if _, err := u.tx.Exec(u.ctx, `
			INSERT INTO match_results (
				room_id, completion_version, tournament_id, round_number, slot_id,
				disposition, ranked_result, quarantine_reason, source_event_id, processed_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, $8, $9)
		`, string(cmd.RoomID), int64(cmd.CompletionVersion), u.tid, rn, slotID,
			string(domain.DispositionRecorded), ranked, sourceEvent, now); err != nil {
			return err
		}
		if len(d.Advancing) > 0 {
			advID := fmt.Sprintf("adv:%s:r%d:%s", u.tid, rn, slotID)
			tieBreak, _ := json.Marshal(map[string]string{"fingerprint": d.Fingerprint})
			adv := make([]string, len(d.Advancing))
			for i, p := range d.Advancing {
				adv[i] = string(p)
			}
			if _, err := u.tx.Exec(u.ctx, `
				INSERT INTO advancement_records (
					tournament_id, round_number, slot_id, advancement_id, advancing_player_ids,
					tie_break_inputs, source_room_id, source_completion_version, recorded_at
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			`, u.tid, rn, slotID, advID, adv, tieBreak, string(cmd.RoomID), int64(cmd.CompletionVersion), now); err != nil {
				return err
			}
			// Set-based normalized rows; fail closed on any identity/rank drift (no overwrite).
			var conflictCount int
			err = u.tx.QueryRow(u.ctx, `
				WITH incoming AS (
					SELECT p.player_id, (p.ord - 1)::int AS advancement_rank
					FROM unnest($4::text[]) WITH ORDINALITY AS p(player_id, ord)
				),
				ins AS (
					INSERT INTO round_advancing_players (
						tournament_id, source_round_number, player_id, source_slot_id, advancement_rank
					)
					SELECT $1, $2, i.player_id, $3, i.advancement_rank
					FROM incoming i
					ON CONFLICT (tournament_id, source_round_number, player_id) DO NOTHING
					RETURNING player_id
				)
				SELECT COUNT(*)::int
				FROM incoming i
				INNER JOIN round_advancing_players r
					ON r.tournament_id = $1
					AND r.source_round_number = $2
					AND r.player_id = i.player_id
				WHERE NOT EXISTS (SELECT 1 FROM ins WHERE ins.player_id = i.player_id)
				  AND (
					r.source_slot_id IS DISTINCT FROM $3
					OR r.advancement_rank IS DISTINCT FROM i.advancement_rank
				  )
			`, u.tid, rn, slotID, adv).Scan(&conflictCount)
			if err != nil {
				return err
			}
			if conflictCount > 0 {
				return fmt.Errorf("round_advancing_players immutable conflict for slot %s", slotID)
			}
		}
		if _, err := u.tx.Exec(u.ctx, `
			UPDATE bracket_slots SET status = $4, updated_at = $5
			WHERE tournament_id = $1 AND round_number = $2 AND slot_id = $3
		`, u.tid, rn, slotID, string(d.SlotStatus), now); err != nil {
			return err
		}
		if d.IncrementResolved {
			advInc := len(d.Advancing)
			if _, err := u.tx.Exec(u.ctx, `
				UPDATE round_progress_shards
				SET resolved_count = resolved_count + 1,
				    advancing_count = advancing_count + $4
				WHERE tournament_id = $1 AND round_number = $2 AND shard_id = $3
			`, u.tid, rn, shardID, advInc); err != nil {
				return err
			}
		}
		if u.loaded.IsFinal && len(d.Advancing) > 0 {
			if _, err := u.tx.Exec(u.ctx, `
				UPDATE tournaments
				SET rules = jsonb_set(COALESCE(rules, '{}'::jsonb), '{championId}', to_jsonb($2::text), true),
				    updated_at = $3
				WHERE tournament_id = $1
			`, u.tid, string(d.Advancing[0]), now); err != nil {
				return err
			}
		}
		return nil

	case domain.RoundMatchQuarantineUnresolved:
		if err := u.insertQuarantineLedger(req, d, cmd, now, rn, slotID); err != nil {
			return err
		}
		// Only write match_results when resolved assignment exactly matches claimed room+slot FK.
		if d.WriteMatchResult && u.loaded.Slot.AssignmentResolved && rn >= 1 && slotID != "" {
			standings := d.Standings
			if len(standings) == 0 {
				standings = cmd.Standings
			}
			ranked, err := json.Marshal(standingsPayload(standings, d.Fingerprint))
			if err != nil {
				return err
			}
			srcEvent := ""
			if cmd.EventID.Valid() {
				srcEvent = string(cmd.EventID)
			}
			var sourceEvent any
			if srcEvent != "" {
				sourceEvent = srcEvent
			}
			if _, err := u.tx.Exec(u.ctx, `
				INSERT INTO match_results (
					room_id, completion_version, tournament_id, round_number, slot_id,
					disposition, ranked_result, quarantine_reason, source_event_id, processed_at
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
				ON CONFLICT (room_id, completion_version) DO NOTHING
			`, string(cmd.RoomID), int64(cmd.CompletionVersion), u.tid, rn, slotID,
				string(domain.DispositionQuarantined), ranked, d.QuarantineReason, sourceEvent, now); err != nil {
				return err
			}
		}
		if d.AffectsSlot && rn >= 1 && slotID != "" {
			if _, err := u.tx.Exec(u.ctx, `
				UPDATE bracket_slots
				SET status = $4, updated_at = $5
				WHERE tournament_id = $1 AND round_number = $2 AND slot_id = $3
				  AND status NOT IN ('result_recorded', 'advanced')
			`, u.tid, rn, slotID, string(domain.SlotQuarantined), now); err != nil {
				return err
			}
			if d.BlockRound {
				if _, err := u.tx.Exec(u.ctx, `
					SELECT round_number FROM tournament_rounds
					WHERE tournament_id = $1 AND round_number = $2 FOR UPDATE
				`, u.tid, rn); err != nil {
					return err
				}
				if _, err := u.tx.Exec(u.ctx, `
					UPDATE tournament_rounds SET status = $3
					WHERE tournament_id = $1 AND round_number = $2
					  AND status <> $4
				`, u.tid, rn, string(domain.RoundBlocked), string(domain.RoundCompleted)); err != nil {
					return err
				}
			}
			if d.IncrementQuarantined {
				if _, err := u.tx.Exec(u.ctx, `
					UPDATE round_progress_shards
					SET quarantined_count = quarantined_count + 1
					WHERE tournament_id = $1 AND round_number = $2 AND shard_id = $3
				`, u.tid, rn, shardID); err != nil {
					return err
				}
			}
		}
		return nil

	case domain.RoundMatchQuarantineConflict:
		// Preserve disposition=recorded, advancement, slot terminal status, and counters.
		// Conflict metadata goes to the quarantine ledger only — never mutate recorded row.
		return u.insertQuarantineLedger(req, d, cmd, now, rn, slotID)

	default:
		return nil
	}
}

func (u *RoundMatchUnitOfWork) insertQuarantineLedger(
	req RoundMatchCommitRequest,
	d domain.RoundMatchDecision,
	cmd domain.RecordMatchResultCommand,
	now time.Time,
	resolvedRound int,
	resolvedSlot string,
) error {
	qid := string(cmd.CommandID)
	if qid == "" {
		qid = fmt.Sprintf("q:%s:%s:%d", u.tid, cmd.RoomID, cmd.CompletionVersion)
	}
	srcEvent := ""
	if req.MatchResultSource != nil {
		srcEvent = req.MatchResultSource.EventID
	}
	if srcEvent == "" && cmd.EventID.Valid() {
		srcEvent = string(cmd.EventID)
	}
	var sourceEvent any
	if srcEvent != "" {
		sourceEvent = srcEvent
	}
	var claimedRound any
	if cmd.RoundNumber >= 1 {
		claimedRound = cmd.RoundNumber
	}
	var claimedSlot any
	if cmd.SlotID.Valid() {
		claimedSlot = string(cmd.SlotID)
	}
	var resRound, resSlot any
	affects := d.AffectsSlot && resolvedRound >= 1 && resolvedSlot != ""
	if affects {
		resRound = resolvedRound
		resSlot = resolvedSlot
	}
	var fp any
	if d.Fingerprint != "" {
		fp = d.Fingerprint
	}
	// Serialize with QuarantineTournamentResult on the same business key.
	// Reentrant: AttachPriorResult already holds this advisory in the same tx.
	if _, err := u.tx.Exec(u.ctx, quarantineResultBizLockSQL,
		quarantineResultBusinessLockKey(string(cmd.RoomID), uint64(cmd.CompletionVersion))); err != nil {
		return err
	}
	tag, err := u.tx.Exec(u.ctx, `
		INSERT INTO match_result_quarantines (
			quarantine_id, source_event_id, tournament_id,
			claimed_room_id, claimed_round_number, claimed_slot_id,
			completion_version, fingerprint, reason,
			resolved_round_number, resolved_slot_id, affects_slot, created_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		ON CONFLICT (claimed_room_id, completion_version) DO NOTHING
	`, qid, sourceEvent, u.tid, string(cmd.RoomID), claimedRound, claimedSlot,
		int64(cmd.CompletionVersion), fp, d.QuarantineReason,
		resRound, resSlot, affects, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	// Business-key already present (explicit QuarantineTournamentResult or prior conflict).
	// Idempotent when tournament_id matches; fail closed on immutable tournament drift.
	// Never mutate disposition=recorded match_results from this path.
	var existingTID string
	err = u.tx.QueryRow(u.ctx, `
		SELECT tournament_id FROM match_result_quarantines
		WHERE claimed_room_id = $1 AND completion_version = $2
	`, string(cmd.RoomID), int64(cmd.CompletionVersion)).Scan(&existingTID)
	if err != nil {
		return err
	}
	if existingTID != u.tid {
		return fmt.Errorf("%w: ledger tournament_id=%q want %q", ErrImmutableLedgerDrift, existingTID, u.tid)
	}
	return nil
}

// Rollback aborts the held transaction.
func (u *RoundMatchUnitOfWork) Rollback() error {
	if u == nil || u.done {
		return nil
	}
	u.done = true
	return u.tx.Rollback(u.ctx)
}

func insertCommandOutcomeWithTournament(ctx context.Context, tx pgx.Tx, commandID, tournamentID, commandType string, outcome envelope.Result) error {
	body, err := json.Marshal(outcome)
	if err != nil {
		return err
	}
	status := string(outcome.Status)
	if status == "" {
		status = "accepted"
	}
	if commandType == "" {
		commandType = outcome.Type
	}
	if commandType == "" {
		commandType = "unknown"
	}
	var tid any
	if tournamentID != "" {
		tid = tournamentID
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO command_idempotency (
			command_id, tournament_id, player_id, command_type, outcome_status, outcome_body
		) VALUES ($1, $2, NULL, $3, $4, $5)
	`, commandID, tid, commandType, status, body)
	return err
}
