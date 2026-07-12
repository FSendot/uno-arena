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

// Differential QuarantineTournamentResult lock order (all paths must match):
//  1. Shared rewrite barrier (tournament:rewrite:{id})
//  2. Global command lock (AcquireCommandLock)
//  3. Plain tournaments existence/phase SELECT (never tournaments FOR UPDATE —
//     command remains valid in terminal phases)
//  4. Resolve assignment; when known, lock exact bracket_slots row FOR UPDATE
//  5. Business-key advisory lock for (roomId, completionVersion)
//  6. Exact match_results / match_result_quarantines rows for the business key
//
// Known room: shared → command → slot → business → exact result/ledger.
// Unknown room: shared → command → business → exact result/ledger.
//
// Never: whole-tournament hydrate, subtree DELETE, Service.mu, BeginExisting,
// scans, slot/counter/advancement mutation, or outbox (no contract topic).
// Never: business lock before slot (deadlocks RoundMatch slot → business).

const (
	quarantineResultCommandType = "QuarantineTournamentResult"
	quarantineResultBizLockSQL  = `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`
	quarantineResultBizLockPref = "match-result-quarantine:"
)

func quarantineResultBusinessLockKey(roomID string, completionVersion uint64) string {
	return fmt.Sprintf("%s%s:%d", quarantineResultBizLockPref, roomID, completionVersion)
}

// QuarantineResultCommitRequest is one atomic differential QuarantineTournamentResult unit.
type QuarantineResultCommitRequest struct {
	TournamentID      string
	CommandID         string
	CommandType       string
	CorrelationID     string
	Outcome           envelope.Result
	Decision          domain.QuarantineTournamentResultDecision
	Command           domain.QuarantineTournamentResultCommand
	ProjectionChanged bool
}

// QuarantineResultUnitOfWork holds one READ COMMITTED tx for bounded QuarantineTournamentResult.
type QuarantineResultUnitOfWork struct {
	store             *TournamentStore
	ctx               context.Context
	tx                pgx.Tx
	tid               string
	roomID            string
	completionVersion uint64
	commandID         string
	loaded            domain.QuarantineTournamentResultContext
	exists            bool
	done              bool
}

func (u *QuarantineResultUnitOfWork) Exists() bool { return u != nil && u.exists }

func (u *QuarantineResultUnitOfWork) Loaded() domain.QuarantineTournamentResultContext {
	if u == nil {
		return domain.QuarantineTournamentResultContext{}
	}
	return u.loaded
}

func (u *QuarantineResultUnitOfWork) LookupOutcome(commandID string) (envelope.Result, bool) {
	if u == nil || u.done || commandID == "" || commandID != u.commandID {
		return envelope.Result{}, false
	}
	var body []byte
	err := u.tx.QueryRow(u.ctx, `
		SELECT outcome_body FROM command_idempotency WHERE command_id = $1
	`, commandID).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		return envelope.Result{}, false
	}
	if err != nil {
		return envelope.Result{}, false
	}
	var out envelope.Result
	if json.Unmarshal(body, &out) != nil {
		return envelope.Result{}, false
	}
	return out, true
}

func (u *QuarantineResultUnitOfWork) finishWithPrior(body []byte) error {
	var out envelope.Result
	_ = json.Unmarshal(body, &out)
	_ = u.tx.Rollback(u.ctx)
	u.done = true
	return &PriorCommandOutcome{Outcome: out}
}

func (u *QuarantineResultUnitOfWork) Rollback() error {
	if u == nil || u.done {
		return nil
	}
	u.done = true
	return u.tx.Rollback(u.ctx)
}

// BeginStandaloneQuarantineResultCommand locks only the global command id for invalid rejects.
func (s *TournamentStore) BeginStandaloneQuarantineResultCommand(ctx context.Context, commandID string) (*QuarantineResultUnitOfWork, error) {
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
	uow := &QuarantineResultUnitOfWork{store: s, ctx: ctx, tx: tx, commandID: commandID}
	if err := AcquireCommandLock(ctx, tx, commandID); err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	return uow, nil
}

// BeginQuarantineTournamentResult starts a differential UoW for QuarantineTournamentResult.
func (s *TournamentStore) BeginQuarantineTournamentResult(ctx context.Context, tournamentID, roomID string, completionVersion uint64, commandID string) (*QuarantineResultUnitOfWork, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("nil store")
	}
	if tournamentID == "" || roomID == "" {
		return nil, fmt.Errorf("tournamentId and roomId required")
	}
	if commandID == "" {
		return nil, fmt.Errorf("commandId required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, wrapUnavailable(err)
	}
	uow := &QuarantineResultUnitOfWork{
		store: s, ctx: ctx, tx: tx, tid: tournamentID, roomID: roomID,
		completionVersion: completionVersion, commandID: commandID,
	}
	if err := acquireRewriteBarrierShared(ctx, tx, tournamentID); err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	if err := AcquireCommandLock(ctx, tx, commandID); err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}

	var phase string
	err = tx.QueryRow(ctx, `
		SELECT phase FROM tournaments WHERE tournament_id = $1
	`, tournamentID).Scan(&phase)
	if errors.Is(err, pgx.ErrNoRows) {
		uow.exists = false
		uow.loaded = domain.QuarantineTournamentResultContext{
			TournamentID:      domain.TournamentID(tournamentID),
			RoomID:            domain.RoomID(roomID),
			CompletionVersion: domain.CompletionVersion(completionVersion),
		}
		return uow, nil
	}
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	uow.exists = true
	_ = phase // existence/phase read only; quarantine remains valid in terminal phases

	loaded := domain.QuarantineTournamentResultContext{
		TournamentID:      domain.TournamentID(tournamentID),
		Exists:            true,
		RoomID:            domain.RoomID(roomID),
		CompletionVersion: domain.CompletionVersion(completionVersion),
	}

	var (
		roundNumber int
		slotID      string
	)
	err = tx.QueryRow(ctx, `
		SELECT a.round_number, a.slot_id
		FROM assigned_matches a
		WHERE a.tournament_id = $1 AND a.room_id = $2
	`, tournamentID, roomID).Scan(&roundNumber, &slotID)
	switch {
	case err == nil:
		loaded.AssignmentResolved = true
		loaded.RoundNumber = roundNumber
		loaded.SlotID = domain.SlotID(slotID)
		// Lock the unique assigned slot row before business key (no status mutation).
		if _, err := tx.Exec(ctx, `
			SELECT slot_id FROM bracket_slots
			WHERE tournament_id = $1 AND round_number = $2 AND slot_id = $3
			FOR UPDATE
		`, tournamentID, roundNumber, slotID); err != nil {
			_ = tx.Rollback(ctx)
			return nil, wrapUnavailable(err)
		}
	case errors.Is(err, pgx.ErrNoRows):
		// Unknown room — no slot lock; business key serializes with RoundMatch.
	default:
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}

	if _, err := tx.Exec(ctx, quarantineResultBizLockSQL, quarantineResultBusinessLockKey(roomID, completionVersion)); err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}

	// Exact match_results row for business key.
	var disposition string
	err = tx.QueryRow(ctx, `
		SELECT disposition FROM match_results
		WHERE room_id = $1 AND completion_version = $2
		FOR UPDATE
	`, roomID, int64(completionVersion)).Scan(&disposition)
	if err == nil {
		loaded.PriorDisposition = domain.ResultDisposition(disposition)
	} else if !errors.Is(err, pgx.ErrNoRows) {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}

	// Exact quarantine ledger row for business key.
	var ledgerTID, ledgerReason string
	var affects bool
	err = tx.QueryRow(ctx, `
		SELECT tournament_id, reason, affects_slot
		FROM match_result_quarantines
		WHERE claimed_room_id = $1 AND completion_version = $2
		FOR UPDATE
	`, roomID, int64(completionVersion)).Scan(&ledgerTID, &ledgerReason, &affects)
	if err == nil {
		if ledgerTID != tournamentID {
			_ = tx.Rollback(ctx)
			return nil, fmt.Errorf("%w: ledger tournament_id=%q want %q", ErrImmutableLedgerDrift, ledgerTID, tournamentID)
		}
		loaded.LedgerExists = true
	} else if !errors.Is(err, pgx.ErrNoRows) {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}

	uow.loaded = loaded
	return uow, nil
}

func (u *QuarantineResultUnitOfWork) bindCommit(req QuarantineResultCommitRequest) error {
	if u == nil || u.done {
		return fmt.Errorf("unit of work already finished")
	}
	if req.CommandID == "" {
		return fmt.Errorf("commandId required for commit")
	}
	if req.CommandID != u.commandID {
		return fmt.Errorf("commandId mismatch: locked %q got %q", u.commandID, req.CommandID)
	}
	cmdType := req.CommandType
	if cmdType == "" {
		cmdType = quarantineResultCommandType
	}
	if cmdType != quarantineResultCommandType {
		return fmt.Errorf("command type mismatch: want %q got %q", quarantineResultCommandType, cmdType)
	}
	cmd := req.Command
	if string(cmd.CommandID) != "" && string(cmd.CommandID) != u.commandID {
		return fmt.Errorf("commandId mismatch: locked %q got cmd.CommandID %q", u.commandID, cmd.CommandID)
	}
	if u.tid != "" {
		if req.TournamentID != "" && req.TournamentID != u.tid {
			return fmt.Errorf("tournamentId mismatch: locked %q got %q", u.tid, req.TournamentID)
		}
	}
	d := req.Decision
	switch d.Kind {
	case domain.QuarantineResultReject, domain.QuarantineResultAlreadyDone:
		// Outcome-only: standalone and tournament-bound both allowed.
		if string(cmd.RoomID) != "" && u.roomID != "" && string(cmd.RoomID) != u.roomID {
			return fmt.Errorf("roomId mismatch: locked %q got %q", u.roomID, cmd.RoomID)
		}
		if cmd.CompletionVersion != 0 && u.completionVersion != 0 && uint64(cmd.CompletionVersion) != u.completionVersion {
			return fmt.Errorf("completionVersion mismatch: locked %d got %d", u.completionVersion, cmd.CompletionVersion)
		}
	case domain.QuarantineResultLedgerOnly, domain.QuarantineResultInsertQuarantined:
		if u.tid == "" {
			return fmt.Errorf("success decision incompatible with standalone quarantine result uow")
		}
		if string(cmd.CommandID) != u.commandID {
			return fmt.Errorf("commandId mismatch: locked %q got cmd.CommandID %q", u.commandID, cmd.CommandID)
		}
		if string(cmd.RoomID) != u.roomID {
			return fmt.Errorf("roomId mismatch: locked %q got %q", u.roomID, cmd.RoomID)
		}
		if uint64(cmd.CompletionVersion) != u.completionVersion {
			return fmt.Errorf("completionVersion mismatch: locked %d got %d", u.completionVersion, cmd.CompletionVersion)
		}
		if err := u.validateSuccessDecisionShape(d); err != nil {
			return err
		}
	default:
		if d.Kind != "" {
			return fmt.Errorf("unknown quarantine result decision kind %q", d.Kind)
		}
	}
	return nil
}

func (u *QuarantineResultUnitOfWork) validateSuccessDecisionShape(d domain.QuarantineTournamentResultDecision) error {
	switch d.Kind {
	case domain.QuarantineResultInsertQuarantined:
		if !u.loaded.AssignmentResolved {
			return fmt.Errorf("insert quarantined requires resolved assignment")
		}
		if !d.WriteMatchResult || !d.AffectsSlot {
			return fmt.Errorf("insert quarantined decision shape mismatch")
		}
		if d.PersistRound != u.loaded.RoundNumber || d.PersistSlot != u.loaded.SlotID {
			return fmt.Errorf("persist target mismatch: locked assignment round=%d slot=%q got round=%d slot=%q",
				u.loaded.RoundNumber, u.loaded.SlotID, d.PersistRound, d.PersistSlot)
		}
	case domain.QuarantineResultLedgerOnly:
		if d.WriteMatchResult {
			return fmt.Errorf("ledger_only must not WriteMatchResult")
		}
		if u.loaded.AssignmentResolved {
			if !d.AffectsSlot {
				return fmt.Errorf("assigned ledger_only must AffectsSlot")
			}
			if d.PersistRound != u.loaded.RoundNumber || d.PersistSlot != u.loaded.SlotID {
				return fmt.Errorf("persist target mismatch: locked assignment round=%d slot=%q got round=%d slot=%q",
					u.loaded.RoundNumber, u.loaded.SlotID, d.PersistRound, d.PersistSlot)
			}
		} else if d.AffectsSlot || d.PersistRound >= 1 || d.PersistSlot.Valid() {
			return fmt.Errorf("unknown-room ledger_only must not claim persist target")
		}
	}
	return nil
}

// Commit applies the bounded QuarantineTournamentResult mutation + outcome (no outbox).
func (u *QuarantineResultUnitOfWork) Commit(req QuarantineResultCommitRequest) error {
	if err := u.bindCommit(req); err != nil {
		return err
	}
	cmdType := req.CommandType
	if cmdType == "" {
		cmdType = quarantineResultCommandType
	}
	tid := req.TournamentID
	if tid == "" {
		tid = u.tid
	}
	now := time.Now().UTC()
	d := req.Decision
	cmd := req.Command

	switch d.Kind {
	case domain.QuarantineResultReject, domain.QuarantineResultAlreadyDone:
		// outcome only
	case domain.QuarantineResultLedgerOnly, domain.QuarantineResultInsertQuarantined:
		won, err := u.applyQuarantine(d, cmd, now)
		if err != nil {
			return err
		}
		if !won {
			// Concurrent business-key winner already inserted ledger: accept factless, no bump.
			req.Outcome = envelope.Accepted(req.CommandID, cmdType, nil, mustFactlessQuarantineBody())
			req.ProjectionChanged = false
		} else if req.ProjectionChanged {
			if err := bumpProjectionVersionTx(u.ctx, u.tx, u.tid, now); err != nil {
				return wrapUnavailable(err)
			}
		}
	}

	if err := insertCommandOutcomeWithTournament(u.ctx, u.tx, req.CommandID, tid, cmdType, req.Outcome); err != nil {
		var existing []byte
		err2 := u.tx.QueryRow(u.ctx, `
			SELECT outcome_body FROM command_idempotency WHERE command_id = $1
		`, req.CommandID).Scan(&existing)
		if err2 == nil {
			return u.finishWithPrior(existing)
		}
		return wrapUnavailable(err)
	}

	if u.store != nil && u.store.FailNextCommits > 0 {
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

func (u *QuarantineResultUnitOfWork) applyQuarantine(
	d domain.QuarantineTournamentResultDecision,
	cmd domain.QuarantineTournamentResultCommand,
	now time.Time,
) (wonLedger bool, err error) {
	reason := d.ReasonCode
	if reason == "" {
		reason = domain.SanitizeExplicitQuarantineReason(cmd.Reason)
	}
	if reason != domain.ExplicitQuarantineReasonCode {
		reason = domain.ExplicitQuarantineReasonCode
	}

	if d.Kind == domain.QuarantineResultInsertQuarantined && d.WriteMatchResult {
		rn := d.PersistRound
		slotID := string(d.PersistSlot)
		if rn < 1 || slotID == "" || !u.loaded.AssignmentResolved {
			return false, fmt.Errorf("insert quarantined result requires resolved assignment")
		}
		tag, err := u.tx.Exec(u.ctx, `
			INSERT INTO match_results (
				room_id, completion_version, tournament_id, round_number, slot_id,
				disposition, ranked_result, quarantine_reason, source_event_id, processed_at
			) VALUES ($1, $2, $3, $4, $5, $6, '{}'::jsonb, $7, NULL, $8)
			ON CONFLICT (room_id, completion_version) DO NOTHING
		`, string(cmd.RoomID), int64(cmd.CompletionVersion), u.tid, rn, slotID,
			string(domain.DispositionQuarantined), reason, now)
		if err != nil {
			return false, wrapUnavailable(err)
		}
		if tag.RowsAffected() == 0 {
			var disp string
			err = u.tx.QueryRow(u.ctx, `
				SELECT disposition FROM match_results
				WHERE room_id = $1 AND completion_version = $2
			`, string(cmd.RoomID), int64(cmd.CompletionVersion)).Scan(&disp)
			if err != nil {
				return false, wrapUnavailable(err)
			}
			if disp == string(domain.DispositionRecorded) {
				// Preserve recorded row.
			} else if disp != string(domain.DispositionQuarantined) {
				return false, fmt.Errorf("%w: unexpected match_results disposition %q", ErrImmutableLedgerDrift, disp)
			}
		}
	}

	qid := string(cmd.CommandID)
	if qid == "" {
		qid = fmt.Sprintf("q:%s:%s:%d", u.tid, cmd.RoomID, cmd.CompletionVersion)
	}
	var claimedRound, claimedSlot, resRound, resSlot any
	affects := d.AffectsSlot && d.PersistRound >= 1 && d.PersistSlot.Valid()
	if affects {
		claimedRound = d.PersistRound
		claimedSlot = string(d.PersistSlot)
		resRound = d.PersistRound
		resSlot = string(d.PersistSlot)
	}
	tag, err := u.tx.Exec(u.ctx, `
		INSERT INTO match_result_quarantines (
			quarantine_id, source_event_id, tournament_id,
			claimed_room_id, claimed_round_number, claimed_slot_id,
			completion_version, fingerprint, reason,
			resolved_round_number, resolved_slot_id, affects_slot, created_at
		) VALUES ($1, NULL, $2, $3, $4, $5, $6, NULL, $7, $8, $9, $10, $11)
		ON CONFLICT (claimed_room_id, completion_version) DO NOTHING
	`, qid, u.tid, string(cmd.RoomID), claimedRound, claimedSlot,
		int64(cmd.CompletionVersion), reason, resRound, resSlot, affects, now)
	if err != nil {
		return false, wrapUnavailable(err)
	}
	if tag.RowsAffected() == 1 {
		return true, nil
	}
	var existingTID string
	err = u.tx.QueryRow(u.ctx, `
		SELECT tournament_id
		FROM match_result_quarantines
		WHERE claimed_room_id = $1 AND completion_version = $2
	`, string(cmd.RoomID), int64(cmd.CompletionVersion)).Scan(&existingTID)
	if err != nil {
		return false, wrapUnavailable(err)
	}
	if existingTID != u.tid {
		return false, fmt.Errorf("%w: ledger tournament_id=%q want %q", ErrImmutableLedgerDrift, existingTID, u.tid)
	}
	return false, nil
}

func mustFactlessQuarantineBody() json.RawMessage {
	b, _ := json.Marshal(map[string]any{"facts": []any{}})
	return b
}

// LookupQuarantineResultOutcome reads command_idempotency outside a UoW.
func (s *TournamentStore) LookupQuarantineResultOutcome(ctx context.Context, commandID string) (envelope.Result, bool) {
	if s == nil || s.pool == nil || commandID == "" {
		return envelope.Result{}, false
	}
	var body []byte
	err := s.pool.QueryRow(ctx, `
		SELECT outcome_body FROM command_idempotency WHERE command_id = $1
	`, commandID).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		return envelope.Result{}, false
	}
	if err != nil {
		return envelope.Result{}, false
	}
	var out envelope.Result
	if json.Unmarshal(body, &out) != nil {
		return envelope.Result{}, false
	}
	return out, true
}

func quarantineResultBizLockSQLOK() bool {
	return quarantineResultBizLockSQL == `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))` &&
		quarantineResultBizLockPref == "match-result-quarantine:"
}
