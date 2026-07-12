package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"unoarena/services/room-gameplay/app"
	"unoarena/services/room-gameplay/domain"
	"unoarena/shared/audit"
)

// SessionStore is the durable Postgres SessionRepository + RoomCommandStore.
type SessionStore struct {
	pool       *pgxpool.Pool
	intentPool *pgxpool.Pool // dedicated pool for reconciliation intent begin/finalize/cancel
	Timers     *TimerIndex   // optional; schedules non-authoritative Redis dispatch on deadline writes

	// FailNextCommitAccepted is a test hook: next CommitAccepted rolls back and returns this error.
	FailNextCommitAccepted error
	// FailNextBeginIntent is a test hook: next BeginReconciliationIntent returns this error.
	FailNextBeginIntent error
	// FailNextFinalizeIntent is a test hook: next FinalizeReconciliationIntent returns this error.
	FailNextFinalizeIntent error
}

// NewSessionStore constructs a durable session/command store that shares one pool
// for commands and intents (nonconcurrent unit tests only).
func NewSessionStore(pool *pgxpool.Pool) *SessionStore {
	return &SessionStore{pool: pool, intentPool: pool}
}

// NewSessionStoreWithPools constructs a store with a dedicated reconciliation-intent pool.
// Production/runtime and concurrent integration tests must use separate pools so intent
// acquisition cannot deadlock when the main command pool is saturated by held UoWs.
func NewSessionStoreWithPools(main, intent *pgxpool.Pool) *SessionStore {
	return &SessionStore{pool: main, intentPool: intent}
}

// WithTimers attaches a Redis timer index for schedule-on-commit (Postgres remains authority).
func (s *SessionStore) WithTimers(t *TimerIndex) *SessionStore {
	s.Timers = t
	return s
}

func (s *SessionStore) Get(ctx context.Context, roomID domain.RoomID) (*domain.Session, bool) {
	sess, err := s.loadSession(ctx, s.pool, string(roomID))
	if err != nil || sess == nil {
		return nil, false
	}
	return sess, true
}

func (s *SessionStore) Commit(ctx context.Context, req app.CommitRequest) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return wrapUnavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if req.GlobalOutcome != nil && req.GlobalOutcome.CommandID.Valid() {
		if err := upsertIdempotency(ctx, tx, "", string(req.GlobalOutcome.CommandID), "", "", *req.GlobalOutcome); err != nil {
			return wrapUnavailable(err)
		}
	}
	if req.PendingAudit != nil && req.PendingAudit.CommandID != "" {
		if err := upsertPendingAudit(ctx, tx, *req.PendingAudit); err != nil {
			return wrapUnavailable(err)
		}
	}
	if req.Session != nil && req.Session.Room() != nil {
		if err := persistSessionTx(ctx, tx, req.Session, req, true); err != nil {
			return wrapUnavailable(err)
		}
	} else if req.GlobalOutcome == nil && req.PendingAudit == nil {
		return fmt.Errorf("session required")
	}
	if err := tx.Commit(ctx); err != nil {
		return wrapUnavailable(err)
	}
	if s.Timers != nil && req.Session != nil {
		_ = scheduleTimersFromSession(ctx, s.Timers, req.Session)
	}
	return nil
}

func (s *SessionStore) Delete(ctx context.Context, roomID domain.RoomID) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM rooms WHERE room_id = $1`, string(roomID))
	return wrapUnavailable(err)
}

func (s *SessionStore) ListPendingOutbox(context.Context, int) ([]app.OutboxEntry, error) {
	return nil, ErrCapabilityOnly
}

func (s *SessionStore) MarkOutboxEntryPublished(context.Context, string, time.Time) error {
	return ErrCapabilityOnly
}

func (s *SessionStore) PlayerSession(ctx context.Context, roomID, playerID string) (string, bool) {
	var sid string
	err := s.pool.QueryRow(ctx,
		`SELECT session_id FROM player_session_bindings WHERE room_id = $1 AND player_id = $2`,
		roomID, playerID,
	).Scan(&sid)
	if err != nil {
		return "", false
	}
	return sid, true
}

func (s *SessionStore) GetProvision(ctx context.Context, key app.ProvisionKey) (string, bool) {
	var roomID string
	err := s.pool.QueryRow(ctx,
		`SELECT room_id FROM tournament_provisions WHERE tournament_id = $1 AND round_number = $2 AND slot_id = $3`,
		key.TournamentID, key.RoundNumber, key.SlotID,
	).Scan(&roomID)
	if err != nil {
		return "", false
	}
	return roomID, true
}

func (s *SessionStore) IntegrityRevision(ctx context.Context, roomID string) int64 {
	var rev int64
	err := s.pool.QueryRow(ctx, `SELECT integrity_log_offset FROM rooms WHERE room_id = $1`, roomID).Scan(&rev)
	if err != nil {
		return 0
	}
	return rev
}

func (s *SessionStore) PeekStreamSeq(ctx context.Context, roomID string) int64 {
	var seq int64
	err := s.pool.QueryRow(ctx, `SELECT sequence_number FROM player_stream_highwater WHERE room_id = $1`, roomID).Scan(&seq)
	if err != nil {
		return 0
	}
	return seq
}

func (s *SessionStore) GetGlobalOutcome(ctx context.Context, commandID string) (domain.CommandOutcome, bool) {
	var body []byte
	err := s.pool.QueryRow(ctx,
		`SELECT outcome_body FROM command_idempotency WHERE command_id = $1 AND room_id IS NULL`,
		commandID,
	).Scan(&body)
	if err != nil {
		return domain.CommandOutcome{}, false
	}
	out, err := decodeOutcomeBody(body)
	if err != nil {
		return domain.CommandOutcome{}, false
	}
	return out, true
}

func (s *SessionStore) GetPendingAudit(ctx context.Context, commandID string) (audit.RejectionRecord, bool) {
	var body []byte
	err := s.pool.QueryRow(ctx, `SELECT record FROM pending_rejection_audits WHERE command_id = $1`, commandID).Scan(&body)
	if err != nil {
		return audit.RejectionRecord{}, false
	}
	var rec audit.RejectionRecord
	if err := json.Unmarshal(body, &rec); err != nil {
		return audit.RejectionRecord{}, false
	}
	return rec, true
}

func (s *SessionStore) MarkAuditComplete(ctx context.Context, commandID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM pending_rejection_audits WHERE command_id = $1`, commandID)
	return wrapUnavailable(err)
}

// BeginExisting implements RoomCommandStore (ADR-0019 FOR UPDATE).
func (s *SessionStore) BeginExisting(ctx context.Context, roomID domain.RoomID) (app.SessionUnitOfWork, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, wrapUnavailable(err)
	}
	uow := &sessionUoW{store: s, ctx: ctx, tx: tx, roomID: string(roomID)}
	var lockedID string
	err = tx.QueryRow(ctx, `SELECT room_id FROM rooms WHERE room_id = $1 FOR UPDATE`, string(roomID)).Scan(&lockedID)
	if err == pgx.ErrNoRows {
		uow.exists = false
		return uow, nil
	}
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	uow.exists = true
	sess, err := s.loadSessionTx(ctx, tx, string(roomID))
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	uow.loaded = sess
	_ = tx.QueryRow(ctx, `SELECT integrity_log_offset FROM rooms WHERE room_id = $1`, string(roomID)).Scan(&uow.revision)
	_ = tx.QueryRow(ctx, `SELECT COALESCE((SELECT sequence_number FROM player_stream_highwater WHERE room_id = $1), 0)`, string(roomID)).Scan(&uow.streamSeq)
	return uow, nil
}

// BeginCreate implements RoomCommandStore unique-insert create path.
func (s *SessionStore) BeginCreate(ctx context.Context, roomID domain.RoomID) (app.SessionUnitOfWork, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, wrapUnavailable(err)
	}
	uow := &sessionUoW{store: s, ctx: ctx, tx: tx, roomID: string(roomID), createPath: true}
	var exists bool
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM rooms WHERE room_id = $1 FOR UPDATE)`, string(roomID)).Scan(&exists)
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	uow.exists = exists
	if exists {
		sess, err := s.loadSessionTx(ctx, tx, string(roomID))
		if err != nil {
			_ = tx.Rollback(ctx)
			return nil, wrapUnavailable(err)
		}
		uow.loaded = sess
		_ = tx.QueryRow(ctx, `SELECT integrity_log_offset FROM rooms WHERE room_id = $1`, string(roomID)).Scan(&uow.revision)
	}
	return uow, nil
}

type sessionUoW struct {
	store      *SessionStore
	ctx        context.Context
	tx         pgx.Tx
	roomID     string
	loaded     *domain.Session
	exists     bool
	createPath bool
	revision   int64
	streamSeq  int64
	done       bool
}

func (u *sessionUoW) Loaded() *domain.Session  { return u.loaded }
func (u *sessionUoW) Exists() bool             { return u.exists }
func (u *sessionUoW) IntegrityRevision() int64 { return u.revision }
func (u *sessionUoW) PeekStreamSeq() int64     { return u.streamSeq }

func (u *sessionUoW) PlayerSession(playerID string) (string, bool) {
	var sid string
	err := u.tx.QueryRow(u.ctx,
		`SELECT session_id FROM player_session_bindings WHERE room_id = $1 AND player_id = $2`,
		u.roomID, playerID,
	).Scan(&sid)
	if err != nil {
		return "", false
	}
	return sid, true
}

func (u *sessionUoW) GetGlobalOutcome(commandID string) (domain.CommandOutcome, bool) {
	var body []byte
	err := u.tx.QueryRow(u.ctx,
		`SELECT outcome_body FROM command_idempotency WHERE command_id = $1 AND room_id IS NULL`,
		commandID,
	).Scan(&body)
	if err != nil {
		return domain.CommandOutcome{}, false
	}
	out, err := decodeOutcomeBody(body)
	if err != nil {
		return domain.CommandOutcome{}, false
	}
	return out, true
}

func (u *sessionUoW) GetProvision(key app.ProvisionKey) (string, bool) {
	var roomID string
	err := u.tx.QueryRow(u.ctx,
		`SELECT room_id FROM tournament_provisions WHERE tournament_id = $1 AND round_number = $2 AND slot_id = $3`,
		key.TournamentID, key.RoundNumber, key.SlotID,
	).Scan(&roomID)
	if err != nil {
		return "", false
	}
	return roomID, true
}

func (u *sessionUoW) CommitAccepted(req app.DurableAcceptedCommit) error {
	if u.done {
		return fmt.Errorf("uow already finished")
	}
	if u.store.FailNextCommitAccepted != nil {
		err := u.store.FailNextCommitAccepted
		u.store.FailNextCommitAccepted = nil
		_ = u.tx.Rollback(u.ctx)
		u.done = true
		return err
	}
	cr := app.CommitRequest{
		Session:              req.Session,
		Outbox:               req.Outbox,
		ProvisionKey:         req.ProvisionKey,
		ProvisionRoomID:      req.ProvisionRoomID,
		PlayerSessionID:      req.PlayerSessionID,
		PlayerID:             req.PlayerID,
		BindPlayerSession:    req.BindPlayerSession,
		IntegrityRevision:    req.IntegrityRevision,
		SetIntegrityRevision: req.SetIntegrityRevision,
		StreamSeqHighWater:   req.StreamSeqHighWater,
		SetStreamSeq:         req.SetStreamSeq,
	}
	if err := persistSessionTx(u.ctx, u.tx, req.Session, cr, req.CreatePath); err != nil {
		return wrapUnavailable(err)
	}
	if err := upsertIdempotency(u.ctx, u.tx, u.roomID, req.CommandID, req.CommandType, req.PlayerID, req.Outcome); err != nil {
		return wrapUnavailable(err)
	}
	// Mark reconciliation done + record processed offset (fail-closed single apply).
	if req.SetIntegrityRevision && req.CommandID != "" {
		_, err := u.tx.Exec(u.ctx, `
			UPDATE pending_integrity_reconciliations
			SET status = 'done', completed_at = now()
			WHERE command_id = $1 AND status = 'pending'
		`, req.CommandID)
		if err != nil {
			return wrapUnavailable(err)
		}
		_, err = u.tx.Exec(u.ctx, `
			INSERT INTO processed_reconciliation_offsets (room_id, log_offset, notes)
			VALUES ($1, $2, 'accepted_commit')
			ON CONFLICT DO NOTHING
		`, u.roomID, req.LogOffset)
		if err != nil {
			return wrapUnavailable(err)
		}
	}
	if err := u.tx.Commit(u.ctx); err != nil {
		return wrapUnavailable(err)
	}
	u.done = true
	if u.store.Timers != nil && req.Session != nil {
		_ = scheduleTimersFromSession(u.ctx, u.store.Timers, req.Session)
	}
	return nil
}

func (u *sessionUoW) CommitRejected(req app.DurableRejectedCommit) error {
	if u.done {
		return fmt.Errorf("uow already finished")
	}
	if req.GlobalOutcome != nil {
		if err := upsertIdempotency(u.ctx, u.tx, "", string(req.GlobalOutcome.CommandID), req.CommandType, req.PlayerID, *req.GlobalOutcome); err != nil {
			return wrapUnavailable(err)
		}
	}
	if req.Session != nil && req.Session.Room() != nil {
		cr := app.CommitRequest{Session: req.Session}
		if err := persistSessionTx(u.ctx, u.tx, req.Session, cr, false); err != nil {
			return wrapUnavailable(err)
		}
		if prior, ok := req.Session.Room().PriorOutcome(domain.CommandID(req.CommandID)); ok {
			if err := upsertIdempotency(u.ctx, u.tx, u.roomID, req.CommandID, req.CommandType, req.PlayerID, prior); err != nil {
				return wrapUnavailable(err)
			}
		}
	}
	if req.PendingAudit != nil {
		if err := upsertPendingAudit(u.ctx, u.tx, *req.PendingAudit); err != nil {
			return wrapUnavailable(err)
		}
	}
	if err := u.tx.Commit(u.ctx); err != nil {
		return wrapUnavailable(err)
	}
	u.done = true
	return nil
}

func (u *sessionUoW) Rollback() error {
	if u.done || u.tx == nil {
		return nil
	}
	u.done = true
	return u.tx.Rollback(u.ctx)
}

func upsertIdempotency(ctx context.Context, tx pgx.Tx, roomID, commandID, commandType, playerID string, out domain.CommandOutcome) error {
	body, err := encodeOutcomeBody(out)
	if err != nil {
		return err
	}
	var room any
	if roomID == "" {
		room = nil
	} else {
		room = roomID
	}
	seq := int64(out.Sequence)
	_, err = tx.Exec(ctx, `
		INSERT INTO command_idempotency (command_id, room_id, player_id, command_type, outcome_status, outcome_body, applied_sequence)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (command_id) DO NOTHING
	`, commandID, room, nullIfEmpty(playerID), commandType, outcomeStatus(out), body, seq)
	return err
}

func upsertPendingAudit(ctx context.Context, tx pgx.Tx, rec audit.RejectionRecord) error {
	body, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO pending_rejection_audits (command_id, record)
		VALUES ($1, $2)
		ON CONFLICT (command_id) DO UPDATE SET record = EXCLUDED.record
	`, rec.CommandID, body)
	return err
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func (s *SessionStore) loadSession(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, roomID string) (*domain.Session, error) {
	return s.loadSessionTx(ctx, q, roomID)
}

func (s *SessionStore) loadSessionTx(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, roomID string) (*domain.Session, error) {
	var (
		roomType, status, visibility  string
		capacity                      int
		sequence, turnVersion         int64
		hostID                        *string
		matchNumber                   int
		matchScore                    []byte
		tournamentID                  *string
		roundNumber                   *int
		slotID                        *string
		integrityOffset               int64
		gameCompleted, hasUno         bool
		usedGameIDs, skippedTurns     []byte
		unoWindow, disconnects        []byte
		nextDisc, matchSnap, outcomes []byte
	)
	err := q.QueryRow(ctx, `
		SELECT room_type, status, visibility, capacity, sequence_number, host_player_id,
		       match_number, match_score, tournament_id, round_number, slot_id,
		       integrity_log_offset, turn_version, game_completed_in_match, used_game_ids,
		       skipped_turns, has_uno, uno_window, disconnects, next_disconnect_versions,
		       match_snapshot, outcomes
		FROM rooms WHERE room_id = $1
	`, roomID).Scan(
		&roomType, &status, &visibility, &capacity, &sequence, &hostID,
		&matchNumber, &matchScore, &tournamentID, &roundNumber, &slotID,
		&integrityOffset, &turnVersion, &gameCompleted, &usedGameIDs,
		&skippedTurns, &hasUno, &unoWindow, &disconnects, &nextDisc,
		&matchSnap, &outcomes,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	rows, err := q.Query(ctx, `
		SELECT seat_number, player_id, occupied, connection_status, disconnect_version, wins, points
		FROM room_roster WHERE room_id = $1 ORDER BY seat_number
	`, roomID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var seats []domain.Seat
	for rows.Next() {
		var seatNum int
		var playerID string
		var occupied bool
		var connStatus string
		var discVer int64
		var wins, points int
		if err := rows.Scan(&seatNum, &playerID, &occupied, &connStatus, &discVer, &wins, &points); err != nil {
			return nil, err
		}
		seats = append(seats, domain.Seat{
			Index:    domain.SeatIndex(seatNum),
			PlayerID: domain.PlayerID(playerID),
			Occupied: true,
		})
		_ = occupied
		_ = connStatus
		_ = discVer
		_ = wins
		_ = points
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	outcomesMap, err := domainOutcomesFromJSON(outcomes)
	if err != nil {
		return nil, err
	}
	discMap, err := decodeDisconnects(disconnects)
	if err != nil {
		return nil, err
	}
	nextMap, err := decodeNextDisconnect(nextDisc)
	if err != nil {
		return nil, err
	}
	uno, hasUnoDecoded, err := decodeDomainUno(unoWindow)
	if err != nil {
		return nil, err
	}
	if !hasUno {
		hasUnoDecoded = false
	}

	host := domain.PlayerID("")
	if hostID != nil {
		host = domain.PlayerID(*hostID)
	}
	var tid domain.TournamentID
	var rn int
	var sid string
	if tournamentID != nil {
		tid = domain.TournamentID(*tournamentID)
	}
	if roundNumber != nil {
		rn = *roundNumber
	}
	if slotID != nil {
		sid = *slotID
	}

	room := domain.RestoreRoom(domain.RestoreRoomInput{
		ID:                   domain.RoomID(roomID),
		RoomType:             domain.RoomType(roomType),
		TournamentID:         tid,
		RoundNumber:          rn,
		SlotID:               sid,
		Status:               domain.RoomStatus(status),
		Visibility:           domain.Visibility(visibility),
		HostID:               host,
		Roster:               domain.RestoreRoster(capacity, seats),
		Sequence:             domain.SequenceNumber(sequence),
		Outcomes:             outcomesMap,
		Disconnects:          discMap,
		NextDisconnectVer:    nextMap,
		UnoWindow:            uno,
		HasUno:               hasUnoDecoded,
		GameCompletedInMatch: gameCompleted,
	})
	_ = matchNumber
	_ = matchScore
	_ = integrityOffset

	var gGame interface{} = nil
	var engine []byte
	var gameIDStr string
	err = q.QueryRow(ctx, `SELECT game_id, engine_state FROM current_games WHERE room_id = $1`, roomID).Scan(&gameIDStr, &engine)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	var restoredGame *domain.Session
	_ = restoredGame
	g, err := decodeGame(engine)
	if err != nil {
		return nil, err
	}
	m, err := decodeMatch(matchSnap)
	if err != nil {
		return nil, err
	}
	_ = gGame

	used, err := decodeUsedGameIDs(usedGameIDs)
	if err != nil {
		return nil, err
	}
	skipped, err := decodeSkipped(skippedTurns)
	if err != nil {
		return nil, err
	}
	gid := domain.GameID(gameIDStr)
	return domain.RestoreSession(room, g, m, gid, used, domain.SequenceNumber(turnVersion), skipped), nil
}
