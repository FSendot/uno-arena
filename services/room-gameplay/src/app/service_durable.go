package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"unoarena/services/room-gameplay/domain"
	"unoarena/shared/audit"
	"unoarena/shared/envelope"
)

// handleCommandDurable runs ADR-0019 command flow: Identity validate → BEGIN →
// FOR UPDATE → apply → GI append (holding lock) → atomic local write → COMMIT.
func (s *Service) handleCommandDurable(ctx context.Context, in CommandInput) CommandResult {
	if strings.TrimSpace(in.CommandID) == "" || strings.TrimSpace(in.Type) == "" {
		return s.rejectDurable(ctx, nil, in, "invalid_envelope", nil, nil)
	}
	if in.SchemaVersion != envelope.CurrentSchemaVersion {
		return s.rejectDurable(ctx, nil, in, "invalid_schema_version", nil, nil)
	}

	// Identity validate BEFORE mutation / lock (requirement order).
	if err := s.validateSession(ctx, in); err != nil {
		return s.rejectDurable(ctx, nil, in, "invalid_session", nil, nil)
	}

	switch in.Type {
	case CmdCreateRoom:
		return s.handleCreateRoomDurable(ctx, in)
	case CmdJoinRoom, CmdLeaveRoom, CmdLockRoom, CmdCancelRoom,
		CmdStartMatch, CmdStartNextGame, CmdPlayCard, CmdDrawCard, CmdChooseColor,
		CmdCallUno, CmdReportMissingUno, CmdReconnectToRoom, CmdDisconnectPlayer,
		CmdExpireUnoWindow, CmdForfeitPlayer, CmdSkipDisconnectedTurn:
		return s.handleExistingDurable(ctx, in)
	default:
		return s.rejectDurable(ctx, nil, in, "unknown_command_type", nil, nil)
	}
}

func (s *Service) handleCreateRoomDurable(ctx context.Context, in CommandInput) CommandResult {
	var payload struct {
		RoomID     string `json:"roomId"`
		HostID     string `json:"hostId"`
		Visibility string `json:"visibility"`
		MaxSeats   int    `json:"maxSeats"`
	}
	_ = json.Unmarshal(in.Payload, &payload)
	roomID := in.RoomID
	if roomID == "" {
		roomID = payload.RoomID
	}
	hostID := in.PlayerID
	if hostID == "" {
		hostID = payload.HostID
	}
	if roomID == "" {
		return s.rejectDurable(ctx, nil, in, "invalid_identity", nil, nil)
	}

	uow, err := s.deps.Commands.BeginCreate(ctx, domain.RoomID(roomID))
	if err != nil {
		return CommandResult{Err: err}
	}
	defer func() { _ = uow.Rollback() }()

	if prior, ok := uow.GetGlobalOutcome(in.CommandID); ok {
		_ = uow.Rollback()
		return s.replayPrior(ctx, in, prior)
	}
	if uow.Exists() {
		live := uow.Loaded()
		if live != nil && live.Room() != nil {
			if prior, ok := live.Room().PriorOutcome(domain.CommandID(in.CommandID)); ok {
				_ = uow.Rollback()
				return s.replayPrior(ctx, in, prior)
			}
		}
		return s.rejectDurable(ctx, uow, in, "room_already_exists", nil, live)
	}

	vis := domain.Visibility(payload.Visibility)
	room, out := domain.CreateRoom(domain.CreateRoomCommand{
		CommandID:  domain.CommandID(in.CommandID),
		RoomID:     domain.RoomID(roomID),
		HostID:     domain.PlayerID(hostID),
		Visibility: vis,
		MaxSeats:   payload.MaxSeats,
	})
	if out.Rejection != nil || room == nil {
		reason := "create_rejected"
		if out.Rejection != nil {
			reason = string(out.Rejection.Code)
		}
		return s.rejectDurable(ctx, uow, in, reason, out.Rejection, nil)
	}
	sess := domain.OpenSession(room)
	return s.commitAcceptedDurable(ctx, uow, in, nil, sess, out, MaterialReservation{}, true)
}

func (s *Service) handleExistingDurable(ctx context.Context, in CommandInput) CommandResult {
	roomID := in.RoomID
	if roomID == "" {
		var p struct {
			RoomID string `json:"roomId"`
		}
		_ = json.Unmarshal(in.Payload, &p)
		roomID = p.RoomID
	}
	if roomID == "" {
		return s.rejectDurable(ctx, nil, in, "missing_room_id", nil, nil)
	}
	in.RoomID = roomID

	var uow SessionUnitOfWork
	var err error
	if in.RuntimeGeneration > 0 {
		fenced, ok := s.deps.Commands.(RuntimeGenerationStore)
		if !ok {
			return CommandResult{Err: ErrRuntimeGenerationStale}
		}
		uow, err = fenced.BeginExistingGeneration(ctx, domain.RoomID(roomID), in.RuntimeGeneration)
	} else {
		uow, err = s.deps.Commands.BeginExisting(ctx, domain.RoomID(roomID))
	}
	if err != nil {
		return CommandResult{Err: err}
	}
	defer func() { _ = uow.Rollback() }()

	if prior, ok := uow.GetGlobalOutcome(in.CommandID); ok {
		_ = uow.Rollback()
		return s.replayPrior(ctx, in, prior)
	}

	live := uow.Loaded()
	if live == nil || live.Room() == nil {
		return s.rejectDurable(ctx, uow, in, "room_not_found", nil, nil)
	}
	// Duplicate command: return stored outcome WITHOUT second GI call.
	if prior, ok := live.Room().PriorOutcome(domain.CommandID(in.CommandID)); ok {
		_ = uow.Rollback()
		return s.replayPrior(ctx, in, prior)
	}
	// The Room-owned continuation worker is serialized by the aggregate lock.
	// Resolve its optimistic token from the locked snapshot so unrelated commands
	// between claim and dispatch cannot permanently poison the deterministic ID.
	if in.AsSystem && in.Type == CmdStartNextGame {
		expected := int64(live.Room().Sequence())
		in.ExpectedSequenceNumber = &expected
	}

	if rej := s.prevalidate(live, in); rej != nil {
		if in.AsSystem && in.Type == CmdStartNextGame {
			_ = uow.Rollback()
			seq := int64(live.Room().Sequence())
			return CommandResult{Result: envelope.Rejected(in.CommandID, in.Type, string(rej.Code), &seq)}
		}
		return s.rejectDurable(ctx, uow, in, string(rej.Code), rej, live)
	}

	var reservation MaterialReservation
	stage := live.Clone()
	out, err := s.apply(ctx, stage, in, &reservation)
	if err != nil {
		if reservation.ID != "" {
			_ = s.deps.Deals.Cancel(ctx, reservation.ID)
		}
		_ = uow.Rollback()
		return CommandResult{Err: err}
	}
	if out.Rejection != nil {
		if reservation.ID != "" {
			_ = s.deps.Deals.Cancel(ctx, reservation.ID)
		}
		// Worker-owned continuations must not persist a deterministic rejected
		// outcome: transient deal/material/precondition failures need to retry the
		// same command identity. The worker decides whether this reason is a safe
		// terminal no-op or should be released.
		if in.AsSystem && in.Type == CmdStartNextGame {
			_ = uow.Rollback()
			seq := int64(live.Room().Sequence())
			return CommandResult{Result: envelope.Rejected(in.CommandID, in.Type, string(out.Rejection.Code), &seq)}
		}
		return s.rejectDurable(ctx, uow, in, string(out.Rejection.Code), out.Rejection, live)
	}
	if out.Kind == domain.OutcomeDuplicate {
		if reservation.ID != "" {
			_ = s.deps.Deals.Cancel(ctx, reservation.ID)
		}
		_ = uow.Rollback()
		return s.resultFromPrior(in, out)
	}
	if !out.Accepted() {
		if reservation.ID != "" {
			_ = s.deps.Deals.Cancel(ctx, reservation.ID)
		}
		return s.rejectDurable(ctx, uow, in, "rejected", out.Rejection, live)
	}
	return s.commitAcceptedDurable(ctx, uow, in, live, stage, out, reservation, false)
}

func (s *Service) commitAcceptedDurable(
	ctx context.Context,
	uow SessionUnitOfWork,
	in CommandInput,
	live *domain.Session,
	stage *domain.Session,
	out domain.CommandOutcome,
	reservation MaterialReservation,
	createPath bool,
) CommandResult {
	roomID := string(stage.Room().ID())
	expectedRev := uow.IntegrityRevision()
	payloadBody := map[string]any{
		"commandId": in.CommandID,
		"type":      in.Type,
		"facts":     out.Facts,
		"sequence":  out.Sequence,
	}
	if reservation.ID != "" {
		payloadBody["reservationId"] = reservation.ID
		if reservation.Deal != nil {
			payloadBody["deal"] = reservation.Deal
		}
		if len(reservation.Cards) > 0 {
			payloadBody["cards"] = reservation.Cards
		}
	}
	payload, _ := json.Marshal(payloadBody)

	seq := int64(out.Sequence)
	prevSeq := int64(0)
	if live != nil && live.Room() != nil {
		prevSeq = int64(live.Room().Sequence())
	}
	noop := len(out.Facts) == 0 && seq == prevSeq

	gameID := string(stage.GameID())
	audiences := s.feedAudiencesDurable(uow, roomID, stage.Room(), in)
	playerStart := uow.PeekStreamSeq() + 1
	now := s.deps.Clock.Now()
	feedEvents, playerHighWater := BuildFeedEvents(stage, seq, playerStart, in.CorrelationID, in.CommandID, out.Facts, audiences, prevSeq, now)
	completion := BuildCompletionEvents(stage.Room(), stage.Game(), gameID, seq, in.CorrelationID, in.CommandID, out.Facts, now)
	all := append(feedEvents, completion...)

	// Precompute repair material BEFORE GI Append (intent closes the GI-success-before-marker gap).
	req := DurableAcceptedCommit{
		Session:              stage,
		PlayerID:             in.PlayerID,
		PlayerSessionID:      in.SessionID,
		BindPlayerSession:    !in.AsSystem && in.PlayerID != "" && in.SessionID != "",
		SetIntegrityRevision: true,
		StreamSeqHighWater:   playerHighWater,
		SetStreamSeq:         playerHighWater > 0,
		CommandID:            in.CommandID,
		CommandType:          in.Type,
		Outcome:              out,
		CreatePath:           createPath,
	}
	if reservation.ID != "" {
		req.ReservationID = reservation.ID
		req.ReservationRoomID = roomID
		req.ReservationGameID = gameID
		if noop {
			req.ReservationAction = ReservationActionCancel
		} else {
			req.ReservationAction = ReservationActionConfirm
		}
	}
	if len(all) > 0 {
		req.Outbox = OutboxEntry{RoomID: roomID, CommandID: in.CommandID, Events: all}
	}
	if in.Type == "ProvisionTournamentRoom" {
		var p struct {
			TournamentID string `json:"tournamentId"`
			RoundNumber  int    `json:"roundNumber"`
			SlotID       string `json:"slotId"`
		}
		_ = json.Unmarshal(in.Payload, &p)
		req.ProvisionKey = &ProvisionKey{
			TournamentID: p.TournamentID, RoundNumber: p.RoundNumber, SlotID: p.SlotID,
		}
		req.ProvisionRoomID = roomID
	}

	if err := s.deps.Commands.BeginReconciliationIntent(ctx, in.CommandID, roomID, expectedRev, payload, req); err != nil {
		_ = uow.Rollback()
		if errors.Is(err, ErrReconciliationPending) {
			return CommandResult{Err: errors.Join(ErrRetryableUnavailable, err)}
		}
		return CommandResult{Err: fmt.Errorf("reconciliation intent: %w", err)}
	}

	appendRes, err := s.deps.Integrity.Append(ctx, AppendRequest{
		RoomID:           roomID,
		GameID:           gameID,
		EventID:          in.CommandID,
		ExpectedRevision: expectedRev,
		EventType:        in.Type,
		Payload:          payload,
	})
	if err != nil {
		_ = uow.Rollback()
		if isDefinitiveIntegrityAppendFailure(err) {
			_ = s.deps.Commands.CancelReconciliationIntent(ctx, in.CommandID)
		}
		// Uncertain outcomes leave the pending intent fail-closed for the worker.
		return CommandResult{Err: fmt.Errorf("game integrity append failed: %w", err)}
	}

	if req.ReservationID != "" {
		if err := performReservationAction(ctx, s.deps.Deals, req); err != nil {
			_ = uow.Rollback()
			// Leave intent pending for reconciler (append already succeeded).
			return CommandResult{Err: fmt.Errorf("reservation %s failed: %w", req.ReservationAction, err)}
		}
	}

	req.LogOffset = appendRes.LogOffset
	req.IntegrityRevision = appendRes.Revision
	req.Outbox.LogOffset = appendRes.LogOffset

	if err := s.deps.Commands.FinalizeReconciliationIntent(ctx, in.CommandID, appendRes.LogOffset, appendRes.Revision); err != nil {
		_ = uow.Rollback()
		return CommandResult{Err: fmt.Errorf("reconciliation finalize: %w", err)}
	}

	if err := uow.CommitAccepted(req); err != nil {
		return CommandResult{Err: err}
	}
	// Durable mode: no DrainOutbox / MultiDestinationPublisher polling.
	acceptedPayload := map[string]any{"facts": factNames(out.Facts)}
	if in.Type == "ProvisionTournamentRoom" {
		acceptedPayload["roomId"] = roomID
	}
	return CommandResult{Result: envelope.Accepted(in.CommandID, in.Type, &seq, MustJSON(acceptedPayload))}
}

func performReservationAction(ctx context.Context, deals DealSource, req DurableAcceptedCommit) error {
	if deals == nil || req.ReservationID == "" {
		return nil
	}
	switch req.ReservationAction {
	case ReservationActionConfirm:
		return deals.ConfirmAt(ctx, req.ReservationRoomID, req.ReservationGameID, req.ReservationID)
	case ReservationActionCancel:
		return deals.CancelAt(ctx, req.ReservationRoomID, req.ReservationGameID, req.ReservationID)
	case "":
		return nil
	default:
		return fmt.Errorf("unknown reservation action %q", req.ReservationAction)
	}
}

func isDefinitiveIntegrityAppendFailure(err error) bool {
	// Only typed definitive rejections cancel the intent. Never guess from generic errors.
	return errors.Is(err, ErrIntegrityAppendDefinitive)
}

func (s *Service) feedAudiencesDurable(uow SessionUnitOfWork, roomID string, room *domain.Room, in CommandInput) []FeedAudience {
	seen := map[string]struct{}{}
	var out []FeedAudience
	add := func(playerID, sessionID string) {
		if playerID == "" || sessionID == "" {
			return
		}
		if _, ok := seen[playerID]; ok {
			return
		}
		seen[playerID] = struct{}{}
		out = append(out, FeedAudience{PlayerID: playerID, SessionID: sessionID})
	}
	if !in.AsSystem && in.PlayerID != "" && in.SessionID != "" {
		add(in.PlayerID, in.SessionID)
	}
	if room != nil {
		for _, seat := range room.Roster().Seats() {
			if !seat.Occupied {
				continue
			}
			pid := string(seat.PlayerID)
			if sid, ok := uow.PlayerSession(pid); ok {
				add(pid, sid)
			}
		}
	}
	_ = roomID
	return out
}

func (s *Service) rejectDurable(
	ctx context.Context,
	uow SessionUnitOfWork,
	in CommandInput,
	reason string,
	rej *domain.Rejection,
	live *domain.Session,
) CommandResult {
	var room *domain.Room
	if live != nil {
		room = live.Room()
	}

	var submitted, current *int64
	if in.ExpectedSequenceNumber != nil {
		submitted = in.ExpectedSequenceNumber
	}
	if rej != nil {
		cur := int64(rej.CurrentSequence)
		current = &cur
		if submitted == nil && rej.SubmittedSequence > 0 {
			s := int64(rej.SubmittedSequence)
			submitted = &s
		}
	} else if room != nil {
		cur := int64(room.Sequence())
		current = &cur
	}

	seqNum := domain.SequenceNumber(0)
	if current != nil {
		seqNum = domain.SequenceNumber(*current)
	} else if room != nil {
		seqNum = room.Sequence()
	}
	storedRej := rej
	if storedRej == nil {
		storedRej = &domain.Rejection{Code: domain.RejectionCode(reason), CurrentSequence: seqNum}
	} else if storedRej.Code == "" {
		cp := *storedRej
		cp.Code = domain.RejectionCode(reason)
		storedRej = &cp
	}
	out := domain.CommandOutcome{
		Kind:      domain.OutcomeRejected,
		CommandID: domain.CommandID(in.CommandID),
		Rejection: storedRej,
		Sequence:  seqNum,
	}

	if live != nil && room != nil {
		if prior, ok := room.PriorOutcome(domain.CommandID(in.CommandID)); ok {
			if uow != nil {
				_ = uow.Rollback()
			}
			return s.replayPrior(ctx, in, prior)
		}
	} else if uow != nil {
		if prior, ok := uow.GetGlobalOutcome(in.CommandID); ok {
			_ = uow.Rollback()
			return s.replayPrior(ctx, in, prior)
		}
	} else if prior, ok := s.deps.Sessions.GetGlobalOutcome(ctx, in.CommandID); ok {
		return s.replayPrior(ctx, in, prior)
	}

	rec := audit.NewRejection(in.CommandID, corrOrCmd(in), in.SessionID, in.PlayerID, reason, s.deps.Clock.Now()).
		WithRoom(in.RoomID).
		WithSequences(submitted, current)
	if room != nil && room.TournamentID().Valid() {
		rec = rec.WithTournament(string(room.TournamentID()))
	}

	commitReq := DurableRejectedCommit{
		PendingAudit: &rec,
		CommandID:    in.CommandID,
		CommandType:  in.Type,
		PlayerID:     in.PlayerID,
	}
	if live != nil && room != nil {
		live.Room().RememberOutcome(out)
		commitReq.Session = live
	} else {
		commitReq.GlobalOutcome = &out
	}

	if uow != nil {
		if err := uow.CommitRejected(commitReq); err != nil {
			return CommandResult{Err: err}
		}
	} else {
		// No open UoW (pre-lock rejection): persist via SessionRepository.
		cr := CommitRequest{PendingAudit: &rec, GlobalOutcome: commitReq.GlobalOutcome, Session: commitReq.Session}
		if err := s.deps.Sessions.Commit(ctx, cr); err != nil {
			return CommandResult{Err: err}
		}
	}

	if s.deps.Audit == nil {
		return CommandResult{Err: fmt.Errorf("audit sink not configured")}
	}
	if err := s.deps.Audit.Record(ctx, rec); err != nil {
		return CommandResult{Err: err}
	}
	if err := s.deps.Sessions.MarkAuditComplete(ctx, in.CommandID); err != nil {
		return CommandResult{Err: err}
	}

	var seq *int64
	if current != nil {
		seq = current
	}
	return CommandResult{Result: envelope.Rejected(in.CommandID, in.Type, reason, seq)}
}

func (s *Service) provisionDurable(ctx context.Context, in ProvisionInput) CommandResult {
	cmdIn := CommandInput{
		CommandID:     in.CommandID,
		Type:          "ProvisionTournamentRoom",
		SchemaVersion: envelope.CurrentSchemaVersion,
		PlayerID:      in.HostID,
		RoomID:        in.RoomID,
		CorrelationID: in.CorrelationID,
		AsSystem:      true,
		Payload: MustJSON(map[string]any{
			"tournamentId": in.TournamentID,
			"roundNumber":  in.RoundNumber,
			"slotId":       in.SlotID,
			"roomId":       in.RoomID,
			"hostId":       in.HostID,
			"visibility":   in.Visibility,
			"maxSeats":     in.MaxSeats,
			"playerIds":    in.PlayerIDs,
		}),
	}
	key := ProvisionKey{
		TournamentID: in.TournamentID,
		RoundNumber:  in.RoundNumber,
		SlotID:       in.SlotID,
	}
	if existing, ok := s.deps.Sessions.GetProvision(ctx, key); ok {
		seq := int64(0)
		if sess, found := s.deps.Sessions.Get(ctx, domain.RoomID(existing)); found {
			seq = int64(sess.Room().Sequence())
		}
		return CommandResult{Result: envelope.Accepted(in.CommandID, "ProvisionTournamentRoom", &seq, MustJSON(map[string]string{
			"roomId": existing,
			"status": "duplicate",
		}))}
	}

	uow, err := s.deps.Commands.BeginCreate(ctx, domain.RoomID(in.RoomID))
	if err != nil {
		return CommandResult{Err: err}
	}
	defer func() { _ = uow.Rollback() }()

	if prior, ok := uow.GetGlobalOutcome(in.CommandID); ok {
		_ = uow.Rollback()
		return s.replayPrior(ctx, cmdIn, prior)
	}
	if rid, ok := uow.GetProvision(key); ok {
		_ = uow.Rollback()
		seq := int64(0)
		if sess, found := s.deps.Sessions.Get(ctx, domain.RoomID(rid)); found {
			seq = int64(sess.Room().Sequence())
		}
		return CommandResult{Result: envelope.Accepted(in.CommandID, "ProvisionTournamentRoom", &seq, MustJSON(map[string]string{
			"roomId": rid,
			"status": "duplicate",
		}))}
	}
	if uow.Exists() {
		return s.rejectDurable(ctx, uow, cmdIn, "room_already_exists", nil, uow.Loaded())
	}

	vis := domain.Visibility(in.Visibility)
	if vis == "" {
		vis = domain.VisibilityPrivate
	}
	host := domain.PlayerID(in.HostID)
	if host == "" && len(in.PlayerIDs) > 0 {
		host = domain.PlayerID(in.PlayerIDs[0])
	}
	room, out := domain.ProvisionTournamentRoom(domain.ProvisionTournamentRoomCommand{
		CommandID:    domain.CommandID(in.CommandID),
		RoomID:       domain.RoomID(in.RoomID),
		TournamentID: domain.TournamentID(in.TournamentID),
		RoundNumber:  in.RoundNumber,
		SlotID:       in.SlotID,
		HostID:       host,
		Visibility:   vis,
		MaxSeats:     in.MaxSeats,
	})
	if out.Rejection != nil || room == nil {
		reason := "provision_rejected"
		if out.Rejection != nil {
			reason = string(out.Rejection.Code)
		}
		return s.rejectDurable(ctx, uow, cmdIn, reason, out.Rejection, nil)
	}
	for _, pid := range in.PlayerIDs {
		if domain.PlayerID(pid) == host {
			continue
		}
		joinOut := room.JoinRoom(domain.JoinRoomCommand{
			CommandID:        domain.CommandID(in.CommandID + "-join-" + pid),
			PlayerID:         domain.PlayerID(pid),
			ExpectedSequence: room.Sequence(),
		})
		if joinOut.Rejection != nil {
			return s.rejectDurable(ctx, uow, cmdIn, string(joinOut.Rejection.Code), joinOut.Rejection, nil)
		}
		out.Facts = append(out.Facts, joinOut.Facts...)
		out.Sequence = joinOut.Sequence
	}
	sess := domain.OpenSession(room)
	return s.commitAcceptedDurable(ctx, uow, cmdIn, nil, sess, out, MaterialReservation{}, true)
}
