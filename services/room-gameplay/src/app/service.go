package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"unoarena/services/room-gameplay/domain"
	"unoarena/services/room-gameplay/game"
	"unoarena/shared/audit"
	"unoarena/shared/correlation"
	"unoarena/shared/envelope"
)

// Command type catalog (OpenAPI + internal timer/provision).
const (
	CmdCreateRoom           = "CreateRoom"
	CmdJoinRoom             = "JoinRoom"
	CmdLeaveRoom            = "LeaveRoom"
	CmdLockRoom             = "LockRoom"
	CmdStartMatch           = "StartMatch"
	CmdStartNextGame        = "StartNextGame"
	CmdCancelRoom           = "CancelRoom"
	CmdPlayCard             = "PlayCard"
	CmdDrawCard             = "DrawCard"
	CmdChooseColor          = "ChooseColor"
	CmdCallUno              = "CallUno"
	CmdReportMissingUno     = "ReportMissingUno"
	CmdReconnectToRoom      = "ReconnectToRoom"
	CmdDisconnectPlayer     = "DisconnectPlayer"
	CmdExpireUnoWindow      = "ExpireUnoWindow"
	CmdForfeitPlayer        = "ForfeitPlayer"
	CmdSkipDisconnectedTurn = "SkipDisconnectedTurn"
)

// ServiceDeps wires application ports.
type ServiceDeps struct {
	Sessions  SessionRepository
	Commands  RoomCommandStore // durable only; nil keeps capability Memory + Service.mu path
	Integrity GameIntegrity
	Publisher EventPublisher
	Audit     AuditSink
	Deals     DealSource
	Clock     Clock
	SessionsV SessionValidator // optional; when set, forwarded SessionID is enforced
}

// Service is the Room Gameplay application service.
type Service struct {
	deps     ServiceDeps
	mu       sync.Mutex // serialize per-process command handling for in-memory repo
	outboxMu sync.Mutex // serialize DrainOutbox across worker + post-commit paths
}

// durable returns true when RoomCommandStore drives FOR UPDATE command transactions.
func (s *Service) durable() bool { return s.deps.Commands != nil }

// NewService constructs the application service.
func NewService(deps ServiceDeps) *Service {
	if deps.Clock == nil {
		deps.Clock = SystemClock{}
	}
	if deps.SessionsV == nil {
		deps.SessionsV = ClosedSessionValidator{}
	}
	return &Service{deps: deps}
}

// CommandInput is a decoded internal/BFF command dispatch.
type CommandInput struct {
	CommandID              string
	Type                   string
	SchemaVersion          int
	Payload                json.RawMessage
	PlayerID               string
	SessionID              string
	RoomID                 string
	ExpectedSequenceNumber *int64
	CorrelationID          string
	AsSystem               bool
}

// CommandResult is the application outcome (maps to envelope.Result).
type CommandResult struct {
	Result envelope.Result
	Err    error
}

// HandleCommand maps a catalog command onto Session/Room with GI append-before-commit.
func (s *Service) HandleCommand(ctx context.Context, in CommandInput) CommandResult {
	if s.durable() {
		return s.handleCommandDurable(ctx, in)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if strings.TrimSpace(in.CommandID) == "" || strings.TrimSpace(in.Type) == "" {
		return s.reject(ctx, in, "invalid_envelope", nil, nil)
	}
	if in.SchemaVersion != envelope.CurrentSchemaVersion {
		return s.reject(ctx, in, "invalid_schema_version", nil, nil)
	}

	// Stable replay of pre-room / global rejections — retry pending audit first.
	if prior, ok := s.deps.Sessions.GetGlobalOutcome(ctx, in.CommandID); ok {
		return s.replayPrior(ctx, in, prior)
	}

	switch in.Type {
	case CmdCreateRoom:
		return s.handleCreateRoom(ctx, in)
	case CmdJoinRoom, CmdLeaveRoom, CmdLockRoom, CmdCancelRoom,
		CmdStartMatch, CmdStartNextGame, CmdPlayCard, CmdDrawCard, CmdChooseColor,
		CmdCallUno, CmdReportMissingUno, CmdReconnectToRoom, CmdDisconnectPlayer,
		CmdExpireUnoWindow, CmdForfeitPlayer, CmdSkipDisconnectedTurn:
		return s.handleExisting(ctx, in)
	default:
		return s.reject(ctx, in, "unknown_command_type", nil, nil)
	}
}

// ProvisionInput is the tournament room provision request.
type ProvisionInput struct {
	CommandID     string
	TournamentID  string
	RoundNumber   int
	SlotID        string
	RoomID        string
	HostID        string
	PlayerIDs     []string
	Visibility    string
	MaxSeats      int
	CorrelationID string
}

// Provision handles POST /internal/v1/rooms/provision (idempotent by tournament/round/slot).
func (s *Service) Provision(ctx context.Context, in ProvisionInput) CommandResult {
	if s.durable() {
		return s.provisionDurable(ctx, in)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

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
			n := int64(sess.Room().Sequence())
			seq = n
		}
		return CommandResult{Result: envelope.Accepted(in.CommandID, "ProvisionTournamentRoom", &seq, MustJSON(map[string]string{
			"roomId": existing,
			"status": "duplicate",
		}))}
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
		return s.reject(ctx, cmdIn, reason, out.Rejection, nil)
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
			return s.reject(ctx, cmdIn, string(joinOut.Rejection.Code), joinOut.Rejection, nil)
		}
		out.Facts = append(out.Facts, joinOut.Facts...)
		out.Sequence = joinOut.Sequence
	}

	sess := domain.OpenSession(room)
	return s.commitAccepted(ctx, cmdIn, nil, sess, out, MaterialReservation{})
}

// PlayerSnapshot builds a player-private reconnect snapshot.
func (s *Service) PlayerSnapshot(ctx context.Context, roomID, playerID string) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.deps.Sessions.Get(ctx, domain.RoomID(roomID))
	if !ok {
		return nil, ErrNotFound
	}
	room := sess.Room()
	if !room.Roster().IsSeated(domain.PlayerID(playerID)) {
		if _, disc := room.DisconnectState(domain.PlayerID(playerID)); !disc {
			return nil, ErrForbidden
		}
	}
	snap := map[string]any{
		"roomId":         string(room.ID()),
		"status":         string(room.Status()),
		"sequenceNumber": int64(room.Sequence()),
		"visibility":     string(room.Visibility()),
		"hostId":         string(room.HostID()),
		"roomType":       string(room.Type()),
		"playerId":       playerID,
	}
	if room.TournamentID().Valid() {
		snap["tournamentId"] = string(room.TournamentID())
	}
	if room.RoundNumber() > 0 {
		snap["roundNumber"] = room.RoundNumber()
	}
	if room.SlotID() != "" {
		snap["slotId"] = room.SlotID()
	}
	seats := make([]map[string]any, 0)
	for _, seat := range room.Roster().Seats() {
		if !seat.Occupied {
			continue
		}
		seats = append(seats, map[string]any{
			"seatIndex": int(seat.Index),
			"playerId":  string(seat.PlayerID),
		})
	}
	snap["roster"] = seats
	if uw, ok := room.UnoWindow(); ok && uw.IsOpen() {
		snap["uno"] = map[string]any{
			"playerId":        string(uw.PlayerID),
			"expiresAt":       uw.ExpiresAt.UTC().Format(timeRFC3339Nano),
			"openingSequence": int64(uw.OpeningSequence),
		}
	}
	if g := sess.Game(); g != nil {
		pub := g.PublicSnapshot()
		snap["game"] = map[string]any{
			"gameId":         string(sess.GameID()),
			"sequenceNumber": int64(pub.Sequence),
			"discardTop":     pub.DiscardTop,
			"activeColor":    string(pub.ActiveColor),
			"currentPlayer":  string(pub.CurrentPlayer),
			"direction":      int(pub.Direction),
			"handCounts":     pub.HandCounts,
			"completed":      pub.Completed,
		}
		snap["hand"] = g.Hand(game.PlayerID(playerID))
	}
	if st, ok := room.DisconnectState(domain.PlayerID(playerID)); ok {
		snap["disconnect"] = map[string]any{
			"disconnectVersion": int64(st.DisconnectVersion),
			"deadlineUtc":       st.DeadlineUTC.UTC().Format(timeRFC3339Nano),
		}
	}
	return snap, nil
}

// DrainOutbox publishes pending outbox entries asynchronously with retry semantics.
// Stops on first publish failure so remaining entries stay pending.
// Serialized via outboxMu so concurrent worker + post-commit drains cannot reorder.
func (s *Service) DrainOutbox(ctx context.Context, limit int) (int, error) {
	if s.deps.Publisher == nil || s.deps.Sessions == nil {
		return 0, errors.New("outbox dependencies not configured")
	}
	s.outboxMu.Lock()
	defer s.outboxMu.Unlock()
	pending, err := s.deps.Sessions.ListPendingOutbox(ctx, limit)
	if err != nil {
		return 0, err
	}
	published := 0
	now := s.deps.Clock.Now()
	for _, entry := range pending {
		for _, ev := range entry.Events {
			pubCtx := WithCorrelation(ctx, correlation.Headers{
				CorrelationID: ev.CorrelationID,
				CommandID:     entry.CommandID,
			})
			if err := s.deps.Publisher.Publish(pubCtx, ev); err != nil {
				return published, err
			}
		}
		if err := s.deps.Sessions.MarkOutboxEntryPublished(ctx, entry.CommandID, now); err != nil {
			return published, err
		}
		published++
	}
	return published, nil
}

var (
	ErrNotFound  = errors.New("room not found")
	ErrForbidden = errors.New("forbidden")
)

const timeRFC3339Nano = "2006-01-02T15:04:05.999999999Z07:00"

func (s *Service) handleCreateRoom(ctx context.Context, in CommandInput) CommandResult {
	if err := s.validateSession(ctx, in); err != nil {
		return s.reject(ctx, in, "invalid_session", nil, nil)
	}
	var payload struct {
		RoomID     string `json:"roomId"`
		Visibility string `json:"visibility"`
		MaxSeats   int    `json:"maxSeats"`
	}
	_ = json.Unmarshal(in.Payload, &payload)
	roomID := payload.RoomID
	if roomID == "" {
		roomID = in.RoomID
	}
	if roomID == "" {
		return s.reject(ctx, in, "missing_room_id", nil, nil)
	}
	if in.PlayerID == "" {
		return s.reject(ctx, in, "missing_player_id", nil, nil)
	}
	if live, exists := s.deps.Sessions.Get(ctx, domain.RoomID(roomID)); exists {
		if prior, ok := live.Room().PriorOutcome(domain.CommandID(in.CommandID)); ok {
			return s.replayPrior(ctx, in, prior)
		}
		return s.reject(ctx, in, "room_already_exists", nil, live)
	}
	vis := domain.Visibility(payload.Visibility)
	if vis == "" {
		vis = domain.VisibilityPublic
	}
	room, out := domain.CreateRoom(domain.CreateRoomCommand{
		CommandID:  domain.CommandID(in.CommandID),
		RoomID:     domain.RoomID(roomID),
		HostID:     domain.PlayerID(in.PlayerID),
		Visibility: vis,
		MaxSeats:   payload.MaxSeats,
	})
	if out.Rejection != nil || room == nil {
		reason := "create_rejected"
		if out.Rejection != nil {
			reason = string(out.Rejection.Code)
		}
		return s.reject(ctx, in, reason, out.Rejection, nil)
	}
	sess := domain.OpenSession(room)
	in.RoomID = roomID
	return s.commitAccepted(ctx, in, nil, sess, out, MaterialReservation{})
}

func (s *Service) handleExisting(ctx context.Context, in CommandInput) CommandResult {
	if err := s.validateSession(ctx, in); err != nil {
		// Persist invalid-session on the room when it exists; otherwise globally.
		roomID := in.RoomID
		if roomID == "" {
			var p struct {
				RoomID string `json:"roomId"`
			}
			_ = json.Unmarshal(in.Payload, &p)
			roomID = p.RoomID
		}
		in.RoomID = roomID
		if roomID != "" {
			if live, ok := s.deps.Sessions.Get(ctx, domain.RoomID(roomID)); ok {
				if prior, ok := live.Room().PriorOutcome(domain.CommandID(in.CommandID)); ok {
					return s.replayPrior(ctx, in, prior)
				}
				return s.reject(ctx, in, "invalid_session", nil, live)
			}
		}
		return s.reject(ctx, in, "invalid_session", nil, nil)
	}
	roomID := in.RoomID
	if roomID == "" {
		var p struct {
			RoomID string `json:"roomId"`
		}
		_ = json.Unmarshal(in.Payload, &p)
		roomID = p.RoomID
	}
	if roomID == "" {
		return s.reject(ctx, in, "missing_room_id", nil, nil)
	}
	in.RoomID = roomID

	live, ok := s.deps.Sessions.Get(ctx, domain.RoomID(roomID))
	if !ok {
		return s.reject(ctx, in, "room_not_found", nil, nil)
	}

	// Stable replay of prior accepted/rejected outcomes — retry pending audit first.
	if prior, ok := live.Room().PriorOutcome(domain.CommandID(in.CommandID)); ok {
		return s.replayPrior(ctx, in, prior)
	}

	// Non-mutating prevalidation before any material reservation.
	if rej := s.prevalidate(live, in); rej != nil {
		return s.reject(ctx, in, string(rej.Code), rej, live)
	}

	var reservation MaterialReservation
	stage := live.Clone()
	out, err := s.apply(ctx, stage, in, &reservation)
	if err != nil {
		if reservation.ID != "" {
			_ = s.deps.Deals.Cancel(ctx, reservation.ID)
		}
		return CommandResult{Err: err}
	}
	if out.Rejection != nil {
		if reservation.ID != "" {
			_ = s.deps.Deals.Cancel(ctx, reservation.ID)
		}
		return s.reject(ctx, in, string(out.Rejection.Code), out.Rejection, live)
	}
	if out.Kind == domain.OutcomeDuplicate {
		if reservation.ID != "" {
			_ = s.deps.Deals.Cancel(ctx, reservation.ID)
		}
		return s.resultFromPrior(in, out)
	}
	if !out.Accepted() {
		if reservation.ID != "" {
			_ = s.deps.Deals.Cancel(ctx, reservation.ID)
		}
		return s.reject(ctx, in, "rejected", out.Rejection, live)
	}
	return s.commitAccepted(ctx, in, live, stage, out, reservation)
}

func (s *Service) prevalidate(sess *domain.Session, in CommandInput) *domain.Rejection {
	expected := sequenceOrZero(in.ExpectedSequenceNumber)
	player := domain.PlayerID(in.PlayerID)
	switch in.Type {
	case CmdStartMatch:
		var p struct {
			GameID string `json:"gameId"`
		}
		_ = json.Unmarshal(in.Payload, &p)
		gameID := p.GameID
		if gameID == "" {
			gameID = "game-" + in.CommandID
		}
		return sess.PrevalidateStartMatch(domain.StartMatchCommand{
			CommandID: domain.CommandID(in.CommandID), ActorID: player, AsSystem: in.AsSystem,
			GameID: domain.GameID(gameID), ExpectedSequence: expected,
		})
	case CmdStartNextGame:
		var p struct {
			GameID string `json:"gameId"`
		}
		_ = json.Unmarshal(in.Payload, &p)
		gameID := p.GameID
		if gameID == "" {
			gameID = "game-" + in.CommandID
		}
		return sess.PrevalidateStartNextGame(domain.StartNextGameCommand{
			CommandID: domain.CommandID(in.CommandID), GameID: domain.GameID(gameID), ExpectedSequence: expected,
		})
	case CmdDrawCard:
		return sess.PrevalidateDrawCard(domain.DrawCardCommand{
			CommandID: domain.CommandID(in.CommandID), PlayerID: player, ExpectedSequence: expected,
		})
	case CmdReportMissingUno:
		return sess.PrevalidateReportMissingUno(domain.ReportMissingUnoCommand{
			CommandID: domain.CommandID(in.CommandID), ChallengerID: player, ExpectedSequence: expected,
		})
	default:
		return nil
	}
}

func (s *Service) apply(ctx context.Context, sess *domain.Session, in CommandInput, reservation *MaterialReservation) (domain.CommandOutcome, error) {
	now := s.deps.Clock.Now()
	expected := sequenceOrZero(in.ExpectedSequenceNumber)
	room := sess.Room()
	player := domain.PlayerID(in.PlayerID)

	switch in.Type {
	case CmdJoinRoom:
		return room.JoinRoom(domain.JoinRoomCommand{
			CommandID: domain.CommandID(in.CommandID), PlayerID: player, ExpectedSequence: expected,
		}), nil
	case CmdLeaveRoom:
		return room.LeaveRoom(domain.LeaveRoomCommand{
			CommandID: domain.CommandID(in.CommandID), PlayerID: player, ExpectedSequence: expected,
		}), nil
	case CmdLockRoom:
		return room.LockRoom(domain.LockRoomCommand{
			CommandID: domain.CommandID(in.CommandID), ActorID: player, AsSystem: in.AsSystem, ExpectedSequence: expected,
		}), nil
	case CmdCancelRoom:
		return room.CancelRoom(domain.CancelRoomCommand{
			CommandID: domain.CommandID(in.CommandID), ActorID: player, AsSystem: in.AsSystem, ExpectedSequence: expected,
		}), nil
	case CmdStartMatch:
		var p struct {
			GameID string `json:"gameId"`
		}
		_ = json.Unmarshal(in.Payload, &p)
		gameID := p.GameID
		if gameID == "" {
			gameID = "game-" + in.CommandID
		}
		seats := seatedIDs(room)
		res, err := s.deps.Deals.ReserveDeal(ctx, string(room.ID()), gameID, StableOperationID(in.CommandID, PurposeDeal), seats)
		if err != nil {
			return domain.CommandOutcome{}, err
		}
		*reservation = res
		return sess.StartMatch(domain.StartMatchCommand{
			CommandID: domain.CommandID(in.CommandID), ActorID: player, AsSystem: in.AsSystem,
			GameID: domain.GameID(gameID), ExpectedSequence: expected,
		}, *res.Deal), nil
	case CmdStartNextGame:
		var p struct {
			GameID string `json:"gameId"`
		}
		_ = json.Unmarshal(in.Payload, &p)
		gameID := p.GameID
		if gameID == "" {
			gameID = "game-" + in.CommandID
		}
		seats := seatedIDs(room)
		res, err := s.deps.Deals.ReserveDeal(ctx, string(room.ID()), gameID, StableOperationID(in.CommandID, PurposeDeal), seats)
		if err != nil {
			return domain.CommandOutcome{}, err
		}
		*reservation = res
		return sess.StartNextGame(domain.StartNextGameCommand{
			CommandID: domain.CommandID(in.CommandID), GameID: domain.GameID(gameID),
			ExpectedSequence: expected, Deal: *res.Deal,
		}), nil
	case CmdPlayCard:
		var p struct {
			CardID string `json:"cardId"`
		}
		_ = json.Unmarshal(in.Payload, &p)
		return sess.PlayCard(domain.PlayCardCommand{
			CommandID: domain.CommandID(in.CommandID), PlayerID: player,
			CardID: game.CardID(p.CardID), ExpectedSequence: expected, NowUTC: now,
		}), nil
	case CmdDrawCard:
		count := 1
		if g := sess.Game(); g != nil && g.PenaltyAmount() > 0 {
			count = g.PenaltyAmount()
		}
		res, err := s.deps.Deals.ReserveDraw(ctx, string(room.ID()), string(sess.GameID()), StableOperationID(in.CommandID, PurposeDraw), count)
		if err != nil {
			return domain.CommandOutcome{}, err
		}
		*reservation = res
		return sess.DrawCard(domain.DrawCardCommand{
			CommandID: domain.CommandID(in.CommandID), PlayerID: player,
			Cards: res.Cards, ExpectedSequence: expected,
		}), nil
	case CmdChooseColor:
		var p struct {
			Color string `json:"color"`
		}
		_ = json.Unmarshal(in.Payload, &p)
		return sess.ChooseColor(domain.ChooseColorCommand{
			CommandID: domain.CommandID(in.CommandID), PlayerID: player,
			Color: game.Color(p.Color), ExpectedSequence: expected,
		}), nil
	case CmdCallUno:
		return sess.CallUno(domain.CallUnoCommand{
			CommandID: domain.CommandID(in.CommandID), PlayerID: player,
			ExpectedSequence: expected, NowUTC: now,
		}), nil
	case CmdReportMissingUno:
		var p struct {
			TargetID string `json:"targetId"`
		}
		_ = json.Unmarshal(in.Payload, &p)
		res, err := s.deps.Deals.ReserveDraw(ctx, string(room.ID()), string(sess.GameID()), StableOperationID(in.CommandID, PurposeUnoPenalty), 2)
		if err != nil {
			return domain.CommandOutcome{}, err
		}
		*reservation = res
		return sess.ReportMissingUno(domain.ReportMissingUnoCommand{
			CommandID: domain.CommandID(in.CommandID), ChallengerID: player,
			TargetID: domain.PlayerID(p.TargetID), Cards: res.Cards,
			ExpectedSequence: expected, NowUTC: now,
		}), nil
	case CmdReconnectToRoom:
		var p struct {
			DisconnectVersion uint64 `json:"disconnectVersion"`
		}
		_ = json.Unmarshal(in.Payload, &p)
		return room.ReconnectPlayer(domain.ReconnectPlayerCommand{
			CommandID: domain.CommandID(in.CommandID), PlayerID: player,
			DisconnectVersion: domain.DisconnectVersion(p.DisconnectVersion),
			NowUTC:            now, ExpectedSequence: expected,
		}), nil
	case CmdDisconnectPlayer:
		return room.DisconnectPlayer(domain.DisconnectPlayerCommand{
			CommandID: domain.CommandID(in.CommandID), PlayerID: player,
			NowUTC: now, ExpectedSequence: expected,
		}), nil
	case CmdExpireUnoWindow:
		var p struct {
			PlayerID              string `json:"playerId"`
			GameID                string `json:"gameId"`
			TriggeringGameEventID string `json:"triggeringGameEventId"`
			OpeningSequence       uint64 `json:"openingSequence"`
		}
		_ = json.Unmarshal(in.Payload, &p)
		pid := domain.PlayerID(p.PlayerID)
		if pid == "" {
			pid = player
		}
		return sess.ExpireUnoWindow(domain.ExpireUnoWindowCommand{
			CommandID: domain.CommandID(in.CommandID), PlayerID: pid,
			GameID: domain.GameID(p.GameID), TriggeringGameEventID: domain.TriggeringGameEventID(p.TriggeringGameEventID),
			OpeningSequence: domain.SequenceNumber(p.OpeningSequence), NowUTC: now, ExpectedSequence: expected,
		}), nil
	case CmdForfeitPlayer:
		var p struct {
			PlayerID          string `json:"playerId"`
			DisconnectVersion uint64 `json:"disconnectVersion"`
		}
		_ = json.Unmarshal(in.Payload, &p)
		pid := domain.PlayerID(p.PlayerID)
		if pid == "" {
			pid = player
		}
		return sess.ForfeitPlayer(domain.ForfeitPlayerCommand{
			CommandID: domain.CommandID(in.CommandID), PlayerID: pid,
			DisconnectVersion: domain.DisconnectVersion(p.DisconnectVersion),
			NowUTC:            now, ExpectedSequence: expected,
		}), nil
	case CmdSkipDisconnectedTurn:
		var p struct {
			PlayerID    string `json:"playerId"`
			TurnVersion uint64 `json:"turnVersion"`
		}
		_ = json.Unmarshal(in.Payload, &p)
		pid := domain.PlayerID(p.PlayerID)
		if pid == "" {
			pid = player
		}
		return sess.SkipDisconnectedTurn(domain.SkipDisconnectedTurnCommand{
			CommandID: domain.CommandID(in.CommandID), PlayerID: pid,
			TurnVersion: domain.SequenceNumber(p.TurnVersion), ExpectedSequence: expected,
		}), nil
	default:
		return domain.CommandOutcome{}, fmt.Errorf("unhandled command type %s", in.Type)
	}
}

func (s *Service) commitAccepted(
	ctx context.Context,
	in CommandInput,
	live *domain.Session,
	stage *domain.Session,
	out domain.CommandOutcome,
	reservation MaterialReservation,
) CommandResult {
	roomID := string(stage.Room().ID())
	expectedRev := s.deps.Sessions.IntegrityRevision(ctx, roomID)
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
	gameID := string(stage.GameID())
	appendRes, err := s.deps.Integrity.Append(ctx, AppendRequest{
		RoomID:           roomID,
		GameID:           gameID,
		EventID:          in.CommandID,
		ExpectedRevision: expectedRev,
		EventType:        in.Type,
		Payload:          payload,
	})
	if err != nil {
		// Do not cancel: retry reuses the same reservation and GI duplicate append.
		return CommandResult{Err: fmt.Errorf("game integrity append failed: %w", err)}
	}

	seq := int64(out.Sequence)
	prevSeq := int64(0)
	if live != nil && live.Room() != nil {
		prevSeq = int64(live.Room().Sequence())
	}
	noop := len(out.Facts) == 0 && seq == prevSeq

	if reservation.ID != "" {
		if noop {
			// No-op did not consume material; release rather than confirm.
			_ = s.deps.Deals.Cancel(ctx, reservation.ID)
		} else if err := s.deps.Deals.Confirm(ctx, reservation.ID); err != nil {
			// Do not commit or cancel: retry reuses reservation and retries confirm.
			return CommandResult{Err: fmt.Errorf("reservation confirm failed: %w", err)}
		}
	}

	audiences := s.feedAudiences(ctx, roomID, stage.Room(), in)
	playerStart := s.deps.Sessions.PeekStreamSeq(ctx, roomID) + 1
	feedEvents, playerHighWater := BuildFeedEvents(stage, seq, playerStart, in.CorrelationID, in.CommandID, out.Facts, audiences, prevSeq)
	completion := BuildCompletionEvents(stage.Room(), stage.Game(), gameID, seq, in.CorrelationID, in.CommandID, out.Facts, s.deps.Clock.Now())
	all := append(feedEvents, completion...)

	req := CommitRequest{
		Session:              stage,
		PlayerID:             in.PlayerID,
		PlayerSessionID:      in.SessionID,
		BindPlayerSession:    !in.AsSystem && in.PlayerID != "" && in.SessionID != "",
		IntegrityRevision:    appendRes.Revision,
		SetIntegrityRevision: true,
		StreamSeqHighWater:   playerHighWater,
		SetStreamSeq:         playerHighWater > 0,
	}
	// Never enqueue an empty outbox row — it blocks later real-event drains.
	if len(all) > 0 {
		req.Outbox = OutboxEntry{
			RoomID: roomID, CommandID: in.CommandID, LogOffset: appendRes.LogOffset, Events: all,
		}
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
	if err := s.deps.Sessions.Commit(ctx, req); err != nil {
		// Reservation already confirmed; do not cancel. Retry gets confirmed material and commits locally.
		return CommandResult{Err: err}
	}

	// Async publish from pending outbox (errors leave entries pending for retry).
	_, _ = s.DrainOutbox(ctx, 1)

	return CommandResult{Result: envelope.Accepted(in.CommandID, in.Type, &seq, MustJSON(map[string]any{
		"facts": factNames(out.Facts),
	}))}
}

func (s *Service) feedAudiences(ctx context.Context, roomID string, room *domain.Room, in CommandInput) []FeedAudience {
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
	// System/timer commands must not inject the timer input session as a player
	// feed audience — only durable stored player bindings are authoritative.
	if !in.AsSystem && in.PlayerID != "" && in.SessionID != "" {
		add(in.PlayerID, in.SessionID)
	}
	if room != nil {
		for _, seat := range room.Roster().Seats() {
			if !seat.Occupied {
				continue
			}
			pid := string(seat.PlayerID)
			if sid, ok := s.deps.Sessions.PlayerSession(ctx, roomID, pid); ok {
				add(pid, sid)
			}
		}
	}
	return out
}

func (s *Service) validateSession(ctx context.Context, in CommandInput) error {
	if in.AsSystem {
		return nil
	}
	// Player commands require both identifiers unless explicitly trusted as system.
	if in.SessionID == "" || in.PlayerID == "" {
		return errors.New("session and player required")
	}
	return s.deps.SessionsV.Validate(ctx, in.SessionID, in.PlayerID)
}

func (s *Service) resultFromPrior(in CommandInput, prior domain.CommandOutcome) CommandResult {
	seq := int64(prior.Sequence)
	if prior.Kind == domain.OutcomeRejected || prior.Rejection != nil {
		reason := "rejected"
		if prior.Rejection != nil && prior.Rejection.Code != "" {
			reason = string(prior.Rejection.Code)
		}
		return CommandResult{Result: envelope.Rejected(in.CommandID, in.Type, reason, &seq)}
	}
	res := envelope.Result{
		CommandID:     in.CommandID,
		Type:          in.Type,
		Status:        envelope.StatusAccepted,
		SchemaVersion: envelope.CurrentSchemaVersion,
		Sequence:      &seq,
	}
	return CommandResult{Result: res}
}

// replayPrior returns a stable prior outcome, retrying any pending rejection audit first.
func (s *Service) replayPrior(ctx context.Context, in CommandInput, prior domain.CommandOutcome) CommandResult {
	if prior.Kind == domain.OutcomeRejected || prior.Rejection != nil {
		if err := s.deliverPendingAudit(ctx, in.CommandID); err != nil {
			return CommandResult{Err: err}
		}
	}
	return s.resultFromPrior(in, prior)
}

func (s *Service) deliverPendingAudit(ctx context.Context, commandID string) error {
	rec, ok := s.deps.Sessions.GetPendingAudit(ctx, commandID)
	if !ok {
		return nil
	}
	if s.deps.Audit == nil {
		return errors.New("audit sink not configured")
	}
	if err := s.deps.Audit.Record(ctx, rec); err != nil {
		return err
	}
	return s.deps.Sessions.MarkAuditComplete(ctx, commandID)
}

func (s *Service) reject(
	ctx context.Context,
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

	// Exact replay of a prior rejection: retry pending audit, never re-emit after complete.
	if live != nil && room != nil {
		if prior, ok := room.PriorOutcome(domain.CommandID(in.CommandID)); ok {
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

	// Atomically persist rejection outcome + audit-pending record keyed by commandId.
	commitReq := CommitRequest{PendingAudit: &rec}
	if live != nil && room != nil {
		live.Room().RememberOutcome(out)
		commitReq.Session = live
	} else {
		commitReq.GlobalOutcome = &out
	}
	if err := s.deps.Sessions.Commit(ctx, commitReq); err != nil {
		return CommandResult{Err: err}
	}

	if s.deps.Audit == nil {
		return CommandResult{Err: errors.New("audit sink not configured")}
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

func sequenceOrZero(n *int64) domain.SequenceNumber {
	if n == nil {
		return 0
	}
	return domain.SequenceNumber(*n)
}

func seatedIDs(room *domain.Room) []string {
	seats := room.Roster().Seats()
	out := make([]string, 0, len(seats))
	for _, seat := range seats {
		if seat.Occupied {
			out = append(out, string(seat.PlayerID))
		}
	}
	return out
}

func factNames(facts []domain.Fact) []string {
	out := make([]string, len(facts))
	for i, f := range facts {
		out[i] = string(f.Name)
	}
	return out
}

func corrOrCmd(in CommandInput) string {
	if in.CorrelationID != "" {
		return in.CorrelationID
	}
	return in.CommandID
}
