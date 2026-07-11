package domain

import (
	"encoding/json"
	"time"
)

// SpectatorSafeEvent is a versioned inbound room.spectator-safe event.
type SpectatorSafeEvent struct {
	EventID       EventID
	EventType     EventType
	SchemaVersion int
	RoomID        RoomID
	Sequence      SequenceNumber
	Payload       map[string]any
}

// VisibleSeat is a public roster seat in the spectator projection.
type VisibleSeat struct {
	SeatIndex   int      `json:"seatIndex"`
	PlayerID    PlayerID `json:"playerId"`
	DisplayName string   `json:"displayName"`
	CardCount   int      `json:"cardCount"`
	Occupied    bool     `json:"occupied"`
}

// VisibleDiscardState is the public discard top and active color.
type VisibleDiscardState struct {
	DiscardTop  string `json:"discardTop"`
	ActiveColor string `json:"activeColor"`
}

// PublicUnoWindow exposes absolute expiry and opening sequence only.
type PublicUnoWindow struct {
	PlayerID        PlayerID       `json:"playerId"`
	ExpiresAt       time.Time      `json:"expiresAt"`
	OpeningSequence SequenceNumber `json:"openingSequence"`
	Called          bool           `json:"called"`
}

// SanitizedSnapshot is the privacy-filtered public projection view.
type SanitizedSnapshot struct {
	RoomID          RoomID              `json:"roomId"`
	Status          RoomStatus          `json:"status"`
	Visibility      Visibility          `json:"visibility"`
	Sequence        SequenceNumber      `json:"sequence"`
	Seats           []VisibleSeat       `json:"seats"`
	Discard         VisibleDiscardState `json:"discard"`
	Direction       string              `json:"direction"`
	CurrentPlayerID PlayerID            `json:"currentPlayerId"`
	PenaltyAmount   int                 `json:"penaltyAmount"`
	PenaltyTarget   PlayerID            `json:"penaltyTarget"`
	GameScore       map[PlayerID]int    `json:"gameScore"`
	MatchWinner     PlayerID            `json:"matchWinner"`
	GameCompleted   bool                `json:"gameCompleted"`
	MatchCompleted  bool                `json:"matchCompleted"`
	Uno             *PublicUnoWindow    `json:"uno,omitempty"`
	StreamClosed    bool                `json:"streamClosed"`
}

// SpectatorRoomProjection is the aggregate consistency boundary for one room's
// spectator-safe read model. Pure stdlib; not goroutine-safe.
type SpectatorRoomProjection struct {
	roomID     RoomID
	status     RoomStatus
	visibility Visibility
	sequence   SequenceNumber

	seats          []VisibleSeat
	discard        VisibleDiscardState
	direction      string
	currentPlayer  PlayerID
	penaltyAmount  int
	penaltyTarget  PlayerID
	gameScore      map[PlayerID]int
	matchWinner    PlayerID
	gameCompleted  bool
	matchCompleted bool
	uno            *PublicUnoWindow
	streamClosed   bool

	policy   SpectatorVisibilityPolicy
	outcomes map[EventID]ApplyOutcome
}

// NewSpectatorRoomProjection creates an empty projection for roomID.
func NewSpectatorRoomProjection(roomID RoomID) *SpectatorRoomProjection {
	return &SpectatorRoomProjection{
		roomID:     roomID,
		status:     RoomStatusWaiting,
		visibility: VisibilityPublic,
		sequence:   0,
		seats:      nil,
		gameScore:  map[PlayerID]int{},
		policy:     SpectatorVisibilityPolicy{},
		outcomes:   map[EventID]ApplyOutcome{},
	}
}

// RebuildFrom creates a projection and reapplies sanitized events in order.
func RebuildFrom(roomID RoomID, events []SpectatorSafeEvent) (*SpectatorRoomProjection, []ApplyOutcome) {
	p := NewSpectatorRoomProjection(roomID)
	return p, p.RebuildFrom(events)
}

// RebuildFrom resets this projection and reapplies sanitized events in order.
func (p *SpectatorRoomProjection) RebuildFrom(events []SpectatorSafeEvent) []ApplyOutcome {
	roomID := p.roomID
	*p = *NewSpectatorRoomProjection(roomID)
	outs := make([]ApplyOutcome, 0, len(events))
	for _, evt := range events {
		outs = append(outs, p.Apply(evt))
	}
	return outs
}

// RoomID returns the projected room identity.
func (p *SpectatorRoomProjection) RoomID() RoomID { return p.roomID }

// Status returns the projected room status.
func (p *SpectatorRoomProjection) Status() RoomStatus { return p.status }

// Sequence returns the last applied upstream sequence.
func (p *SpectatorRoomProjection) Sequence() SequenceNumber { return p.sequence }

// StreamClosed reports whether terminal stream-close has been signaled.
func (p *SpectatorRoomProjection) StreamClosed() bool { return p.streamClosed }

// Admission evaluates spectator admission against current projected status/visibility.
func (p *SpectatorRoomProjection) Admission(auth SpectatorAuth) SpectatorAdmissionDecision {
	a := auth
	if p.visibility == VisibilityPublic {
		a.IsPublicRoom = true
	} else {
		a.IsPublicRoom = false
	}
	return EvaluateSpectatorAdmission(p.status, a, p.participantSet())
}

// ParticipantIDs returns the trusted projected roster player ids.
func (p *SpectatorRoomProjection) ParticipantIDs() []PlayerID {
	out := make([]PlayerID, 0, len(p.seats))
	for _, s := range p.seats {
		if s.PlayerID.Valid() {
			out = append(out, s.PlayerID)
		}
	}
	return out
}

func (p *SpectatorRoomProjection) participantSet() map[PlayerID]struct{} {
	out := make(map[PlayerID]struct{}, len(p.seats))
	for _, s := range p.seats {
		if s.PlayerID.Valid() {
			out[s.PlayerID] = struct{}{}
		}
	}
	return out
}

// Apply consumes one versioned spectator-safe event with monotonic sequence rules.
// Duplicate eventId returns the prior stable outcome. Stale sequences are ignored.
// Privacy/schema violations are dropped before state mutation. Out-of-order gaps
// are quarantined by explicit typed outcome.
func (p *SpectatorRoomProjection) Apply(evt SpectatorSafeEvent) ApplyOutcome {
	if prior, ok := p.outcomes[evt.EventID]; ok {
		return duplicateOutcome(prior)
	}

	if rej := p.policy.ValidateEnvelope(evt); rej != nil {
		if rej.Code == RejectUnknownEventType {
			return p.drop(evt, *rej)
		}
		return p.quarantine(evt, *rej)
	}
	if rej := p.policy.ValidateEventType(evt.EventType); rej != nil {
		return p.drop(evt, *rej)
	}
	if evt.RoomID != p.roomID {
		return p.quarantine(evt, Rejection{
			Code:    RejectRoomMismatch,
			Message: "event roomId does not match projection",
		})
	}
	if rej := p.policy.ValidatePayload(evt.Payload); rej != nil {
		return p.drop(evt, *rej)
	}

	return p.applyValidated(evt)
}

// ApplyBatch applies multiple facts that share one room sequence atomically.
// Sequence advances once; one SpectatorRoomProjectionUpdated is emitted on success.
// Any validation failure leaves projection state unchanged.
func (p *SpectatorRoomProjection) ApplyBatch(events []SpectatorSafeEvent) ApplyOutcome {
	if len(events) == 0 {
		return ApplyOutcome{
			Kind: OutcomeQuarantined,
			Rejection: &Rejection{
				Code:    RejectInvalidSchema,
				Message: "batch requires at least one fact",
			},
			Facts: []Fact{},
		}
	}
	seq := events[0].Sequence
	batchID := events[0].EventID
	for _, evt := range events {
		if evt.Sequence != seq {
			return ApplyOutcome{
				Kind:     OutcomeQuarantined,
				EventID:  batchID,
				Sequence: p.sequence,
				Rejection: &Rejection{
					Code:    RejectInvalidSchema,
					Message: "batch facts must share one sequence",
				},
				Facts: []Fact{},
			}
		}
		if prior, ok := p.outcomes[evt.EventID]; ok {
			return duplicateOutcome(prior)
		}
	}

	// Validate all before mutating (atomic — no outcome side effects on failure).
	for _, evt := range events {
		if rej := p.policy.ValidateEnvelope(evt); rej != nil {
			return ApplyOutcome{
				Kind:      outcomeKindForRejection(rej.Code),
				EventID:   evt.EventID,
				Sequence:  p.sequence,
				Rejection: rej,
				Facts:     []Fact{},
			}
		}
		if rej := p.policy.ValidateEventType(evt.EventType); rej != nil {
			return ApplyOutcome{
				Kind:      OutcomeDropped,
				EventID:   evt.EventID,
				Sequence:  p.sequence,
				Rejection: rej,
				Facts:     []Fact{},
			}
		}
		if evt.RoomID != p.roomID {
			rej := Rejection{Code: RejectRoomMismatch, Message: "event roomId does not match projection"}
			return ApplyOutcome{
				Kind:      OutcomeQuarantined,
				EventID:   evt.EventID,
				Sequence:  p.sequence,
				Rejection: &rej,
				Facts:     []Fact{},
			}
		}
		if rej := p.policy.ValidatePayload(evt.Payload); rej != nil {
			return ApplyOutcome{
				Kind:      OutcomeDropped,
				EventID:   evt.EventID,
				Sequence:  p.sequence,
				Rejection: rej,
				Facts:     []Fact{},
			}
		}
	}

	if p.sequence > 0 && seq <= p.sequence {
		out := ApplyOutcome{
			Kind:     OutcomeIgnored,
			EventID:  batchID,
			Sequence: p.sequence,
			Rejection: &Rejection{
				Code:    RejectStaleSequence,
				Message: "stale or already-applied sequence ignored",
			},
			Facts: []Fact{},
		}
		for _, evt := range events {
			p.outcomes[evt.EventID] = out
		}
		return copyOutcome(out)
	}
	if p.sequence > 0 && seq > p.sequence+1 {
		return p.quarantine(events[0], Rejection{
			Code:    RejectOutOfOrderSequence,
			Message: "out-of-order sequence quarantined",
		})
	}
	if p.sequence == 0 && seq != 1 {
		return p.quarantine(events[0], Rejection{
			Code:    RejectOutOfOrderSequence,
			Message: "first event sequence must be 1",
		})
	}

	terminal := false
	var terminalType EventType
	for _, evt := range events {
		p.applyPayload(evt)
		if IsTerminalEvent(evt.EventType) {
			terminal = true
			terminalType = evt.EventType
		}
	}
	p.sequence = seq

	facts := []Fact{
		newFact(FactSpectatorRoomProjectionUpdated, map[string]string{
			"eventId":  string(batchID),
			"roomId":   string(p.roomID),
			"sequence": u64toa(uint64(p.sequence)),
			"status":   string(p.status),
		}),
	}
	if terminal {
		p.streamClosed = true
		p.matchCompleted = true
		if p.status != RoomStatusCancelled && terminalType != EventRoomCancelled {
			p.status = RoomStatusCompleted
		}
		facts = append(facts, newFact(FactStreamClose, map[string]string{
			"eventId": string(batchID),
			"roomId":  string(p.roomID),
			"status":  string(p.status),
			"reason":  string(terminalType),
		}))
	}

	out := ApplyOutcome{
		Kind:     OutcomeAccepted,
		EventID:  batchID,
		Sequence: p.sequence,
		Facts:    facts,
	}
	for _, evt := range events {
		p.outcomes[evt.EventID] = out
	}
	return copyOutcome(out)
}

func (p *SpectatorRoomProjection) applyValidated(evt SpectatorSafeEvent) ApplyOutcome {
	if p.sequence > 0 && evt.Sequence <= p.sequence {
		out := ApplyOutcome{
			Kind:     OutcomeIgnored,
			EventID:  evt.EventID,
			Sequence: p.sequence,
			Rejection: &Rejection{
				Code:    RejectStaleSequence,
				Message: "stale or already-applied sequence ignored",
			},
			Facts: []Fact{},
		}
		p.outcomes[evt.EventID] = out
		return copyOutcome(out)
	}
	if p.sequence > 0 && evt.Sequence > p.sequence+1 {
		return p.quarantine(evt, Rejection{
			Code:    RejectOutOfOrderSequence,
			Message: "out-of-order sequence quarantined",
		})
	}
	if p.sequence == 0 && evt.Sequence != 1 {
		return p.quarantine(evt, Rejection{
			Code:    RejectOutOfOrderSequence,
			Message: "first event sequence must be 1",
		})
	}

	p.applyPayload(evt)
	p.sequence = evt.Sequence

	facts := []Fact{
		newFact(FactSpectatorRoomProjectionUpdated, map[string]string{
			"eventId":  string(evt.EventID),
			"roomId":   string(p.roomID),
			"sequence": u64toa(uint64(p.sequence)),
			"status":   string(p.status),
		}),
	}
	if IsTerminalEvent(evt.EventType) {
		p.streamClosed = true
		p.matchCompleted = true
		if evt.EventType != EventRoomCancelled && p.status != RoomStatusCancelled {
			if p.status != RoomStatusCompleted {
				p.status = RoomStatusCompleted
			}
		}
		facts = append(facts, newFact(FactStreamClose, map[string]string{
			"eventId": string(evt.EventID),
			"roomId":  string(p.roomID),
			"status":  string(p.status),
			"reason":  string(evt.EventType),
		}))
	}

	out := ApplyOutcome{
		Kind:     OutcomeAccepted,
		EventID:  evt.EventID,
		Sequence: p.sequence,
		Facts:    facts,
	}
	p.outcomes[evt.EventID] = out
	return copyOutcome(out)
}

// Snapshot returns a defensive copy of the sanitized public projection.
func (p *SpectatorRoomProjection) Snapshot() SanitizedSnapshot {
	seats := make([]VisibleSeat, len(p.seats))
	copy(seats, p.seats)

	score := make(map[PlayerID]int, len(p.gameScore))
	for k, v := range p.gameScore {
		score[k] = v
	}

	var uno *PublicUnoWindow
	if p.uno != nil {
		cp := *p.uno
		uno = &cp
	}

	return SanitizedSnapshot{
		RoomID:          p.roomID,
		Status:          p.status,
		Visibility:      p.visibility,
		Sequence:        p.sequence,
		Seats:           seats,
		Discard:         p.discard,
		Direction:       p.direction,
		CurrentPlayerID: p.currentPlayer,
		PenaltyAmount:   p.penaltyAmount,
		PenaltyTarget:   p.penaltyTarget,
		GameScore:       score,
		MatchWinner:     p.matchWinner,
		GameCompleted:   p.gameCompleted,
		MatchCompleted:  p.matchCompleted,
		Uno:             uno,
		StreamClosed:    p.streamClosed,
	}
}

// SnapshotJSON encodes the public snapshot; private fields are absent by construction.
func (p *SpectatorRoomProjection) SnapshotJSON() ([]byte, error) {
	return json.Marshal(p.Snapshot())
}

// MarshalSanitizedJSON is an alias for SnapshotJSON.
func (p *SpectatorRoomProjection) MarshalSanitizedJSON() ([]byte, error) {
	return p.SnapshotJSON()
}

func (p *SpectatorRoomProjection) drop(evt SpectatorSafeEvent, rej Rejection) ApplyOutcome {
	facts := []Fact{
		newFact(FactProjectionEventQuarantined, map[string]string{
			"eventId": string(evt.EventID),
			"roomId":  string(evt.RoomID),
			"code":    string(rej.Code),
			"reason":  rej.Message,
		}),
		newFact(FactSpectatorEventDropped, map[string]string{
			"eventId": string(evt.EventID),
			"roomId":  string(evt.RoomID),
			"code":    string(rej.Code),
			"reason":  rej.Message,
		}),
	}
	out := ApplyOutcome{
		Kind:      OutcomeDropped,
		EventID:   evt.EventID,
		Sequence:  p.sequence,
		Rejection: &rej,
		Facts:     facts,
	}
	if evt.EventID.Valid() {
		p.outcomes[evt.EventID] = out
	}
	return copyOutcome(out)
}

func (p *SpectatorRoomProjection) quarantine(evt SpectatorSafeEvent, rej Rejection) ApplyOutcome {
	facts := []Fact{
		newFact(FactProjectionEventQuarantined, map[string]string{
			"eventId": string(evt.EventID),
			"roomId":  string(evt.RoomID),
			"code":    string(rej.Code),
			"reason":  rej.Message,
		}),
	}
	out := ApplyOutcome{
		Kind:      OutcomeQuarantined,
		EventID:   evt.EventID,
		Sequence:  p.sequence,
		Rejection: &rej,
		Facts:     facts,
	}
	if evt.EventID.Valid() {
		p.outcomes[evt.EventID] = out
	}
	return copyOutcome(out)
}

func (p *SpectatorRoomProjection) applyPayload(evt SpectatorSafeEvent) {
	pl := evt.Payload
	if pl == nil {
		pl = map[string]any{}
	}

	switch evt.EventType {
	case EventRoomCreated:
		p.status = RoomStatusWaiting
		if v, ok := stringField(pl, "visibility"); ok {
			p.visibility = Visibility(v)
		}
		p.applyRoster(pl)
	case EventRoomLocked:
		p.status = RoomStatusLocked
		if v, ok := stringField(pl, "status"); ok {
			p.status = RoomStatus(v)
		}
	case EventMatchStarted, EventGameStarted:
		p.status = RoomStatusInProgress
		p.gameCompleted = false
		if v, ok := stringField(pl, "status"); ok {
			p.status = RoomStatus(v)
		}
		p.applyPublicGameState(pl)
		p.applyScores(pl)
		p.applyUno(pl)
	case EventPlayerJoinedRoom, EventPlayerLeftRoom, EventHostReassigned,
		EventPlayerDisconnected, EventPlayerReconnected, EventPlayerForfeited:
		p.applyRoster(pl)
		p.applyPublicGameState(pl)
	case EventCardPlayed, EventTurnAdvanced, EventColorChosen, EventDirectionChanged, EventPenaltyUpdated:
		p.applyPublicGameState(pl)
		p.applyUno(pl)
	case EventUnoWindowOpened:
		p.applyUno(pl)
	case EventUnoWindowClosed, EventUnoWindowExpired:
		p.uno = nil
	case EventGameCompleted:
		p.gameCompleted = true
		p.applyScores(pl)
		p.uno = nil
		if !p.status.IsTerminal() {
			p.status = RoomStatusInProgress
		}
	case EventMatchCompleted:
		// Map MatchCompleted into terminal match semantics; only public score/winner.
		p.status = RoomStatusCompleted
		p.matchCompleted = true
		p.applyRoster(pl)
		p.applyPublicGameState(pl)
		p.applyScores(pl)
		p.applyPublicWinner(pl)
		p.uno = nil
	case EventRoomCompleted:
		p.status = RoomStatusCompleted
		p.matchCompleted = true
		p.applyFullSnapshot(pl)
		p.uno = nil
	case EventSpectatorStreamsClose:
		p.matchCompleted = true
		p.streamClosed = true
		if !p.status.IsTerminal() {
			p.status = RoomStatusCompleted
		}
		p.uno = nil
	case EventRoomCancelled:
		p.status = RoomStatusCancelled
		p.matchCompleted = true
		p.applyFullSnapshot(pl)
		p.uno = nil
	case EventSnapshotSanitized:
		p.applyFullSnapshot(pl)
	default:
		p.applyPublicGameState(pl)
		p.applyRoster(pl)
		p.applyUno(pl)
	}
}

// applyFullSnapshot applies the canonical sanitized projection fields from one
// SnapshotSanitized / terminal room event payload.
func (p *SpectatorRoomProjection) applyFullSnapshot(pl map[string]any) {
	if v, ok := stringField(pl, "status"); ok {
		p.status = RoomStatus(v)
	}
	if v, ok := stringField(pl, "roomStatus"); ok {
		p.status = RoomStatus(v)
	}
	if v, ok := stringField(pl, "visibility"); ok {
		p.visibility = Visibility(v)
	}
	if gc, ok := boolField(pl, "gameCompleted"); ok {
		p.gameCompleted = gc
		if gc && !p.status.IsTerminal() {
			p.status = RoomStatusInProgress
		}
	}
	p.applyRoster(pl)
	p.applyPublicGameState(pl)
	p.applyScores(pl)
	if _, hasUno := pl["unoWindow"]; hasUno {
		p.applyUno(pl)
	} else if _, hasExp := stringField(pl, "expiresAt"); hasExp {
		p.applyUno(pl)
	} else {
		p.uno = nil
	}
}

func (p *SpectatorRoomProjection) applyRoster(pl map[string]any) {
	if raw, ok := pl["roster"]; ok {
		if seats := parseSeats(raw); seats != nil {
			p.seats = seats
			return
		}
	}
	if raw, ok := pl["seats"]; ok {
		if seats := parseSeats(raw); seats != nil {
			p.seats = seats
		}
	}
}

func (p *SpectatorRoomProjection) applyPublicGameState(pl map[string]any) {
	if v, ok := stringField(pl, "discardTop"); ok {
		p.discard.DiscardTop = v
	}
	if v, ok := stringField(pl, "discard"); ok {
		p.discard.DiscardTop = v
	}
	if v, ok := stringField(pl, "activeColor"); ok {
		p.discard.ActiveColor = v
	}
	if v, ok := stringField(pl, "color"); ok {
		p.discard.ActiveColor = v
	}
	if v, ok := stringField(pl, "direction"); ok {
		p.direction = v
	}
	if v, ok := stringField(pl, "currentPlayerId"); ok {
		p.currentPlayer = PlayerID(v)
	}
	if v, ok := stringField(pl, "turnPlayerId"); ok {
		p.currentPlayer = PlayerID(v)
	}
	if n, ok := intField(pl, "penaltyAmount"); ok {
		p.penaltyAmount = n
	}
	if v, ok := stringField(pl, "penaltyTarget"); ok {
		p.penaltyTarget = PlayerID(v)
	}
	p.applyRoster(pl)
}

func (p *SpectatorRoomProjection) applyScores(pl map[string]any) {
	if raw, ok := pl["gameScore"]; ok {
		if m := parseScoreMap(raw); m != nil {
			p.gameScore = m
		}
	}
	if raw, ok := pl["score"]; ok {
		if m := parseScoreMap(raw); m != nil {
			p.gameScore = m
		}
	}
	if raw, ok := pl["matchWins"]; ok {
		if m := parseScoreMap(raw); m != nil {
			p.gameScore = m
		}
	}
	if raw, ok := pl["gameWins"]; ok {
		if m := parseScoreMap(raw); m != nil {
			p.gameScore = m
		}
	}
	if raw, ok := pl["matchWinsByPlayer"]; ok {
		if m := parseScoreMap(raw); m != nil {
			p.gameScore = m
		}
	}
	p.applyPublicWinner(pl)
}

func (p *SpectatorRoomProjection) applyPublicWinner(pl map[string]any) {
	if w, ok := stringField(pl, "matchWinner"); ok {
		p.matchWinner = PlayerID(w)
	}
	if w, ok := stringField(pl, "winnerPlayerId"); ok && p.matchWinner == "" {
		p.matchWinner = PlayerID(w)
	}
}

func (p *SpectatorRoomProjection) applyUno(pl map[string]any) {
	raw, ok := pl["unoWindow"]
	if !ok {
		exp, hasExp := stringField(pl, "expiresAt")
		seq, hasSeq := uintField(pl, "openingSequence")
		if !hasSeq {
			seq, hasSeq = uintField(pl, "openingRoomSequence")
		}
		pid, hasPid := stringField(pl, "playerId")
		if hasExp && hasSeq && hasPid {
			t, err := time.Parse(time.RFC3339, exp)
			if err == nil {
				called, _ := boolField(pl, "called")
				p.uno = &PublicUnoWindow{
					PlayerID:        PlayerID(pid),
					ExpiresAt:       t.UTC(),
					OpeningSequence: SequenceNumber(seq),
					Called:          called,
				}
			}
		}
		return
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return
	}
	pid, _ := stringField(m, "playerId")
	exp, _ := stringField(m, "expiresAt")
	seq, okSeq := uintField(m, "openingSequence")
	if !okSeq {
		seq, okSeq = uintField(m, "openingRoomSequence")
	}
	if pid == "" || exp == "" || !okSeq {
		return
	}
	t, err := time.Parse(time.RFC3339, exp)
	if err != nil {
		return
	}
	called, _ := boolField(m, "called")
	p.uno = &PublicUnoWindow{
		PlayerID:        PlayerID(pid),
		ExpiresAt:       t.UTC(),
		OpeningSequence: SequenceNumber(seq),
		Called:          called,
	}
}

func parseSeats(raw any) []VisibleSeat {
	switch t := raw.(type) {
	case []any:
		out := make([]VisibleSeat, 0, len(t))
		for i, item := range t {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			seat := VisibleSeat{SeatIndex: i, Occupied: true}
			if n, ok := intField(m, "seatIndex"); ok {
				seat.SeatIndex = n
			}
			if v, ok := stringField(m, "playerId"); ok {
				seat.PlayerID = PlayerID(v)
			}
			if v, ok := stringField(m, "displayName"); ok {
				seat.DisplayName = v
			}
			if n, ok := intField(m, "cardCount"); ok {
				seat.CardCount = n
			} else if n, ok := intField(m, "handCount"); ok {
				seat.CardCount = n
			}
			if seat.PlayerID == "" {
				seat.Occupied = false
			}
			out = append(out, seat)
		}
		return out
	default:
		return nil
	}
}

func parseScoreMap(raw any) map[PlayerID]int {
	m, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[PlayerID]int, len(m))
	for k, v := range m {
		switch n := v.(type) {
		case int:
			out[PlayerID(k)] = n
		case int64:
			out[PlayerID(k)] = int(n)
		case float64:
			out[PlayerID(k)] = int(n)
		}
	}
	return out
}

func stringField(m map[string]any, key string) (string, bool) {
	v, ok := m[key]
	if !ok || v == nil {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

func intField(m map[string]any, key string) (int, bool) {
	v, ok := m[key]
	if !ok || v == nil {
		return 0, false
	}
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case float64:
		return int(n), true
	default:
		return 0, false
	}
}

func uintField(m map[string]any, key string) (uint64, bool) {
	n, ok := intField(m, key)
	if !ok || n < 0 {
		return 0, false
	}
	return uint64(n), true
}

func boolField(m map[string]any, key string) (bool, bool) {
	v, ok := m[key]
	if !ok || v == nil {
		return false, false
	}
	b, ok := v.(bool)
	return b, ok
}

func outcomeKindForRejection(code RejectionCode) OutcomeKind {
	if code == RejectUnknownEventType || code == RejectForbiddenField || code == RejectDisallowedField {
		return OutcomeDropped
	}
	return OutcomeQuarantined
}

func duplicateOutcome(prior ApplyOutcome) ApplyOutcome {
	dup := copyOutcome(prior)
	dup.Kind = OutcomeDuplicate
	return dup
}

func copyOutcome(in ApplyOutcome) ApplyOutcome {
	out := in
	out.Facts = copyFacts(in.Facts)
	if in.Rejection != nil {
		r := *in.Rejection
		out.Rejection = &r
	}
	return out
}
