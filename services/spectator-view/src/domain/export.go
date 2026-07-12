package domain

import (
	"encoding/json"
	"fmt"
)

// ProjectionExport is the exact durable serialization of SpectatorRoomProjection
// fields (public state, terminal flags, and processed outcomes). Restore must
// not re-apply fake events — it reconstructs aggregate state directly.
type ProjectionExport struct {
	RoomID          RoomID                    `json:"roomId"`
	Status          RoomStatus                `json:"status"`
	Visibility      Visibility                `json:"visibility"`
	Sequence        SequenceNumber            `json:"sequence"`
	Seats           []VisibleSeat             `json:"seats"`
	Discard         VisibleDiscardState       `json:"discard"`
	Direction       string                    `json:"direction"`
	CurrentPlayerID PlayerID                  `json:"currentPlayerId"`
	PenaltyAmount   int                       `json:"penaltyAmount"`
	PenaltyTarget   PlayerID                  `json:"penaltyTarget"`
	DrawPileSize    int                       `json:"drawPileSize"`
	GameScore       map[PlayerID]int          `json:"gameScore"`
	MatchWinner     PlayerID                  `json:"matchWinner"`
	GameCompleted   bool                      `json:"gameCompleted"`
	MatchCompleted  bool                      `json:"matchCompleted"`
	Uno             *PublicUnoWindow          `json:"uno,omitempty"`
	StreamClosed    bool                      `json:"streamClosed"`
	Outcomes        map[EventID]OutcomeExport `json:"outcomes"`
}

// OutcomeExport is the durable form of ApplyOutcome (stable disposition + facts).
type OutcomeExport struct {
	Kind      OutcomeKind    `json:"kind"`
	EventID   EventID        `json:"eventId"`
	Sequence  SequenceNumber `json:"sequence"`
	Rejection *Rejection     `json:"rejection,omitempty"`
	Facts     []Fact         `json:"facts"`
}

// ExportState copies every projection field needed for exact Redis restore.
func (p *SpectatorRoomProjection) ExportState() ProjectionExport {
	if p == nil {
		return ProjectionExport{Outcomes: map[EventID]OutcomeExport{}}
	}
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
	outcomes := make(map[EventID]OutcomeExport, len(p.outcomes))
	for id, o := range p.outcomes {
		outcomes[id] = exportOutcome(o)
	}
	return ProjectionExport{
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
		DrawPileSize:    p.drawPileSize,
		GameScore:       score,
		MatchWinner:     p.matchWinner,
		GameCompleted:   p.gameCompleted,
		MatchCompleted:  p.matchCompleted,
		Uno:             uno,
		StreamClosed:    p.streamClosed,
		Outcomes:        outcomes,
	}
}

// RestoreProjection rebuilds a SpectatorRoomProjection from an exact export
// without applying events.
func RestoreProjection(exp ProjectionExport) (*SpectatorRoomProjection, error) {
	if !exp.RoomID.Valid() {
		return nil, fmt.Errorf("projection export: roomId required")
	}
	p := NewSpectatorRoomProjection(exp.RoomID)
	p.status = exp.Status
	if p.status == "" {
		p.status = RoomStatusWaiting
	}
	p.visibility = exp.Visibility
	if p.visibility == "" {
		p.visibility = VisibilityPublic
	}
	p.sequence = exp.Sequence
	if exp.Seats != nil {
		p.seats = make([]VisibleSeat, len(exp.Seats))
		copy(p.seats, exp.Seats)
	}
	p.discard = exp.Discard
	p.direction = exp.Direction
	p.currentPlayer = exp.CurrentPlayerID
	p.penaltyAmount = exp.PenaltyAmount
	p.penaltyTarget = exp.PenaltyTarget
	if exp.DrawPileSize < 0 {
		return nil, fmt.Errorf("projection export: drawPileSize must be >= 0")
	}
	p.drawPileSize = exp.DrawPileSize
	if exp.GameScore != nil {
		p.gameScore = make(map[PlayerID]int, len(exp.GameScore))
		for k, v := range exp.GameScore {
			p.gameScore[k] = v
		}
	}
	p.matchWinner = exp.MatchWinner
	p.gameCompleted = exp.GameCompleted
	p.matchCompleted = exp.MatchCompleted
	if exp.Uno != nil {
		cp := *exp.Uno
		if !cp.ExpiresAt.IsZero() {
			cp.ExpiresAt = cp.ExpiresAt.UTC()
		}
		p.uno = &cp
	}
	p.streamClosed = exp.StreamClosed
	if exp.Outcomes != nil {
		p.outcomes = make(map[EventID]ApplyOutcome, len(exp.Outcomes))
		for id, o := range exp.Outcomes {
			p.outcomes[id] = importOutcome(o)
		}
	}
	return p, nil
}

// MarshalExport encodes a ProjectionExport as JSON.
func MarshalExport(exp ProjectionExport) ([]byte, error) {
	return json.Marshal(exp)
}

// UnmarshalExport decodes a ProjectionExport from JSON.
func UnmarshalExport(raw []byte) (ProjectionExport, error) {
	var exp ProjectionExport
	if err := json.Unmarshal(raw, &exp); err != nil {
		return ProjectionExport{}, err
	}
	if exp.Outcomes == nil {
		exp.Outcomes = map[EventID]OutcomeExport{}
	}
	if exp.Uno != nil && !exp.Uno.ExpiresAt.IsZero() {
		exp.Uno.ExpiresAt = exp.Uno.ExpiresAt.UTC()
	}
	return exp, nil
}

// MarshalOutcome encodes one durable outcome.
func MarshalOutcome(o ApplyOutcome) ([]byte, error) {
	return json.Marshal(exportOutcome(o))
}

// UnmarshalOutcome decodes one durable outcome.
func UnmarshalOutcome(raw []byte) (ApplyOutcome, error) {
	var exp OutcomeExport
	if err := json.Unmarshal(raw, &exp); err != nil {
		return ApplyOutcome{}, err
	}
	return importOutcome(exp), nil
}

func exportOutcome(o ApplyOutcome) OutcomeExport {
	out := OutcomeExport{
		Kind:     o.Kind,
		EventID:  o.EventID,
		Sequence: o.Sequence,
		Facts:    copyFacts(o.Facts),
	}
	if o.Rejection != nil {
		r := *o.Rejection
		out.Rejection = &r
	}
	return out
}

func importOutcome(o OutcomeExport) ApplyOutcome {
	out := ApplyOutcome{
		Kind:     o.Kind,
		EventID:  o.EventID,
		Sequence: o.Sequence,
		Facts:    copyFacts(o.Facts),
	}
	if o.Rejection != nil {
		r := *o.Rejection
		out.Rejection = &r
	}
	return out
}
