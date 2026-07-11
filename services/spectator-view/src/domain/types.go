package domain

import "fmt"

// RoomStatus is the projected room lifecycle state.
type RoomStatus string

const (
	RoomStatusWaiting    RoomStatus = "waiting"
	RoomStatusLocked     RoomStatus = "locked"
	RoomStatusInProgress RoomStatus = "in_progress"
	RoomStatusCompleted  RoomStatus = "completed"
	RoomStatusCancelled  RoomStatus = "cancelled"
)

func (s RoomStatus) String() string { return string(s) }

func (s RoomStatus) IsTerminal() bool {
	return s == RoomStatusCompleted || s == RoomStatusCancelled
}

func (s RoomStatus) AllowsSpectatorAdmission() bool {
	switch s {
	case RoomStatusWaiting, RoomStatusLocked, RoomStatusInProgress:
		return true
	default:
		return false
	}
}

// Visibility controls spectator authorization requirements.
type Visibility string

const (
	VisibilityPublic  Visibility = "public"
	VisibilityPrivate Visibility = "private"
)

func (v Visibility) String() string { return string(v) }

// EventType is a spectator-safe upstream event type.
type EventType string

const (
	EventRoomCreated           EventType = "RoomCreated"
	EventPlayerJoinedRoom      EventType = "PlayerJoinedRoom"
	EventPlayerLeftRoom        EventType = "PlayerLeftRoom"
	EventHostReassigned        EventType = "HostReassigned"
	EventRoomLocked            EventType = "RoomLocked"
	EventMatchStarted          EventType = "MatchStarted"
	EventGameStarted           EventType = "GameStarted"
	EventCardPlayed            EventType = "CardPlayed"
	EventTurnAdvanced          EventType = "TurnAdvanced"
	EventColorChosen           EventType = "ColorChosen"
	EventDirectionChanged      EventType = "DirectionChanged"
	EventPenaltyUpdated        EventType = "PenaltyUpdated"
	EventUnoWindowOpened       EventType = "UnoWindowOpened"
	EventUnoWindowClosed       EventType = "UnoWindowClosed"
	EventUnoWindowExpired      EventType = "UnoWindowExpired"
	EventGameCompleted         EventType = "GameCompleted"
	EventMatchCompleted        EventType = "MatchCompleted"
	EventRoomCompleted         EventType = "RoomCompleted"
	EventRoomCancelled         EventType = "RoomCancelled"
	EventSpectatorStreamsClose EventType = "SpectatorStreamsClose"
	EventPlayerDisconnected    EventType = "PlayerDisconnected"
	EventPlayerReconnected     EventType = "PlayerReconnected"
	EventPlayerForfeited       EventType = "PlayerForfeited"
	EventSnapshotSanitized     EventType = "SnapshotSanitized"
)

// RejectionCode classifies why an event was not applied.
type RejectionCode string

const (
	RejectInvalidIdentity       RejectionCode = "invalid_identity"
	RejectInvalidSchema         RejectionCode = "invalid_schema"
	RejectUnknownEventType      RejectionCode = "unknown_event_type"
	RejectForbiddenField        RejectionCode = "forbidden_field"
	RejectDisallowedField       RejectionCode = "disallowed_field"
	RejectStaleSequence         RejectionCode = "stale_sequence"
	RejectOutOfOrderSequence    RejectionCode = "out_of_order_sequence"
	RejectRoomMismatch          RejectionCode = "room_mismatch"
	RejectSpectatorTerminal     RejectionCode = "spectator_terminal_denied"
	RejectSpectatorUnauthorized RejectionCode = "spectator_unauthorized"
)

// Rejection is a typed non-mutating outcome detail.
type Rejection struct {
	Code    RejectionCode
	Message string
}

func (r Rejection) Error() string {
	if r.Message != "" {
		return fmt.Sprintf("%s: %s", r.Code, r.Message)
	}
	return string(r.Code)
}

// OutcomeKind classifies projection apply results.
type OutcomeKind string

const (
	OutcomeAccepted    OutcomeKind = "accepted"
	OutcomeDuplicate   OutcomeKind = "duplicate"
	OutcomeIgnored     OutcomeKind = "ignored"
	OutcomeDropped     OutcomeKind = "dropped"
	OutcomeQuarantined OutcomeKind = "quarantined"
)

// ApplyOutcome is the stable result of applying one spectator-safe event.
type ApplyOutcome struct {
	Kind      OutcomeKind
	EventID   EventID
	Sequence  SequenceNumber
	Rejection *Rejection
	Facts     []Fact
}

func (o ApplyOutcome) Accepted() bool {
	return o.Kind == OutcomeAccepted || (o.Kind == OutcomeDuplicate && o.Rejection == nil)
}

func (o ApplyOutcome) Mutated() bool {
	return o.Kind == OutcomeAccepted
}
