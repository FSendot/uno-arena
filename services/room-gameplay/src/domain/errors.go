package domain

import "fmt"

// RejectionCode classifies why a command was rejected without mutating state.
type RejectionCode string

const (
	RejectInvalidCommand        RejectionCode = "invalid_command"
	RejectInvalidIdentity       RejectionCode = "invalid_identity"
	RejectInvalidMaxSeats       RejectionCode = "invalid_max_seats"
	RejectNotWaiting            RejectionCode = "not_waiting"
	RejectNotLocked             RejectionCode = "not_locked"
	RejectNotInProgress         RejectionCode = "not_in_progress"
	RejectAlreadyTerminal       RejectionCode = "already_terminal"
	RejectNotHost               RejectionCode = "not_host"
	RejectHostAuthorityEnded    RejectionCode = "host_authority_ended"
	RejectRoomFull              RejectionCode = "room_full"
	RejectAlreadySeated         RejectionCode = "already_seated" // unused when join is idempotent
	RejectNotSeated             RejectionCode = "not_seated"
	RejectJoinAfterLock         RejectionCode = "join_after_lock"
	RejectInsufficientRoster    RejectionCode = "insufficient_roster"
	RejectStaleSequence         RejectionCode = "stale_sequence"
	RejectFutureSequence        RejectionCode = "future_sequence"
	RejectSequenceRequired      RejectionCode = "sequence_required"
	RejectDisconnectInactive    RejectionCode = "disconnect_inactive"
	RejectDisconnectVersion     RejectionCode = "disconnect_version_mismatch"
	RejectReconnectExpired      RejectionCode = "reconnect_expired"
	RejectForfeitNotDue         RejectionCode = "forfeit_not_due"
	RejectUnoWindowInactive     RejectionCode = "uno_window_inactive"
	RejectUnoWindowMismatch     RejectionCode = "uno_window_mismatch"
	RejectUnoWindowNotExpired   RejectionCode = "uno_window_not_expired"
	RejectUnoAlreadyCalled      RejectionCode = "uno_already_called"
	RejectSpectatorTerminal     RejectionCode = "spectator_terminal_denied"
	RejectSpectatorUnauthorized RejectionCode = "spectator_unauthorized"
	RejectWrongRoomType         RejectionCode = "wrong_room_type"
	RejectTournamentIdentity    RejectionCode = "tournament_identity_required"
	RejectNotInHand             RejectionCode = "not_in_hand"
	RejectIllegalCard           RejectionCode = "illegal_card"
	RejectOutOfTurn             RejectionCode = "out_of_turn"
	RejectPendingColor          RejectionCode = "pending_color_choice"
	RejectNotPenaltyTarget      RejectionCode = "not_penalty_target"
	RejectJumpInBlocked         RejectionCode = "jump_in_blocked"
	RejectJumpInMismatch        RejectionCode = "jump_in_mismatch"
	RejectColorNotPending       RejectionCode = "color_not_pending"
	RejectInvalidColor          RejectionCode = "invalid_color"
	RejectDrawBatchMismatch     RejectionCode = "draw_batch_mismatch"
	RejectGameCompleted         RejectionCode = "game_completed"
	RejectNoActiveGame          RejectionCode = "no_active_game"
	RejectGameStillActive       RejectionCode = "game_still_active"
	RejectNotDisconnected       RejectionCode = "not_disconnected"
	RejectTurnVersionMismatch   RejectionCode = "turn_version_mismatch"
	RejectDealMismatch          RejectionCode = "deal_mismatch"
	RejectGameIDReused          RejectionCode = "game_id_reused"
	RejectPlayerDisconnected    RejectionCode = "player_disconnected"
	RejectMatchOwnsCompletion   RejectionCode = "match_owns_completion"
	RejectSessionOwnsForfeit    RejectionCode = "session_owns_forfeit"
)

// Rejection is a typed domain rejection outcome. It is not a domain event.
type Rejection struct {
	Code              RejectionCode
	Message           string
	SubmittedSequence SequenceNumber
	CurrentSequence   SequenceNumber
}

func (r Rejection) Error() string {
	if r.Message != "" {
		return fmt.Sprintf("%s: %s", r.Code, r.Message)
	}
	return string(r.Code)
}

// OutcomeKind classifies command handling results.
type OutcomeKind string

const (
	OutcomeAccepted  OutcomeKind = "accepted"
	OutcomeRejected  OutcomeKind = "rejected"
	OutcomeDuplicate OutcomeKind = "duplicate"
)

// CommandOutcome is the stable result of handling a command.
// Duplicate command IDs return the prior outcome without reapplying.
type CommandOutcome struct {
	Kind      OutcomeKind
	CommandID CommandID
	Rejection *Rejection
	Facts     []Fact
	Sequence  SequenceNumber // room sequence after handling (unchanged on reject/duplicate)
}

func (o CommandOutcome) Accepted() bool {
	return o.Kind == OutcomeAccepted || (o.Kind == OutcomeDuplicate && o.Rejection == nil)
}

func (o CommandOutcome) Rejected() bool {
	return o.Rejection != nil
}

func acceptedOutcome(commandID CommandID, sequence SequenceNumber, facts []Fact) CommandOutcome {
	if facts == nil {
		facts = []Fact{}
	}
	return CommandOutcome{
		Kind:      OutcomeAccepted,
		CommandID: commandID,
		Facts:     facts,
		Sequence:  sequence,
	}
}

func rejectedOutcome(commandID CommandID, sequence SequenceNumber, rej Rejection) CommandOutcome {
	r := rej
	r.CurrentSequence = sequence
	return CommandOutcome{
		Kind:      OutcomeRejected,
		CommandID: commandID,
		Rejection: &r,
		Facts:     nil,
		Sequence:  sequence,
	}
}

func duplicateOutcome(prior CommandOutcome) CommandOutcome {
	dup := prior
	dup.Kind = OutcomeDuplicate
	return dup
}
