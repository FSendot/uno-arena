package game

import "fmt"

type PlayerID string
type GameID string
type CommandID string
type SequenceNumber uint64

type PlayMode string

const (
	PlayModeTurn   PlayMode = "turn"
	PlayModeStack  PlayMode = "stack"
	PlayModeJumpIn PlayMode = "jump_in"
)

type Direction int

const (
	DirectionClockwise        Direction = 1
	DirectionCounterClockwise Direction = -1
)

type RejectionCode string

const (
	RejectInvalidCommand      RejectionCode = "invalid_command"
	RejectInvalidIdentity     RejectionCode = "invalid_identity"
	RejectNotInHand           RejectionCode = "not_in_hand"
	RejectIllegalCard         RejectionCode = "illegal_card"
	RejectOutOfTurn           RejectionCode = "out_of_turn"
	RejectStaleSequence       RejectionCode = "stale_sequence"
	RejectFutureSequence      RejectionCode = "future_sequence"
	RejectSequenceRequired    RejectionCode = "sequence_required"
	RejectPendingColor        RejectionCode = "pending_color_choice"
	RejectNotPenaltyTarget    RejectionCode = "not_penalty_target"
	RejectJumpInBlocked       RejectionCode = "jump_in_blocked"
	RejectJumpInMismatch      RejectionCode = "jump_in_mismatch"
	RejectColorNotPending     RejectionCode = "color_not_pending"
	RejectInvalidColor        RejectionCode = "invalid_color"
	RejectUnoWindowInactive   RejectionCode = "uno_window_inactive"
	RejectUnoWindowMismatch   RejectionCode = "uno_window_mismatch"
	RejectUnoWindowNotExpired RejectionCode = "uno_window_not_expired"
	RejectUnoAlreadyCalled    RejectionCode = "uno_already_called"
	RejectDrawBatchMismatch   RejectionCode = "draw_batch_mismatch"
	RejectGameCompleted       RejectionCode = "game_completed"
	RejectWrongPlayerCount    RejectionCode = "wrong_player_count"
	RejectDealMismatch        RejectionCode = "deal_mismatch"
	RejectNotCurrentPlayer    RejectionCode = "not_current_player"
	RejectPlayerNotActive     RejectionCode = "player_not_active"
	RejectTooFewPlayers       RejectionCode = "too_few_players"
)

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

type OutcomeKind string

const (
	OutcomeAccepted  OutcomeKind = "accepted"
	OutcomeRejected  OutcomeKind = "rejected"
	OutcomeDuplicate OutcomeKind = "duplicate"
)

type FactName string

const (
	FactCardPlayed            FactName = "CardPlayed"
	FactPenaltyStackIncreased FactName = "PenaltyStackIncreased"
	FactPenaltyStackResolved  FactName = "PenaltyStackResolved"
	FactCardDrawn             FactName = "CardDrawn"
	FactColorChosen           FactName = "ColorChosen"
	FactTurnAdvanced          FactName = "TurnAdvanced"
	FactDrawTurnRetained      FactName = "DrawTurnRetained"
	FactUnoWindowOpened       FactName = "UnoWindowOpened"
	FactUnoCalled             FactName = "UnoCalled"
	FactUnoChallengeIssued    FactName = "UnoChallengeIssued"
	FactUnoPenaltyApplied     FactName = "UnoPenaltyApplied"
	FactUnoWindowExpired      FactName = "UnoWindowExpired"
	FactUnoWindowClosed       FactName = "UnoWindowClosed"
	FactGameCompleted         FactName = "GameCompleted"
	FactMatchScoreUpdated     FactName = "MatchScoreUpdated"
	FactTurnSkipped           FactName = "TurnSkipped"
	FactPlayerRemoved         FactName = "PlayerRemoved"
)

type Fact struct {
	Name FactName
	Data map[string]string
}

type CommandOutcome struct {
	Kind      OutcomeKind
	CommandID CommandID
	Rejection *Rejection
	Facts     []Fact
	Sequence  SequenceNumber
}

func (o CommandOutcome) Accepted() bool {
	return o.Kind == OutcomeAccepted || (o.Kind == OutcomeDuplicate && o.Rejection == nil)
}

func acceptedOutcome(commandID CommandID, sequence SequenceNumber, facts []Fact) CommandOutcome {
	if facts == nil {
		facts = []Fact{}
	}
	return CommandOutcome{Kind: OutcomeAccepted, CommandID: commandID, Facts: facts, Sequence: sequence}
}

func rejectedOutcome(commandID CommandID, sequence SequenceNumber, rej Rejection) CommandOutcome {
	r := rej
	r.CurrentSequence = sequence
	return CommandOutcome{Kind: OutcomeRejected, CommandID: commandID, Rejection: &r, Sequence: sequence}
}

func duplicateOutcome(prior CommandOutcome) CommandOutcome {
	dup := prior
	dup.Kind = OutcomeDuplicate
	return dup
}

func newFact(name FactName, data map[string]string) Fact {
	if data == nil {
		data = map[string]string{}
	}
	return Fact{Name: name, Data: data}
}
