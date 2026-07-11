package domain

import "fmt"

// RejectionCode classifies why a command was rejected without mutating state.
type RejectionCode string

const (
	RejectInvalidCommand   RejectionCode = "invalid_command"
	RejectInvalidIdentity  RejectionCode = "invalid_identity"
	RejectIneligibleGame   RejectionCode = "ineligible_game"
	RejectNotAuthoritative RejectionCode = "not_authoritative"
	RejectAbandonedGame    RejectionCode = "abandoned_game"
	RejectTournamentGame   RejectionCode = "tournament_game"
	RejectInvalidPlacement RejectionCode = "invalid_placement"
	RejectInvalidOpponents RejectionCode = "invalid_opponents"
)

// Rejection is a typed domain rejection outcome. It is not a domain event.
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

// OutcomeKind classifies command handling results.
type OutcomeKind string

const (
	OutcomeAccepted  OutcomeKind = "accepted"
	OutcomeRejected  OutcomeKind = "rejected"
	OutcomeDuplicate OutcomeKind = "duplicate"
)

// CommandOutcome is the stable result of handling a command.
// Duplicate business/event keys return the prior outcome without reapplying.
type CommandOutcome struct {
	Kind      OutcomeKind
	CommandID CommandID
	Rejection *Rejection
	Facts     []Fact
}

func (o CommandOutcome) Accepted() bool {
	return o.Kind == OutcomeAccepted || (o.Kind == OutcomeDuplicate && o.Rejection == nil)
}

func (o CommandOutcome) Rejected() bool {
	return o.Rejection != nil
}

func acceptedOutcome(commandID CommandID, facts []Fact) CommandOutcome {
	if facts == nil {
		facts = []Fact{}
	}
	return CommandOutcome{Kind: OutcomeAccepted, CommandID: commandID, Facts: facts}
}

func rejectedOutcome(commandID CommandID, rej Rejection) CommandOutcome {
	r := rej
	return CommandOutcome{Kind: OutcomeRejected, CommandID: commandID, Rejection: &r, Facts: nil}
}

func duplicateOutcome(prior CommandOutcome) CommandOutcome {
	dup := prior
	dup.Kind = OutcomeDuplicate
	return dup
}
