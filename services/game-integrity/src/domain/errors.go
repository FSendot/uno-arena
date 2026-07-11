package domain

import "fmt"

// RejectionCode classifies why a command was rejected without mutating state.
type RejectionCode string

const (
	RejectInvalidCommand       RejectionCode = "invalid_command"
	RejectInvalidIdentity      RejectionCode = "invalid_identity"
	RejectInsufficientCards    RejectionCode = "insufficient_cards"
	RejectConflictingDuplicate RejectionCode = "conflicting_duplicate"
	RejectRevisionMismatch     RejectionCode = "revision_mismatch"
	RejectInvalidOffset        RejectionCode = "invalid_offset"
)

// Rejection is a typed domain rejection outcome. It is not a domain event.
type Rejection struct {
	Code              RejectionCode
	Message           string
	SubmittedRevision Revision
	CurrentRevision   Revision
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

// CommandOutcome is the stable result of handling a draw or append command.
// Duplicate keys return the prior outcome without reapplying.
type CommandOutcome struct {
	Kind      OutcomeKind
	Rejection *Rejection
	Facts     []Fact
	Cards     []Card    // drawn cards (AuthoritativeDeck only); defensive copies
	Offset    LogOffset // appended offset (GameLog only)
	Revision  Revision  // stream/deck revision after handling (unchanged on reject)
}

func (o CommandOutcome) Accepted() bool {
	return o.Kind == OutcomeAccepted || (o.Kind == OutcomeDuplicate && o.Rejection == nil)
}

func (o CommandOutcome) Rejected() bool {
	return o.Rejection != nil
}

func acceptedDraw(facts []Fact, cards []Card, revision Revision) CommandOutcome {
	return CommandOutcome{
		Kind:     OutcomeAccepted,
		Facts:    copyFacts(facts),
		Cards:    copyCards(cards),
		Revision: revision,
	}
}

func acceptedAppend(facts []Fact, offset LogOffset, revision Revision) CommandOutcome {
	return CommandOutcome{
		Kind:     OutcomeAccepted,
		Facts:    copyFacts(facts),
		Offset:   offset,
		Revision: revision,
	}
}

func rejectedOutcome(revision Revision, rej Rejection) CommandOutcome {
	r := rej
	r.CurrentRevision = revision
	return CommandOutcome{Kind: OutcomeRejected, Rejection: &r, Facts: nil, Revision: revision}
}

func duplicateOutcome(prior CommandOutcome) CommandOutcome {
	return CommandOutcome{
		Kind:      OutcomeDuplicate,
		Rejection: prior.Rejection,
		Facts:     copyFacts(prior.Facts),
		Cards:     copyCards(prior.Cards),
		Offset:    prior.Offset,
		Revision:  prior.Revision,
	}
}
