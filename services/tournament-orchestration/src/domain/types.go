package domain

import (
	"fmt"
	"time"
)

// --- Identities ---

type TournamentID string

func (id TournamentID) String() string { return string(id) }
func (id TournamentID) Valid() bool    { return id != "" }

type PlayerID string

func (id PlayerID) String() string { return string(id) }
func (id PlayerID) Valid() bool    { return id != "" }

type CommandID string

func (id CommandID) String() string { return string(id) }
func (id CommandID) Valid() bool    { return id != "" }

type RoomID string

func (id RoomID) String() string { return string(id) }
func (id RoomID) Valid() bool    { return id != "" }

type SlotID string

func (id SlotID) String() string { return string(id) }
func (id SlotID) Valid() bool    { return id != "" }

type BatchID string

func (id BatchID) String() string { return string(id) }
func (id BatchID) Valid() bool    { return id != "" }

type EventID string

func (id EventID) String() string { return string(id) }
func (id EventID) Valid() bool    { return id != "" }

type CompletionVersion uint64

// --- Lifecycle ---

type TournamentPhase string

const (
	PhaseRegistration TournamentPhase = "registration"
	PhaseSeeding      TournamentPhase = "seeding"
	PhaseInProgress   TournamentPhase = "in_progress"
	PhaseCompleted    TournamentPhase = "completed"
	PhaseCancelled    TournamentPhase = "cancelled"
)

func (p TournamentPhase) String() string { return string(p) }

func (p TournamentPhase) IsTerminal() bool {
	return p == PhaseCompleted || p == PhaseCancelled
}

type RoundStatus string

const (
	RoundPending      RoundStatus = "pending"
	RoundSeeded       RoundStatus = "seeded"
	RoundProvisioning RoundStatus = "provisioning"
	RoundInProgress   RoundStatus = "in_progress"
	RoundCompleted    RoundStatus = "completed"
	RoundBlocked      RoundStatus = "blocked"
)

type SlotStatus string

const (
	SlotPending        SlotStatus = "pending"
	SlotAssigned       SlotStatus = "assigned"
	SlotInProgress     SlotStatus = "in_progress"
	SlotResultRecorded SlotStatus = "result_recorded"
	SlotAdvanced       SlotStatus = "advanced"
	SlotQuarantined    SlotStatus = "quarantined"
	SlotCancelled      SlotStatus = "cancelled"
)

type BatchStatus string

const (
	BatchPending     BatchStatus = "pending"
	BatchInProgress  BatchStatus = "in_progress"
	BatchCompleted   BatchStatus = "completed"
	BatchRetried     BatchStatus = "retried"
	BatchQuarantined BatchStatus = "quarantined"
	BatchCancelled   BatchStatus = "cancelled"
)

type ResultDisposition string

const (
	DispositionRecorded         ResultDisposition = "recorded"
	DispositionDuplicateIgnored ResultDisposition = "duplicate_ignored"
	DispositionQuarantined      ResultDisposition = "quarantined"
)

// Policy constants from docs.
const (
	PlayersPerRoom       = 10
	FinalPlayerThreshold = 10
	AdvancersPerMatch    = 3
	DefaultBatchSize     = 100
	DefaultRetryBudget   = 3
	// MaxProvisioningBatchSize bounds a single worker shard (million-player fanout).
	MaxProvisioningBatchSize = 1000
)

// --- Facts ---

type FactName string

const (
	FactTournamentCreated                      FactName = "TournamentCreated"
	FactPlayerRegisteredInTournament           FactName = "PlayerRegisteredInTournament"
	FactTournamentRegistrationClosed           FactName = "TournamentRegistrationClosed"
	FactTournamentRoundSeeded                  FactName = "TournamentRoundSeeded" // internal-only; no Kafka channel
	FactTournamentMatchAssigned                FactName = "TournamentMatchAssigned"
	FactTournamentMatchResultRecorded          FactName = "TournamentMatchResultRecorded"
	FactPlayersAdvanced                        FactName = "PlayersAdvanced"
	FactTournamentRoundCompleted               FactName = "TournamentRoundCompleted"
	FactTournamentCompleted                    FactName = "TournamentCompleted"
	FactTournamentCancelled                    FactName = "TournamentCancelled"
	FactTournamentProvisioningBatchRetried     FactName = "TournamentProvisioningBatchRetried"
	FactTournamentProvisioningBatchQuarantined FactName = "TournamentProvisioningBatchQuarantined"
	// FactTournamentProvisioningBatchCompleted is internal-only (no Kafka topic).
	// Emitted on a real pending→completed transition. Public BracketPage bumps only when
	// fact data carries FactDataPublicBracketVisible (last batch → round in_progress).
	// Semantic duplicates remain factless and version-stable.
	FactTournamentProvisioningBatchCompleted FactName = "TournamentProvisioningBatchCompleted"
	FactTournamentResultQuarantined          FactName = "TournamentResultQuarantined"
)

// FactDataPublicBracketVisible is an internal Fact.Data key. When set to "true", the fact
// changed BracketPage-visible summary fields (e.g. round status). Not part of Kafka contracts.
const FactDataPublicBracketVisible = "publicBracketVisible"

// Fact is a named domain fact from an accepted state change. Rejected commands never produce facts.
type Fact struct {
	Name FactName
	Data map[string]string
}

func newFact(name FactName, data map[string]string) Fact {
	if data == nil {
		data = map[string]string{}
	}
	return Fact{Name: name, Data: data}
}

// --- Rejections / outcomes ---

type RejectionCode string

const (
	RejectInvalidCommand        RejectionCode = "invalid_command"
	RejectInvalidIdentity       RejectionCode = "invalid_identity"
	RejectWrongPhase            RejectionCode = "wrong_phase"
	RejectAlreadyTerminal       RejectionCode = "already_terminal"
	RejectCapacityExceeded      RejectionCode = "capacity_exceeded"
	RejectNotRegistered         RejectionCode = "not_registered"
	RejectRoundNotFound         RejectionCode = "round_not_found"
	RejectRoundNotReady         RejectionCode = "round_not_ready"
	RejectSlotNotFound          RejectionCode = "slot_not_found"
	RejectBatchNotFound         RejectionCode = "batch_not_found"
	RejectRetryBudgetExhausted  RejectionCode = "retry_budget_exhausted"
	RejectConflictingAssignment RejectionCode = "conflicting_assignment"
	RejectRoomMismatch          RejectionCode = "room_mismatch"
	RejectResultConflict        RejectionCode = "result_conflict"
	RejectRoundIncomplete       RejectionCode = "round_incomplete"
	RejectNotFinal              RejectionCode = "not_final"
	RejectQuarantined           RejectionCode = "quarantined"
	RejectBatchCancelled        RejectionCode = "batch_cancelled"
	RejectUnexpectedBatchStatus RejectionCode = "unexpected_batch_status"
)

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

type OutcomeKind string

const (
	OutcomeAccepted  OutcomeKind = "accepted"
	OutcomeRejected  OutcomeKind = "rejected"
	OutcomeDuplicate OutcomeKind = "duplicate"
)

// CommandOutcome is the stable result of handling a command.
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

// AcceptedWithFacts builds an accepted outcome (durable T-Reg envelope assembly).
func AcceptedWithFacts(commandID CommandID, facts []Fact) CommandOutcome {
	return acceptedOutcome(commandID, facts)
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

// --- Ranked match facts ---

// PlayerMatchStanding is one player's ranked facts from MatchCompleted.
type PlayerMatchStanding struct {
	PlayerID             PlayerID
	MatchWins            int
	CumulativeCardPoints int
	FinalGameCompletedAt time.Time
	Forfeited            bool
}

// --- Commands ---

type CreateTournamentCommand struct {
	CommandID    CommandID
	TournamentID TournamentID
	Capacity     int
	RetryBudget  int // optional; <=0 uses DefaultRetryBudget
	BatchSize    int // optional; <=0 uses DefaultBatchSize
	// Visibility empty defaults to public; unknown values reject as invalid_command.
	Visibility TournamentVisibility
}

type RegisterPlayerCommand struct {
	CommandID CommandID
	PlayerID  PlayerID
}

type CloseRegistrationCommand struct {
	CommandID CommandID
}

type SeedRoundCommand struct {
	CommandID   CommandID
	RoundNumber int
}

type ProvisionRoundMatchesCommand struct {
	CommandID   CommandID
	RoundNumber int
}

type AssignRoomCommand struct {
	CommandID   CommandID
	RoundNumber int
	SlotID      SlotID
	RoomID      RoomID
	BatchID     BatchID
}

type RecordMatchResultCommand struct {
	CommandID         CommandID
	EventID           EventID // processed-event idempotency
	RoomID            RoomID
	RoundNumber       int
	SlotID            SlotID
	CompletionVersion CompletionVersion
	Standings         []PlayerMatchStanding
	IsAbandoned       bool
}

type CompleteTournamentProvisioningBatchCommand struct {
	CommandID   CommandID
	RoundNumber int
	BatchID     BatchID
}

type CompleteRoundCommand struct {
	CommandID   CommandID
	RoundNumber int
}

type CompleteTournamentCommand struct {
	CommandID CommandID
}

type CancelTournamentCommand struct {
	CommandID CommandID
}

type RetryTournamentProvisioningBatchCommand struct {
	CommandID    CommandID
	RoundNumber  int
	BatchID      BatchID
	RetryAttempt int
}

type QuarantineTournamentProvisioningBatchCommand struct {
	CommandID   CommandID
	RoundNumber int
	BatchID     BatchID
	Reason      string
}

type QuarantineTournamentResultCommand struct {
	CommandID         CommandID
	RoomID            RoomID
	CompletionVersion CompletionVersion
	Reason            string
}
