package domain

import (
	"fmt"
	"hash/fnv"
	"strconv"
)

// RegistrationShardCount is the fixed shard fan-out for registration quotas.
const RegistrationShardCount = 64

// AllocateRegistrationQuotas distributes capacity across 64 shards so quotas
// sum EXACTLY to capacity. Shards 0..rem-1 receive base+1; rem..63 receive base
// where base=capacity/64 and rem=capacity%64.
func AllocateRegistrationQuotas(capacity int) []int {
	quotas := make([]int, RegistrationShardCount)
	if capacity <= 0 {
		return quotas
	}
	base := capacity / RegistrationShardCount
	rem := capacity % RegistrationShardCount
	for i := 0; i < RegistrationShardCount; i++ {
		quotas[i] = base
		if i < rem {
			quotas[i]++
		}
	}
	return quotas
}

// RegistrationQuotaSum returns the sum of AllocateRegistrationQuotas(capacity).
func RegistrationQuotaSum(capacity int) int {
	sum := 0
	for _, q := range AllocateRegistrationQuotas(capacity) {
		sum += q
	}
	return sum
}

// RegistrationStartShard returns the stable FNV-1a 64-bit start shard for
// tournamentId + NUL + playerId (deterministic probe origin).
func RegistrationStartShard(tournamentID, playerID string) int {
	h := fnv.New64a()
	_, _ = h.Write([]byte(tournamentID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(playerID))
	return int(h.Sum64() % uint64(RegistrationShardCount))
}

// RegistrationProbeOrder returns the deterministic probe sequence of length 64
// starting at RegistrationStartShard(tournamentID, playerID).
func RegistrationProbeOrder(tournamentID, playerID string) []int {
	start := RegistrationStartShard(tournamentID, playerID)
	out := make([]int, RegistrationShardCount)
	for i := 0; i < RegistrationShardCount; i++ {
		out[i] = (start + i) % RegistrationShardCount
	}
	return out
}

// AssignRegistrationShard picks a shard for a legacy-only player that lacks a
// preserved allocation, preferring the FNV start and probing for a shard with
// remaining quota (usedCounts tracks assignments already claimed in this rebuild).
func AssignRegistrationShard(tournamentID, playerID string, quotas []int, usedCounts []int) (int, bool) {
	if len(quotas) != RegistrationShardCount || len(usedCounts) != RegistrationShardCount {
		return 0, false
	}
	for _, shard := range RegistrationProbeOrder(tournamentID, playerID) {
		if usedCounts[shard] < quotas[shard] {
			return shard, true
		}
	}
	return 0, false
}

// RegistrationContext is the bounded decision input for RegisterPlayer (never a full hydrate).
type RegistrationContext struct {
	TournamentID     TournamentID
	Phase            TournamentPhase
	Capacity         int
	PlayerRegistered bool
	Exists           bool
}

// CloseRegistrationContext is the bounded input for manual CloseRegistration.
type CloseRegistrationContext struct {
	TournamentID    TournamentID
	Phase           TournamentPhase
	Capacity        int
	RegisteredCount int // exact O(64) SUM of shard counts
	Exists          bool
}

// CreateTournamentContext is the bounded input for durable CreateTournament.
type CreateTournamentContext struct {
	TournamentID TournamentID
	Exists       bool
}

// RegistrationDecisionKind classifies durable registration apply.
type RegistrationDecisionKind string

const (
	RegistrationReject            RegistrationDecisionKind = "reject"
	RegistrationAlreadyRegistered RegistrationDecisionKind = "already_registered" // accepted, no facts
	RegistrationReserve           RegistrationDecisionKind = "reserve"
	RegistrationCloseNoop         RegistrationDecisionKind = "close_noop" // accepted, no facts
	RegistrationClose             RegistrationDecisionKind = "close"
	RegistrationCreate            RegistrationDecisionKind = "create"
)

// RegistrationDecision is pure policy output before durable apply.
// Capacity reservation itself is store-atomic (UPDATE count<quota); this only gates phase/identity.
type RegistrationDecision struct {
	Kind    RegistrationDecisionKind
	Outcome CommandOutcome
}

// DecideRegisterPlayer evaluates identity/phase/duplicate against a bounded context.
// Capacity is NOT decided here — durable path probes shards atomically.
func DecideRegisterPlayer(ctx RegistrationContext, cmd RegisterPlayerCommand) RegistrationDecision {
	if !cmd.CommandID.Valid() || !cmd.PlayerID.Valid() {
		return RegistrationDecision{
			Kind: RegistrationReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidIdentity,
				Message: "register requires commandId and playerId",
			}),
		}
	}
	// !Exists is a service-level tournament_not_found reject (not domain phase policy).
	if ctx.Phase.IsTerminal() {
		return RegistrationDecision{
			Kind: RegistrationReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectAlreadyTerminal,
				Message: "tournament is terminal",
			}),
		}
	}
	if ctx.Phase != PhaseRegistration {
		return RegistrationDecision{
			Kind: RegistrationReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectWrongPhase,
				Message: "registration is closed",
			}),
		}
	}
	if ctx.PlayerRegistered {
		return RegistrationDecision{
			Kind:    RegistrationAlreadyRegistered,
			Outcome: acceptedOutcome(cmd.CommandID, nil),
		}
	}
	return RegistrationDecision{
		Kind:    RegistrationReserve,
		Outcome: acceptedOutcome(cmd.CommandID, nil), // facts filled after successful reservation
	}
}

// PlayerRegisteredFact builds the canonical registration fact.
func PlayerRegisteredFact(tournamentID TournamentID, playerID PlayerID) Fact {
	return newFact(FactPlayerRegisteredInTournament, map[string]string{
		"tournamentId": string(tournamentID),
		"playerId":     string(playerID),
	})
}

// RegistrationClosedFact builds the canonical close fact.
func RegistrationClosedFact(tournamentID TournamentID, registeredCount int) Fact {
	return newFact(FactTournamentRegistrationClosed, map[string]string{
		"tournamentId":    string(tournamentID),
		"registeredCount": strconv.Itoa(registeredCount),
		"phase":           string(PhaseSeeding),
	})
}

// DecideCloseRegistration evaluates manual close against a bounded O(64) context.
func DecideCloseRegistration(ctx CloseRegistrationContext, cmd CloseRegistrationCommand) RegistrationDecision {
	if !cmd.CommandID.Valid() {
		return RegistrationDecision{
			Kind: RegistrationReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidCommand,
				Message: "close requires commandId",
			}),
		}
	}
	// !Exists is a service-level tournament_not_found reject.
	if ctx.Phase.IsTerminal() {
		return RegistrationDecision{
			Kind: RegistrationReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectAlreadyTerminal,
				Message: "tournament is terminal",
			}),
		}
	}
	if ctx.Phase != PhaseRegistration {
		return RegistrationDecision{
			Kind:    RegistrationCloseNoop,
			Outcome: acceptedOutcome(cmd.CommandID, nil),
		}
	}
	if ctx.RegisteredCount == 0 {
		return RegistrationDecision{
			Kind: RegistrationReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidCommand,
				Message: "cannot close with zero registrations",
			}),
		}
	}
	return RegistrationDecision{
		Kind: RegistrationClose,
		Outcome: acceptedOutcome(cmd.CommandID, []Fact{
			RegistrationClosedFact(ctx.TournamentID, ctx.RegisteredCount),
		}),
	}
}

// DecideCreateTournament evaluates create policy without constructing a full aggregate.
// Caller must reject tournament_already_exists when Exists before invoking this (service parity).
// On success Kind=RegistrationCreate and Outcome carries TournamentCreated facts.
func DecideCreateTournament(cmd CreateTournamentCommand) RegistrationDecision {
	if !cmd.CommandID.Valid() || !cmd.TournamentID.Valid() {
		return RegistrationDecision{
			Kind: RegistrationReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidIdentity,
				Message: "create requires commandId and tournamentId",
			}),
		}
	}
	if cmd.Capacity <= 0 {
		return RegistrationDecision{
			Kind: RegistrationReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidCommand,
				Message: "capacity must be positive",
			}),
		}
	}
	if cmd.BatchSize > MaxProvisioningBatchSize || cmd.BatchSize < 0 {
		return RegistrationDecision{
			Kind: RegistrationReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidCommand,
				Message: "batchSize out of range",
			}),
		}
	}
	vis, err := NormalizeTournamentVisibility(string(cmd.Visibility))
	if err != nil {
		return RegistrationDecision{
			Kind: RegistrationReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidCommand,
				Message: "visibility must be public or private",
			}),
		}
	}
	return RegistrationDecision{
		Kind: RegistrationCreate,
		Outcome: acceptedOutcome(cmd.CommandID, []Fact{
			TournamentCreatedFact(cmd.TournamentID, cmd.Capacity, vis),
		}),
	}
}

// TournamentCreatedFact builds the canonical create fact (includes visibility).
func TournamentCreatedFact(tournamentID TournamentID, capacity int, visibility TournamentVisibility) Fact {
	return newFact(FactTournamentCreated, map[string]string{
		"tournamentId": string(tournamentID),
		"capacity":     strconv.Itoa(capacity),
		"phase":        string(PhaseRegistration),
		"visibility":   string(visibility),
	})
}

// NormalizedCreateDefaults returns retry/batch matching CreateTournament aggregate defaults.
// Explicit batchSize > MaxProvisioningBatchSize or negative is rejected (never silently capped).
func NormalizedCreateDefaults(retryBudget, batchSize int) (retry int, batch int, err error) {
	if batchSize > MaxProvisioningBatchSize || batchSize < 0 {
		return 0, 0, fmt.Errorf("batchSize out of range")
	}
	retry = retryBudget
	if retry <= 0 {
		retry = DefaultRetryBudget
	}
	batch = batchSize
	if batch <= 0 {
		batch = DefaultBatchSize
	}
	return retry, batch, nil
}

// CapacityExceededOutcome is the stable rejection when all 64 shard reservations fail.
func CapacityExceededOutcome(commandID CommandID) CommandOutcome {
	return rejectedOutcome(commandID, Rejection{
		Code:    RejectCapacityExceeded,
		Message: "tournament at capacity",
	})
}
