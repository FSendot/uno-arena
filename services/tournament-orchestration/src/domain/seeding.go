package domain

import (
	"fmt"
	"sort"
	"strconv"
)

// Seeding chunk bounds for durable seeding worker batches (all rounds).
const (
	MaxSeedingSlotsPerChunk         = 1000
	MaxSeedingRegistrationsPerChunk = 10000
	SeedingSourceRegistrations      = "registrations"
	SeedingSourceAdvancement        = "advancement"
)

// SeedingJobStatus is the durable round_seeding_jobs lifecycle.
type SeedingJobStatus string

const (
	SeedingJobPending     SeedingJobStatus = "pending"
	SeedingJobInProgress  SeedingJobStatus = "in_progress"
	SeedingJobCompleted   SeedingJobStatus = "completed"
	SeedingJobQuarantined SeedingJobStatus = "quarantined"
	SeedingJobCancelled   SeedingJobStatus = "cancelled"
)

// RoundSlotPlan is the immutable bracket partition for any round.
// N<=FinalPlayerThreshold → one final slot; else S=ceil(N/PlayersPerRoom),
// base=floor(N/S), rem=N%S; first rem slots get base+1, then base.
// Source order is always player_id ASC.
type RoundSlotPlan struct {
	PlayerCount int
	SlotCount   int
	BaseSize    int
	Remainder   int
	IsFinal     bool
	// SlotSizes is materialised for small N; nil for large plans (use SizeForSlot).
	SlotSizes []int
}

// Round1SlotPlan is a backward-compatible alias for RoundSlotPlan.
type Round1SlotPlan = RoundSlotPlan

// SizeForSlot returns the player count for zero-based slot index.
func (p RoundSlotPlan) SizeForSlot(index int) int {
	if index < 0 || index >= p.SlotCount {
		return 0
	}
	if len(p.SlotSizes) == p.SlotCount {
		return p.SlotSizes[index]
	}
	size := p.BaseSize
	if index < p.Remainder {
		size++
	}
	return size
}

// PlayersForSlots returns how many source players are needed for slots [from,to).
func (p RoundSlotPlan) PlayersForSlots(from, toExclusive int) int {
	if from < 0 {
		from = 0
	}
	if toExclusive > p.SlotCount {
		toExclusive = p.SlotCount
	}
	sum := 0
	for i := from; i < toExclusive; i++ {
		sum += p.SizeForSlot(i)
	}
	return sum
}

// ComputeRoundSlotPlan builds the immutable slot size plan for N players (any round).
func ComputeRoundSlotPlan(n int) (RoundSlotPlan, error) {
	if n < 1 {
		return RoundSlotPlan{}, fmt.Errorf("at least one player required")
	}
	if n <= FinalPlayerThreshold {
		return RoundSlotPlan{
			PlayerCount: n,
			SlotCount:   1,
			BaseSize:    n,
			Remainder:   0,
			IsFinal:     true,
			SlotSizes:   []int{n},
		}, nil
	}
	slotCount := (n + PlayersPerRoom - 1) / PlayersPerRoom
	base := n / slotCount
	rem := n % slotCount
	plan := RoundSlotPlan{
		PlayerCount: n,
		SlotCount:   slotCount,
		BaseSize:    base,
		Remainder:   rem,
		IsFinal:     false,
	}
	// Materialise sizes when cheap; million-player plans stay O(1) metadata.
	if slotCount <= MaxSeedingSlotsPerChunk*4 {
		sizes := make([]int, slotCount)
		for i := 0; i < slotCount; i++ {
			sizes[i] = base
			if i < rem {
				sizes[i]++
			}
		}
		plan.SlotSizes = sizes
	}
	return plan, nil
}

// ComputeRound1SlotPlan is a backward-compatible alias for ComputeRoundSlotPlan.
func ComputeRound1SlotPlan(n int) (Round1SlotPlan, error) {
	return ComputeRoundSlotPlan(n)
}

// SlotIDForIndex returns the durable public slot id (slot_<zero-based-index>).
func SlotIDForIndex(index int) SlotID {
	return slotIDForIndex(index)
}

// SortPlayerIDsAsc returns a new slice sorted by player_id lexicographic ASC.
func SortPlayerIDsAsc(players []PlayerID) []PlayerID {
	out := make([]PlayerID, len(players))
	copy(out, players)
	sort.Slice(out, func(i, j int) bool {
		return string(out[i]) < string(out[j])
	})
	return out
}

// SeedRoundCommandID is the deterministic seeding job command identity.
func SeedRoundCommandID(tournamentID TournamentID, roundNumber int) string {
	return "seed:" + string(tournamentID) + ":r" + strconv.Itoa(roundNumber)
}

// SeedRoundKickoffContext is the bounded durable SeedRound decision input (any round).
type SeedRoundKickoffContext struct {
	TournamentID TournamentID
	Exists       bool
	Phase        TournamentPhase
	RoundNumber  int
	// Round1 / registrations source.
	RegisteredCount int
	// Later-round / advancement source.
	SourcePlayerCount   int // exact COUNT from round_advancing_players for source round
	PreviousRoundStatus RoundStatus
	PreviousRoundFound  bool
	// Target round + job state.
	RoundStatus  RoundStatus // empty when round row absent
	JobStatus    SeedingJobStatus
	JobCommandID string
}

// SeedRound1KickoffContext is the Round-1 kickoff input (Round1Status aliases RoundStatus).
type SeedRound1KickoffContext struct {
	TournamentID    TournamentID
	Exists          bool
	Phase           TournamentPhase
	RegisteredCount int
	Round1Status    RoundStatus // empty when round row absent
	JobStatus       SeedingJobStatus
	JobCommandID    string
}

// SeedKickoffKind classifies durable SeedRound kickoff apply.
type SeedKickoffKind string

const (
	SeedKickoffReject        SeedKickoffKind = "reject"
	SeedKickoffSchedule      SeedKickoffKind = "schedule"
	SeedKickoffAlreadyDone   SeedKickoffKind = "already_done"    // round already seeded+
	SeedKickoffJobExistsNoop SeedKickoffKind = "job_exists_noop" // other command; job active/completed
)

// SeedRoundKickoffDecision is pure policy before durable kickoff apply.
type SeedRoundKickoffDecision struct {
	Kind               SeedKickoffKind
	Outcome            CommandOutcome
	Plan               RoundSlotPlan
	Source             string // registrations | advancement
	SourceRoundNumber  int    // 0 for round1 registrations; round-1 for later
}

// SeedRound1KickoffDecision is a backward-compatible alias.
type SeedRound1KickoffDecision = SeedRoundKickoffDecision

// DecideSeedRoundKickoff evaluates SeedRound for any round.
// Round 1 retains registration/phase=seeding behavior.
// Round >1 requires phase=in_progress, previous round completed, exact normalized source count>0.
// Accepted schedule/noop outcomes are always factless; TournamentRoundSeeded is worker-finalization internal.
func DecideSeedRoundKickoff(ctx SeedRoundKickoffContext, cmd SeedRoundCommand) SeedRoundKickoffDecision {
	if !cmd.CommandID.Valid() || cmd.RoundNumber < 1 {
		return SeedRoundKickoffDecision{
			Kind: SeedKickoffReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidCommand,
				Message: "seed requires commandId and roundNumber>=1",
			}),
		}
	}
	if !ctx.Exists {
		return SeedRoundKickoffDecision{
			Kind: SeedKickoffReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidIdentity,
				Message: "tournament not found",
			}),
		}
	}
	if ctx.Phase.IsTerminal() {
		return SeedRoundKickoffDecision{
			Kind: SeedKickoffReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectAlreadyTerminal,
				Message: "tournament is terminal",
			}),
		}
	}
	if ctx.RoundStatus != "" && ctx.RoundStatus != RoundPending {
		return SeedRoundKickoffDecision{
			Kind:    SeedKickoffAlreadyDone,
			Outcome: acceptedOutcome(cmd.CommandID, nil),
		}
	}
	switch ctx.JobStatus {
	case SeedingJobPending, SeedingJobInProgress, SeedingJobCompleted:
		if ctx.JobCommandID != "" && ctx.JobCommandID != string(cmd.CommandID) {
			return SeedRoundKickoffDecision{
				Kind:    SeedKickoffJobExistsNoop,
				Outcome: acceptedOutcome(cmd.CommandID, nil),
			}
		}
		// Same command id with existing job: fall through; store ON CONFLICT accepts exact match.
	case SeedingJobQuarantined, SeedingJobCancelled:
		return SeedRoundKickoffDecision{
			Kind: SeedKickoffReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidCommand,
				Message: "seeding job is terminal",
			}),
		}
	}

	if cmd.RoundNumber == 1 {
		return decideSeedRound1Kickoff(ctx, cmd)
	}
	return decideSeedLaterRoundKickoff(ctx, cmd)
}

func decideSeedRound1Kickoff(ctx SeedRoundKickoffContext, cmd SeedRoundCommand) SeedRoundKickoffDecision {
	if ctx.Phase != PhaseSeeding {
		return SeedRoundKickoffDecision{
			Kind: SeedKickoffReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectWrongPhase,
				Message: "round 1 requires seeding phase",
			}),
		}
	}
	if ctx.RegisteredCount < 1 {
		return SeedRoundKickoffDecision{
			Kind: SeedKickoffReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidCommand,
				Message: "no players to seed",
			}),
		}
	}
	plan, err := ComputeRoundSlotPlan(ctx.RegisteredCount)
	if err != nil {
		return SeedRoundKickoffDecision{
			Kind: SeedKickoffReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidCommand,
				Message: "no players to seed",
			}),
		}
	}
	return SeedRoundKickoffDecision{
		Kind:              SeedKickoffSchedule,
		Outcome:           acceptedOutcome(cmd.CommandID, nil),
		Plan:              plan,
		Source:            SeedingSourceRegistrations,
		SourceRoundNumber: 0,
	}
}

func decideSeedLaterRoundKickoff(ctx SeedRoundKickoffContext, cmd SeedRoundCommand) SeedRoundKickoffDecision {
	if ctx.Phase != PhaseInProgress {
		return SeedRoundKickoffDecision{
			Kind: SeedKickoffReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectWrongPhase,
				Message: "later-round seeding requires in_progress phase",
			}),
		}
	}
	srcRound := cmd.RoundNumber - 1
	if !ctx.PreviousRoundFound || ctx.PreviousRoundStatus != RoundCompleted {
		return SeedRoundKickoffDecision{
			Kind: SeedKickoffReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidCommand,
				Message: "previous round must be completed",
			}),
		}
	}
	if ctx.SourcePlayerCount < 1 {
		return SeedRoundKickoffDecision{
			Kind: SeedKickoffReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidCommand,
				Message: "no advancing players to seed",
			}),
		}
	}
	plan, err := ComputeRoundSlotPlan(ctx.SourcePlayerCount)
	if err != nil {
		return SeedRoundKickoffDecision{
			Kind: SeedKickoffReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidCommand,
				Message: "no advancing players to seed",
			}),
		}
	}
	return SeedRoundKickoffDecision{
		Kind:              SeedKickoffSchedule,
		Outcome:           acceptedOutcome(cmd.CommandID, nil),
		Plan:              plan,
		Source:            SeedingSourceAdvancement,
		SourceRoundNumber: srcRound,
	}
}

// DecideSeedRound1Kickoff evaluates SeedRound — delegates to DecideSeedRoundKickoff.
// Round>1 is accepted when later-round context is valid (no longer hard-rejects).
func DecideSeedRound1Kickoff(ctx SeedRound1KickoffContext, cmd SeedRoundCommand) SeedRound1KickoffDecision {
	return DecideSeedRoundKickoff(SeedRoundKickoffContext{
		TournamentID:    ctx.TournamentID,
		Exists:          ctx.Exists,
		Phase:           ctx.Phase,
		RoundNumber:     cmd.RoundNumber,
		RegisteredCount: ctx.RegisteredCount,
		RoundStatus:     ctx.Round1Status,
		JobStatus:       ctx.JobStatus,
		JobCommandID:    ctx.JobCommandID,
	}, cmd)
}

// NextSeedingChunkBounds returns the next slot window and exact player LIMIT for a chunk.
// Caps at MaxSeedingSlotsPerChunk slots and MaxSeedingRegistrationsPerChunk players.
func NextSeedingChunkBounds(plan RoundSlotPlan, nextSlotIndex int) (slotFrom, slotToExclusive, playerLimit int) {
	if nextSlotIndex < 0 || nextSlotIndex >= plan.SlotCount {
		return nextSlotIndex, nextSlotIndex, 0
	}
	slotFrom = nextSlotIndex
	slotToExclusive = nextSlotIndex
	players := 0
	for slotToExclusive < plan.SlotCount && (slotToExclusive-slotFrom) < MaxSeedingSlotsPerChunk {
		size := plan.SizeForSlot(slotToExclusive)
		if players+size > MaxSeedingRegistrationsPerChunk && players > 0 {
			break
		}
		if size > MaxSeedingRegistrationsPerChunk {
			// Single slot cannot exceed the registration cap under normal plans (max 10).
			break
		}
		players += size
		slotToExclusive++
	}
	return slotFrom, slotToExclusive, players
}
