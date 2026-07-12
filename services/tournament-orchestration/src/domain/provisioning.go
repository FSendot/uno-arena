package domain

import "strings"

// ProvisioningBatch is a deterministic slot-range work unit for room creation.
type ProvisioningBatch struct {
	BatchID          BatchID
	SlotFrom         SlotID
	SlotTo           SlotID
	SlotIndexes      []int
	Status           BatchStatus
	RetryAttempt     int
	LastError        string
	QuarantineReason string
}

// BracketSlot holds seeded players and optional room assignment for one match.
type BracketSlot struct {
	SlotID            SlotID
	Index             int
	Status            SlotStatus
	SeededPlayers     []PlayerID
	RoomID            RoomID
	BatchID           BatchID
	CompletionVersion CompletionVersion
	HasResult         bool
	ResultFingerprint string
	Standings         []PlayerMatchStanding
	Advancing         []PlayerID
	QuarantineReason  string
}

func (s BracketSlot) IsTerminal() bool {
	switch s.Status {
	case SlotResultRecorded, SlotAdvanced, SlotCancelled:
		return true
	default:
		return false
	}
}

// Round owns bracket slots and provisioning batches for one elimination tier.
type Round struct {
	Number    int
	Status    RoundStatus
	IsFinal   bool
	Slots     []BracketSlot
	Batches   []ProvisioningBatch
	Completed bool
}

// buildSlots partitions ordered players into deterministic bracket slots.
// When n > FinalPlayerThreshold, slotCount is ceil(n/PlayersPerRoom) and players
// are distributed evenly (size difference at most one) so every non-final room
// has at least AdvancersPerMatch players and at most PlayersPerRoom.
// Input order is preserved; earlier slots receive the remainder extras.
func buildSlots(players []PlayerID) []BracketSlot {
	n := len(players)
	if n == 0 {
		return nil
	}
	if n <= FinalPlayerThreshold {
		seeded := make([]PlayerID, n)
		copy(seeded, players)
		return []BracketSlot{{
			SlotID:        slotIDForIndex(0),
			Index:         0,
			Status:        SlotPending,
			SeededPlayers: seeded,
		}}
	}

	slotCount := (n + PlayersPerRoom - 1) / PlayersPerRoom
	base := n / slotCount
	rem := n % slotCount
	slots := make([]BracketSlot, 0, slotCount)
	offset := 0
	for i := 0; i < slotCount; i++ {
		size := base
		if i < rem {
			size++
		}
		seeded := make([]PlayerID, size)
		copy(seeded, players[offset:offset+size])
		slots = append(slots, BracketSlot{
			SlotID:        slotIDForIndex(i),
			Index:         i,
			Status:        SlotPending,
			SeededPlayers: seeded,
		})
		offset += size
	}
	return slots
}

// buildBatches partitions slot indices into deterministic batches of batchSize.
func buildBatches(slotCount, batchSize int) []ProvisioningBatch {
	if slotCount <= 0 {
		return nil
	}
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	batches := make([]ProvisioningBatch, 0, (slotCount+batchSize-1)/batchSize)
	for start := 0; start < slotCount; start += batchSize {
		end := start + batchSize - 1
		if end >= slotCount {
			end = slotCount - 1
		}
		indexes := make([]int, 0, end-start+1)
		for i := start; i <= end; i++ {
			indexes = append(indexes, i)
		}
		idx := len(batches)
		batches = append(batches, ProvisioningBatch{
			BatchID:     batchIDForIndex(idx),
			SlotFrom:    slotIDForIndex(start),
			SlotTo:      slotIDForIndex(end),
			SlotIndexes: indexes,
			Status:      BatchPending,
		})
	}
	return batches
}

func (r *Round) FindSlot(slotID SlotID) (*BracketSlot, bool) {
	return r.findSlot(slotID)
}

func (r *Round) FindBatch(batchID BatchID) (*ProvisioningBatch, bool) {
	return r.findBatch(batchID)
}

func (r *Round) findSlot(slotID SlotID) (*BracketSlot, bool) {
	for i := range r.Slots {
		if r.Slots[i].SlotID == slotID {
			return &r.Slots[i], true
		}
	}
	return nil, false
}

func (r *Round) findBatch(batchID BatchID) (*ProvisioningBatch, bool) {
	for i := range r.Batches {
		if r.Batches[i].BatchID == batchID {
			return &r.Batches[i], true
		}
	}
	return nil, false
}

func (r *Round) allSlotsTerminalAndAdvanced() bool {
	if len(r.Slots) == 0 {
		return false
	}
	for i := range r.Slots {
		s := &r.Slots[i]
		if !s.IsTerminal() {
			return false
		}
		if r.IsFinal {
			if !s.HasResult {
				return false
			}
			continue
		}
		if len(s.Advancing) == 0 && s.Status != SlotCancelled {
			return false
		}
	}
	return true
}

func (r *Round) advancingPlayers() []PlayerID {
	out := make([]PlayerID, 0)
	for i := range r.Slots {
		out = append(out, r.Slots[i].Advancing...)
	}
	return out
}

func (r *Round) anyBatchQuarantined() bool {
	for i := range r.Batches {
		if r.Batches[i].Status == BatchQuarantined {
			return true
		}
	}
	return false
}

func (b ProvisioningBatch) coversSlotIndex(index int) bool {
	for _, i := range b.SlotIndexes {
		if i == index {
			return true
		}
	}
	return false
}

// SanitizeProvisioningReason keeps only a short class/code; never raw operator text,
// HTTP bodies, or secrets. Empty input stays empty (callers may substitute a default).
func SanitizeProvisioningReason(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	lower := strings.ToLower(raw)
	switch {
	case strings.Contains(lower, "retry_budget_exhausted") || strings.Contains(lower, "retry budget exhausted"):
		return "retry_budget_exhausted"
	case strings.Contains(lower, "room_assignment_conflict") || strings.Contains(lower, "room_id_mismatch"):
		return "room_assignment_conflict"
	case strings.Contains(lower, "conflicting_assignment"):
		return "conflicting_assignment"
	case strings.Contains(lower, "timeout") || strings.Contains(lower, "deadline"):
		return "room_provision_timeout"
	case strings.Contains(lower, "connection refused") || strings.Contains(lower, "connection reset"):
		return "room_provision_unavailable"
	case strings.Contains(lower, "http 5"):
		return "room_provision_http_5xx"
	case strings.Contains(lower, "http 4"):
		return "room_provision_http_4xx"
	case strings.Contains(lower, "lease_owner") || strings.Contains(lower, "provisioning_fence"):
		return "lease_owner_lost"
	case strings.Contains(lower, "quarantined") || strings.Contains(lower, "operator"):
		return "quarantined"
	default:
		return "room_provision_failed"
	}
}
