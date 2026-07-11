package domain

import (
	"strconv"
	"strings"
)

// AssignmentByRoomID resolves the round/slot that owns roomID, if any.
func (t *Tournament) AssignmentByRoomID(roomID RoomID) (roundNumber int, slotID SlotID, ok bool) {
	if t == nil || !roomID.Valid() {
		return 0, "", false
	}
	key, exists := t.roomOwners[roomID]
	if !exists {
		return 0, "", false
	}
	parts := strings.SplitN(key, ":", 2)
	if len(parts) != 2 {
		return 0, "", false
	}
	n, err := strconv.Atoi(parts[0])
	if err != nil || n < 1 || parts[1] == "" {
		return 0, "", false
	}
	return n, SlotID(parts[1]), true
}

// RegisteredPlayers returns registration order (copy).
func (t *Tournament) RegisteredPlayers() []PlayerID {
	if t == nil {
		return nil
	}
	out := make([]PlayerID, len(t.registrationOrder))
	copy(out, t.registrationOrder)
	return out
}

// ResultKeysSnapshot returns durable (roomId:completionVersion) disposition records.
func (t *Tournament) ResultKeysSnapshot() map[string]ResultRecord {
	if t == nil || len(t.resultKeys) == 0 {
		return nil
	}
	out := make(map[string]ResultRecord, len(t.resultKeys))
	for k, v := range t.resultKeys {
		out[k] = ResultRecord{
			Disposition:   v.Disposition,
			Fingerprint:   v.Fingerprint,
			SourceEventID: v.SourceEventID,
		}
	}
	return out
}

// ProcessedEventsSnapshot returns event-id idempotency outcomes (copy).
func (t *Tournament) ProcessedEventsSnapshot() map[EventID]CommandOutcome {
	if t == nil || len(t.processedEvents) == 0 {
		return nil
	}
	return cloneOutcomesByEvent(t.processedEvents)
}

// RoomOwnersSnapshot returns roomId -> "round:slot" ownership map (copy).
func (t *Tournament) RoomOwnersSnapshot() map[RoomID]string {
	if t == nil || len(t.roomOwners) == 0 {
		return nil
	}
	return cloneRoomOwners(t.roomOwners)
}

// RoundsSnapshot returns a copy of known rounds sorted by number ascending.
func (t *Tournament) RoundsSnapshot() []Round {
	if t == nil || len(t.rounds) == 0 {
		return nil
	}
	nums := make([]int, 0, len(t.rounds))
	for n := range t.rounds {
		nums = append(nums, n)
	}
	for i := 0; i < len(nums); i++ {
		for j := i + 1; j < len(nums); j++ {
			if nums[j] < nums[i] {
				nums[i], nums[j] = nums[j], nums[i]
			}
		}
	}
	out := make([]Round, 0, len(nums))
	for _, n := range nums {
		r := t.rounds[n]
		if r == nil {
			continue
		}
		cp := *r
		cp.Slots = append([]BracketSlot(nil), r.Slots...)
		cp.Batches = append([]ProvisioningBatch(nil), r.Batches...)
		out = append(out, cp)
	}
	return out
}
