package store

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseResumeStreamID maps a logical SSE Last-Event-ID (seq_N) or an explicit
// Redis stream ID (N-M) to the Redis stream ID used for bounded lookups.
// Returns ok=false for unrecognized forms (caller treats as snapshot_required).
func ParseResumeStreamID(lastEventID string) (redisID string, ok bool) {
	lastEventID = strings.TrimSpace(lastEventID)
	if lastEventID == "" {
		return "", false
	}
	if strings.HasPrefix(lastEventID, "seq_") {
		n := strings.TrimPrefix(lastEventID, "seq_")
		if n == "" {
			return "", false
		}
		seq, err := strconv.ParseUint(n, 10, 64)
		if err != nil {
			return "", false
		}
		return fmt.Sprintf("%d-0", seq), true
	}
	if isExplicitStreamID(lastEventID) {
		return lastEventID, true
	}
	return "", false
}

func isExplicitStreamID(id string) bool {
	dash := strings.IndexByte(id, '-')
	if dash <= 0 || dash == len(id)-1 {
		return false
	}
	if _, err := strconv.ParseUint(id[:dash], 10, 64); err != nil {
		return false
	}
	if _, err := strconv.ParseUint(id[dash+1:], 10, 64); err != nil {
		return false
	}
	return true
}

// ExplicitStreamID returns the Redis stream entry ID for an accepted room sequence.
// Lua CAS permits exactly one public stream entry per accepted sequence, so
// sequence-0 is stable and addressable without scanning.
func ExplicitStreamID(sequence uint64) string {
	return fmt.Sprintf("%d-0", sequence)
}
