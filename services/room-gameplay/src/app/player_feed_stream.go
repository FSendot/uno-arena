package app

import (
	"fmt"
	"strings"
	"unicode"
)

const (
	// PlayerFeedStreamPrefix is the Redis Streams key prefix for per-room player feeds (ADR-0015).
	PlayerFeedStreamPrefix = "room:"
	// PlayerFeedStreamSuffix is the fixed suffix so keys remain injective with room IDs
	// that cannot contain ':' (see PlayerFeedTargetStream).
	PlayerFeedStreamSuffix = ":player"
)

// PlayerFeedTargetStream returns the deterministic Redis Streams key for a room's
// player feed: room:{roomId}:player. roomId must be non-empty, trimmed, free of
// ':' / whitespace / control runes so the three-segment key stays injective.
func PlayerFeedTargetStream(roomID string) (string, error) {
	id := strings.TrimSpace(roomID)
	if id == "" {
		return "", fmt.Errorf("player feed stream: empty room id")
	}
	if id != roomID {
		return "", fmt.Errorf("player feed stream: room id must not have leading/trailing space")
	}
	for _, r := range id {
		if r == ':' {
			return "", fmt.Errorf("player feed stream: room id must not contain ':'")
		}
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return "", fmt.Errorf("player feed stream: room id must not contain whitespace/control")
		}
	}
	return PlayerFeedStreamPrefix + id + PlayerFeedStreamSuffix, nil
}
