package bff

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"
)

const (
	// PlayerFeedStreamPrefix matches Room Gameplay realtime outbox target_stream.
	PlayerFeedStreamPrefix = "room:"
	// PlayerFeedStreamSuffix is the fixed injective suffix (roomId cannot contain ':').
	PlayerFeedStreamSuffix = ":player"

	spectatorKeySchemaVersion  = "v1"
	defaultSpectatorKeyPrefix  = "spectator:"
	defaultSpectatorGeneration = "1"
)

var roomIDKeyPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)

// PlayerFeedTargetStream returns room:{roomId}:player. roomId must be non-empty,
// trimmed, and free of ':' / whitespace / control so the three-segment key stays injective.
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

// SpectatorKeySpace mirrors Spectator View Redis key layout (prefix + v1 + hash-tagged roomId).
type SpectatorKeySpace struct {
	prefix string
}

// NewSpectatorKeySpace returns a key space. Empty prefix defaults to spectator:.
func NewSpectatorKeySpace(prefix string) SpectatorKeySpace {
	p := strings.TrimSpace(prefix)
	if p == "" {
		p = defaultSpectatorKeyPrefix
	}
	if !strings.HasSuffix(p, ":") {
		p += ":"
	}
	return SpectatorKeySpace{prefix: p}
}

// Prefix returns the configured key prefix.
func (k SpectatorKeySpace) Prefix() string { return k.prefix }

// ValidateSpectatorRoomID rejects room ids that cannot form safe Redis key segments.
func ValidateSpectatorRoomID(roomID string) error {
	id := strings.TrimSpace(roomID)
	if id == "" || id != roomID || !roomIDKeyPattern.MatchString(id) {
		return fmt.Errorf("invalid roomId for redis key")
	}
	return nil
}

func (k SpectatorKeySpace) root(roomID string) string {
	return fmt.Sprintf("%s%s:room:{%s}:", k.prefix, spectatorKeySchemaVersion, roomID)
}

// Meta returns spectator:v1:room:{roomId}:meta.
func (k SpectatorKeySpace) Meta(roomID string) string {
	return k.root(roomID) + "meta"
}

// Stream returns spectator:v1:room:{roomId}:stream:{generation}.
func (k SpectatorKeySpace) Stream(roomID, generation string) string {
	gen := strings.TrimSpace(generation)
	if gen == "" {
		gen = defaultSpectatorGeneration
	}
	return k.root(roomID) + "stream:" + gen
}

// Generation returns the generation marker key.
func (k SpectatorKeySpace) Generation(roomID string) string {
	return k.root(roomID) + "generation"
}
