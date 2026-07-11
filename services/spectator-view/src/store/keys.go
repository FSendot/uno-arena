package store

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"unoarena/services/spectator-view/domain"
)

const (
	keySchemaVersion = "v1"
	defaultKeyPrefix = "spectator:"
)

var roomIDKeyPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)

// KeySpace builds deterministic validated Redis keys rooted in roomId.
type KeySpace struct {
	prefix string
}

// NewKeySpace returns a key space. Empty prefix defaults to spectator:.
// Integration tests pass a per-run random prefix so cleanup stays scoped.
func NewKeySpace(prefix string) KeySpace {
	p := strings.TrimSpace(prefix)
	if p == "" {
		p = defaultKeyPrefix
	}
	if !strings.HasSuffix(p, ":") {
		p += ":"
	}
	return KeySpace{prefix: p}
}

// Prefix returns the configured key prefix.
func (k KeySpace) Prefix() string { return k.prefix }

// ValidateRoomID rejects room ids that cannot form safe Redis key segments.
func ValidateRoomID(roomID domain.RoomID) error {
	if !roomID.Valid() || !roomIDKeyPattern.MatchString(string(roomID)) {
		return fmt.Errorf("invalid roomId for redis key")
	}
	return nil
}

func (k KeySpace) root(roomID domain.RoomID) string {
	// Hash tag {roomId} so all per-room Lua KEYS share one Redis Cluster slot.
	return fmt.Sprintf("%s%s:room:{%s}:", k.prefix, keySchemaVersion, string(roomID))
}

func (k KeySpace) Meta(roomID domain.RoomID) string {
	return k.root(roomID) + "meta"
}

func (k KeySpace) State(roomID domain.RoomID) string {
	return k.root(roomID) + "state"
}

func (k KeySpace) Outcomes(roomID domain.RoomID) string {
	return k.root(roomID) + "outcomes"
}

func (k KeySpace) Invites(roomID domain.RoomID) string {
	return k.root(roomID) + "invites"
}

func (k KeySpace) Generation(roomID domain.RoomID) string {
	return k.root(roomID) + "generation"
}

func (k KeySpace) Stream(roomID domain.RoomID, generation string) string {
	return k.root(roomID) + "stream:" + generation
}

// StreamKeyFor returns the active stream key for roomID (tests/ops).
func (s *RedisProjectionStore) StreamKeyFor(ctx context.Context, roomID domain.RoomID) (string, error) {
	load, err := s.loadRoom(ctx, roomID)
	if err != nil {
		return "", err
	}
	gen := load.generation
	if gen == "" {
		gen = defaultGeneration
	}
	return s.keys.Stream(roomID, gen), nil
}

func (k KeySpace) ScanPattern() string {
	return k.prefix + "*"
}
