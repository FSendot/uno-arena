package store

import (
	"fmt"
	"regexp"
	"strings"
)

const (
	// SessionInvalidationKeyPrefix is the Gateway-owned Redis namespace on DB6.
	SessionInvalidationKeyPrefix = "gateway:si:v1"
	// SessionInvalidationNotifyChannel wakes other Gateway replicas (wake only).
	SessionInvalidationNotifyChannel = SessionInvalidationKeyPrefix + ":notify"
)

var siIDKeyPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)

// ValidateSessionInvalidationID rejects ids that cannot form safe Redis key segments.
func ValidateSessionInvalidationID(id string) error {
	id = strings.TrimSpace(id)
	if id == "" || !siIDKeyPattern.MatchString(id) {
		return fmt.Errorf("invalid id for redis key")
	}
	return nil
}

// SessionInvalidationKeySpace builds deterministic keys under gateway:si:v1.
type SessionInvalidationKeySpace struct {
	prefix string
}

// NewSessionInvalidationKeySpace returns the SI key space. Empty prefix defaults
// to gateway:si:v1.
func NewSessionInvalidationKeySpace(prefix string) SessionInvalidationKeySpace {
	p := strings.TrimSpace(prefix)
	if p == "" {
		p = SessionInvalidationKeyPrefix
	}
	return SessionInvalidationKeySpace{prefix: strings.TrimRight(p, ":")}
}

// Prefix returns the configured key prefix without a trailing colon.
func (k SessionInvalidationKeySpace) Prefix() string { return k.prefix }

// Session is the durable invalidation hash keyed by sessionId (idempotency).
func (k SessionInvalidationKeySpace) Session(sessionID string) string {
	return fmt.Sprintf("%s:session:%s", k.prefix, sessionID)
}

// Event is the control-binding hash keyed by eventId.
func (k SessionInvalidationKeySpace) Event(eventID string) string {
	return fmt.Sprintf("%s:event:%s", k.prefix, eventID)
}

// KafkaQuarantine is the ADR-0017 aggregate quarantine hash for a playerId key.
func (k SessionInvalidationKeySpace) KafkaQuarantine(playerID string) string {
	// Hash tag keeps future multi-key player Lua single-slot on Redis Cluster.
	return fmt.Sprintf("%s:player:{%s}:kafka_quarantine", k.prefix, playerID)
}

// NotifyChannel is the Pub/Sub wake channel for cross-replica Hub closure.
func (k SessionInvalidationKeySpace) NotifyChannel() string {
	return k.prefix + ":notify"
}
