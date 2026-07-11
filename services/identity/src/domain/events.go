package domain

import (
	"sync"
	"time"
)

const (
	EventTypeSessionInvalidated   = "SessionInvalidated"
	SessionInvalidatedSchemaV1    = 1
	OutboxTopicSessionInvalidated = "identity.session.invalidated"
)

// SessionInvalidatedEvent is the outbox-facing payload for identity.session.invalidated.
// Fields align with AsyncAPI EventMetadata + SessionInvalidated body.
type SessionInvalidatedEvent struct {
	EventID       string
	EventType     string
	SchemaVersion int
	CorrelationID string
	PlayerID      PlayerID
	SessionID     SessionID
	Reason        string
	OccurredAt    time.Time
}

// Normalize fills AsyncAPI defaults (EventType, SchemaVersion, CorrelationID).
func (e SessionInvalidatedEvent) Normalize() SessionInvalidatedEvent {
	if e.EventType == "" {
		e.EventType = EventTypeSessionInvalidated
	}
	if e.SchemaVersion == 0 {
		e.SchemaVersion = SessionInvalidatedSchemaV1
	}
	if e.CorrelationID == "" {
		e.CorrelationID = e.EventID
	}
	return e
}

// OutboxPayload returns camelCase AsyncAPI keys for the outbox JSONB payload.
func (e SessionInvalidatedEvent) OutboxPayload() map[string]any {
	e = e.Normalize()
	return map[string]any{
		"eventId":       e.EventID,
		"eventType":     e.EventType,
		"schemaVersion": e.SchemaVersion,
		"correlationId": e.CorrelationID,
		"playerId":      e.PlayerID.String(),
		"sessionId":     e.SessionID.String(),
		"reason":        e.Reason,
		"occurredAt":    e.OccurredAt.UTC().Format(time.RFC3339),
	}
}

// AsyncAPIPayload is an alias of OutboxPayload for call sites that prefer AsyncAPI naming.
func (e SessionInvalidatedEvent) AsyncAPIPayload() map[string]any {
	return e.OutboxPayload()
}

// NewSessionInvalidatedEvent builds a normalized SessionInvalidated event.
func NewSessionInvalidatedEvent(eventID string, playerID PlayerID, sessionID SessionID, reason string, occurredAt time.Time) SessionInvalidatedEvent {
	return SessionInvalidatedEvent{
		EventID:       eventID,
		EventType:     EventTypeSessionInvalidated,
		SchemaVersion: SessionInvalidatedSchemaV1,
		CorrelationID: eventID,
		PlayerID:      playerID,
		SessionID:     sessionID,
		Reason:        reason,
		OccurredAt:    occurredAt,
	}.Normalize()
}

// InvalidationTransport delivers a committed SessionInvalidated event after the
// durable outbox write. Failures leave the outbox pending for retry.
// Capability-mode only — durable production uses Debezium CDC (ADR-0016).
type InvalidationTransport interface {
	Deliver(evt SessionInvalidatedEvent) error
}

// MemoryInvalidationTransport records successful deliveries (offline / local).
// It never silently drops: callers only mark outbox published after Deliver returns nil.
type MemoryInvalidationTransport struct {
	mu  sync.Mutex
	evt []SessionInvalidatedEvent
}

func NewMemoryInvalidationTransport() *MemoryInvalidationTransport {
	return &MemoryInvalidationTransport{}
}

func (m *MemoryInvalidationTransport) Deliver(evt SessionInvalidatedEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.evt = append(m.evt, evt)
	return nil
}

func (m *MemoryInvalidationTransport) Delivered() []SessionInvalidatedEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]SessionInvalidatedEvent, len(m.evt))
	copy(out, m.evt)
	return out
}

// FakeInvalidationTransport records deliveries and can inject failures for tests.
type FakeInvalidationTransport struct {
	mu  sync.Mutex
	evt []SessionInvalidatedEvent
	err error
}

func NewFakeInvalidationTransport() *FakeInvalidationTransport {
	return &FakeInvalidationTransport{}
}

func (f *FakeInvalidationTransport) Deliver(evt SessionInvalidatedEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.evt = append(f.evt, evt)
	return nil
}

func (f *FakeInvalidationTransport) Delivered() []SessionInvalidatedEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]SessionInvalidatedEvent, len(f.evt))
	copy(out, f.evt)
	return out
}

func (f *FakeInvalidationTransport) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.evt = nil
}

func (f *FakeInvalidationTransport) SetError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}
