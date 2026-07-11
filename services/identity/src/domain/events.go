package domain

import (
	"sync"
	"time"
)

// SessionInvalidatedEvent is the outbox-facing payload for identity.session.invalidated.
type SessionInvalidatedEvent struct {
	EventID    string
	PlayerID   PlayerID
	SessionID  SessionID
	Reason     string
	OccurredAt time.Time
}

// InvalidationTransport delivers a committed SessionInvalidated event after the
// durable outbox write. Failures leave the outbox pending for retry.
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
