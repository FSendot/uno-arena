package main

import (
	"sync"

	"unoarena/services/spectator-view/domain"
)

// ProjectionStore is the injectable room projection backing store.
// Implementations must not hand out mutable projection pointers for concurrent use;
// all Apply/admission/snapshot/rebuild work runs under a per-room lock.
type ProjectionStore interface {
	WithRoom(roomID domain.RoomID, fn func(*domain.SpectatorRoomProjection))
	WithRoomCreate(roomID domain.RoomID, fn func(*domain.SpectatorRoomProjection))
	EventCount(roomID domain.RoomID) int
	SetEventCount(roomID domain.RoomID, n int)
	IncrEventCount(roomID domain.RoomID)
	// RoomMeta reads projection status fields and event count under one lock
	// (avoids nested-lock deadlock from EventCount inside WithRoom).
	RoomMeta(roomID domain.RoomID) (sequence uint64, status string, streamClosed bool, eventCount int, ok bool)
	RegisterInvite(roomID domain.RoomID, token string) bool
	HasInvite(roomID domain.RoomID, token string) bool
}

type roomActor struct {
	mu         sync.Mutex
	proj       *domain.SpectatorRoomProjection
	eventCount int
	invites    map[string]struct{}
}

// MemoryProjectionStore is a per-room locked in-memory ProjectionStore.
type MemoryProjectionStore struct {
	mu    sync.Mutex
	rooms map[domain.RoomID]*roomActor
}

// NewMemoryProjectionStore creates an empty in-memory store.
func NewMemoryProjectionStore() *MemoryProjectionStore {
	return &MemoryProjectionStore{
		rooms: make(map[domain.RoomID]*roomActor),
	}
}

func (s *MemoryProjectionStore) actor(roomID domain.RoomID, create bool) *roomActor {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.rooms[roomID]
	if ok {
		return a
	}
	if !create {
		return nil
	}
	a = &roomActor{
		proj:    domain.NewSpectatorRoomProjection(roomID),
		invites: make(map[string]struct{}),
	}
	s.rooms[roomID] = a
	return a
}

// WithRoom runs fn under the per-room lock when the room exists.
// If the room is absent, fn receives a fresh ephemeral projection (not stored).
func (s *MemoryProjectionStore) WithRoom(roomID domain.RoomID, fn func(*domain.SpectatorRoomProjection)) {
	a := s.actor(roomID, false)
	if a == nil {
		fn(domain.NewSpectatorRoomProjection(roomID))
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	fn(a.proj)
}

// WithRoomCreate ensures the room actor exists, then runs fn under its lock.
func (s *MemoryProjectionStore) WithRoomCreate(roomID domain.RoomID, fn func(*domain.SpectatorRoomProjection)) {
	a := s.actor(roomID, true)
	a.mu.Lock()
	defer a.mu.Unlock()
	fn(a.proj)
}

// EventCount returns the tracked applied-event count for roomID.
func (s *MemoryProjectionStore) EventCount(roomID domain.RoomID) int {
	a := s.actor(roomID, false)
	if a == nil {
		return 0
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.eventCount
}

// SetEventCount sets the tracked event count after a rebuild.
func (s *MemoryProjectionStore) SetEventCount(roomID domain.RoomID, n int) {
	a := s.actor(roomID, true)
	a.mu.Lock()
	defer a.mu.Unlock()
	a.eventCount = n
}

// IncrEventCount increments the tracked event count after a non-duplicate apply.
func (s *MemoryProjectionStore) IncrEventCount(roomID domain.RoomID) {
	a := s.actor(roomID, true)
	a.mu.Lock()
	defer a.mu.Unlock()
	a.eventCount++
}

// RoomMeta returns status fields and event count under a single per-room lock.
func (s *MemoryProjectionStore) RoomMeta(roomID domain.RoomID) (sequence uint64, status string, streamClosed bool, eventCount int, ok bool) {
	a := s.actor(roomID, false)
	if a == nil {
		proj := domain.NewSpectatorRoomProjection(roomID)
		return uint64(proj.Sequence()), string(proj.Status()), proj.StreamClosed(), 0, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return uint64(a.proj.Sequence()), string(a.proj.Status()), a.proj.StreamClosed(), a.eventCount, true
}

// RegisterInvite registers an opaque room invite capability token.
func (s *MemoryProjectionStore) RegisterInvite(roomID domain.RoomID, token string) bool {
	token = trimInvite(token)
	if !roomID.Valid() || token == "" {
		return false
	}
	a := s.actor(roomID, true)
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.invites == nil {
		a.invites = make(map[string]struct{})
	}
	a.invites[token] = struct{}{}
	return true
}

// HasInvite reports whether token is a registered invite for roomID.
func (s *MemoryProjectionStore) HasInvite(roomID domain.RoomID, token string) bool {
	token = trimInvite(token)
	if token == "" {
		return false
	}
	a := s.actor(roomID, false)
	if a == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	_, ok := a.invites[token]
	return ok
}

// ReplaceProjection atomically replaces the room projection (used by rebuild).
func (s *MemoryProjectionStore) ReplaceProjection(roomID domain.RoomID, p *domain.SpectatorRoomProjection, eventCount int) {
	a := s.actor(roomID, true)
	a.mu.Lock()
	defer a.mu.Unlock()
	a.proj = p
	a.eventCount = eventCount
}

func trimInvite(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	return s
}
