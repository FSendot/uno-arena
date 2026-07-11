package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync"

	"unoarena/services/spectator-view/domain"
)

// MemoryProjectionStore is the capability-mode in-memory SpectatorApplication.
// Explicit non-production SPECTATOR_CAPABILITY_MODE only — not production durable state.
type MemoryProjectionStore struct {
	mu    sync.Mutex
	rooms map[domain.RoomID]*roomActor
}

type roomActor struct {
	mu         sync.Mutex
	proj       *domain.SpectatorRoomProjection
	eventCount int
	invites    map[string]struct{} // SHA-256 digests only
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

// Ready always succeeds for in-memory capability mode.
func (s *MemoryProjectionStore) Ready(ctx context.Context) error {
	_ = ctx
	return nil
}

// Apply runs domain policy under the per-room lock.
func (s *MemoryProjectionStore) Apply(ctx context.Context, roomID domain.RoomID, events []domain.SpectatorSafeEvent) (domain.ApplyOutcome, error) {
	_ = ctx
	if !roomID.Valid() {
		return domain.ApplyOutcome{}, errInvalidRoom
	}
	a := s.actor(roomID, true)
	a.mu.Lock()
	defer a.mu.Unlock()
	var out domain.ApplyOutcome
	if len(events) == 1 {
		out = a.proj.Apply(events[0])
	} else {
		out = a.proj.ApplyBatch(events)
	}
	if out.Kind == domain.OutcomeAccepted {
		a.eventCount++
	}
	return out, nil
}

// Rebuild validates/replays then atomically replaces the room projection.
func (s *MemoryProjectionStore) Rebuild(ctx context.Context, roomID domain.RoomID, events []domain.SpectatorSafeEvent) (RebuildResult, error) {
	_ = ctx
	if !roomID.Valid() {
		return RebuildResult{}, errInvalidRoom
	}
	proj, outcomes := domain.RebuildFrom(roomID, events)
	a := s.actor(roomID, true)
	a.mu.Lock()
	defer a.mu.Unlock()
	a.proj = proj
	a.eventCount = len(events)
	return RebuildResult{
		Outcomes:     outcomes,
		Sequence:     uint64(proj.Sequence()),
		Status:       string(proj.Status()),
		StreamClosed: proj.StreamClosed(),
		EventCount:   a.eventCount,
	}, nil
}

// Admission evaluates spectator admission against durable/memory projection state.
func (s *MemoryProjectionStore) Admission(ctx context.Context, roomID domain.RoomID, auth domain.SpectatorAuth) (domain.SpectatorAdmissionDecision, error) {
	_ = ctx
	a := s.actor(roomID, false)
	if a == nil {
		proj := domain.NewSpectatorRoomProjection(roomID)
		return proj.Admission(auth), nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.proj.Admission(auth), nil
}

// SnapshotJSON returns the sanitized public snapshot JSON.
func (s *MemoryProjectionStore) SnapshotJSON(ctx context.Context, roomID domain.RoomID) ([]byte, error) {
	_ = ctx
	a := s.actor(roomID, true)
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.proj.SnapshotJSON()
}

// RegisterInvite stores the SHA-256 digest of the invite token (never plaintext).
func (s *MemoryProjectionStore) RegisterInvite(ctx context.Context, roomID domain.RoomID, token string) (bool, error) {
	_ = ctx
	token = trimInvite(token)
	if !roomID.Valid() || token == "" {
		return false, nil
	}
	a := s.actor(roomID, true)
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.invites == nil {
		a.invites = make(map[string]struct{})
	}
	a.invites[inviteDigest(token)] = struct{}{}
	return true, nil
}

// HasInvite reports whether token matches a registered invite digest.
func (s *MemoryProjectionStore) HasInvite(ctx context.Context, roomID domain.RoomID, token string) (bool, error) {
	_ = ctx
	token = trimInvite(token)
	if token == "" {
		return false, nil
	}
	a := s.actor(roomID, false)
	if a == nil {
		return false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	_, ok := a.invites[inviteDigest(token)]
	return ok, nil
}

// RoomMeta returns status fields and event count under one lock.
func (s *MemoryProjectionStore) RoomMeta(ctx context.Context, roomID domain.RoomID) (RoomMeta, error) {
	_ = ctx
	a := s.actor(roomID, false)
	if a == nil {
		proj := domain.NewSpectatorRoomProjection(roomID)
		return RoomMeta{
			Sequence:     uint64(proj.Sequence()),
			Status:       string(proj.Status()),
			StreamClosed: proj.StreamClosed(),
			EventCount:   0,
			Exists:       false,
		}, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return RoomMeta{
		Sequence:     uint64(a.proj.Sequence()),
		Status:       string(a.proj.Status()),
		StreamClosed: a.proj.StreamClosed(),
		EventCount:   a.eventCount,
		Exists:       true,
	}, nil
}

// ProjectionForTest returns a copy export for tests (capability only).
func (s *MemoryProjectionStore) ProjectionForTest(roomID domain.RoomID) domain.ProjectionExport {
	a := s.actor(roomID, false)
	if a == nil {
		return domain.NewSpectatorRoomProjection(roomID).ExportState()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.proj.ExportState()
}

func inviteDigest(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
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

var errInvalidRoom = errString("invalid roomId")

type errString string

func (e errString) Error() string { return string(e) }
