package main

import (
	"sync"

	"unoarena/services/analytics/domain"
)

// AnalyticsStore is the injectable projection boundary for the offline HTTP runtime.
type AnalyticsStore interface {
	Projection() *domain.PublicAnalyticsProjection
	Apply(evt domain.UpstreamEvent) domain.ApplyOutcome
	Rebuild(events []domain.UpstreamEvent) []domain.ApplyOutcome
	SnapshotJSON() ([]byte, error)
}

// MemoryAnalyticsStore wraps one PublicAnalyticsProjection with a mutex.
type MemoryAnalyticsStore struct {
	mu   sync.Mutex
	proj *domain.PublicAnalyticsProjection
}

// NewMemoryAnalyticsStore creates an empty in-memory analytics store.
func NewMemoryAnalyticsStore() *MemoryAnalyticsStore {
	return &MemoryAnalyticsStore{proj: domain.NewPublicAnalyticsProjection()}
}

// Projection returns the underlying projection. Callers must not use it concurrently
// with Apply/Rebuild/SnapshotJSON; prefer those methods for HTTP handlers.
func (s *MemoryAnalyticsStore) Projection() *domain.PublicAnalyticsProjection {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.proj
}

// Apply projects one upstream event under the store lock.
func (s *MemoryAnalyticsStore) Apply(evt domain.UpstreamEvent) domain.ApplyOutcome {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.proj.Apply(evt)
}

// Rebuild resets and reapplies events under the store lock.
func (s *MemoryAnalyticsStore) Rebuild(events []domain.UpstreamEvent) []domain.ApplyOutcome {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.proj.RebuildFrom(events)
}

// SnapshotJSON returns the public snapshot encoding under the store lock.
func (s *MemoryAnalyticsStore) SnapshotJSON() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.proj.SnapshotJSON()
}

// Snapshot returns a defensive copy of the public snapshot under the store lock.
func (s *MemoryAnalyticsStore) Snapshot() domain.AnalyticsSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.proj.Snapshot()
}

// ProjectionVersion returns the current projection version under the store lock.
func (s *MemoryAnalyticsStore) ProjectionVersion() domain.ProjectionVersion {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.proj.ProjectionVersion()
}
