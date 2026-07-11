package main

import (
	"context"
	"sync"

	"unoarena/services/analytics/domain"
)

// MemoryAnalyticsStore wraps one PublicAnalyticsProjection with a mutex.
// Capability / offline mode only — no durable ClickHouse coupling.
type MemoryAnalyticsStore struct {
	mu   sync.Mutex
	proj *domain.PublicAnalyticsProjection
}

// NewMemoryAnalyticsStore creates an empty in-memory analytics store.
func NewMemoryAnalyticsStore() *MemoryAnalyticsStore {
	return &MemoryAnalyticsStore{proj: domain.NewPublicAnalyticsProjection()}
}

// Apply projects one upstream event under the store lock.
func (s *MemoryAnalyticsStore) Apply(_ context.Context, evt domain.UpstreamEvent) (domain.ApplyOutcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.proj.Apply(evt), nil
}

// Rebuild resets and reapplies events under the store lock.
func (s *MemoryAnalyticsStore) Rebuild(_ context.Context, events []domain.UpstreamEvent) ([]domain.ApplyOutcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.proj.RebuildFrom(events), nil
}

// Snapshot returns a defensive copy of the public snapshot under the store lock.
func (s *MemoryAnalyticsStore) Snapshot(_ context.Context) (domain.AnalyticsSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.proj.Snapshot(), nil
}

// SnapshotJSON returns the public snapshot encoding under the store lock.
func (s *MemoryAnalyticsStore) SnapshotJSON(_ context.Context) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.proj.SnapshotJSON()
}

// ProjectionVersion returns the current projection version under the store lock.
func (s *MemoryAnalyticsStore) ProjectionVersion(_ context.Context) (domain.ProjectionVersion, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.proj.ProjectionVersion(), nil
}

// Ready always succeeds for the in-memory capability store.
func (s *MemoryAnalyticsStore) Ready(_ context.Context) error {
	return nil
}
