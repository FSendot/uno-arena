package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/shared/audit"
	"unoarena/shared/envelope"
)

// MemoryTournamentRepository is an in-memory TournamentRepository with atomic Commit.
type MemoryTournamentRepository struct {
	mu       sync.Mutex
	byID     map[domain.TournamentID]*domain.Tournament
	outcomes map[string]envelope.Result
	outbox   []OutboxEvent
	projVer  map[domain.TournamentID]int64
	projAt   map[domain.TournamentID]time.Time

	// Fault injection (tests).
	CommitErr       error
	FailNextCommits int
}

func NewMemoryTournamentRepository() *MemoryTournamentRepository {
	return &MemoryTournamentRepository{
		byID:     make(map[domain.TournamentID]*domain.Tournament),
		outcomes: make(map[string]envelope.Result),
		outbox:   make([]OutboxEvent, 0),
		projVer:  make(map[domain.TournamentID]int64),
		projAt:   make(map[domain.TournamentID]time.Time),
	}
}

func (r *MemoryTournamentRepository) BeginExisting(id domain.TournamentID) (TournamentUnitOfWork, error) {
	if !id.Valid() {
		return nil, fmt.Errorf("tournamentId required")
	}
	r.mu.Lock()
	uow := &memoryUoW{repo: r, tid: id}
	if t, ok := r.byID[id]; ok {
		uow.exists = true
		uow.loaded = t.Clone()
	}
	return uow, nil
}

func (r *MemoryTournamentRepository) BeginCreate(id domain.TournamentID) (TournamentUnitOfWork, error) {
	if !id.Valid() {
		return nil, fmt.Errorf("tournamentId required")
	}
	r.mu.Lock()
	uow := &memoryUoW{repo: r, tid: id, createPath: true}
	if t, ok := r.byID[id]; ok {
		uow.exists = true
		uow.loaded = t.Clone()
	}
	return uow, nil
}

type memoryUoW struct {
	repo       *MemoryTournamentRepository
	tid        domain.TournamentID
	loaded     *domain.Tournament
	exists     bool
	createPath bool
	done       bool
}

func (u *memoryUoW) Loaded() *domain.Tournament { return u.loaded }
func (u *memoryUoW) Exists() bool               { return u.exists }

func (u *memoryUoW) LookupOutcome(commandID string) (envelope.Result, bool) {
	if u == nil || u.done || commandID == "" {
		return envelope.Result{}, false
	}
	out, ok := u.repo.outcomes[commandID]
	return out, ok
}

func (u *memoryUoW) Commit(req CommitRequest) error {
	if u == nil || u.done {
		return fmt.Errorf("unit of work already finished")
	}
	defer u.unlock()
	if u.repo.CommitErr != nil {
		return u.repo.CommitErr
	}
	if u.repo.FailNextCommits > 0 {
		u.repo.FailNextCommits--
		return fmt.Errorf("injected commit failure")
	}
	return u.repo.applyCommitLocked(req)
}

func (u *memoryUoW) Rollback() error {
	if u == nil || u.done {
		return nil
	}
	u.unlock()
	return nil
}

func (u *memoryUoW) unlock() {
	if u.done {
		return
	}
	u.done = true
	u.repo.mu.Unlock()
}

func (r *MemoryTournamentRepository) ProjectionCheckpoint(id domain.TournamentID) (int64, time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.projVer[id], r.projAt[id]
}

func (r *MemoryTournamentRepository) Get(id domain.TournamentID) (*domain.Tournament, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.byID[id]
	if !ok {
		return nil, false
	}
	// Copy-on-write: callers mutate a deep clone; live state changes only on Commit.
	return t.Clone(), true
}

func (r *MemoryTournamentRepository) Commit(req CommitRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.CommitErr != nil {
		return r.CommitErr
	}
	if r.FailNextCommits > 0 {
		r.FailNextCommits--
		return fmt.Errorf("injected commit failure")
	}
	return r.applyCommitLocked(req)
}

func (r *MemoryTournamentRepository) applyCommitLocked(req CommitRequest) error {
	if req.CommandID == "" {
		return fmt.Errorf("commandId required for commit")
	}
	if prior, ok := r.outcomes[req.CommandID]; ok {
		// Idempotent commit of the same outcome is a no-op success.
		_ = prior
		return nil
	}
	if req.Tournament != nil {
		// Store a deep clone so post-commit caller mutations cannot alias live state.
		r.byID[req.Tournament.ID()] = req.Tournament.Clone()
		// Only accepted bracket-visible changes bump the public projection checkpoint.
		if req.ProjectionChanged {
			id := req.Tournament.ID()
			r.projVer[id]++
			r.projAt[id] = time.Now().UTC()
		}
	}
	r.outcomes[req.CommandID] = req.Outcome
	if len(req.Events) > 0 {
		// Deduplicate by EventID so retries cannot lose or double-append facts.
		existing := make(map[string]struct{}, len(r.outbox))
		for _, e := range r.outbox {
			existing[e.EventID] = struct{}{}
		}
		for _, e := range req.Events {
			if e.EventID == "" {
				continue
			}
			if _, ok := existing[e.EventID]; ok {
				continue
			}
			r.outbox = append(r.outbox, e)
			existing[e.EventID] = struct{}{}
		}
	}
	return nil
}

func (r *MemoryTournamentRepository) LookupOutcome(commandID string) (envelope.Result, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out, ok := r.outcomes[commandID]
	return out, ok
}

func (r *MemoryTournamentRepository) ListPendingOutbox(limit int) ([]OutboxEvent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []OutboxEvent
	for _, e := range r.outbox {
		if e.PublishedAt != nil {
			continue
		}
		out = append(out, e)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (r *MemoryTournamentRepository) MarkOutboxPublished(eventID string, at time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.outbox {
		if r.outbox[i].EventID == eventID && r.outbox[i].PublishedAt == nil {
			ts := at.UTC()
			r.outbox[i].PublishedAt = &ts
			return nil
		}
	}
	return nil
}

func (r *MemoryTournamentRepository) OutboxLen() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.outbox)
}

func (r *MemoryTournamentRepository) PendingOutboxLen() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, e := range r.outbox {
		if e.PublishedAt == nil {
			n++
		}
	}
	return n
}

func (r *MemoryTournamentRepository) Events() []OutboxEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]OutboxEvent, len(r.outbox))
	copy(out, r.outbox)
	return out
}

// MemoryAudit is an in-process AuditSink.
type MemoryAudit struct {
	mu      sync.Mutex
	records []audit.RejectionRecord
}

func NewMemoryAudit() *MemoryAudit {
	return &MemoryAudit{records: make([]audit.RejectionRecord, 0)}
}

func (a *MemoryAudit) RecordRejection(record audit.RejectionRecord) error {
	if err := record.Validate(); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.records = append(a.records, record)
	return nil
}

func (a *MemoryAudit) Records() []audit.RejectionRecord {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]audit.RejectionRecord, len(a.records))
	copy(out, a.records)
	return out
}

func (a *MemoryAudit) Len() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.records)
}

// FakeRoomProvisioner records provision calls (offline Room Gameplay port).
type FakeRoomProvisioner struct {
	mu sync.Mutex

	Calls []RoomProvisionRequest
	Err   error

	// FailOnCall fails the Nth call (1-based); 0 disables.
	FailOnCall int
	callN      int

	// ReturnMismatchedRoomID forces ErrRoomIDMismatch (quarantine path).
	ReturnMismatchedRoomID bool

	// HoldDuringCall blocks inside Provision while the callback runs (concurrency tests).
	HoldDuringCall func()
	// OnCall is invoked under the provisioner lock after recording (optional).
	OnCall func(req RoomProvisionRequest)
}

func NewFakeRoomProvisioner() *FakeRoomProvisioner {
	return &FakeRoomProvisioner{Calls: make([]RoomProvisionRequest, 0)}
}

func (f *FakeRoomProvisioner) Provision(_ context.Context, req RoomProvisionRequest) (RoomProvisionResult, error) {
	f.mu.Lock()
	f.callN++
	n := f.callN
	f.Calls = append(f.Calls, req)
	hold := f.HoldDuringCall
	onCall := f.OnCall
	failOn := f.FailOnCall
	staticErr := f.Err
	mismatch := f.ReturnMismatchedRoomID
	f.mu.Unlock()

	if onCall != nil {
		onCall(req)
	}
	if hold != nil {
		hold()
	}
	if staticErr != nil {
		return RoomProvisionResult{}, staticErr
	}
	if failOn > 0 && n == failOn {
		return RoomProvisionResult{}, fmt.Errorf("injected room provision failure")
	}
	roomID := string(req.RoomID)
	if mismatch {
		roomID = roomID + "-mismatch"
		return RoomProvisionResult{}, fmt.Errorf("%w: requested %s", ErrRoomIDMismatch, req.RoomID)
	}
	return RoomProvisionResult{RoomID: roomID}, nil
}

func (f *FakeRoomProvisioner) CallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.Calls)
}

func (f *FakeRoomProvisioner) ResetCalls() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = f.Calls[:0]
	f.callN = 0
}

// NoopRoomProvisioner ignores provision calls.
type NoopRoomProvisioner struct{}

func (NoopRoomProvisioner) Provision(_ context.Context, req RoomProvisionRequest) (RoomProvisionResult, error) {
	return RoomProvisionResult{RoomID: string(req.RoomID)}, nil
}

// FakePublisher records publish attempts and supports fault injection.
type FakePublisher struct {
	mu sync.Mutex

	Calls     int
	FailNext  int
	Err       error
	Published []OutboxEvent
}

func NewFakePublisher() *FakePublisher {
	return &FakePublisher{Published: make([]OutboxEvent, 0)}
}

func (f *FakePublisher) Publish(_ context.Context, events ...OutboxEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls++
	if f.Err != nil {
		return f.Err
	}
	if f.FailNext > 0 {
		f.FailNext--
		return fmt.Errorf("injected publish failure")
	}
	f.Published = append(f.Published, events...)
	return nil
}

// NoopPublisher drops publishes.
type NoopPublisher struct{}

func (NoopPublisher) Publish(context.Context, ...OutboxEvent) error { return nil }
