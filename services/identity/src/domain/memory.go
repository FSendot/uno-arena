package domain

import (
	"context"
	"sync"
	"time"
)

// MemoryPlayerRepository is an in-memory PlayerRepository for offline tests.
type MemoryPlayerRepository struct {
	mu       sync.Mutex
	byID     map[PlayerID]Player
	byUser   map[string]Player
	roles    map[PlayerID][]string
	external map[string]PlayerID // issuer\x00subject -> playerID
}

func NewMemoryPlayerRepository() *MemoryPlayerRepository {
	return &MemoryPlayerRepository{
		byID:     make(map[PlayerID]Player),
		byUser:   make(map[string]Player),
		roles:    make(map[PlayerID][]string),
		external: make(map[string]PlayerID),
	}
}

func externalKey(issuer, subject string) string {
	return issuer + "\x00" + subject
}

func (r *MemoryPlayerRepository) Save(_ context.Context, player Player) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.byUser[player.Username]; ok && existing.ID != player.ID {
		return ErrUsernameTaken
	}
	r.byID[player.ID] = player
	r.byUser[player.Username] = player
	return nil
}

func (r *MemoryPlayerRepository) FindByUsername(_ context.Context, username string) (Player, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.byUser[username]
	return p, ok, nil
}

func (r *MemoryPlayerRepository) FindByID(_ context.Context, id PlayerID) (Player, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.byID[id]
	return p, ok, nil
}

func (r *MemoryPlayerRepository) RegisterWithDefaultACL(_ context.Context, player Player, role string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.byUser[player.Username]; ok && existing.ID != player.ID {
		return ErrUsernameTaken
	}
	if _, exists := r.byID[player.ID]; exists {
		return ErrUsernameTaken
	}
	r.byID[player.ID] = player
	r.byUser[player.Username] = player
	if role == "" {
		role = "player"
	}
	r.roles[player.ID] = []string{role}
	return nil
}

func (r *MemoryPlayerRepository) ListRoles(_ context.Context, playerID PlayerID) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	roles := r.roles[playerID]
	out := make([]string, len(roles))
	copy(out, roles)
	return out, nil
}

func (r *MemoryPlayerRepository) LinkOrResolveExternal(_ context.Context, issuer, subject, preferredUsername string, newPlayer Player, acceptedRoles []string) (Player, []string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := externalKey(issuer, subject)
	roles := NormalizeAcceptedRoles(acceptedRoles, acceptedRoles)
	if len(roles) == 0 {
		roles = []string{"player"}
	}
	if pid, ok := r.external[key]; ok {
		p := r.byID[pid]
		r.roles[pid] = append([]string(nil), roles...)
		return p, append([]string(nil), roles...), nil
	}

	username := preferredUsername
	if username == "" {
		username = newPlayer.Username
	}
	if username == "" {
		username = string(newPlayer.ID)
	}
	if existing, ok := r.byUser[username]; ok && existing.ID != newPlayer.ID {
		// Prefer stable uniqueness when preferred username is taken.
		username = username + "-" + string(newPlayer.ID)
	}
	player := newPlayer
	player.Username = username
	r.byID[player.ID] = player
	r.byUser[player.Username] = player
	r.roles[player.ID] = append([]string(nil), roles...)
	r.external[key] = player.ID
	return player, append([]string(nil), roles...), nil
}

// ByUsername is a test helper.
func (r *MemoryPlayerRepository) ByUsername(username string) (Player, bool) {
	p, ok, _ := r.FindByUsername(context.Background(), username)
	return p, ok
}

// MemorySessionRepository is an in-memory SessionRepository with a durable outbox.
type MemorySessionRepository struct {
	mu            sync.Mutex
	byID          map[SessionID]Session
	byToken       map[string]Session
	outbox        []OutboxEntry
	replaceErr    error
	invalidateErr error
}

func NewMemorySessionRepository() *MemorySessionRepository {
	return &MemorySessionRepository{
		byID:    make(map[SessionID]Session),
		byToken: make(map[string]Session),
	}
}

// SetReplaceError injects a failure into ReplaceActiveSession before any mutation (tests).
func (r *MemorySessionRepository) SetReplaceError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.replaceErr = err
}

// SetInvalidateError injects a failure into InvalidateWithOutbox before any mutation (tests).
func (r *MemorySessionRepository) SetInvalidateError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.invalidateErr = err
}

func (r *MemorySessionRepository) Save(_ context.Context, session Session) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.saveLocked(session)
	return nil
}

func (r *MemorySessionRepository) saveLocked(session Session) {
	if prev, ok := r.byID[session.ID]; ok && prev.TokenHash != session.TokenHash {
		delete(r.byToken, prev.TokenHash)
	}
	r.byID[session.ID] = session
	r.byToken[session.TokenHash] = session
}

func (r *MemorySessionRepository) FindByID(_ context.Context, id SessionID) (Session, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.byID[id]
	return s, ok, nil
}

func (r *MemorySessionRepository) FindByTokenHash(_ context.Context, tokenHash string) (Session, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.byToken[tokenHash]
	return s, ok, nil
}

func (r *MemorySessionRepository) FindActiveByPlayer(_ context.Context, playerID PlayerID) (Session, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.findActiveLocked(playerID)
	return s, ok, nil
}

func (r *MemorySessionRepository) findActiveLocked(playerID PlayerID) (Session, bool) {
	for _, s := range r.byID {
		if s.PlayerID == playerID && s.Status == SessionStatusActive {
			return s, true
		}
	}
	return Session{}, false
}

// ReplaceActiveSession applies invalidations + next + outbox under one lock.
func (r *MemorySessionRepository) ReplaceActiveSession(_ context.Context, previous *Session, next Session, eventFor func(Session) SessionInvalidatedEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.replaceErr != nil {
		return r.replaceErr
	}

	var newOutbox []OutboxEntry
	for id, s := range r.byID {
		if s.PlayerID != next.PlayerID || s.Status != SessionStatusActive {
			continue
		}
		var inv Session
		if previous != nil && s.ID == previous.ID {
			inv = *previous
		} else {
			inv = s
			ts := next.CreatedAt
			inv.Status = SessionStatusInvalidated
			inv.InvalidatedAt = &ts
			inv.InvalidationReason = ReasonLoginTakeover
		}
		r.byID[id] = inv
		r.byToken[inv.TokenHash] = inv
		if eventFor != nil {
			evt := eventFor(inv).Normalize()
			newOutbox = append(newOutbox, OutboxEntry{
				Event:     evt,
				CreatedAt: next.CreatedAt,
			})
		}
	}
	r.saveLocked(next)
	r.outbox = append(r.outbox, newOutbox...)
	return nil
}

// InvalidateWithOutbox persists session status change and outbox event atomically
// only when the stored session is still active (conditional CAS-style write).
func (r *MemorySessionRepository) InvalidateWithOutbox(_ context.Context, session Session, event SessionInvalidatedEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.invalidateErr != nil {
		return r.invalidateErr
	}
	current, ok := r.byID[session.ID]
	if !ok {
		return ErrSessionNotFound
	}
	if current.Status != SessionStatusActive {
		// Idempotent: concurrent revoke/expiry already committed the logical event.
		return nil
	}
	r.saveLocked(session)
	r.outbox = append(r.outbox, OutboxEntry{
		Event:     event.Normalize(),
		CreatedAt: event.OccurredAt,
	})
	return nil
}

// OutboxLen returns total outbox entries including published (test helper).
func (r *MemorySessionRepository) OutboxLen() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.outbox)
}

func (r *MemorySessionRepository) ListPendingOutbox(_ context.Context, limit int) ([]OutboxEntry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if limit <= 0 {
		limit = len(r.outbox)
	}
	var out []OutboxEntry
	for _, e := range r.outbox {
		if e.PublishedAt != nil {
			continue
		}
		out = append(out, e)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (r *MemorySessionRepository) MarkOutboxPublished(_ context.Context, eventID string, publishedAt time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.outbox {
		if r.outbox[i].Event.EventID == eventID && r.outbox[i].PublishedAt == nil {
			ts := publishedAt
			r.outbox[i].PublishedAt = &ts
			return nil
		}
	}
	return nil
}

// ByID is a test helper.
func (r *MemorySessionRepository) ByID(id SessionID) (Session, bool) {
	s, ok, _ := r.FindByID(context.Background(), id)
	return s, ok
}

// All returns a snapshot of all sessions (test helper).
func (r *MemorySessionRepository) All() []Session {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Session, 0, len(r.byID))
	for _, s := range r.byID {
		out = append(out, s)
	}
	return out
}
