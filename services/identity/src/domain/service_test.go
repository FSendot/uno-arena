package domain_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"unoarena/services/identity/domain"
)

func newTestService(t *testing.T) (*domain.Service, *domain.MemoryPlayerRepository, *domain.MemorySessionRepository, *fixedClock, *seqIDs, *domain.FakeInvalidationTransport) {
	t.Helper()
	players := domain.NewMemoryPlayerRepository()
	sessions := domain.NewMemorySessionRepository()
	clock := &fixedClock{now: time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)}
	ids := &seqIDs{}
	transport := domain.NewFakeInvalidationTransport()
	svc := domain.NewService(domain.ServiceDeps{
		Players:    players,
		Sessions:   sessions,
		Hasher:     domain.NewCheckpointPasswordHasher(ids),
		IDs:        ids,
		Tokens:     ids,
		Clock:      clock,
		SessionTTL: time.Hour,
		Transport:  transport,
	})
	return svc, players, sessions, clock, ids, transport
}

type fixedClock struct{ now time.Time }

func (c *fixedClock) Now() time.Time { return c.now }

type seqIDs struct {
	n atomic.Int64
}

func (s *seqIDs) NewID(prefix string) string {
	n := s.n.Add(1)
	return prefix + "-" + itoa(int(n))
}

func (s *seqIDs) NewToken() (string, error) {
	n := s.n.Add(1)
	return "tok-" + itoa(int(n)), nil
}

func (s *seqIDs) NewSalt() ([]byte, error) {
	n := s.n.Add(1)
	salt := make([]byte, 16)
	for i := range salt {
		salt[i] = byte(int(n) + i)
	}
	return salt, nil
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func TestRegisterCreatesPlayer(t *testing.T) {
	svc, players, _, _, _, _ := newTestService(t)

	p, err := svc.Register("alice", "secret")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if p.ID == "" || p.Username != "alice" {
		t.Fatalf("unexpected player: %+v", p)
	}
	stored, ok := players.ByUsername("alice")
	if !ok {
		t.Fatal("player not stored")
	}
	if stored.PasswordHash == "" || stored.PasswordHash == "secret" {
		t.Fatalf("password must be hashed, got %q", stored.PasswordHash)
	}
}

func TestRegisterRejectsDuplicateUsername(t *testing.T) {
	svc, _, _, _, _, _ := newTestService(t)
	if _, err := svc.Register("alice", "secret"); err != nil {
		t.Fatalf("first register: %v", err)
	}
	_, err := svc.Register("alice", "other")
	if !errors.Is(err, domain.ErrUsernameTaken) {
		t.Fatalf("expected ErrUsernameTaken, got %v", err)
	}
}

func TestLoginCreatesActiveSession(t *testing.T) {
	svc, _, _, _, _, _ := newTestService(t)
	p, err := svc.Register("alice", "secret")
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	res, err := svc.Login("alice", "secret")
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	if res.PlayerID != p.ID || res.SessionID == "" || res.Token == "" {
		t.Fatalf("unexpected login result: %+v", res)
	}

	principal, err := svc.ValidateToken(res.Token)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if principal.SessionID != res.SessionID || principal.PlayerID != p.ID {
		t.Fatalf("unexpected principal: %+v", principal)
	}
}

func TestLoginInvalidatesPriorActiveSession(t *testing.T) {
	svc, _, sessions, _, _, _ := newTestService(t)
	if _, err := svc.Register("alice", "secret"); err != nil {
		t.Fatalf("register: %v", err)
	}

	first, err := svc.Login("alice", "secret")
	if err != nil {
		t.Fatalf("first login: %v", err)
	}
	second, err := svc.Login("alice", "secret")
	if err != nil {
		t.Fatalf("second login: %v", err)
	}
	if first.SessionID == second.SessionID {
		t.Fatal("expected distinct session ids")
	}
	if first.Token == second.Token {
		t.Fatal("expected distinct tokens")
	}

	if _, err := svc.ValidateToken(first.Token); !errors.Is(err, domain.ErrSessionInvalid) {
		t.Fatalf("old token should be invalid, got %v", err)
	}
	if _, err := svc.ValidateToken(second.Token); err != nil {
		t.Fatalf("new token should be valid: %v", err)
	}

	old, ok := sessions.ByID(first.SessionID)
	if !ok {
		t.Fatal("old session missing")
	}
	if old.Status != domain.SessionStatusInvalidated {
		t.Fatalf("expected invalidated, got %s", old.Status)
	}
	if old.InvalidationReason != domain.ReasonLoginTakeover {
		t.Fatalf("expected login_takeover, got %s", old.InvalidationReason)
	}
}

func TestLoginRejectsBadPassword(t *testing.T) {
	svc, _, _, _, _, _ := newTestService(t)
	if _, err := svc.Register("alice", "secret"); err != nil {
		t.Fatalf("register: %v", err)
	}
	_, err := svc.Login("alice", "wrong")
	if !errors.Is(err, domain.ErrInvalidCredentials) {
		t.Fatalf("expected ErrInvalidCredentials, got %v", err)
	}
}

func TestRevokeRejectsValidation(t *testing.T) {
	svc, _, _, _, _, _ := newTestService(t)
	if _, err := svc.Register("alice", "secret"); err != nil {
		t.Fatalf("register: %v", err)
	}
	res, err := svc.Login("alice", "secret")
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	if err := svc.RevokeSession(res.SessionID, domain.ReasonLogout); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if _, err := svc.ValidateToken(res.Token); !errors.Is(err, domain.ErrSessionInvalid) {
		t.Fatalf("expected ErrSessionInvalid, got %v", err)
	}
}

func TestValidateRejectsExpiredSession(t *testing.T) {
	svc, _, _, clock, _, _ := newTestService(t)
	if _, err := svc.Register("alice", "secret"); err != nil {
		t.Fatalf("register: %v", err)
	}
	res, err := svc.Login("alice", "secret")
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	clock.now = clock.now.Add(2 * time.Hour)
	if _, err := svc.ValidateToken(res.Token); !errors.Is(err, domain.ErrSessionExpired) {
		t.Fatalf("expected ErrSessionExpired, got %v", err)
	}
}

func TestWhoamiReturnsPrincipal(t *testing.T) {
	svc, _, _, _, _, _ := newTestService(t)
	p, err := svc.Register("alice", "secret")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	res, err := svc.Login("alice", "secret")
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	who, err := svc.Whoami(res.Token)
	if err != nil {
		t.Fatalf("whoami: %v", err)
	}
	if who.PlayerID != p.ID || who.SessionID != res.SessionID || who.Username != "alice" {
		t.Fatalf("unexpected whoami: %+v", who)
	}
}

func TestLoginConcurrentSingleActiveSession(t *testing.T) {
	svc, _, sessions, _, _, _ := newTestService(t)
	if _, err := svc.Register("alice", "secret"); err != nil {
		t.Fatalf("register: %v", err)
	}

	const n = 32
	var wg sync.WaitGroup
	wg.Add(n)
	var successes atomic.Int64
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			if _, err := svc.Login("alice", "secret"); err == nil {
				successes.Add(1)
			}
		}()
	}
	wg.Wait()
	if successes.Load() == 0 {
		t.Fatal("expected at least one successful login")
	}

	active := 0
	for _, s := range sessions.All() {
		if s.Status == domain.SessionStatusActive {
			active++
		}
	}
	if active != 1 {
		t.Fatalf("expected exactly 1 active session, got %d", active)
	}
}

func TestReplaceActiveSessionFailureLeavesPriorValid(t *testing.T) {
	svc, _, sessions, _, _, _ := newTestService(t)
	if _, err := svc.Register("alice", "secret"); err != nil {
		t.Fatalf("register: %v", err)
	}
	first, err := svc.Login("alice", "secret")
	if err != nil {
		t.Fatalf("first login: %v", err)
	}

	boom := errors.New("replace failed")
	sessions.SetReplaceError(boom)
	_, err = svc.Login("alice", "secret")
	if !errors.Is(err, boom) {
		t.Fatalf("expected replace error, got %v", err)
	}
	sessions.SetReplaceError(nil)

	if _, err := svc.ValidateToken(first.Token); err != nil {
		t.Fatalf("prior session must remain valid after failed replace: %v", err)
	}
	active := 0
	for _, s := range sessions.All() {
		if s.Status == domain.SessionStatusActive {
			active++
		}
	}
	if active != 1 {
		t.Fatalf("expected 1 active session, got %d", active)
	}
}

func TestLoginTakeoverPublishesSessionInvalidated(t *testing.T) {
	svc, _, sessions, _, _, transport := newTestService(t)
	if _, err := svc.Register("alice", "secret"); err != nil {
		t.Fatalf("register: %v", err)
	}
	first, err := svc.Login("alice", "secret")
	if err != nil {
		t.Fatalf("first login: %v", err)
	}
	if got := transport.Delivered(); len(got) != 0 {
		t.Fatalf("no invalidate on first login, got %+v", got)
	}

	second, err := svc.Login("alice", "secret")
	if err != nil {
		t.Fatalf("second login: %v", err)
	}
	events := transport.Delivered()
	if len(events) != 1 {
		t.Fatalf("expected 1 SessionInvalidated, got %d", len(events))
	}
	evt := events[0]
	if evt.SessionID != first.SessionID || evt.PlayerID != second.PlayerID {
		t.Fatalf("unexpected event: %+v", evt)
	}
	if evt.Reason != domain.ReasonLoginTakeover {
		t.Fatalf("expected login_takeover, got %s", evt.Reason)
	}
	if evt.EventID == "" || evt.OccurredAt.IsZero() {
		t.Fatalf("event metadata incomplete: %+v", evt)
	}
	if pending, _ := sessions.ListPendingOutbox(10); len(pending) != 0 {
		t.Fatalf("outbox should be drained after successful publish, got %d", len(pending))
	}
}

func TestRevokePublishesSessionInvalidated(t *testing.T) {
	svc, _, sessions, _, _, transport := newTestService(t)
	if _, err := svc.Register("alice", "secret"); err != nil {
		t.Fatalf("register: %v", err)
	}
	res, err := svc.Login("alice", "secret")
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	transport.Reset()
	if err := svc.RevokeSession(res.SessionID, domain.ReasonLogout); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	events := transport.Delivered()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].SessionID != res.SessionID || events[0].Reason != domain.ReasonLogout {
		t.Fatalf("unexpected event: %+v", events[0])
	}
	if pending, _ := sessions.ListPendingOutbox(10); len(pending) != 0 {
		t.Fatalf("outbox should be drained, got %d", len(pending))
	}
}

func TestPublisherFailureLeavesOutboxPendingKeepsNewSession(t *testing.T) {
	svc, _, sessions, _, _, transport := newTestService(t)
	if _, err := svc.Register("alice", "secret"); err != nil {
		t.Fatalf("register: %v", err)
	}
	first, err := svc.Login("alice", "secret")
	if err != nil {
		t.Fatalf("first login: %v", err)
	}

	boom := errors.New("transport unavailable")
	transport.SetError(boom)
	second, err := svc.Login("alice", "secret")
	if err != nil {
		t.Fatalf("login must succeed after durable outbox commit even if publish fails: %v", err)
	}
	if _, err := svc.ValidateToken(first.Token); !errors.Is(err, domain.ErrSessionInvalid) {
		t.Fatalf("prior session must stay invalidated, got %v", err)
	}
	if _, err := svc.ValidateToken(second.Token); err != nil {
		t.Fatalf("new session must remain active: %v", err)
	}
	pending, err := sessions.ListPendingOutbox(10)
	if err != nil {
		t.Fatalf("list pending: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending outbox event, got %d", len(pending))
	}
	if pending[0].Event.SessionID != first.SessionID {
		t.Fatalf("pending event for wrong session: %+v", pending[0].Event)
	}

	transport.SetError(nil)
	n, err := svc.DrainOutbox(10)
	if err != nil {
		t.Fatalf("drain retry: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 published on retry, got %d", n)
	}
	if pending, _ := sessions.ListPendingOutbox(10); len(pending) != 0 {
		t.Fatalf("outbox should be empty after retry, got %d", len(pending))
	}
	delivered := transport.Delivered()
	if len(delivered) != 1 || delivered[0].SessionID != first.SessionID {
		t.Fatalf("unexpected delivered after retry: %+v", delivered)
	}
}

func TestConcurrentLoginPublisherFailureKeepsSingleActive(t *testing.T) {
	svc, _, sessions, _, _, transport := newTestService(t)
	if _, err := svc.Register("alice", "secret"); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := svc.Login("alice", "secret"); err != nil {
		t.Fatalf("seed login: %v", err)
	}

	transport.SetError(errors.New("intermittent publish failure"))
	const n = 32
	var wg sync.WaitGroup
	wg.Add(n)
	var successes atomic.Int64
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			if _, err := svc.Login("alice", "secret"); err == nil {
				successes.Add(1)
			}
		}()
	}
	wg.Wait()
	if successes.Load() == 0 {
		t.Fatal("expected at least one successful login")
	}

	active := 0
	for _, s := range sessions.All() {
		if s.Status == domain.SessionStatusActive {
			active++
		}
	}
	if active != 1 {
		t.Fatalf("publisher failures must not violate single-active-session, got %d active", active)
	}
	pending, err := sessions.ListPendingOutbox(100)
	if err != nil {
		t.Fatalf("list pending: %v", err)
	}
	if len(pending) == 0 {
		t.Fatal("expected pending outbox events after publish failures")
	}

	transport.SetError(nil)
	if _, err := svc.DrainOutbox(100); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if pending, _ := sessions.ListPendingOutbox(10); len(pending) != 0 {
		t.Fatalf("expected empty outbox after drain, got %d", len(pending))
	}
}

func TestExpiryPersistsOutboxAndPublishes(t *testing.T) {
	svc, _, sessions, clock, _, transport := newTestService(t)
	if _, err := svc.Register("alice", "secret"); err != nil {
		t.Fatalf("register: %v", err)
	}
	res, err := svc.Login("alice", "secret")
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	transport.Reset()
	clock.now = clock.now.Add(2 * time.Hour)
	if _, err := svc.ValidateToken(res.Token); !errors.Is(err, domain.ErrSessionExpired) {
		t.Fatalf("expected ErrSessionExpired, got %v", err)
	}
	stored, ok := sessions.ByID(res.SessionID)
	if !ok || stored.Status != domain.SessionStatusExpired {
		t.Fatalf("session must be persisted as expired: ok=%v status=%s", ok, stored.Status)
	}
	events := transport.Delivered()
	if len(events) != 1 || events[0].Reason != domain.ReasonExpired || events[0].SessionID != res.SessionID {
		t.Fatalf("expected expiry SessionInvalidated, got %+v", events)
	}
	if pending, _ := sessions.ListPendingOutbox(10); len(pending) != 0 {
		t.Fatalf("outbox should be drained, got %d", len(pending))
	}
}

func TestExpiryPublishFailureLeavesPendingAndRetries(t *testing.T) {
	svc, _, sessions, clock, _, transport := newTestService(t)
	if _, err := svc.Register("alice", "secret"); err != nil {
		t.Fatalf("register: %v", err)
	}
	res, err := svc.Login("alice", "secret")
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	transport.Reset()
	transport.SetError(errors.New("gateway down"))
	clock.now = clock.now.Add(2 * time.Hour)
	if _, err := svc.ValidateToken(res.Token); !errors.Is(err, domain.ErrSessionExpired) {
		t.Fatalf("expected ErrSessionExpired after durable expiry commit, got %v", err)
	}
	pending, err := sessions.ListPendingOutbox(10)
	if err != nil || len(pending) != 1 {
		t.Fatalf("expected 1 pending expiry event, pending=%d err=%v", len(pending), err)
	}
	transport.SetError(nil)
	n, err := svc.DrainOutbox(10)
	if err != nil || n != 1 {
		t.Fatalf("drain retry: n=%d err=%v", n, err)
	}
	if got := transport.Delivered(); len(got) != 1 || got[0].Reason != domain.ReasonExpired {
		t.Fatalf("unexpected delivered: %+v", got)
	}
}

func TestExpiryPersistenceFailureReturnsSafeError(t *testing.T) {
	svc, _, sessions, clock, _, _ := newTestService(t)
	if _, err := svc.Register("alice", "secret"); err != nil {
		t.Fatalf("register: %v", err)
	}
	res, err := svc.Login("alice", "secret")
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	clock.now = clock.now.Add(2 * time.Hour)
	boom := errors.New("persist failed")
	sessions.SetInvalidateError(boom)
	_, err = svc.ValidateToken(res.Token)
	if err == nil || errors.Is(err, domain.ErrSessionExpired) {
		t.Fatalf("persistence failure must not report expired; got %v", err)
	}
	if !errors.Is(err, domain.ErrSessionInvalid) && !errors.Is(err, boom) {
		// Safe client-facing error: invalid (or wrapped persist). Never authorize.
		t.Fatalf("expected safe ErrSessionInvalid (or persist err), got %v", err)
	}
	stored, ok := sessions.ByID(res.SessionID)
	if !ok || stored.Status != domain.SessionStatusActive {
		t.Fatalf("failed expiry must leave session active until persisted, status=%s", stored.Status)
	}
}

func TestLoginMissingUserUsesDummyHashCompare(t *testing.T) {
	svc, _, _, _, _, _ := newTestService(t)
	_, err := svc.Login("nobody", "secret")
	if !errors.Is(err, domain.ErrInvalidCredentials) {
		t.Fatalf("expected ErrInvalidCredentials, got %v", err)
	}
	// Dummy path must reject empty/malformed stored hashes the same way Compare does.
	if domain.DummyPasswordHash() == "" {
		t.Fatal("dummy password hash must be non-empty fixed encoding")
	}
	h := domain.NewCheckpointPasswordHasher(nil)
	if h.Compare(domain.DummyPasswordHash(), "secret") {
		t.Fatal("dummy hash must not verify arbitrary passwords")
	}
}

func TestInvalidateWithOutboxIdempotentWhenAlreadyInactive(t *testing.T) {
	sessions := domain.NewMemorySessionRepository()
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	sess := domain.Session{
		ID:        domain.SessionID("session-1"),
		PlayerID:  domain.PlayerID("player-1"),
		TokenHash: "hash",
		Status:    domain.SessionStatusActive,
		CreatedAt: now,
		ExpiresAt: now.Add(time.Hour),
	}
	if err := sessions.Save(sess); err != nil {
		t.Fatalf("save: %v", err)
	}
	inv := sess
	inv.Status = domain.SessionStatusInvalidated
	ts := now
	inv.InvalidatedAt = &ts
	inv.InvalidationReason = domain.ReasonLogout
	evt := domain.SessionInvalidatedEvent{
		EventID:    "evt-1",
		PlayerID:   sess.PlayerID,
		SessionID:  sess.ID,
		Reason:     domain.ReasonLogout,
		OccurredAt: now,
	}
	if err := sessions.InvalidateWithOutbox(inv, evt); err != nil {
		t.Fatalf("first invalidate: %v", err)
	}
	expired := sess
	expired.Status = domain.SessionStatusExpired
	expired.InvalidatedAt = &ts
	expired.InvalidationReason = domain.ReasonExpired
	evt2 := evt
	evt2.EventID = "evt-2"
	evt2.Reason = domain.ReasonExpired
	if err := sessions.InvalidateWithOutbox(expired, evt2); err != nil {
		t.Fatalf("second invalidate must be idempotent: %v", err)
	}
	if n := sessions.OutboxLen(); n != 1 {
		t.Fatalf("expected exactly 1 outbox record, got %d", n)
	}
	stored, ok := sessions.ByID(sess.ID)
	if !ok || stored.Status != domain.SessionStatusInvalidated {
		t.Fatalf("winner status must stick, got %s", stored.Status)
	}
}

func TestConcurrentRevokeAndExpiryOneOutbox(t *testing.T) {
	svc, _, sessions, clock, _, transport := newTestService(t)
	if _, err := svc.Register("alice", "secret"); err != nil {
		t.Fatalf("register: %v", err)
	}
	res, err := svc.Login("alice", "secret")
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	transport.Reset()
	transport.SetError(errors.New("hold publish"))
	clock.now = clock.now.Add(2 * time.Hour)

	const n = 64
	var wg sync.WaitGroup
	wg.Add(n * 2)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			_ = svc.RevokeSession(res.SessionID, domain.ReasonAdmin)
		}()
		go func() {
			defer wg.Done()
			_, _ = svc.ValidateToken(res.Token)
		}()
	}
	wg.Wait()

	if n := sessions.OutboxLen(); n != 1 {
		t.Fatalf("concurrent revoke/expiry must produce exactly 1 outbox record, got %d", n)
	}
	pending, err := sessions.ListPendingOutbox(10)
	if err != nil || len(pending) != 1 {
		t.Fatalf("expected 1 pending, got %d err=%v", len(pending), err)
	}
	stored, ok := sessions.ByID(res.SessionID)
	if !ok || stored.Status == domain.SessionStatusActive {
		t.Fatalf("session must not remain active, status=%s", stored.Status)
	}
}

func TestConcurrentInvalidateWithOutboxRace(t *testing.T) {
	sessions := domain.NewMemorySessionRepository()
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	sess := domain.Session{
		ID:        domain.SessionID("session-race"),
		PlayerID:  domain.PlayerID("player-1"),
		TokenHash: "hash-race",
		Status:    domain.SessionStatusActive,
		CreatedAt: now,
		ExpiresAt: now.Add(time.Hour),
	}
	if err := sessions.Save(sess); err != nil {
		t.Fatalf("save: %v", err)
	}

	const n = 64
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			inv := sess
			ts := now
			if i%2 == 0 {
				inv.Status = domain.SessionStatusInvalidated
				inv.InvalidationReason = domain.ReasonLogout
			} else {
				inv.Status = domain.SessionStatusExpired
				inv.InvalidationReason = domain.ReasonExpired
			}
			inv.InvalidatedAt = &ts
			_ = sessions.InvalidateWithOutbox(inv, domain.SessionInvalidatedEvent{
				EventID:    "evt-" + itoa(i+1),
				PlayerID:   sess.PlayerID,
				SessionID:  sess.ID,
				Reason:     inv.InvalidationReason,
				OccurredAt: now,
			})
		}()
	}
	wg.Wait()
	if got := sessions.OutboxLen(); got != 1 {
		t.Fatalf("expected exactly 1 outbox record under race, got %d", got)
	}
}

func TestValidateSessionBinding_ActiveMatchingSession(t *testing.T) {
	svc, _, _, _, _, _ := newTestService(t)
	_, err := svc.Register("bind-user", "secret")
	if err != nil {
		t.Fatal(err)
	}
	login, err := svc.Login("bind-user", "secret")
	if err != nil {
		t.Fatal(err)
	}

	// sessionId is not a bearer token — binding lookup must succeed without the token.
	principal, err := svc.ValidateSessionBinding(login.SessionID, login.PlayerID)
	if err != nil {
		t.Fatalf("ValidateSessionBinding: %v", err)
	}
	if principal.SessionID != login.SessionID || principal.PlayerID != login.PlayerID {
		t.Fatalf("principal=%+v", principal)
	}

	if _, err := svc.ValidateSessionBinding(login.SessionID, "wrong-player"); !errors.Is(err, domain.ErrSessionInvalid) {
		t.Fatalf("mismatch want ErrSessionInvalid, got %v", err)
	}
	if _, err := svc.ValidateSessionBinding(domain.SessionID(login.Token), login.PlayerID); !errors.Is(err, domain.ErrSessionNotFound) {
		t.Fatalf("token-as-sessionId must not authorize, got %v", err)
	}
}

func TestValidateSessionBinding_RejectsExpired(t *testing.T) {
	svc, _, _, clock, _, _ := newTestService(t)
	_, _ = svc.Register("exp-user", "secret")
	login, err := svc.Login("exp-user", "secret")
	if err != nil {
		t.Fatal(err)
	}
	clock.now = clock.now.Add(2 * time.Hour)
	if _, err := svc.ValidateSessionBinding(login.SessionID, login.PlayerID); !errors.Is(err, domain.ErrSessionExpired) {
		t.Fatalf("want expired, got %v", err)
	}
}

func TestFinding_DrainOutboxNilTransportOrRepoErrors(t *testing.T) {
	ids := &seqIDs{}
	svc := domain.NewService(domain.ServiceDeps{
		Players:  domain.NewMemoryPlayerRepository(),
		Sessions: domain.NewMemorySessionRepository(),
		Hasher:   domain.NewCheckpointPasswordHasher(ids),
		IDs:      ids,
		Tokens:   ids,
		Clock:    &fixedClock{now: time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)},
		// Transport nil
	})
	if _, err := svc.DrainOutbox(10); err == nil {
		t.Fatal("nil transport must error")
	}
	svcNilRepo := domain.NewService(domain.ServiceDeps{
		Players:   domain.NewMemoryPlayerRepository(),
		Sessions:  nil,
		Hasher:    domain.NewCheckpointPasswordHasher(ids),
		IDs:       ids,
		Tokens:    ids,
		Clock:     &fixedClock{now: time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)},
		Transport: domain.NewFakeInvalidationTransport(),
	})
	if _, err := svcNilRepo.DrainOutbox(10); err == nil {
		t.Fatal("nil sessions repo must error")
	}
}
