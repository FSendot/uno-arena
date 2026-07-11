package domain

import (
	"errors"
	"strings"
	"time"
)

// ServiceDeps wires Identity application use-cases for the offline slice.
type ServiceDeps struct {
	Players    PlayerRepository
	Sessions   SessionRepository
	Hasher     PasswordHasher
	IDs        IDGenerator
	Tokens     TokenGenerator
	Clock      Clock
	SessionTTL time.Duration
	Transport  InvalidationTransport
	DrainLimit int
}

// Service implements register/login/session validation with single-active-session.
type Service struct {
	players    PlayerRepository
	sessions   SessionRepository
	hasher     PasswordHasher
	ids        IDGenerator
	tokens     TokenGenerator
	clock      Clock
	sessionTTL time.Duration
	transport  InvalidationTransport
	drainLimit int
}

func NewService(d ServiceDeps) *Service {
	ttl := d.SessionTTL
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	clock := d.Clock
	if clock == nil {
		clock = SystemClock{}
	}
	limit := d.DrainLimit
	if limit <= 0 {
		limit = 32
	}
	return &Service{
		players:    d.Players,
		sessions:   d.Sessions,
		hasher:     d.Hasher,
		ids:        d.IDs,
		tokens:     d.Tokens,
		clock:      clock,
		sessionTTL: ttl,
		transport:  d.Transport,
		drainLimit: limit,
	}
}

// LoginResult is returned by a successful Login.
type LoginResult struct {
	SessionID SessionID
	PlayerID  PlayerID
	Token     string
}

// SessionPrincipal is the validated session identity.
type SessionPrincipal struct {
	PlayerID  PlayerID
	SessionID SessionID
	Roles     []string
	ExpiresAt time.Time
}

// WhoamiResult matches the BFF WhoamiResponse shape.
type WhoamiResult struct {
	PlayerID  PlayerID
	SessionID SessionID
	Username  string
}

func (s *Service) Register(username, password string) (Player, error) {
	username = strings.TrimSpace(username)
	if username == "" || password == "" {
		return Player{}, ErrInvalidInput
	}
	if _, exists := s.players.FindByUsername(username); exists {
		return Player{}, ErrUsernameTaken
	}
	hash, err := s.hasher.Hash(password)
	if err != nil {
		return Player{}, err
	}
	now := s.clock.Now()
	p := Player{
		ID:           PlayerID(s.ids.NewID("player")),
		Username:     username,
		PasswordHash: hash,
		CreatedAt:    now,
	}
	if err := s.players.Save(p); err != nil {
		return Player{}, err
	}
	return p, nil
}

func (s *Service) Login(username, password string) (LoginResult, error) {
	username = strings.TrimSpace(username)
	if username == "" || password == "" {
		return LoginResult{}, ErrInvalidCredentials
	}
	player, ok := s.players.FindByUsername(username)
	encoded := DummyPasswordHash()
	if ok {
		encoded = player.PasswordHash
	}
	if !s.hasher.Compare(encoded, password) || !ok {
		return LoginResult{}, ErrInvalidCredentials
	}

	now := s.clock.Now()
	token, err := s.tokens.NewToken()
	if err != nil {
		return LoginResult{}, err
	}
	sess := Session{
		ID:        SessionID(s.ids.NewID("session")),
		PlayerID:  player.ID,
		TokenHash: HashToken(token),
		Status:    SessionStatusActive,
		CreatedAt: now,
		ExpiresAt: now.Add(s.sessionTTL),
	}

	var previous *Session
	if active, found := s.sessions.FindActiveByPlayer(player.ID); found {
		inv := active
		ts := now
		inv.Status = SessionStatusInvalidated
		inv.InvalidatedAt = &ts
		inv.InvalidationReason = ReasonLoginTakeover
		previous = &inv
	}

	eventFor := func(inv Session) SessionInvalidatedEvent {
		return SessionInvalidatedEvent{
			EventID:    s.ids.NewID("evt"),
			PlayerID:   inv.PlayerID,
			SessionID:  inv.ID,
			Reason:     inv.InvalidationReason,
			OccurredAt: now,
		}
	}
	if err := s.sessions.ReplaceActiveSession(previous, sess, eventFor); err != nil {
		return LoginResult{}, err
	}
	// Publish after commit; failure leaves outbox pending — never compensate sessions.
	_, _ = s.DrainOutbox(s.drainLimit)

	return LoginResult{
		SessionID: sess.ID,
		PlayerID:  player.ID,
		Token:     token,
	}, nil
}

func (s *Service) RevokeSession(id SessionID, reason string) error {
	sess, ok := s.sessions.FindByID(id)
	if !ok {
		return ErrSessionNotFound
	}
	if sess.Status != SessionStatusActive {
		return nil // idempotent
	}
	if reason == "" {
		reason = ReasonLogout
	}
	now := s.clock.Now()
	if err := s.invalidateWithOutbox(sess, reason, now); err != nil {
		return err
	}
	_, _ = s.DrainOutbox(s.drainLimit)
	return nil
}

func (s *Service) ValidateToken(token string) (SessionPrincipal, error) {
	if token == "" {
		return SessionPrincipal{}, ErrSessionInvalid
	}
	sess, ok := s.sessions.FindByTokenHash(HashToken(token))
	if !ok {
		return SessionPrincipal{}, ErrSessionNotFound
	}
	return s.principalFromSession(sess)
}

// ValidateSessionBinding authorizes a forwarded sessionId+playerId pair under a
// service credential. sessionId is a durable session identity, not an opaque
// bearer token — callers must not present it as Authorization Bearer.
func (s *Service) ValidateSessionBinding(sessionID SessionID, playerID PlayerID) (SessionPrincipal, error) {
	if sessionID == "" || playerID == "" {
		return SessionPrincipal{}, ErrInvalidInput
	}
	sess, ok := s.sessions.FindByID(sessionID)
	if !ok {
		return SessionPrincipal{}, ErrSessionNotFound
	}
	if sess.PlayerID != playerID {
		return SessionPrincipal{}, ErrSessionInvalid
	}
	return s.principalFromSession(sess)
}

func (s *Service) principalFromSession(sess Session) (SessionPrincipal, error) {
	now := s.clock.Now()
	if err := sess.IsActiveAt(now); err != nil {
		if err == ErrSessionExpired && sess.Status == SessionStatusActive {
			if invErr := s.invalidateWithOutbox(sess, ReasonExpired, now); invErr != nil {
				// Do not claim expiry succeeded; never authorize on persist failure.
				return SessionPrincipal{}, ErrSessionInvalid
			}
			_, _ = s.DrainOutbox(s.drainLimit)
		}
		return SessionPrincipal{}, err
	}
	return SessionPrincipal{
		PlayerID:  sess.PlayerID,
		SessionID: sess.ID,
		Roles:     []string{"player"},
		ExpiresAt: sess.ExpiresAt,
	}, nil
}

func (s *Service) Whoami(token string) (WhoamiResult, error) {
	principal, err := s.ValidateToken(token)
	if err != nil {
		return WhoamiResult{}, err
	}
	player, ok := s.players.FindByID(principal.PlayerID)
	if !ok {
		return WhoamiResult{}, ErrPlayerNotFound
	}
	return WhoamiResult{
		PlayerID:  principal.PlayerID,
		SessionID: principal.SessionID,
		Username:  player.Username,
	}, nil
}

// DrainOutbox delivers pending SessionInvalidated events via the transport and
// marks them published. Stops on first delivery failure (remaining stay pending).
func (s *Service) DrainOutbox(limit int) (int, error) {
	if s.transport == nil || s.sessions == nil {
		return 0, errors.New("outbox dependencies not configured")
	}
	pending, err := s.sessions.ListPendingOutbox(limit)
	if err != nil {
		return 0, err
	}
	published := 0
	now := s.clock.Now()
	for _, entry := range pending {
		if err := s.transport.Deliver(entry.Event); err != nil {
			return published, err
		}
		if err := s.sessions.MarkOutboxPublished(entry.Event.EventID, now); err != nil {
			return published, err
		}
		published++
	}
	return published, nil
}

func (s *Service) invalidateWithOutbox(sess Session, reason string, now time.Time) error {
	ts := now
	sess.Status = SessionStatusInvalidated
	if reason == ReasonExpired {
		sess.Status = SessionStatusExpired
	}
	sess.InvalidatedAt = &ts
	sess.InvalidationReason = reason
	evt := SessionInvalidatedEvent{
		EventID:    s.ids.NewID("evt"),
		PlayerID:   sess.PlayerID,
		SessionID:  sess.ID,
		Reason:     reason,
		OccurredAt: now,
	}
	return s.sessions.InvalidateWithOutbox(sess, evt)
}
