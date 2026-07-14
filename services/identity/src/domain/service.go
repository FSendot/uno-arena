package domain

import (
	"context"
	"errors"
	"strings"
	"time"
)

// ServiceDeps wires Identity application use-cases for the offline slice.
type ServiceDeps struct {
	Players       PlayerRepository
	Sessions      SessionRepository
	Hasher        PasswordHasher
	IDs           IDGenerator
	Tokens        TokenGenerator
	Clock         Clock
	SessionTTL    time.Duration
	Transport     InvalidationTransport
	DrainLimit    int
	AllowedRoles  []string // OIDC ACL allowlist; empty → DefaultAllowedRoles
	PlayerCreated PlayerCreationRecorder
}

// PlayerCreationRecorder owns Identity's post-commit player-creation signal.
// Implementations must be process-local and must not add player identifiers as labels.
type PlayerCreationRecorder interface {
	RecordPlayerCreated(context.Context)
}

// Service implements register/login/session validation with single-active-session.
type Service struct {
	players       PlayerRepository
	sessions      SessionRepository
	hasher        PasswordHasher
	ids           IDGenerator
	tokens        TokenGenerator
	clock         Clock
	sessionTTL    time.Duration
	transport     InvalidationTransport
	drainLimit    int
	allowedRoles  []string
	playerCreated PlayerCreationRecorder
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
	allowed := d.AllowedRoles
	if len(allowed) == 0 {
		allowed = append([]string(nil), DefaultAllowedRoles...)
	}
	return &Service{
		players:       d.Players,
		sessions:      d.Sessions,
		hasher:        d.Hasher,
		ids:           d.IDs,
		tokens:        d.Tokens,
		clock:         clock,
		sessionTTL:    ttl,
		transport:     d.Transport,
		drainLimit:    limit,
		allowedRoles:  allowed,
		playerCreated: d.PlayerCreated,
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

func (s *Service) Register(ctx context.Context, username, password string) (Player, error) {
	username = strings.TrimSpace(username)
	if username == "" || password == "" {
		return Player{}, ErrInvalidInput
	}
	if _, exists, err := s.players.FindByUsername(ctx, username); err != nil {
		return Player{}, MapRepoError(err)
	} else if exists {
		return Player{}, ErrUsernameTaken
	}
	hash, err := s.hasher.Hash(password)
	if err != nil {
		return Player{}, MapRepoError(err)
	}
	now := s.clock.Now()
	p := Player{
		ID:           PlayerID(s.ids.NewID("player")),
		Username:     username,
		PasswordHash: hash,
		CreatedAt:    now,
	}
	if err := s.players.RegisterWithDefaultACL(ctx, p, "player"); err != nil {
		return Player{}, MapRepoError(err)
	}
	if s.playerCreated != nil {
		s.playerCreated.RecordPlayerCreated(ctx)
	}
	return p, nil
}

func (s *Service) Login(ctx context.Context, username, password string) (LoginResult, error) {
	username = strings.TrimSpace(username)
	if username == "" || password == "" {
		return LoginResult{}, ErrInvalidCredentials
	}
	player, ok, err := s.players.FindByUsername(ctx, username)
	if err != nil {
		return LoginResult{}, MapRepoError(err)
	}
	encoded := DummyPasswordHash()
	if ok {
		encoded = player.PasswordHash
	}
	if !s.hasher.Compare(encoded, password) || !ok {
		return LoginResult{}, ErrInvalidCredentials
	}
	return s.establishSession(ctx, player)
}

// EstablishOIDCSession creates an UnoArena session for an already-validated OIDC principal.
// Never accepts or stores provider tokens/raw claims — only issuer/subject/username/roles.
func (s *Service) EstablishOIDCSession(ctx context.Context, issuer, subject, username string, roles []string) (LoginResult, error) {
	issuer = strings.TrimSpace(issuer)
	subject = strings.TrimSpace(subject)
	username = strings.TrimSpace(username)
	if issuer == "" || subject == "" || username == "" {
		return LoginResult{}, ErrInvalidInput
	}
	now := s.clock.Now()
	newPlayer := Player{
		ID:           PlayerID(s.ids.NewID("player")),
		Username:     username,
		PasswordHash: "",
		CreatedAt:    now,
	}
	accepted := NormalizeAcceptedRoles(roles, s.allowedRoles)
	player, _, err := s.players.LinkOrResolveExternal(ctx, issuer, subject, username, newPlayer, accepted)
	if err != nil {
		return LoginResult{}, MapRepoError(err)
	}
	if player.ID == newPlayer.ID && s.playerCreated != nil {
		s.playerCreated.RecordPlayerCreated(ctx)
	}
	return s.establishSession(ctx, player)
}

func (s *Service) establishSession(ctx context.Context, player Player) (LoginResult, error) {
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
	if active, found, err := s.sessions.FindActiveByPlayer(ctx, player.ID); err != nil {
		return LoginResult{}, MapRepoError(err)
	} else if found {
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
		}.Normalize()
	}
	if err := s.sessions.ReplaceActiveSession(ctx, previous, sess, eventFor); err != nil {
		return LoginResult{}, MapRepoError(err)
	}
	if s.transport != nil {
		_, _ = s.DrainOutbox(ctx, s.drainLimit)
	}

	return LoginResult{
		SessionID: sess.ID,
		PlayerID:  player.ID,
		Token:     token,
	}, nil
}

func (s *Service) RevokeSession(ctx context.Context, id SessionID, reason string) error {
	sess, ok, err := s.sessions.FindByID(ctx, id)
	if err != nil {
		return MapRepoError(err)
	}
	if !ok {
		return ErrSessionNotFound
	}
	if sess.Status != SessionStatusActive {
		return nil
	}
	if reason == "" {
		reason = ReasonLogout
	}
	now := s.clock.Now()
	if err := s.invalidateWithOutbox(ctx, sess, reason, now); err != nil {
		return MapRepoError(err)
	}
	if s.transport != nil {
		_, _ = s.DrainOutbox(ctx, s.drainLimit)
	}
	return nil
}

// LogoutByToken revokes the UnoArena session identified by bearer token. It is
// deliberately idempotent: unknown, expired, and already-inactive tokens do
// not disclose session existence and do not enqueue another invalidation.
func (s *Service) LogoutByToken(ctx context.Context, token string) error {
	token = strings.TrimSpace(token)
	if token == "" {
		return ErrInvalidInput
	}
	sess, ok, err := s.sessions.FindByTokenHash(ctx, HashToken(token))
	if err != nil {
		return MapRepoError(err)
	}
	if !ok || sess.Status != SessionStatusActive {
		return nil
	}
	now := s.clock.Now()
	if err := sess.IsActiveAt(now); err != nil {
		// Expired bearers are a successful, non-revealing logout outcome. The
		// normal validation path owns expiry invalidation and its outbox fact.
		return nil
	}
	if err := s.invalidateWithOutbox(ctx, sess, ReasonLogout, now); err != nil {
		return MapRepoError(err)
	}
	if s.transport != nil {
		_, _ = s.DrainOutbox(ctx, s.drainLimit)
	}
	return nil
}

func (s *Service) ValidateToken(ctx context.Context, token string) (SessionPrincipal, error) {
	if token == "" {
		return SessionPrincipal{}, ErrSessionInvalid
	}
	sess, ok, err := s.sessions.FindByTokenHash(ctx, HashToken(token))
	if err != nil {
		return SessionPrincipal{}, MapRepoError(err)
	}
	if !ok {
		return SessionPrincipal{}, ErrSessionNotFound
	}
	return s.principalFromSession(ctx, sess)
}

// ValidateSessionBinding authorizes a forwarded sessionId+playerId pair under a
// service credential. sessionId is a durable session identity, not an opaque
// bearer token — callers must not present it as Authorization Bearer.
func (s *Service) ValidateSessionBinding(ctx context.Context, sessionID SessionID, playerID PlayerID) (SessionPrincipal, error) {
	if sessionID == "" || playerID == "" {
		return SessionPrincipal{}, ErrInvalidInput
	}
	sess, ok, err := s.sessions.FindByID(ctx, sessionID)
	if err != nil {
		return SessionPrincipal{}, MapRepoError(err)
	}
	if !ok {
		return SessionPrincipal{}, ErrSessionNotFound
	}
	if sess.PlayerID != playerID {
		return SessionPrincipal{}, ErrSessionInvalid
	}
	return s.principalFromSession(ctx, sess)
}

func (s *Service) principalFromSession(ctx context.Context, sess Session) (SessionPrincipal, error) {
	now := s.clock.Now()
	if err := sess.IsActiveAt(now); err != nil {
		if err == ErrSessionExpired && sess.Status == SessionStatusActive {
			if invErr := s.invalidateWithOutbox(ctx, sess, ReasonExpired, now); invErr != nil {
				return SessionPrincipal{}, MapRepoError(invErr)
			}
			if s.transport != nil {
				_, _ = s.DrainOutbox(ctx, s.drainLimit)
			}
		}
		return SessionPrincipal{}, err
	}
	roles := []string{"player"}
	if listed, err := s.players.ListRoles(ctx, sess.PlayerID); err != nil {
		return SessionPrincipal{}, MapRepoError(err)
	} else if len(listed) > 0 {
		roles = listed
	}
	return SessionPrincipal{
		PlayerID:  sess.PlayerID,
		SessionID: sess.ID,
		Roles:     roles,
		ExpiresAt: sess.ExpiresAt,
	}, nil
}

func (s *Service) Whoami(ctx context.Context, token string) (WhoamiResult, error) {
	principal, err := s.ValidateToken(ctx, token)
	if err != nil {
		return WhoamiResult{}, err
	}
	player, ok, err := s.players.FindByID(ctx, principal.PlayerID)
	if err != nil {
		return WhoamiResult{}, MapRepoError(err)
	}
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
// marks them published. Capability-mode only — durable production never polls.
func (s *Service) DrainOutbox(ctx context.Context, limit int) (int, error) {
	if s.transport == nil || s.sessions == nil {
		return 0, errors.New("outbox dependencies not configured")
	}
	pending, err := s.sessions.ListPendingOutbox(ctx, limit)
	if err != nil {
		return 0, err
	}
	published := 0
	now := s.clock.Now()
	for _, entry := range pending {
		if err := s.transport.Deliver(entry.Event); err != nil {
			return published, err
		}
		if err := s.sessions.MarkOutboxPublished(ctx, entry.Event.EventID, now); err != nil {
			return published, err
		}
		published++
	}
	return published, nil
}

func (s *Service) invalidateWithOutbox(ctx context.Context, sess Session, reason string, now time.Time) error {
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
	}.Normalize()
	return s.sessions.InvalidateWithOutbox(ctx, sess, evt)
}
