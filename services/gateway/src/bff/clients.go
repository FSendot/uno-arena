package bff

import (
	"context"
	"encoding/json"

	"unoarena/shared/correlation"
	"unoarena/shared/envelope"
)

// Principal is the authenticated session identity returned by Identity.
// OperatorScope is derived only from a validated Identity response (roles/scopes
// or explicit operator claim). Raw client headers/query booleans never set it.
type Principal struct {
	PlayerID      string
	SessionID     string
	Username      string
	Roles         []string
	Scopes        []string
	OperatorScope bool
}

// RegisterResult is the Identity register outcome proxied by the BFF.
type RegisterResult struct {
	Status   string
	Username string
}

// LoginResult is the Identity login outcome proxied by the BFF.
type LoginResult struct {
	SessionID string
	PlayerID  string
	Token     string
}

// IdentityClient validates sessions and proxies auth operations.
type IdentityClient interface {
	Register(ctx context.Context, username, password string, corr correlation.Headers) (RegisterResult, error)
	Login(ctx context.Context, username, password string, corr correlation.Headers) (LoginResult, error)
	Logout(ctx context.Context, token string, corr correlation.Headers) error
	Whoami(ctx context.Context, token string, corr correlation.Headers) (Principal, error)
	ValidateSession(ctx context.Context, token string, corr correlation.Headers) (Principal, error)
}

// CommandDispatch is a command forwarded to a bounded-context backend.
type CommandDispatch struct {
	Command     envelope.Command
	Principal   Principal
	Correlation correlation.Headers
	RoomID      string // set for room-scoped routes; may be empty for CreateRoom
}

// RoomClient dispatches room/gameplay commands and player reconnect snapshots.
type RoomClient interface {
	SubmitCommand(ctx context.Context, req CommandDispatch) (envelope.Result, error)
	// PlayerSnapshot returns the authoritative player-private reconnect snapshot.
	PlayerSnapshot(ctx context.Context, roomID, playerID string, corr correlation.Headers) (json.RawMessage, error)
	// PublicList proxies GET /internal/v1/rooms/public-list with safe query params.
	PublicList(ctx context.Context, rawQuery string, corr correlation.Headers) (json.RawMessage, error)
}

// TournamentClient dispatches tournament commands and proxies tournament reads.
type TournamentClient interface {
	SubmitCommand(ctx context.Context, req CommandDispatch) (envelope.Result, error)
	// Bracket proxies GET /v1/tournaments/{id}/bracket, forwarding rawQuery unchanged.
	Bracket(ctx context.Context, tournamentID, rawQuery string, corr correlation.Headers, principal *Principal) (json.RawMessage, error)
	// Standings proxies GET /v1/tournaments/{id}/standings.
	Standings(ctx context.Context, tournamentID string, corr correlation.Headers, principal *Principal) (json.RawMessage, error)
	// Assignment proxies GET /v1/tournaments/{id}/players/{playerId}/assignment.
	Assignment(ctx context.Context, tournamentID, playerID string, corr correlation.Headers, principal *Principal) (json.RawMessage, error)
}

// ReadModelClient serves public read proxies (rankings/analytics).
type ReadModelClient interface {
	// Leaderboard forwards the public Ranking query contract (boardType, cursor, limit).
	Leaderboard(ctx context.Context, rawQuery string, corr correlation.Headers) (json.RawMessage, error)
	PublicAnalytics(ctx context.Context, corr correlation.Headers) (json.RawMessage, error)
}

// SpectatorAdmitRequest carries validated principal/token context for admission.
// InviteCapability is the opaque X-Room-Invite value forwarded to Spectator View
// for validation; Gateway never treats a client boolean as invite proof.
// Operator scope reaches the gate only via Principal.OperatorScope from Identity.
type SpectatorAdmitRequest struct {
	RoomID           string
	Token            string     // raw bearer when supplied; empty when anonymous
	Principal        *Principal // non-nil when token validated
	InviteCapability string     // opaque room invite; empty when absent
	Correlation      correlation.Headers
}

// SpectatorGate decides whether a new spectator SSE connection may open and
// fetches authoritative spectator-safe projection snapshots.
type SpectatorGate interface {
	// Admit returns false when the room is terminal or otherwise closed to spectators.
	// Callers must validate any supplied bearer before calling; invalid tokens must not reach Admit.
	Admit(ctx context.Context, req SpectatorAdmitRequest) (allowed bool, reason string, err error)
	// Snapshot returns the spectator-safe projection snapshot for resync after snapshot_required.
	Snapshot(ctx context.Context, req SpectatorAdmitRequest) (json.RawMessage, error)
}
