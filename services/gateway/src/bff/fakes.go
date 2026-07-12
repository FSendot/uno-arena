package bff

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"unoarena/shared/audit"
	"unoarena/shared/correlation"
	"unoarena/shared/envelope"
)

var (
	ErrUnauthorized       = errors.New("unauthorized")
	ErrInvalidCredentials = errors.New("invalid credentials")
	ErrNotFound           = errors.New("not found")
	ErrSpectatorDenied    = errors.New("spectator_denied")
)

// FakeIdentity is an injectable in-memory Identity client for offline/tests.
type FakeIdentity struct {
	mu       sync.Mutex
	tokens   map[string]Principal // token -> principal
	users    map[string]string    // username -> password
	nextID   int
	FailNext bool
	LastCorr correlation.Headers
}

// NewFakeIdentity creates an empty fake identity client.
func NewFakeIdentity() *FakeIdentity {
	return &FakeIdentity{
		tokens: make(map[string]Principal),
		users:  make(map[string]string),
	}
}

// SeedSession registers a token -> principal mapping.
func (f *FakeIdentity) SeedSession(token string, p Principal) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tokens[token] = p
}

// InvalidateToken removes a session token (simulates SessionInvalidated).
func (f *FakeIdentity) InvalidateToken(token string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.tokens, token)
}

func (f *FakeIdentity) Register(_ context.Context, username, password string, corr correlation.Headers) (RegisterResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.LastCorr = corr
	if username == "" || password == "" {
		return RegisterResult{}, errors.New("username and password required")
	}
	if _, exists := f.users[username]; exists {
		return RegisterResult{}, errors.New("username taken")
	}
	f.users[username] = password
	return RegisterResult{Status: "ok", Username: username}, nil
}

func (f *FakeIdentity) Login(_ context.Context, username, password string, corr correlation.Headers) (LoginResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.LastCorr = corr
	pw, ok := f.users[username]
	if !ok || pw != password {
		return LoginResult{}, ErrInvalidCredentials
	}
	f.nextID++
	token := "tok_" + username
	p := Principal{
		PlayerID:  "player_" + username,
		SessionID: "session_" + username,
		Username:  username,
	}
	f.tokens[token] = p
	return LoginResult{SessionID: p.SessionID, PlayerID: p.PlayerID, Token: token}, nil
}

func (f *FakeIdentity) Whoami(ctx context.Context, token string, corr correlation.Headers) (Principal, error) {
	return f.ValidateSession(ctx, token, corr)
}

func (f *FakeIdentity) ValidateSession(_ context.Context, token string, corr correlation.Headers) (Principal, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.LastCorr = corr
	if f.FailNext {
		f.FailNext = false
		return Principal{}, errors.New("identity unavailable")
	}
	p, ok := f.tokens[token]
	if !ok {
		return Principal{}, ErrUnauthorized
	}
	return p, nil
}

// FakeRoom records dispatched room commands and returns configurable results.
type FakeRoom struct {
	mu                   sync.Mutex
	Dispatched           []CommandDispatch
	Results              map[string]envelope.Result // by commandId
	Default              envelope.Result
	Err                  error
	SnapshotJSON         json.RawMessage
	SnapshotErr          error
	LastSnapshotRoomID   string
	LastSnapshotPlayerID string
	PublicListJSON       json.RawMessage
	PublicListErr        error
	LastPublicListQuery  string
}

// NewFakeRoom creates a room client that accepts by default.
func NewFakeRoom() *FakeRoom {
	return &FakeRoom{
		Dispatched: make([]CommandDispatch, 0),
		Results:    make(map[string]envelope.Result),
		Default:    envelope.Result{Status: envelope.StatusAccepted, SchemaVersion: envelope.CurrentSchemaVersion},
	}
}

func (f *FakeRoom) SubmitCommand(_ context.Context, req CommandDispatch) (envelope.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Dispatched = append(f.Dispatched, req)
	if f.Err != nil {
		return envelope.Result{}, f.Err
	}
	if res, ok := f.Results[req.Command.CommandID]; ok {
		if res.CommandID == "" {
			res.CommandID = req.Command.CommandID
		}
		if res.Type == "" {
			res.Type = req.Command.Type
		}
		if res.SchemaVersion < 1 {
			res.SchemaVersion = envelope.CurrentSchemaVersion
		}
		return res, nil
	}
	out := f.Default
	out.CommandID = req.Command.CommandID
	out.Type = req.Command.Type
	if out.SchemaVersion < 1 {
		out.SchemaVersion = envelope.CurrentSchemaVersion
	}
	return out, nil
}

// DispatchCount returns how many commands were dispatched.
func (f *FakeRoom) DispatchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.Dispatched)
}

func (f *FakeRoom) PlayerSnapshot(_ context.Context, roomID, playerID string, _ correlation.Headers) (json.RawMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.LastSnapshotRoomID = roomID
	f.LastSnapshotPlayerID = playerID
	if f.SnapshotErr != nil {
		return nil, f.SnapshotErr
	}
	if len(f.SnapshotJSON) == 0 {
		return json.RawMessage(`{"roomId":"` + roomID + `","playerId":"` + playerID + `","schemaVersion":1}`), nil
	}
	return f.SnapshotJSON, nil
}

func (f *FakeRoom) PublicList(_ context.Context, rawQuery string, _ correlation.Headers) (json.RawMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.LastPublicListQuery = rawQuery
	if f.PublicListErr != nil {
		return nil, f.PublicListErr
	}
	if len(f.PublicListJSON) == 0 {
		return json.RawMessage(`{"schemaVersion":1,"rooms":[]}`), nil
	}
	return f.PublicListJSON, nil
}

// FakeTournament records dispatched tournament commands and serves configurable reads.
type FakeTournament struct {
	mu               sync.Mutex
	Dispatched       []CommandDispatch
	Results          map[string]envelope.Result
	Default          envelope.Result
	Err              error
	BracketJSON      json.RawMessage
	StandingsJSON    json.RawMessage
	AssignmentJSON   json.RawMessage
	BracketErr       error
	StandingsErr     error
	AssignmentErr    error
	LastBracketQuery string
	LastCorr         correlation.Headers
	LastPrincipal    *Principal
	LastAssignmentID string
}

// NewFakeTournament creates a tournament client that accepts by default.
func NewFakeTournament() *FakeTournament {
	return &FakeTournament{
		Dispatched: make([]CommandDispatch, 0),
		Results:    make(map[string]envelope.Result),
		Default:    envelope.Result{Status: envelope.StatusAccepted, SchemaVersion: envelope.CurrentSchemaVersion},
	}
}

func (f *FakeTournament) SubmitCommand(_ context.Context, req CommandDispatch) (envelope.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Dispatched = append(f.Dispatched, req)
	if f.Err != nil {
		return envelope.Result{}, f.Err
	}
	if res, ok := f.Results[req.Command.CommandID]; ok {
		if res.CommandID == "" {
			res.CommandID = req.Command.CommandID
		}
		if res.Type == "" {
			res.Type = req.Command.Type
		}
		if res.SchemaVersion < 1 {
			res.SchemaVersion = envelope.CurrentSchemaVersion
		}
		return res, nil
	}
	out := f.Default
	out.CommandID = req.Command.CommandID
	out.Type = req.Command.Type
	if out.SchemaVersion < 1 {
		out.SchemaVersion = envelope.CurrentSchemaVersion
	}
	return out, nil
}

func (f *FakeTournament) Bracket(_ context.Context, _ string, rawQuery string, corr correlation.Headers, principal *Principal) (json.RawMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.LastBracketQuery = rawQuery
	f.LastCorr = corr
	f.LastPrincipal = principal
	if f.BracketErr != nil {
		return nil, f.BracketErr
	}
	if len(f.BracketJSON) == 0 {
		return json.RawMessage(`{"tournamentId":"t1","projectionVersion":1,"generatedAt":"2026-01-01T00:00:00Z","summary":{},"slots":[]}`), nil
	}
	return f.BracketJSON, nil
}

func (f *FakeTournament) Standings(_ context.Context, _ string, corr correlation.Headers, principal *Principal) (json.RawMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.LastCorr = corr
	f.LastPrincipal = principal
	if f.StandingsErr != nil {
		return nil, f.StandingsErr
	}
	if len(f.StandingsJSON) == 0 {
		return json.RawMessage(`{"tournamentId":"t1","projectionVersion":1,"generatedAt":"2026-01-01T00:00:00Z","phase":"registration","registeredCount":0,"currentRound":0,"finalStandings":[]}`), nil
	}
	return f.StandingsJSON, nil
}

func (f *FakeTournament) Assignment(_ context.Context, _, playerID string, corr correlation.Headers, principal *Principal) (json.RawMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.LastCorr = corr
	f.LastPrincipal = principal
	f.LastAssignmentID = playerID
	if f.AssignmentErr != nil {
		return nil, f.AssignmentErr
	}
	if len(f.AssignmentJSON) == 0 {
		return json.RawMessage(`{"tournamentId":"t1","playerId":"` + playerID + `","visibility":"public","phase":"registration","registrationStatus":"registered","currentRound":0,"assignment":null}`), nil
	}
	return f.AssignmentJSON, nil
}

func (f *FakeTournament) DispatchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.Dispatched)
}

// FakeReads serves static public read models.
type FakeReads struct {
	LeaderboardJSON json.RawMessage
	AnalyticsJSON   json.RawMessage
}

func (f *FakeReads) Leaderboard(_ context.Context, _ correlation.Headers) (json.RawMessage, error) {
	if len(f.LeaderboardJSON) == 0 {
		return json.RawMessage(`{"entries":[]}`), nil
	}
	return f.LeaderboardJSON, nil
}

func (f *FakeReads) PublicAnalytics(_ context.Context, _ correlation.Headers) (json.RawMessage, error) {
	if len(f.AnalyticsJSON) == 0 {
		return json.RawMessage(`{"metrics":[]}`), nil
	}
	return f.AnalyticsJSON, nil
}

// FakeSpectatorGate admits by default; DenyRooms blocks listed room IDs.
// PrivateRooms require an explicit participant ID, a valid opaque invite
// capability, or Identity-derived operator scope — never any principal.
type FakeSpectatorGate struct {
	mu           sync.Mutex
	DenyRooms    map[string]string              // roomId -> reason
	PrivateRooms map[string]struct{}            // roomId
	Participants map[string]map[string]struct{} // roomId -> playerIds
	ValidInvites map[string]map[string]struct{} // roomId -> opaque invite tokens
	LastReq      SpectatorAdmitRequest
	SnapshotJSON json.RawMessage
	SnapshotErr  error
}

// NewFakeSpectatorGate creates an admitting gate.
func NewFakeSpectatorGate() *FakeSpectatorGate {
	return &FakeSpectatorGate{
		DenyRooms:    make(map[string]string),
		PrivateRooms: make(map[string]struct{}),
		Participants: make(map[string]map[string]struct{}),
		ValidInvites: make(map[string]map[string]struct{}),
	}
}

// Deny marks a room as terminal / not admitting spectators.
func (g *FakeSpectatorGate) Deny(roomID, reason string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.DenyRooms[roomID] = reason
}

// MarkPrivate requires participant, invite, or operator scope for admission.
func (g *FakeSpectatorGate) MarkPrivate(roomID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.PrivateRooms[roomID] = struct{}{}
}

// AllowParticipant registers a trusted roster playerId for a private room.
func (g *FakeSpectatorGate) AllowParticipant(roomID, playerID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.Participants[roomID] == nil {
		g.Participants[roomID] = make(map[string]struct{})
	}
	g.Participants[roomID][playerID] = struct{}{}
}

// AllowInvite registers a valid opaque room invite capability.
func (g *FakeSpectatorGate) AllowInvite(roomID, invite string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.ValidInvites[roomID] == nil {
		g.ValidInvites[roomID] = make(map[string]struct{})
	}
	g.ValidInvites[roomID][invite] = struct{}{}
}

func (g *FakeSpectatorGate) Admit(_ context.Context, req SpectatorAdmitRequest) (bool, string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.LastReq = req
	if reason, ok := g.DenyRooms[req.RoomID]; ok {
		return false, reason, nil
	}
	if _, private := g.PrivateRooms[req.RoomID]; !private {
		return true, "", nil
	}
	if req.Principal != nil && req.Principal.OperatorScope {
		return true, "", nil
	}
	if req.InviteCapability != "" {
		if invites, ok := g.ValidInvites[req.RoomID]; ok {
			if _, ok := invites[req.InviteCapability]; ok {
				return true, "", nil
			}
		}
	}
	if req.Principal != nil && req.Principal.PlayerID != "" && req.Principal.SessionID != "" {
		if roster, ok := g.Participants[req.RoomID]; ok {
			if _, ok := roster[req.Principal.PlayerID]; ok {
				return true, "", nil
			}
		}
	}
	return false, "private room requires participant, invite, or operator scope", nil
}

func (g *FakeSpectatorGate) Snapshot(ctx context.Context, req SpectatorAdmitRequest) (json.RawMessage, error) {
	allowed, reason, err := g.Admit(ctx, req)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return nil, fmt.Errorf("%w: %s", ErrSpectatorDenied, reason)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.SnapshotErr != nil {
		return nil, g.SnapshotErr
	}
	if len(g.SnapshotJSON) == 0 {
		return json.RawMessage(`{"roomId":"` + req.RoomID + `","schemaVersion":1}`), nil
	}
	return g.SnapshotJSON, nil
}

// FailingAudit returns an error on every RecordRejection (tests operational failure).
type FailingAudit struct {
	Err error
}

func (f FailingAudit) RecordRejection(audit.RejectionRecord) error {
	if f.Err != nil {
		return f.Err
	}
	return errors.New("audit sink unavailable")
}
