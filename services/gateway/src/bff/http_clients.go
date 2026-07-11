package bff

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"unoarena/shared/correlation"
	"unoarena/shared/envelope"
)

const (
	headerServiceCredential = "X-Service-Credential"
	headerRoomInvite        = "X-Room-Invite"
	defaultHTTPTimeout      = 10 * time.Second
)

// HTTPClientConfig configures stdlib HTTP backend clients.
type HTTPClientConfig struct {
	BaseURL           string
	ServiceCredential string
	HTTPClient        *http.Client
}

func (c HTTPClientConfig) client() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return &http.Client{Timeout: defaultHTTPTimeout}
}

func (c HTTPClientConfig) requireBase() error {
	if strings.TrimSpace(c.BaseURL) == "" {
		return fmt.Errorf("backend base URL not configured")
	}
	return nil
}

// HTTPIdentityClient talks to Identity over stdlib HTTP.
type HTTPIdentityClient struct {
	cfg HTTPClientConfig
}

// NewHTTPIdentityClient constructs an Identity HTTP client.
func NewHTTPIdentityClient(cfg HTTPClientConfig) *HTTPIdentityClient {
	return &HTTPIdentityClient{cfg: cfg}
}

func (c *HTTPIdentityClient) Register(ctx context.Context, username, password string, corr correlation.Headers) (RegisterResult, error) {
	var out RegisterResult
	err := c.doJSON(ctx, http.MethodPost, "/v1/auth/register", "", corr, map[string]string{
		"username": username,
		"password": password,
	}, &out)
	return out, err
}

func (c *HTTPIdentityClient) Login(ctx context.Context, username, password string, corr correlation.Headers) (LoginResult, error) {
	var out LoginResult
	err := c.doJSON(ctx, http.MethodPost, "/v1/auth/login", "", corr, map[string]string{
		"username": username,
		"password": password,
	}, &out)
	if err != nil {
		if isUnauthorized(err) {
			return LoginResult{}, ErrInvalidCredentials
		}
		return LoginResult{}, err
	}
	return out, nil
}

func (c *HTTPIdentityClient) Whoami(ctx context.Context, token string, corr correlation.Headers) (Principal, error) {
	return c.ValidateSession(ctx, token, corr)
}

func (c *HTTPIdentityClient) ValidateSession(ctx context.Context, token string, corr correlation.Headers) (Principal, error) {
	var out struct {
		PlayerID  string   `json:"playerId"`
		SessionID string   `json:"sessionId"`
		Username  string   `json:"username"`
		Roles     []string `json:"roles"`
		Scopes    []string `json:"scopes"`
		Operator  bool     `json:"operator"`
	}
	var err error
	if strings.TrimSpace(c.cfg.ServiceCredential) != "" {
		err = c.doJSON(ctx, http.MethodPost, "/internal/v1/sessions/validate", token, corr, nil, &out)
	} else {
		err = c.doJSON(ctx, http.MethodGet, "/v1/auth/whoami", token, corr, nil, &out)
	}
	if err != nil {
		if isUnauthorized(err) {
			return Principal{}, ErrUnauthorized
		}
		return Principal{}, err
	}
	if out.PlayerID == "" || out.SessionID == "" {
		return Principal{}, ErrUnauthorized
	}
	return Principal{
		PlayerID:      out.PlayerID,
		SessionID:     out.SessionID,
		Username:      out.Username,
		Roles:         out.Roles,
		Scopes:        out.Scopes,
		OperatorScope: operatorScopeFromIdentity(out.Roles, out.Scopes, out.Operator),
	}, nil
}

func operatorScopeFromIdentity(roles, scopes []string, operator bool) bool {
	if operator {
		return true
	}
	for _, r := range roles {
		if strings.EqualFold(strings.TrimSpace(r), "operator") {
			return true
		}
	}
	for _, s := range scopes {
		if strings.EqualFold(strings.TrimSpace(s), "operator") {
			return true
		}
	}
	return false
}

func (c *HTTPIdentityClient) doJSON(ctx context.Context, method, path, bearer string, corr correlation.Headers, body any, dest any) error {
	if err := c.cfg.requireBase(); err != nil {
		return err
	}
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.cfg.BaseURL, "/")+path, rdr)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	if c.cfg.ServiceCredential != "" && strings.Contains(path, "/internal/") {
		req.Header.Set(headerServiceCredential, c.cfg.ServiceCredential)
	}
	corr.Apply(req.Header)
	resp, err := c.cfg.client().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, MaxRequestBodyBytes))
	if resp.StatusCode >= 400 {
		return &httpStatusError{status: resp.StatusCode, body: string(raw)}
	}
	if dest == nil {
		return nil
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil
	}
	return json.Unmarshal(raw, dest)
}

// HTTPRoomClient dispatches room commands over HTTP.
type HTTPRoomClient struct {
	cfg HTTPClientConfig
}

// NewHTTPRoomClient constructs a Room Gameplay HTTP client.
func NewHTTPRoomClient(cfg HTTPClientConfig) *HTTPRoomClient {
	return &HTTPRoomClient{cfg: cfg}
}

func (c *HTTPRoomClient) SubmitCommand(ctx context.Context, req CommandDispatch) (envelope.Result, error) {
	return submitCommandHTTP(ctx, c.cfg, "/internal/v1/commands", req)
}

func (c *HTTPRoomClient) PlayerSnapshot(ctx context.Context, roomID, playerID string, corr correlation.Headers) (json.RawMessage, error) {
	if err := c.cfg.requireBase(); err != nil {
		return nil, err
	}
	roomID = strings.TrimSpace(roomID)
	playerID = strings.TrimSpace(playerID)
	if roomID == "" || playerID == "" {
		return nil, fmt.Errorf("roomId and playerId required")
	}
	url := strings.TrimRight(c.cfg.BaseURL, "/") + "/v1/rooms/" + roomID + "/snapshot?playerId=" + urlQueryEscape(playerID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if c.cfg.ServiceCredential != "" {
		req.Header.Set(headerServiceCredential, c.cfg.ServiceCredential)
	}
	req.Header.Set("X-Player-Id", playerID)
	corr.Apply(req.Header)
	resp, err := c.cfg.client().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, MaxRequestBodyBytes))
	if resp.StatusCode >= 400 {
		return nil, &httpStatusError{status: resp.StatusCode, body: string(raw)}
	}
	return json.RawMessage(raw), nil
}

func urlQueryEscape(s string) string {
	return url.QueryEscape(s)
}

// HTTPTournamentClient dispatches tournament commands over HTTP.
type HTTPTournamentClient struct {
	cfg HTTPClientConfig
}

// NewHTTPTournamentClient constructs a Tournament Orchestration HTTP client.
func NewHTTPTournamentClient(cfg HTTPClientConfig) *HTTPTournamentClient {
	return &HTTPTournamentClient{cfg: cfg}
}

func (c *HTTPTournamentClient) SubmitCommand(ctx context.Context, req CommandDispatch) (envelope.Result, error) {
	return submitCommandHTTP(ctx, c.cfg, "/internal/v1/commands", req)
}

func submitCommandHTTP(ctx context.Context, cfg HTTPClientConfig, path string, req CommandDispatch) (envelope.Result, error) {
	if err := cfg.requireBase(); err != nil {
		return envelope.Result{}, err
	}
	payload := map[string]any{
		"commandId":     req.Command.CommandID,
		"type":          req.Command.Type,
		"schemaVersion": req.Command.SchemaVersion,
		"payload":       json.RawMessage(req.Command.Payload),
		"playerId":      req.Principal.PlayerID,
		"sessionId":     req.Principal.SessionID,
		"roomId":        req.RoomID,
	}
	if req.Command.ExpectedSequenceNumber != nil {
		payload["expectedSequenceNumber"] = *req.Command.ExpectedSequenceNumber
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return envelope.Result{}, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(cfg.BaseURL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return envelope.Result{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if cfg.ServiceCredential != "" {
		httpReq.Header.Set(headerServiceCredential, cfg.ServiceCredential)
	}
	req.Correlation.Apply(httpReq.Header)
	resp, err := cfg.client().Do(httpReq)
	if err != nil {
		return envelope.Result{}, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, MaxRequestBodyBytes))
	if resp.StatusCode >= 400 {
		return envelope.Result{}, &httpStatusError{status: resp.StatusCode, body: string(raw)}
	}
	var result envelope.Result
	if err := json.Unmarshal(raw, &result); err != nil {
		return envelope.Result{}, fmt.Errorf("decode upstream result: %w", err)
	}
	return result, nil
}

// HTTPReadModelClient proxies ranking/analytics reads.
type HTTPReadModelClient struct {
	rankingURL   string
	analyticsURL string
	httpClient   *http.Client
}

// NewHTTPReadModelClient constructs read-model HTTP clients.
func NewHTTPReadModelClient(rankingURL, analyticsURL string, httpClient *http.Client) *HTTPReadModelClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultHTTPTimeout}
	}
	return &HTTPReadModelClient{rankingURL: rankingURL, analyticsURL: analyticsURL, httpClient: httpClient}
}

func (c *HTTPReadModelClient) Leaderboard(ctx context.Context, corr correlation.Headers) (json.RawMessage, error) {
	if strings.TrimSpace(c.rankingURL) == "" {
		return nil, fmt.Errorf("ranking URL not configured")
	}
	return c.get(ctx, strings.TrimRight(c.rankingURL, "/")+"/v1/rankings/leaderboards", corr)
}

func (c *HTTPReadModelClient) PublicAnalytics(ctx context.Context, corr correlation.Headers) (json.RawMessage, error) {
	if strings.TrimSpace(c.analyticsURL) == "" {
		return nil, fmt.Errorf("analytics URL not configured")
	}
	return c.get(ctx, strings.TrimRight(c.analyticsURL, "/")+"/v1/analytics/public", corr)
}

func (c *HTTPReadModelClient) get(ctx context.Context, url string, corr correlation.Headers) (json.RawMessage, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	corr.Apply(req.Header)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, MaxRequestBodyBytes))
	if resp.StatusCode >= 400 {
		return nil, &httpStatusError{status: resp.StatusCode, body: string(raw)}
	}
	return json.RawMessage(raw), nil
}

// HTTPSpectatorGate calls Spectator View admission over HTTP.
type HTTPSpectatorGate struct {
	cfg HTTPClientConfig
}

// NewHTTPSpectatorGate constructs a spectator admission HTTP client.
func NewHTTPSpectatorGate(cfg HTTPClientConfig) *HTTPSpectatorGate {
	return &HTTPSpectatorGate{cfg: cfg}
}

func (g *HTTPSpectatorGate) Admit(ctx context.Context, req SpectatorAdmitRequest) (bool, string, error) {
	if err := g.cfg.requireBase(); err != nil {
		return false, "", err
	}
	body := map[string]any{
		"roomId": req.RoomID,
	}
	if req.Principal != nil {
		body["playerId"] = req.Principal.PlayerID
		body["sessionId"] = req.Principal.SessionID
		if req.Principal.OperatorScope {
			body["operator"] = true
		}
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return false, "", err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(g.cfg.BaseURL, "/")+"/internal/v1/rooms/"+req.RoomID+"/spectator-admission",
		bytes.NewReader(raw))
	if err != nil {
		return false, "", err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if g.cfg.ServiceCredential != "" {
		httpReq.Header.Set(headerServiceCredential, g.cfg.ServiceCredential)
	}
	if req.Token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+req.Token)
	}
	if req.InviteCapability != "" {
		httpReq.Header.Set(headerRoomInvite, req.InviteCapability)
	}
	req.Correlation.Apply(httpReq.Header)
	resp, err := g.cfg.client().Do(httpReq)
	if err != nil {
		return false, "", err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, MaxRequestBodyBytes))
	if resp.StatusCode == http.StatusForbidden {
		var errBody struct {
			Reason  string `json:"reason"`
			Message string `json:"message"`
		}
		_ = json.Unmarshal(respBody, &errBody)
		reason := errBody.Reason
		if reason == "" {
			reason = errBody.Message
		}
		if reason == "" {
			reason = "spectator_admission_denied"
		}
		return false, reason, nil
	}
	if resp.StatusCode >= 400 {
		return false, "", &httpStatusError{status: resp.StatusCode, body: string(respBody)}
	}
	var out struct {
		Allowed bool   `json:"allowed"`
		Reason  string `json:"reason"`
	}
	if err := json.Unmarshal(respBody, &out); err != nil {
		return false, "", err
	}
	if !out.Allowed {
		reason := out.Reason
		if reason == "" {
			reason = "spectator_admission_denied"
		}
		return false, reason, nil
	}
	return true, "", nil
}

func (g *HTTPSpectatorGate) Snapshot(ctx context.Context, req SpectatorAdmitRequest) (json.RawMessage, error) {
	if err := g.cfg.requireBase(); err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet,
		strings.TrimRight(g.cfg.BaseURL, "/")+"/v1/spectator/rooms/"+req.RoomID+"/snapshot", nil)
	if err != nil {
		return nil, err
	}
	if g.cfg.ServiceCredential != "" {
		httpReq.Header.Set(headerServiceCredential, g.cfg.ServiceCredential)
	}
	if req.Token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+req.Token)
	}
	if req.Principal != nil {
		httpReq.Header.Set("X-Player-Id", req.Principal.PlayerID)
		httpReq.Header.Set("X-Session-Id", req.Principal.SessionID)
		if req.Principal.OperatorScope {
			httpReq.Header.Set("X-Operator-Scope", "1")
		}
	}
	if req.InviteCapability != "" {
		httpReq.Header.Set(headerRoomInvite, req.InviteCapability)
	}
	req.Correlation.Apply(httpReq.Header)
	resp, err := g.cfg.client().Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, MaxRequestBodyBytes))
	if resp.StatusCode == http.StatusForbidden {
		return nil, fmt.Errorf("%w: spectator_admission_denied", ErrSpectatorDenied)
	}
	if resp.StatusCode >= 400 {
		return nil, &httpStatusError{status: resp.StatusCode, body: string(respBody)}
	}
	return json.RawMessage(respBody), nil
}

type httpStatusError struct {
	status int
	body   string
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("upstream status %d: %s", e.status, e.body)
}

func isUnauthorized(err error) bool {
	if err == nil {
		return false
	}
	he, ok := err.(*httpStatusError)
	if !ok {
		return false
	}
	return he.status == http.StatusUnauthorized
}

// Closed clients fail every request when a backend URL is missing.

// ClosedIdentity rejects all Identity calls.
type ClosedIdentity struct{}

func (ClosedIdentity) Register(context.Context, string, string, correlation.Headers) (RegisterResult, error) {
	return RegisterResult{}, fmt.Errorf("identity client not configured")
}
func (ClosedIdentity) Login(context.Context, string, string, correlation.Headers) (LoginResult, error) {
	return LoginResult{}, fmt.Errorf("identity client not configured")
}
func (ClosedIdentity) Whoami(context.Context, string, correlation.Headers) (Principal, error) {
	return Principal{}, fmt.Errorf("identity client not configured")
}
func (ClosedIdentity) ValidateSession(context.Context, string, correlation.Headers) (Principal, error) {
	return Principal{}, fmt.Errorf("identity client not configured")
}

// ClosedRoom rejects all room dispatches.
type ClosedRoom struct{}

func (ClosedRoom) SubmitCommand(context.Context, CommandDispatch) (envelope.Result, error) {
	return envelope.Result{}, fmt.Errorf("room client not configured")
}
func (ClosedRoom) PlayerSnapshot(context.Context, string, string, correlation.Headers) (json.RawMessage, error) {
	return nil, fmt.Errorf("room client not configured")
}

// ClosedTournament rejects all tournament dispatches.
type ClosedTournament struct{}

func (ClosedTournament) SubmitCommand(context.Context, CommandDispatch) (envelope.Result, error) {
	return envelope.Result{}, fmt.Errorf("tournament client not configured")
}

// ClosedReads rejects all read-model proxies.
type ClosedReads struct{}

func (ClosedReads) Leaderboard(context.Context, correlation.Headers) (json.RawMessage, error) {
	return nil, fmt.Errorf("read model client not configured")
}
func (ClosedReads) PublicAnalytics(context.Context, correlation.Headers) (json.RawMessage, error) {
	return nil, fmt.Errorf("read model client not configured")
}

// ClosedSpectator rejects all spectator admissions.
type ClosedSpectator struct{}

func (ClosedSpectator) Admit(context.Context, SpectatorAdmitRequest) (bool, string, error) {
	return false, "", fmt.Errorf("spectator gate not configured")
}
func (ClosedSpectator) Snapshot(context.Context, SpectatorAdmitRequest) (json.RawMessage, error) {
	return nil, fmt.Errorf("spectator gate not configured")
}
