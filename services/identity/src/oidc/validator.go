package oidc

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

var (
	ErrInvalidToken     = errors.New("oidc: invalid token")
	ErrConfig           = errors.New("oidc: invalid config")
	ErrUnavailable      = errors.New("oidc: provider unavailable")
	ErrNotAllowed       = errors.New("oidc: not allowed")
	ErrAudienceMismatch = errors.New("oidc: audience mismatch")
	ErrIssuerMismatch   = errors.New("oidc: issuer mismatch")
)

// Config is the provider-neutral OIDC anti-corruption settings (ADR-0023).
type Config struct {
	IssuerURL      string   // exact iss claim / discovery base
	TokenEndpoint  string   // optional override (externally reachable for direct grant)
	Audiences      []string // allowlist; at least one required
	ClientID       string   // direct-grant client id
	AllowHTTP      bool     // local/kind only
	HTTPClient     *http.Client
	ClockSkew      time.Duration
	JWKSCacheTTL   time.Duration
	MaxJWKSEntries int
}

// Claims are the only fields extracted across the Identity boundary.
type Claims struct {
	Subject  string
	Username string
	Roles    []string
	Issuer   string
	Audience []string
	Expiry   time.Time
}

// Validator discovers JWKS and validates RS256 ID tokens fail-closed.
type Validator struct {
	cfg    Config
	client *http.Client
	mu     sync.Mutex
	jwks   map[string]*rsa.PublicKey // kid -> key
	jwksAt time.Time
	jwksURI string
	discAt  time.Time
	tokenEP string
}

func NewValidator(cfg Config) (*Validator, error) {
	cfg.IssuerURL = strings.TrimRight(strings.TrimSpace(cfg.IssuerURL), "/")
	if cfg.IssuerURL == "" {
		return nil, fmt.Errorf("%w: issuer required", ErrConfig)
	}
	if len(cfg.Audiences) == 0 {
		return nil, fmt.Errorf("%w: audience allowlist required", ErrConfig)
	}
	if !cfg.AllowHTTP && strings.HasPrefix(strings.ToLower(cfg.IssuerURL), "http://") {
		return nil, fmt.Errorf("%w: insecure HTTP issuer rejected", ErrConfig)
	}
	if cfg.ClockSkew <= 0 {
		cfg.ClockSkew = 2 * time.Minute
	}
	if cfg.JWKSCacheTTL <= 0 {
		cfg.JWKSCacheTTL = 10 * time.Minute
	}
	if cfg.MaxJWKSEntries <= 0 {
		cfg.MaxJWKSEntries = 32
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &Validator{
		cfg:     cfg,
		client:  client,
		jwks:    make(map[string]*rsa.PublicKey),
		tokenEP: strings.TrimSpace(cfg.TokenEndpoint),
	}, nil
}

// ProductionReadinessCheck fail-closes production misconfiguration (ADR-0023).
func ProductionReadinessCheck(deploymentEnv string, cfg Config, allowPasswordLogin, allowDirectGrant, allowTestProvisioning bool) error {
	env := strings.ToLower(strings.TrimSpace(deploymentEnv))
	prodLike := env == "production" || env == "staging" || env == "prod"
	if !prodLike {
		return nil
	}
	if allowPasswordLogin || allowDirectGrant || allowTestProvisioning {
		return fmt.Errorf("%w: direct grant/test provisioning forbidden in %s", ErrNotAllowed, env)
	}
	if cfg.IssuerURL == "" || len(cfg.Audiences) == 0 {
		return fmt.Errorf("%w: issuer and audience required", ErrConfig)
	}
	lower := strings.ToLower(cfg.IssuerURL)
	if strings.HasPrefix(lower, "http://") {
		return fmt.Errorf("%w: insecure HTTP issuer forbidden in %s", ErrNotAllowed, env)
	}
	if strings.Contains(lower, "localhost") || strings.Contains(lower, "127.0.0.1") ||
		strings.Contains(lower, "/realms/master") || strings.Contains(lower, "dev-issuer") ||
		strings.Contains(lower, ".local/") {
		return fmt.Errorf("%w: development issuer forbidden in %s", ErrNotAllowed, env)
	}
	return nil
}

// TokenEndpoint returns the configured or discovered token endpoint.
func (v *Validator) TokenEndpoint(ctx context.Context) (string, error) {
	if v.tokenEP != "" {
		return v.tokenEP, nil
	}
	if err := v.ensureDiscovery(ctx); err != nil {
		return "", err
	}
	if v.tokenEP == "" {
		return "", fmt.Errorf("%w: token_endpoint missing", ErrUnavailable)
	}
	return v.tokenEP, nil
}

// ValidateIDToken verifies RS256 signature and required claims; returns boundary Claims only.
func (v *Validator) ValidateIDToken(ctx context.Context, raw string, nonce string) (Claims, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return Claims{}, ErrInvalidToken
	}
	parts := strings.Split(raw, ".")
	if len(parts) != 3 {
		return Claims{}, ErrInvalidToken
	}
	headerJSON, err := b64JSON(parts[0])
	if err != nil {
		return Claims{}, ErrInvalidToken
	}
	var header struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
		Typ string `json:"typ"`
	}
	if err := json.Unmarshal(headerJSON, &header); err != nil {
		return Claims{}, ErrInvalidToken
	}
	if header.Alg != "RS256" {
		return Claims{}, fmt.Errorf("%w: alg %q not RS256", ErrInvalidToken, header.Alg)
	}
	if header.Kid == "" {
		return Claims{}, fmt.Errorf("%w: missing kid", ErrInvalidToken)
	}
	key, err := v.keyForKid(ctx, header.Kid)
	if err != nil {
		return Claims{}, err
	}
	signingInput := parts[0] + "." + parts[1]
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return Claims{}, ErrInvalidToken
	}
	sum := sha256.Sum256([]byte(signingInput))
	if err := rsa.VerifyPKCS1v15(key, crypto.SHA256, sum[:], sig); err != nil {
		return Claims{}, ErrInvalidToken
	}
	payloadJSON, err := b64JSON(parts[1])
	if err != nil {
		return Claims{}, ErrInvalidToken
	}
	var payload map[string]any
	if err := json.Unmarshal(payloadJSON, &payload); err != nil {
		return Claims{}, ErrInvalidToken
	}
	return v.extractClaims(payload, nonce)
}

func (v *Validator) extractClaims(payload map[string]any, nonce string) (Claims, error) {
	now := time.Now().UTC()
	iss, _ := payload["iss"].(string)
	if iss != v.cfg.IssuerURL {
		return Claims{}, ErrIssuerMismatch
	}
	sub, _ := payload["sub"].(string)
	if strings.TrimSpace(sub) == "" {
		return Claims{}, fmt.Errorf("%w: blank sub", ErrInvalidToken)
	}
	audOK := false
	auds := audienceStrings(payload["aud"])
	for _, s := range auds {
		for _, a := range v.cfg.Audiences {
			if s == a {
				audOK = true
				break
			}
		}
	}
	if !audOK {
		return Claims{}, ErrAudienceMismatch
	}
	if len(auds) > 1 {
		azp, _ := payload["azp"].(string)
		azp = strings.TrimSpace(azp)
		if azp == "" {
			return Claims{}, fmt.Errorf("%w: multi-audience token missing azp", ErrInvalidToken)
		}
		azpOK := false
		if v.cfg.ClientID != "" && azp == v.cfg.ClientID {
			azpOK = true
		}
		if !azpOK {
			for _, a := range v.cfg.Audiences {
				if azp == a {
					azpOK = true
					break
				}
			}
		}
		if !azpOK {
			return Claims{}, fmt.Errorf("%w: azp not accepted", ErrAudienceMismatch)
		}
	}
	exp, ok := asUnix(payload["exp"])
	if !ok || !now.Before(exp.Add(v.cfg.ClockSkew)) {
		return Claims{}, fmt.Errorf("%w: expired", ErrInvalidToken)
	}
	if nbf, ok := asUnix(payload["nbf"]); ok {
		if now.Add(v.cfg.ClockSkew).Before(nbf) {
			return Claims{}, fmt.Errorf("%w: nbf", ErrInvalidToken)
		}
	}
	if iat, ok := asUnix(payload["iat"]); ok {
		if iat.After(now.Add(v.cfg.ClockSkew + time.Hour)) {
			return Claims{}, fmt.Errorf("%w: iat unreasonable", ErrInvalidToken)
		}
	}
	if nonce != "" {
		got, _ := payload["nonce"].(string)
		if got != nonce {
			return Claims{}, fmt.Errorf("%w: nonce mismatch", ErrInvalidToken)
		}
	}
	username := canonicalUsername(payload)
	if username == "" {
		return Claims{}, fmt.Errorf("%w: missing username", ErrInvalidToken)
	}
	roles := extractRoles(payload)
	return Claims{
		Subject:  sub,
		Username: username,
		Roles:    roles,
		Issuer:   iss,
		Audience: auds,
		Expiry:   exp,
	}, nil
}

// DirectGrant exchanges username/password at the token endpoint (non-prod test only).
// Returns the id_token string only — never persists provider tokens.
func (v *Validator) DirectGrant(ctx context.Context, username, password string) (string, error) {
	ep, err := v.TokenEndpoint(ctx)
	if err != nil {
		return "", err
	}
	if !v.cfg.AllowHTTP && strings.HasPrefix(strings.ToLower(ep), "http://") {
		return "", fmt.Errorf("%w: insecure token endpoint", ErrNotAllowed)
	}
	form := url.Values{}
	form.Set("grant_type", "password")
	form.Set("client_id", v.cfg.ClientID)
	form.Set("username", username)
	form.Set("password", password)
	form.Set("scope", "openid")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ep, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	res, err := v.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if res.StatusCode >= 500 {
		return "", fmt.Errorf("%w: token endpoint status %d", ErrUnavailable, res.StatusCode)
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return "", ErrInvalidToken
	}
	var tok struct {
		IDToken string `json:"id_token"`
	}
	if err := json.Unmarshal(body, &tok); err != nil || tok.IDToken == "" {
		return "", ErrInvalidToken
	}
	return tok.IDToken, nil
}

func (v *Validator) keyForKid(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	v.mu.Lock()
	key, ok := v.jwks[kid]
	fresh := time.Since(v.jwksAt) < v.cfg.JWKSCacheTTL
	v.mu.Unlock()
	if ok && fresh {
		return key, nil
	}
	if err := v.refreshJWKS(ctx); err != nil {
		return nil, err
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	key, ok = v.jwks[kid]
	if !ok {
		// refresh-on-unknown-kid already attempted; fail closed
		return nil, fmt.Errorf("%w: unknown kid", ErrInvalidToken)
	}
	return key, nil
}

func (v *Validator) ensureDiscovery(ctx context.Context) error {
	v.mu.Lock()
	if v.jwksURI != "" && time.Since(v.discAt) < v.cfg.JWKSCacheTTL {
		v.mu.Unlock()
		return nil
	}
	v.mu.Unlock()
	discURL := v.cfg.IssuerURL + "/.well-known/openid-configuration"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, discURL, nil)
	if err != nil {
		return err
	}
	res, err := v.client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: discovery: %v", ErrUnavailable, err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: discovery status %d", ErrUnavailable, res.StatusCode)
	}
	var doc struct {
		Issuer        string `json:"issuer"`
		JWKSURI       string `json:"jwks_uri"`
		TokenEndpoint string `json:"token_endpoint"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return fmt.Errorf("%w: discovery json", ErrUnavailable)
	}
	if doc.Issuer != v.cfg.IssuerURL {
		return ErrIssuerMismatch
	}
	if doc.JWKSURI == "" {
		return fmt.Errorf("%w: jwks_uri missing", ErrUnavailable)
	}
	if !v.cfg.AllowHTTP && strings.HasPrefix(strings.ToLower(doc.JWKSURI), "http://") {
		return fmt.Errorf("%w: insecure JWKS URI", ErrNotAllowed)
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.jwksURI = doc.JWKSURI
	if v.tokenEP == "" {
		v.tokenEP = doc.TokenEndpoint
	}
	v.discAt = time.Now()
	return nil
}

func (v *Validator) refreshJWKS(ctx context.Context) error {
	if err := v.ensureDiscovery(ctx); err != nil {
		return err
	}
	v.mu.Lock()
	uri := v.jwksURI
	v.mu.Unlock()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
	if err != nil {
		return err
	}
	res, err := v.client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: jwks: %v", ErrUnavailable, err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: jwks status %d", ErrUnavailable, res.StatusCode)
	}
	var doc struct {
		Keys []jwk `json:"keys"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return fmt.Errorf("%w: jwks json", ErrUnavailable)
	}
	next := make(map[string]*rsa.PublicKey)
	for _, k := range doc.Keys {
		if k.Kty != "RSA" || k.Kid == "" {
			continue
		}
		if k.Alg != "" && k.Alg != "RS256" {
			continue
		}
		if k.Use != "" && k.Use != "sig" {
			continue
		}
		pub, err := k.publicKey()
		if err != nil {
			continue
		}
		next[k.Kid] = pub
		if len(next) >= v.cfg.MaxJWKSEntries {
			break
		}
	}
	if len(next) == 0 {
		return fmt.Errorf("%w: empty JWKS", ErrUnavailable)
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.jwks = next
	v.jwksAt = time.Now()
	return nil
}

type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	N   string `json:"n"`
	E   string `json:"e"`
	Alg string `json:"alg"`
	Use string `json:"use"`
}

func (k jwk) publicKey() (*rsa.PublicKey, error) {
	nb, err := base64.RawURLEncoding.DecodeString(k.N)
	if err != nil {
		return nil, err
	}
	eb, err := base64.RawURLEncoding.DecodeString(k.E)
	if err != nil {
		return nil, err
	}
	var eInt int
	for _, b := range eb {
		eInt = eInt<<8 + int(b)
	}
	if eInt == 0 {
		return nil, errors.New("bad exponent")
	}
	return &rsa.PublicKey{N: new(big.Int).SetBytes(nb), E: eInt}, nil
}

func b64JSON(seg string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(seg)
}

func asUnix(v any) (time.Time, bool) {
	switch t := v.(type) {
	case float64:
		return time.Unix(int64(t), 0).UTC(), true
	case json.Number:
		i, err := t.Int64()
		if err != nil {
			return time.Time{}, false
		}
		return time.Unix(i, 0).UTC(), true
	default:
		return time.Time{}, false
	}
}

func canonicalUsername(payload map[string]any) string {
	for _, key := range []string{"preferred_username", "username"} {
		if s, ok := payload[key].(string); ok && strings.TrimSpace(s) != "" {
			return strings.TrimSpace(s)
		}
	}
	return ""
}

func extractRoles(payload map[string]any) []string {
	if realm, ok := payload["realm_access"].(map[string]any); ok {
		if arr, ok := realm["roles"].([]any); ok {
			return stringSlice(arr)
		}
	}
	if arr, ok := payload["roles"].([]any); ok {
		return stringSlice(arr)
	}
	return nil
}

func stringSlice(arr []any) []string {
	out := make([]string, 0, len(arr))
	for _, v := range arr {
		if s, ok := v.(string); ok && s != "" {
			out = append(out, s)
		}
	}
	return out
}

func audienceStrings(v any) []string {
	switch aud := v.(type) {
	case string:
		return []string{aud}
	case []any:
		return stringSlice(aud)
	default:
		return nil
	}
}
