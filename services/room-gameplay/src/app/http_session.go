package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const defaultSessionValidateTimeout = 5 * time.Second

// HTTPSessionValidator validates forwarded SessionID/PlayerID bindings against
// Identity via POST /internal/v1/sessions/validate (stdlib net/http only).
type HTTPSessionValidator struct {
	baseURL    string
	credential string
	client     *http.Client
}

// NewHTTPSessionValidator constructs an Identity-backed SessionValidator.
func NewHTTPSessionValidator(baseURL, serviceCredential string, client *http.Client) *HTTPSessionValidator {
	if client == nil {
		client = &http.Client{Timeout: defaultSessionValidateTimeout}
	}
	return &HTTPSessionValidator{
		baseURL:    strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		credential: strings.TrimSpace(serviceCredential),
		client:     client,
	}
}

// Validate calls Identity with the exact sessionId+playerId body under the
// service credential. sessionId is a durable session identity — it must not be
// presented as an Authorization Bearer token.
func (v *HTTPSessionValidator) Validate(ctx context.Context, sessionID, playerID string) error {
	if v == nil || v.baseURL == "" {
		return errors.New("identity session validator not configured")
	}
	if sessionID == "" || playerID == "" {
		return errors.New("session and player required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	body, err := json.Marshal(map[string]string{
		"sessionId": sessionID,
		"playerId":  playerID,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, v.baseURL+"/internal/v1/sessions/validate", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if v.credential != "" {
		req.Header.Set(headerServiceCredential, v.credential)
	}
	resp, err := v.client.Do(req)
	if err != nil {
		return fmt.Errorf("identity validate: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return errors.New("invalid or stale session")
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("identity validate status %d", resp.StatusCode)
	}
	var out struct {
		PlayerID  string `json:"playerId"`
		SessionID string `json:"sessionId"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return fmt.Errorf("identity validate decode: %w", err)
	}
	if out.PlayerID != playerID {
		return errors.New("invalid or stale session")
	}
	if out.SessionID != "" && out.SessionID != sessionID {
		return errors.New("invalid or stale session")
	}
	return nil
}
