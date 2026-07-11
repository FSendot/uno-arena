package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"unoarena/services/identity/domain"
)

// HTTPInvalidationTransport POSTs SessionInvalidated to Gateway control routes
// using stdlib net/http only (IDENTITY_INVALIDATION_URL base/control + credential).
type HTTPInvalidationTransport struct {
	baseURL    string
	credential string
	client     *http.Client
}

func NewHTTPInvalidationTransport(baseOrControlURL, credential string, client *http.Client) *HTTPInvalidationTransport {
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	return &HTTPInvalidationTransport{
		baseURL:    normalizeGatewayBase(baseOrControlURL),
		credential: credential,
		client:     client,
	}
}

func normalizeGatewayBase(raw string) string {
	s := strings.TrimSpace(raw)
	s = strings.TrimRight(s, "/")
	s = strings.TrimSuffix(s, "/internal/v1/control")
	return strings.TrimRight(s, "/")
}

func (t *HTTPInvalidationTransport) deliverURL(sessionID domain.SessionID) string {
	return t.baseURL + "/internal/v1/control/sessions/" + url.PathEscape(string(sessionID)) + "/invalidated"
}

func (t *HTTPInvalidationTransport) Deliver(evt domain.SessionInvalidatedEvent) error {
	// Gateway versioned control contract: schemaVersion=1, nonempty eventId
	// (idempotency), eventType, sessionId matching path, reason when available.
	// eventId is the durable outbox id — never regenerated on retry.
	bodyMap := map[string]any{
		"schemaVersion": 1,
		"eventId":       evt.EventID,
		"eventType":     "SessionInvalidated",
		"sessionId":     evt.SessionID.String(),
		"playerId":      evt.PlayerID.String(),
		"occurredAt":    evt.OccurredAt.UTC().Format(time.RFC3339Nano),
	}
	if reason := strings.TrimSpace(evt.Reason); reason != "" {
		bodyMap["reason"] = reason
	}
	body, err := json.Marshal(bodyMap)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, t.deliverURL(evt.SessionID), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(internalCredentialHeader, t.credential)
	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("invalidation transport status %d", resp.StatusCode)
	}
	return nil
}
