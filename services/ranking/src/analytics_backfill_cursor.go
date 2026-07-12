package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

const (
	analyticsBackfillCursorPrefix = "rab1."
	// documentedNonProdAnalyticsBackfillCursorMACKey is capability/offline fallback only.
	// Durable API mode requires RANKING_ANALYTICS_BACKFILL_CURSOR_SECRET before ready/wiring.
	documentedNonProdAnalyticsBackfillCursorMACKey = "unoarena-ranking-analytics-backfill-cursor-v1-nonprod"
)

// ErrInvalidAnalyticsBackfillCursor is returned for malformed or tampered opaque cursors.
var ErrInvalidAnalyticsBackfillCursor = errors.New("invalid analytics backfill cursor")

// ErrAnalyticsBackfillCursorSecretRequired is returned when no MAC key is configured in a
// production-like environment (and no test inject is set).
var ErrAnalyticsBackfillCursorSecretRequired = errors.New("RANKING_ANALYTICS_BACKFILL_CURSOR_SECRET required")

var (
	abCursorMACKeyMu       sync.RWMutex
	abCursorMACKeyOverride string
)

// SetAnalyticsBackfillCursorMACKeyForTest injects a cursor HMAC key for capability/unit tests.
func SetAnalyticsBackfillCursorMACKeyForTest(key string) (restore func()) {
	abCursorMACKeyMu.Lock()
	prev := abCursorMACKeyOverride
	abCursorMACKeyOverride = strings.TrimSpace(key)
	abCursorMACKeyMu.Unlock()
	return func() {
		abCursorMACKeyMu.Lock()
		abCursorMACKeyOverride = prev
		abCursorMACKeyMu.Unlock()
	}
}

// AnalyticsBackfillCursorSecretConfigured reports whether an explicit cursor MAC secret is
// available via env or test inject (not the documented non-prod fallback).
func AnalyticsBackfillCursorSecretConfigured() bool {
	if strings.TrimSpace(os.Getenv("RANKING_ANALYTICS_BACKFILL_CURSOR_SECRET")) != "" {
		return true
	}
	abCursorMACKeyMu.RLock()
	defer abCursorMACKeyMu.RUnlock()
	return abCursorMACKeyOverride != ""
}

// AnalyticsBackfillCursor is the keyset boundary over immutable outbox_id, bound to the query.
type AnalyticsBackfillCursor struct {
	OutboxID       int64  `json:"o"`
	SourceTopic    string `json:"t"`
	RecoveryJobID  string `json:"j"`
	FromCheckpoint string `json:"fc,omitempty"`
	ToCheckpoint   string `json:"tc,omitempty"`
	FromOccurredAt string `json:"fo,omitempty"`
	ToOccurredAt   string `json:"to,omitempty"`
}

// EncodeAnalyticsBackfillCursor returns a strict opaque cursor. It never embeds SQL offsets.
func EncodeAnalyticsBackfillCursor(c AnalyticsBackfillCursor) (string, error) {
	if c.OutboxID < 1 {
		return "", fmt.Errorf("%w: outbox_id", ErrInvalidAnalyticsBackfillCursor)
	}
	if strings.TrimSpace(c.SourceTopic) == "" || strings.TrimSpace(c.RecoveryJobID) == "" {
		return "", fmt.Errorf("%w: binding", ErrInvalidAnalyticsBackfillCursor)
	}
	key, err := analyticsBackfillCursorMACKey()
	if err != nil {
		return "", err
	}
	payload, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidAnalyticsBackfillCursor, err)
	}
	mac := analyticsBackfillCursorMAC(payload, key)
	enc := base64.RawURLEncoding
	return analyticsBackfillCursorPrefix + enc.EncodeToString(payload) + "." + enc.EncodeToString(mac), nil
}

// DecodeAnalyticsBackfillCursor parses and authenticates an opaque keyset cursor.
func DecodeAnalyticsBackfillCursor(raw string) (AnalyticsBackfillCursor, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return AnalyticsBackfillCursor{}, fmt.Errorf("%w: empty", ErrInvalidAnalyticsBackfillCursor)
	}
	if strings.Contains(raw, "OFFSET") || strings.Contains(raw, "offset=") ||
		strings.Contains(raw, "outbox_id") || strings.Contains(raw, "outbox_events") {
		return AnalyticsBackfillCursor{}, fmt.Errorf("%w: physical key leakage", ErrInvalidAnalyticsBackfillCursor)
	}
	if !strings.HasPrefix(raw, analyticsBackfillCursorPrefix) {
		return AnalyticsBackfillCursor{}, fmt.Errorf("%w: prefix", ErrInvalidAnalyticsBackfillCursor)
	}
	rest := strings.TrimPrefix(raw, analyticsBackfillCursorPrefix)
	parts := strings.Split(rest, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return AnalyticsBackfillCursor{}, fmt.Errorf("%w: shape", ErrInvalidAnalyticsBackfillCursor)
	}
	key, err := analyticsBackfillCursorMACKey()
	if err != nil {
		return AnalyticsBackfillCursor{}, err
	}
	enc := base64.RawURLEncoding
	payload, err := enc.DecodeString(parts[0])
	if err != nil {
		return AnalyticsBackfillCursor{}, fmt.Errorf("%w: payload", ErrInvalidAnalyticsBackfillCursor)
	}
	gotMAC, err := enc.DecodeString(parts[1])
	if err != nil {
		return AnalyticsBackfillCursor{}, fmt.Errorf("%w: mac", ErrInvalidAnalyticsBackfillCursor)
	}
	wantMAC := analyticsBackfillCursorMAC(payload, key)
	if !hmac.Equal(gotMAC, wantMAC) {
		return AnalyticsBackfillCursor{}, fmt.Errorf("%w: tampered", ErrInvalidAnalyticsBackfillCursor)
	}
	var c AnalyticsBackfillCursor
	dec := json.NewDecoder(strings.NewReader(string(payload)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return AnalyticsBackfillCursor{}, fmt.Errorf("%w: json", ErrInvalidAnalyticsBackfillCursor)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return AnalyticsBackfillCursor{}, fmt.Errorf("%w: trailing json", ErrInvalidAnalyticsBackfillCursor)
	}
	if c.OutboxID < 1 || strings.TrimSpace(c.SourceTopic) == "" || strings.TrimSpace(c.RecoveryJobID) == "" {
		return AnalyticsBackfillCursor{}, fmt.Errorf("%w: binding", ErrInvalidAnalyticsBackfillCursor)
	}
	return c, nil
}

func analyticsBackfillCursorMAC(payload []byte, key string) []byte {
	mac := hmac.New(sha256.New, []byte(key))
	_, _ = mac.Write(payload)
	return mac.Sum(nil)
}

func analyticsBackfillCursorMACKey() (string, error) {
	abCursorMACKeyMu.RLock()
	override := abCursorMACKeyOverride
	abCursorMACKeyMu.RUnlock()
	if override != "" {
		return override, nil
	}
	if v := strings.TrimSpace(os.Getenv("RANKING_ANALYTICS_BACKFILL_CURSOR_SECRET")); v != "" {
		return v, nil
	}
	switch strings.ToLower(strings.TrimSpace(os.Getenv("DEPLOYMENT_ENV"))) {
	case "production", "staging", "prod":
		return "", ErrAnalyticsBackfillCursorSecretRequired
	default:
		return documentedNonProdAnalyticsBackfillCursorMACKey, nil
	}
}
