package store

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
	bracketCursorPrefix = "br1."
	// documentedNonProdCursorMACKey is a capability/offline fallback only.
	// Durable API mode never relies on it: TOURNAMENT_BRACKET_CURSOR_SECRET is required
	// before ready/wiring. Production/staging Encode/Decode refuse this fallback.
	documentedNonProdCursorMACKey = "unoarena-tournament-bracket-cursor-v1-nonprod"
)

// ErrInvalidBracketCursor is returned for malformed or tampered opaque cursors.
var ErrInvalidBracketCursor = errors.New("invalid bracket cursor")

// ErrBracketCursorSecretRequired is returned when no MAC key is configured in a
// production-like environment (and no test inject is set).
var ErrBracketCursorSecretRequired = errors.New("TOURNAMENT_BRACKET_CURSOR_SECRET required")

var (
	cursorMACKeyMu       sync.RWMutex
	cursorMACKeyOverride string
)

// SetBracketCursorMACKeyForTest injects a cursor HMAC key for capability/unit tests.
// The returned restore function resets the previous override.
func SetBracketCursorMACKeyForTest(key string) (restore func()) {
	cursorMACKeyMu.Lock()
	prev := cursorMACKeyOverride
	cursorMACKeyOverride = strings.TrimSpace(key)
	cursorMACKeyMu.Unlock()
	return func() {
		cursorMACKeyMu.Lock()
		cursorMACKeyOverride = prev
		cursorMACKeyMu.Unlock()
	}
}

// BracketCursorSecretConfigured reports whether an explicit cursor MAC secret is
// available via env or test inject (not the documented non-prod fallback).
func BracketCursorSecretConfigured() bool {
	if strings.TrimSpace(os.Getenv("TOURNAMENT_BRACKET_CURSOR_SECRET")) != "" {
		return true
	}
	cursorMACKeyMu.RLock()
	defer cursorMACKeyMu.RUnlock()
	return cursorMACKeyOverride != ""
}

// BracketCursor is the live keyset boundary over stable (roundNumber, slotIndex).
type BracketCursor struct {
	RoundNumber int `json:"r"`
	SlotIndex   int `json:"i"`
}

// EncodeBracketCursor returns a strict opaque cursor. It never embeds Redis keys,
// SQL offsets, or other physical storage identifiers.
func EncodeBracketCursor(c BracketCursor) (string, error) {
	if c.RoundNumber < 1 || c.SlotIndex < 0 {
		return "", fmt.Errorf("%w: out of range", ErrInvalidBracketCursor)
	}
	key, err := cursorMACKey()
	if err != nil {
		return "", err
	}
	payload, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidBracketCursor, err)
	}
	mac := cursorMAC(payload, key)
	enc := base64.RawURLEncoding
	return bracketCursorPrefix + enc.EncodeToString(payload) + "." + enc.EncodeToString(mac), nil
}

// DecodeBracketCursor parses and authenticates an opaque live keyset cursor.
func DecodeBracketCursor(raw string) (BracketCursor, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return BracketCursor{}, fmt.Errorf("%w: empty", ErrInvalidBracketCursor)
	}
	if strings.Contains(raw, "redis") || strings.Contains(raw, "bracket:") ||
		strings.Contains(raw, "OFFSET") || strings.Contains(raw, "offset=") {
		return BracketCursor{}, fmt.Errorf("%w: physical key leakage", ErrInvalidBracketCursor)
	}
	if !strings.HasPrefix(raw, bracketCursorPrefix) {
		return BracketCursor{}, fmt.Errorf("%w: prefix", ErrInvalidBracketCursor)
	}
	rest := strings.TrimPrefix(raw, bracketCursorPrefix)
	parts := strings.Split(rest, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return BracketCursor{}, fmt.Errorf("%w: shape", ErrInvalidBracketCursor)
	}
	key, err := cursorMACKey()
	if err != nil {
		return BracketCursor{}, err
	}
	enc := base64.RawURLEncoding
	payload, err := enc.DecodeString(parts[0])
	if err != nil {
		return BracketCursor{}, fmt.Errorf("%w: payload", ErrInvalidBracketCursor)
	}
	gotMAC, err := enc.DecodeString(parts[1])
	if err != nil {
		return BracketCursor{}, fmt.Errorf("%w: mac", ErrInvalidBracketCursor)
	}
	wantMAC := cursorMAC(payload, key)
	if !hmac.Equal(gotMAC, wantMAC) {
		return BracketCursor{}, fmt.Errorf("%w: tampered", ErrInvalidBracketCursor)
	}
	var c BracketCursor
	dec := json.NewDecoder(strings.NewReader(string(payload)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return BracketCursor{}, fmt.Errorf("%w: json", ErrInvalidBracketCursor)
	}
	// Reject trailing JSON (enforce EOF after the cursor object).
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return BracketCursor{}, fmt.Errorf("%w: trailing json", ErrInvalidBracketCursor)
	}
	if c.RoundNumber < 1 || c.SlotIndex < 0 {
		return BracketCursor{}, fmt.Errorf("%w: out of range", ErrInvalidBracketCursor)
	}
	return c, nil
}

func cursorMAC(payload []byte, key string) []byte {
	mac := hmac.New(sha256.New, []byte(key))
	_, _ = mac.Write(payload)
	return mac.Sum(nil)
}

func cursorMACKey() (string, error) {
	cursorMACKeyMu.RLock()
	override := cursorMACKeyOverride
	cursorMACKeyMu.RUnlock()
	if override != "" {
		return override, nil
	}
	if v := strings.TrimSpace(os.Getenv("TOURNAMENT_BRACKET_CURSOR_SECRET")); v != "" {
		return v, nil
	}
	// Documented non-prod fallback for capability/offline tests only.
	// Production, staging, and prod refuse the forgeable default.
	switch strings.ToLower(strings.TrimSpace(os.Getenv("DEPLOYMENT_ENV"))) {
	case "production", "staging", "prod":
		return "", ErrBracketCursorSecretRequired
	default:
		return documentedNonProdCursorMACKey, nil
	}
}
