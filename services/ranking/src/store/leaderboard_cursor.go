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
	leaderboardCursorPrefix = "lb1."
	// documentedNonProdLeaderboardCursorMACKey is a capability/offline fallback only.
	// Durable API mode never relies on it: RANKING_LEADERBOARD_CURSOR_SECRET is required
	// before ready/wiring. Production/staging Encode/Decode refuse this fallback.
	documentedNonProdLeaderboardCursorMACKey = "unoarena-ranking-leaderboard-cursor-v1-nonprod"
)

// ErrInvalidLeaderboardCursor is returned for malformed or tampered opaque cursors.
var ErrInvalidLeaderboardCursor = errors.New("invalid leaderboard cursor")

// ErrLeaderboardCursorSecretRequired is returned when no MAC key is configured in a
// production-like environment (and no test inject is set).
var ErrLeaderboardCursorSecretRequired = errors.New("RANKING_LEADERBOARD_CURSOR_SECRET required")

var (
	lbCursorMACKeyMu       sync.RWMutex
	lbCursorMACKeyOverride string
)

// SetLeaderboardCursorMACKeyForTest injects a cursor HMAC key for capability/unit tests.
// The returned restore function resets the previous override.
func SetLeaderboardCursorMACKeyForTest(key string) (restore func()) {
	lbCursorMACKeyMu.Lock()
	prev := lbCursorMACKeyOverride
	lbCursorMACKeyOverride = strings.TrimSpace(key)
	lbCursorMACKeyMu.Unlock()
	return func() {
		lbCursorMACKeyMu.Lock()
		lbCursorMACKeyOverride = prev
		lbCursorMACKeyMu.Unlock()
	}
}

// LeaderboardCursorSecretConfigured reports whether an explicit cursor MAC secret is
// available via env or test inject (not the documented non-prod fallback).
func LeaderboardCursorSecretConfigured() bool {
	if strings.TrimSpace(os.Getenv("RANKING_LEADERBOARD_CURSOR_SECRET")) != "" {
		return true
	}
	lbCursorMACKeyMu.RLock()
	defer lbCursorMACKeyMu.RUnlock()
	return lbCursorMACKeyOverride != ""
}

// LeaderboardCursor is the live keyset boundary over stable (rating, playerId).
type LeaderboardCursor struct {
	Rating   int    `json:"r"`
	PlayerID string `json:"p"`
}

// EncodeLeaderboardCursor returns a strict opaque cursor. It never embeds Redis keys,
// SQL offsets, or other physical storage identifiers.
func EncodeLeaderboardCursor(c LeaderboardCursor) (string, error) {
	if strings.TrimSpace(c.PlayerID) == "" {
		return "", fmt.Errorf("%w: empty playerId", ErrInvalidLeaderboardCursor)
	}
	key, err := leaderboardCursorMACKey()
	if err != nil {
		return "", err
	}
	payload, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidLeaderboardCursor, err)
	}
	mac := leaderboardCursorMAC(payload, key)
	enc := base64.RawURLEncoding
	return leaderboardCursorPrefix + enc.EncodeToString(payload) + "." + enc.EncodeToString(mac), nil
}

// DecodeLeaderboardCursor parses and authenticates an opaque live keyset cursor.
func DecodeLeaderboardCursor(raw string) (LeaderboardCursor, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return LeaderboardCursor{}, fmt.Errorf("%w: empty", ErrInvalidLeaderboardCursor)
	}
	if strings.Contains(raw, "redis") || strings.Contains(raw, "ranking:") ||
		strings.Contains(raw, "OFFSET") || strings.Contains(raw, "offset=") {
		return LeaderboardCursor{}, fmt.Errorf("%w: physical key leakage", ErrInvalidLeaderboardCursor)
	}
	if !strings.HasPrefix(raw, leaderboardCursorPrefix) {
		return LeaderboardCursor{}, fmt.Errorf("%w: prefix", ErrInvalidLeaderboardCursor)
	}
	rest := strings.TrimPrefix(raw, leaderboardCursorPrefix)
	parts := strings.Split(rest, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return LeaderboardCursor{}, fmt.Errorf("%w: shape", ErrInvalidLeaderboardCursor)
	}
	key, err := leaderboardCursorMACKey()
	if err != nil {
		return LeaderboardCursor{}, err
	}
	enc := base64.RawURLEncoding
	payload, err := enc.DecodeString(parts[0])
	if err != nil {
		return LeaderboardCursor{}, fmt.Errorf("%w: payload", ErrInvalidLeaderboardCursor)
	}
	gotMAC, err := enc.DecodeString(parts[1])
	if err != nil {
		return LeaderboardCursor{}, fmt.Errorf("%w: mac", ErrInvalidLeaderboardCursor)
	}
	wantMAC := leaderboardCursorMAC(payload, key)
	if !hmac.Equal(gotMAC, wantMAC) {
		return LeaderboardCursor{}, fmt.Errorf("%w: tampered", ErrInvalidLeaderboardCursor)
	}
	var c LeaderboardCursor
	dec := json.NewDecoder(strings.NewReader(string(payload)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return LeaderboardCursor{}, fmt.Errorf("%w: json", ErrInvalidLeaderboardCursor)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return LeaderboardCursor{}, fmt.Errorf("%w: trailing json", ErrInvalidLeaderboardCursor)
	}
	if strings.TrimSpace(c.PlayerID) == "" {
		return LeaderboardCursor{}, fmt.Errorf("%w: empty playerId", ErrInvalidLeaderboardCursor)
	}
	return c, nil
}

func leaderboardCursorMAC(payload []byte, key string) []byte {
	mac := hmac.New(sha256.New, []byte(key))
	_, _ = mac.Write(payload)
	return mac.Sum(nil)
}

func leaderboardCursorMACKey() (string, error) {
	lbCursorMACKeyMu.RLock()
	override := lbCursorMACKeyOverride
	lbCursorMACKeyMu.RUnlock()
	if override != "" {
		return override, nil
	}
	if v := strings.TrimSpace(os.Getenv("RANKING_LEADERBOARD_CURSOR_SECRET")); v != "" {
		return v, nil
	}
	switch strings.ToLower(strings.TrimSpace(os.Getenv("DEPLOYMENT_ENV"))) {
	case "production", "staging", "prod":
		return "", ErrLeaderboardCursorSecretRequired
	default:
		return documentedNonProdLeaderboardCursorMACKey, nil
	}
}
