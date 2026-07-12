package app

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
	publicListCursorPrefix  = "pl1."
	publicListCursorVersion = 1
	// documentedNonProdPublicListCursorMACKey is a capability/offline fallback only.
	// Durable API mode never relies on it: ROOM_PUBLIC_LIST_CURSOR_SECRET is required
	// before ready/wiring. Production/staging Encode/Decode refuse this fallback.
	documentedNonProdPublicListCursorMACKey = "unoarena-room-public-list-cursor-v1-nonprod"
)

// ErrInvalidPublicListCursor is returned for malformed or tampered opaque cursors.
var ErrInvalidPublicListCursor = errors.New("invalid public list cursor")

// ErrPublicListCursorSecretRequired is returned when no MAC key is configured in a
// production-like environment (and no test inject is set).
var ErrPublicListCursorSecretRequired = errors.New("ROOM_PUBLIC_LIST_CURSOR_SECRET required")

var (
	publicListCursorMACKeyMu       sync.RWMutex
	publicListCursorMACKeyOverride string
)

// SetPublicListCursorMACKeyForTest injects a cursor HMAC key for capability/unit tests.
// The returned restore function resets the previous override.
func SetPublicListCursorMACKeyForTest(key string) (restore func()) {
	publicListCursorMACKeyMu.Lock()
	prev := publicListCursorMACKeyOverride
	publicListCursorMACKeyOverride = strings.TrimSpace(key)
	publicListCursorMACKeyMu.Unlock()
	return func() {
		publicListCursorMACKeyMu.Lock()
		publicListCursorMACKeyOverride = prev
		publicListCursorMACKeyMu.Unlock()
	}
}

// PublicListCursorSecretConfigured reports whether an explicit cursor MAC secret is
// available via env or test inject (not the documented non-prod fallback).
func PublicListCursorSecretConfigured() bool {
	if strings.TrimSpace(os.Getenv("ROOM_PUBLIC_LIST_CURSOR_SECRET")) != "" {
		return true
	}
	publicListCursorMACKeyMu.RLock()
	defer publicListCursorMACKeyMu.RUnlock()
	return publicListCursorMACKeyOverride != ""
}

// PublicListCursor is the live keyset boundary over stable room_id, bound to status filter.
type PublicListCursor struct {
	V      int    `json:"v"`
	Status string `json:"s"`
	RoomID string `json:"r"`
}

// EncodePublicListCursor returns a strict opaque cursor. It never embeds SQL offsets
// or other physical storage identifiers.
func EncodePublicListCursor(c PublicListCursor) (string, error) {
	if c.V == 0 {
		c.V = publicListCursorVersion
	}
	if c.V != publicListCursorVersion {
		return "", fmt.Errorf("%w: version", ErrInvalidPublicListCursor)
	}
	if strings.TrimSpace(c.Status) == "" || strings.TrimSpace(c.RoomID) == "" {
		return "", fmt.Errorf("%w: empty status/roomId", ErrInvalidPublicListCursor)
	}
	key, err := publicListCursorMACKey()
	if err != nil {
		return "", err
	}
	payload, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidPublicListCursor, err)
	}
	mac := publicListCursorMAC(payload, key)
	enc := base64.RawURLEncoding
	return publicListCursorPrefix + enc.EncodeToString(payload) + "." + enc.EncodeToString(mac), nil
}

// DecodePublicListCursor parses and authenticates an opaque public-list keyset cursor.
func DecodePublicListCursor(raw string) (PublicListCursor, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return PublicListCursor{}, fmt.Errorf("%w: empty", ErrInvalidPublicListCursor)
	}
	if strings.Contains(raw, "OFFSET") || strings.Contains(raw, "offset=") ||
		strings.Contains(raw, "rooms:") || strings.Contains(raw, "redis") {
		return PublicListCursor{}, fmt.Errorf("%w: physical key leakage", ErrInvalidPublicListCursor)
	}
	if !strings.HasPrefix(raw, publicListCursorPrefix) {
		return PublicListCursor{}, fmt.Errorf("%w: prefix", ErrInvalidPublicListCursor)
	}
	rest := strings.TrimPrefix(raw, publicListCursorPrefix)
	parts := strings.Split(rest, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return PublicListCursor{}, fmt.Errorf("%w: shape", ErrInvalidPublicListCursor)
	}
	key, err := publicListCursorMACKey()
	if err != nil {
		return PublicListCursor{}, err
	}
	enc := base64.RawURLEncoding
	payload, err := enc.DecodeString(parts[0])
	if err != nil {
		return PublicListCursor{}, fmt.Errorf("%w: payload", ErrInvalidPublicListCursor)
	}
	gotMAC, err := enc.DecodeString(parts[1])
	if err != nil {
		return PublicListCursor{}, fmt.Errorf("%w: mac", ErrInvalidPublicListCursor)
	}
	wantMAC := publicListCursorMAC(payload, key)
	if !hmac.Equal(gotMAC, wantMAC) {
		return PublicListCursor{}, fmt.Errorf("%w: tampered", ErrInvalidPublicListCursor)
	}
	var c PublicListCursor
	dec := json.NewDecoder(strings.NewReader(string(payload)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return PublicListCursor{}, fmt.Errorf("%w: json", ErrInvalidPublicListCursor)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return PublicListCursor{}, fmt.Errorf("%w: trailing json", ErrInvalidPublicListCursor)
	}
	if c.V != publicListCursorVersion {
		return PublicListCursor{}, fmt.Errorf("%w: version", ErrInvalidPublicListCursor)
	}
	if strings.TrimSpace(c.Status) == "" || strings.TrimSpace(c.RoomID) == "" {
		return PublicListCursor{}, fmt.Errorf("%w: empty status/roomId", ErrInvalidPublicListCursor)
	}
	return c, nil
}

func publicListCursorMAC(payload []byte, key string) []byte {
	mac := hmac.New(sha256.New, []byte(key))
	_, _ = mac.Write(payload)
	return mac.Sum(nil)
}

func publicListCursorMACKey() (string, error) {
	publicListCursorMACKeyMu.RLock()
	override := publicListCursorMACKeyOverride
	publicListCursorMACKeyMu.RUnlock()
	if override != "" {
		return override, nil
	}
	if v := strings.TrimSpace(os.Getenv("ROOM_PUBLIC_LIST_CURSOR_SECRET")); v != "" {
		return v, nil
	}
	switch strings.ToLower(strings.TrimSpace(os.Getenv("DEPLOYMENT_ENV"))) {
	case "production", "staging", "prod":
		return "", ErrPublicListCursorSecretRequired
	default:
		return documentedNonProdPublicListCursorMACKey, nil
	}
}
