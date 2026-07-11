package main

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

const (
	envelopeVersionV1 = 1
	dekSizeBytes      = 32
	gcmNonceSize      = 12
)

// KeyProvider wraps and unwraps per-stream data-encryption keys under a versioned KEK.
type KeyProvider interface {
	KeyVersion() int
	WrapDEK(ctx context.Context, dek []byte) (wrapped, wrapNonce []byte, err error)
	UnwrapDEK(ctx context.Context, keyVersion int, wrapNonce, wrapped []byte) ([]byte, error)
	Ready(ctx context.Context) error
}

// DevKeyProvider is the local-only AES-256-GCM KEK provider with a versioned keyring (ADR-0024).
type DevKeyProvider struct {
	keys       map[int][]byte
	keyVersion int // current wrap version
}

// NewDevKeyProvider constructs a single-key DevKeyProvider (tests/helpers).
func NewDevKeyProvider(masterKeyHex string, keyVersion int) (*DevKeyProvider, error) {
	return NewDevKeyProviderFromKeyring(map[int]string{keyVersion: masterKeyHex}, keyVersion)
}

// NewDevKeyProviderFromKeyring constructs a provider that wraps with current and unwraps historical versions.
func NewDevKeyProviderFromKeyring(keysByVersion map[int]string, currentVersion int) (*DevKeyProvider, error) {
	if currentVersion <= 0 {
		return nil, errors.New("envelope key version must be a positive integer")
	}
	if len(keysByVersion) == 0 {
		return nil, errors.New("envelope keyring required")
	}
	if _, ok := keysByVersion[currentVersion]; !ok {
		return nil, fmt.Errorf("current envelope key version %d missing from keyring", currentVersion)
	}
	keys := make(map[int][]byte, len(keysByVersion))
	for ver, hexKey := range keysByVersion {
		if ver <= 0 {
			return nil, errors.New("envelope key versions must be positive integers")
		}
		kek, err := hex.DecodeString(hexKey)
		if err != nil {
			return nil, fmt.Errorf("envelope key %d hex: %w", ver, err)
		}
		if len(kek) != dekSizeBytes {
			return nil, fmt.Errorf("envelope key %d must be exactly %d bytes", ver, dekSizeBytes)
		}
		keys[ver] = kek
	}
	return &DevKeyProvider{keys: keys, keyVersion: currentVersion}, nil
}

// ParseDevKeyring parses GAME_INTEGRITY_ENVELOPE_DEV_KEYS=1:<64hex>,2:<64hex>.
func ParseDevKeyring(raw string) (map[int]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, errors.New("dev keyring empty")
	}
	out := map[int]string{}
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		verHex := strings.SplitN(part, ":", 2)
		if len(verHex) != 2 {
			return nil, fmt.Errorf("invalid keyring entry %q", part)
		}
		ver, err := strconv.Atoi(strings.TrimSpace(verHex[0]))
		if err != nil || ver <= 0 {
			return nil, fmt.Errorf("invalid keyring version in %q", part)
		}
		key := strings.TrimSpace(verHex[1])
		if len(key) != dekSizeBytes*2 {
			return nil, fmt.Errorf("keyring version %d must be %d hex chars", ver, dekSizeBytes*2)
		}
		if _, exists := out[ver]; exists {
			return nil, fmt.Errorf("duplicate keyring version %d", ver)
		}
		out[ver] = key
	}
	if len(out) == 0 {
		return nil, errors.New("dev keyring empty")
	}
	return out, nil
}

func (p *DevKeyProvider) KeyVersion() int { return p.keyVersion }

func (p *DevKeyProvider) WrapDEK(ctx context.Context, dek []byte) ([]byte, []byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if len(dek) != dekSizeBytes {
		return nil, nil, errors.New("DEK must be 32 bytes")
	}
	return sealWithKey(p.keys[p.keyVersion], nil, dek)
}

func (p *DevKeyProvider) UnwrapDEK(ctx context.Context, keyVersion int, wrapNonce, wrapped []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	kek, ok := p.keys[keyVersion]
	if !ok {
		return nil, fmt.Errorf("envelope key version %d not in keyring", keyVersion)
	}
	return openWithKey(kek, nil, wrapNonce, wrapped)
}

func (p *DevKeyProvider) Ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dek := make([]byte, dekSizeBytes)
	if _, err := io.ReadFull(rand.Reader, dek); err != nil {
		return err
	}
	wrapped, nonce, err := p.WrapDEK(ctx, dek)
	if err != nil {
		return err
	}
	out, err := p.UnwrapDEK(ctx, p.keyVersion, nonce, wrapped)
	if err != nil {
		return err
	}
	if hex.EncodeToString(out) != hex.EncodeToString(dek) {
		return errors.New("envelope provider self-test failed")
	}
	// Prove historical keys in the ring can still unwrap material wrapped by themselves.
	for ver, kek := range p.keys {
		if ver == p.keyVersion {
			continue
		}
		w, n, err := sealWithKey(kek, nil, dek)
		if err != nil {
			return err
		}
		opened, err := p.UnwrapDEK(ctx, ver, n, w)
		if err != nil {
			return fmt.Errorf("historical key %d: %w", ver, err)
		}
		if hex.EncodeToString(opened) != hex.EncodeToString(dek) {
			return fmt.Errorf("historical key %d self-test failed", ver)
		}
	}
	return nil
}

// SealPayload encrypts plaintext under dek with a fresh 12-byte nonce and AAD.
func SealPayload(dek, aad, plaintext []byte) (ciphertext, nonce []byte, err error) {
	return sealWithKey(dek, aad, plaintext)
}

// SealPayloadWithNonce encrypts plaintext under dek with a caller-provided nonce and AAD.
func SealPayloadWithNonce(dek, aad, nonce, plaintext []byte) ([]byte, error) {
	return sealWithKeyNonce(dek, aad, nonce, plaintext)
}

// OpenPayload decrypts ciphertext under dek with AAD and nonce.
func OpenPayload(dek, aad, nonce, ciphertext []byte) ([]byte, error) {
	return openWithKey(dek, aad, nonce, ciphertext)
}

// CanonicalEnvelopeAAD is retained for unit tests of length-prefixed encoding helpers.
func CanonicalEnvelopeAAD(stream, originalEventID, originalEventType string, revision uint64) []byte {
	meta := envelopeMetadataV1{
		EnvelopeVersion:   envelopeVersionV1,
		OriginalEventID:   originalEventID,
		OriginalEventType: originalEventType,
		Stream:            stream,
		KurrentRevision:   revision,
	}
	return meta.canonicalAAD()
}

func sealWithKey(key, aad, plaintext []byte) ([]byte, []byte, error) {
	nonce := make([]byte, gcmNonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, nil, err
	}
	ct, err := sealWithKeyNonce(key, aad, nonce, plaintext)
	if err != nil {
		return nil, nil, err
	}
	return ct, nonce, nil
}

func sealWithKeyNonce(key, aad, nonce, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(nonce) != gcm.NonceSize() {
		return nil, fmt.Errorf("unexpected GCM nonce size %d", len(nonce))
	}
	return gcm.Seal(nil, nonce, plaintext, aad), nil
}

func openWithKey(key, aad, nonce, ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(nonce) != gcm.NonceSize() {
		return nil, errors.New("invalid envelope nonce size")
	}
	return gcm.Open(nil, nonce, ciphertext, aad)
}

func appendLengthPrefixed(dst, val []byte) []byte {
	var lenBuf [4]byte
	lenBuf[0] = byte(len(val) >> 24)
	lenBuf[1] = byte(len(val) >> 16)
	lenBuf[2] = byte(len(val) >> 8)
	lenBuf[3] = byte(len(val))
	dst = append(dst, lenBuf[:]...)
	return append(dst, val...)
}

func parsePositiveInt(raw string) (int, error) {
	if raw == "" {
		return 0, errors.New("required positive integer missing")
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return 0, errors.New("value must be a positive integer")
	}
	return v, nil
}

func deploymentAllowsDevProvider(env string) bool {
	switch strings.ToLower(strings.TrimSpace(env)) {
	case "local", "test", "development", "dev":
		return true
	default:
		return false
	}
}

func resolveDeploymentEnv() string {
	if v := strings.TrimSpace(os.Getenv("DEPLOYMENT_ENV")); v != "" {
		return v
	}
	return strings.TrimSpace(os.Getenv("GAME_INTEGRITY_DEPLOYMENT_ENV"))
}
