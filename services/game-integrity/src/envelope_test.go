package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"testing"
)

func TestDevKeyProvider_WrapUnwrapRoundTrip(t *testing.T) {
	p, err := NewDevKeyProvider("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", 7)
	if err != nil {
		t.Fatal(err)
	}
	if p.KeyVersion() != 7 {
		t.Fatalf("version=%d", p.KeyVersion())
	}
	dek := make([]byte, 32)
	if _, err := rand.Read(dek); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	wrapped, wrapNonce, err := p.WrapDEK(ctx, dek)
	if err != nil {
		t.Fatal(err)
	}
	if len(wrapNonce) != gcmNonceSize {
		t.Fatalf("nonce=%d", len(wrapNonce))
	}
	opened, err := p.UnwrapDEK(ctx, 7, wrapNonce, wrapped)
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(opened) != hex.EncodeToString(dek) {
		t.Fatal("round-trip mismatch")
	}
}

func TestDevKeyProvider_HistoricalKeyringUnwrap(t *testing.T) {
	oldKey := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	newKey := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	p, err := NewDevKeyProviderFromKeyring(map[int]string{1: oldKey, 2: newKey}, 2)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	dek := make([]byte, 32)
	_, _ = rand.Read(dek)
	// Material wrapped under historical key 1 must still unwrap.
	oldProv, err := NewDevKeyProvider(oldKey, 1)
	if err != nil {
		t.Fatal(err)
	}
	wrapped, nonce, err := oldProv.WrapDEK(ctx, dek)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := p.UnwrapDEK(ctx, 1, nonce, wrapped)
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(opened) != hex.EncodeToString(dek) {
		t.Fatal("historical unwrap mismatch")
	}
	// New wraps use current version 2.
	w2, n2, err := p.WrapDEK(ctx, dek)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.UnwrapDEK(ctx, 2, n2, w2); err != nil {
		t.Fatal(err)
	}
}

func TestDevKeyProvider_RejectsUnknownVersionAndTamper(t *testing.T) {
	p, err := NewDevKeyProvider("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", 3)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	dek := make([]byte, 32)
	_, _ = rand.Read(dek)
	wrapped, wrapNonce, err := p.WrapDEK(ctx, dek)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.UnwrapDEK(ctx, 99, wrapNonce, wrapped); err == nil {
		t.Fatal("expected version mismatch")
	}
	tampered := append([]byte(nil), wrapped...)
	tampered[0] ^= 0xff
	if _, err := p.UnwrapDEK(ctx, 3, wrapNonce, tampered); err == nil {
		t.Fatal("expected tamper failure")
	}
}

func TestSealOpen_WithAAD(t *testing.T) {
	dek := make([]byte, 32)
	_, _ = rand.Read(dek)
	aad := CanonicalEnvelopeAAD("stream-a", "evt-1", "PlayCard", 0)
	ct, nonce, err := SealPayload(dek, aad, []byte(`{"x":1}`))
	if err != nil {
		t.Fatal(err)
	}
	plain, err := OpenPayload(dek, aad, nonce, ct)
	if err != nil {
		t.Fatal(err)
	}
	if string(plain) != `{"x":1}` {
		t.Fatalf("plain=%s", plain)
	}
	badAAD := CanonicalEnvelopeAAD("stream-b", "evt-1", "PlayCard", 0)
	if _, err := OpenPayload(dek, badAAD, nonce, ct); err == nil {
		t.Fatal("expected AAD mismatch")
	}
}

func TestParseDevKeyring(t *testing.T) {
	m, err := ParseDevKeyring("1:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef,2:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 2 || m[1] == "" || m[2] == "" {
		t.Fatalf("%+v", m)
	}
}

func TestDeploymentAllowsDevProvider(t *testing.T) {
	if !deploymentAllowsDevProvider("local") || !deploymentAllowsDevProvider("test") {
		t.Fatal("local/test must allow")
	}
	if deploymentAllowsDevProvider("staging") || deploymentAllowsDevProvider("production") {
		t.Fatal("staging/prod must reject")
	}
}
