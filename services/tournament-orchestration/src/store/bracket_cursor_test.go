package store_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"os"
	"strings"
	"testing"

	"unoarena/services/tournament-orchestration/store"
)

func TestBracketCursor_RoundTrip(t *testing.T) {
	restore := store.SetBracketCursorMACKeyForTest("unit-test-cursor-secret")
	t.Cleanup(restore)

	c := store.BracketCursor{RoundNumber: 3, SlotIndex: 42}
	enc, err := store.EncodeBracketCursor(c)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(enc, "redis") || strings.Contains(enc, "slot_") || strings.Contains(enc, "OFFSET") {
		t.Fatalf("cursor leaked physical identity: %q", enc)
	}
	got, err := store.DecodeBracketCursor(enc)
	if err != nil {
		t.Fatal(err)
	}
	if got != c {
		t.Fatalf("got %+v want %+v", got, c)
	}
}

func TestBracketCursor_RejectsMalformedAndTampered(t *testing.T) {
	restore := store.SetBracketCursorMACKeyForTest("unit-test-cursor-secret")
	t.Cleanup(restore)

	valid, err := store.EncodeBracketCursor(store.BracketCursor{RoundNumber: 1, SlotIndex: 0})
	if err != nil {
		t.Fatal(err)
	}
	trailingPayload := []byte(`{"r":1,"i":0}{"extra":true}`)
	trailingMAC := hmacSHA256([]byte("unit-test-cursor-secret"), trailingPayload)
	cases := []string{
		"",
		"not-a-cursor",
		"br1.onlyonepart",
		"br1." + base64.RawURLEncoding.EncodeToString([]byte(`{"r":1,"i":0}`)) + ".deadbeef",
		strings.Replace(valid, "br1.", "br2.", 1),
		"redis:key:1",
		"br1." + base64.RawURLEncoding.EncodeToString([]byte(`{"r":0,"i":0}`)) + "." + valid[strings.LastIndex(valid, ".")+1:],
		"br1." + base64.RawURLEncoding.EncodeToString(trailingPayload) + "." + base64.RawURLEncoding.EncodeToString(trailingMAC),
	}
	for _, raw := range cases {
		if _, err := store.DecodeBracketCursor(raw); err == nil {
			t.Fatalf("expected error for %q", raw)
		}
	}
	// Tamper payload bits while keeping structure.
	parts := strings.Split(strings.TrimPrefix(valid, "br1."), ".")
	payload, _ := base64.RawURLEncoding.DecodeString(parts[0])
	payload[len(payload)-1] ^= 0x01
	tampered := "br1." + base64.RawURLEncoding.EncodeToString(payload) + "." + parts[1]
	if _, err := store.DecodeBracketCursor(tampered); err == nil {
		t.Fatal("expected tamper rejection")
	}
}

func TestEncodeBracketCursor_RejectsOutOfRange(t *testing.T) {
	restore := store.SetBracketCursorMACKeyForTest("unit-test-cursor-secret")
	t.Cleanup(restore)
	if _, err := store.EncodeBracketCursor(store.BracketCursor{RoundNumber: 0, SlotIndex: 0}); err == nil {
		t.Fatal("expected reject round 0")
	}
	if _, err := store.EncodeBracketCursor(store.BracketCursor{RoundNumber: 1, SlotIndex: -1}); err == nil {
		t.Fatal("expected reject negative slot")
	}
}

func TestBracketCursor_ProductionRequiresSecret(t *testing.T) {
	t.Setenv("TOURNAMENT_BRACKET_CURSOR_SECRET", "")
	t.Setenv("DEPLOYMENT_ENV", "production")
	restore := store.SetBracketCursorMACKeyForTest("")
	t.Cleanup(restore)
	if _, err := store.EncodeBracketCursor(store.BracketCursor{RoundNumber: 1, SlotIndex: 0}); !errors.Is(err, store.ErrBracketCursorSecretRequired) {
		t.Fatalf("want ErrBracketCursorSecretRequired, got %v", err)
	}
}

func TestLoadBracketPage_UsesRepeatableRead(t *testing.T) {
	src, err := os.ReadFile("bracket_page.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(src)
	if !strings.Contains(body, "RepeatableRead") {
		t.Fatal("LoadBracketPage must use REPEATABLE READ")
	}
	if !strings.Contains(body, "ReadOnly") {
		t.Fatal("LoadBracketPage must be read-only")
	}
	if !strings.Contains(body, "loadBracketSummaryQ") || !strings.Contains(body, "loadProjectionCheckpointQ") {
		t.Fatal("LoadBracketPage must assemble via dbQuerier helpers in one tx")
	}
}

func hmacSHA256(key, payload []byte) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(payload)
	return mac.Sum(nil)
}
