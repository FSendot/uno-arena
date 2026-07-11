package domain_test

import (
	"testing"
	"time"

	"unoarena/services/identity/domain"
)

func mustParseTime(t *testing.T, s string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("parse time: %v", err)
	}
	return ts
}

func TestCheckpointPasswordHasherRoundTrip(t *testing.T) {
	h := domain.NewCheckpointPasswordHasher(nil)
	hash, err := h.Hash("correct horse battery staple")
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	if !h.Compare(hash, "correct horse battery staple") {
		t.Fatal("expected match")
	}
	if h.Compare(hash, "wrong password") {
		t.Fatal("expected mismatch")
	}
	if h.Compare("not-a-hash", "correct horse battery staple") {
		t.Fatal("garbage hash must not match")
	}
}

func TestCheckpointPasswordHasherRejectsMalformedEncodings(t *testing.T) {
	h := domain.NewCheckpointPasswordHasher(nil)
	valid, err := h.Hash("secret")
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	parts := splitHash(valid)
	if len(parts) != 4 {
		t.Fatalf("expected 4 parts, got %d", len(parts))
	}

	cases := []struct {
		name string
		hash string
	}{
		{"wrong algo", "other-algo$10000$" + parts[2] + "$" + parts[3]},
		{"wrong iterations high", "chk-pbkdf2-sha256$1000000$" + parts[2] + "$" + parts[3]},
		{"wrong iterations low", "chk-pbkdf2-sha256$1$" + parts[2] + "$" + parts[3]},
		{"empty key", "chk-pbkdf2-sha256$10000$" + parts[2] + "$"},
		{"empty salt", "chk-pbkdf2-sha256$10000$$" + parts[3]},
		{"short salt", "chk-pbkdf2-sha256$10000$YQ$" + parts[3]},
		{"garbage", "not$a$valid"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if h.Compare(tc.hash, "secret") {
				t.Fatalf("expected reject for %s", tc.name)
			}
		})
	}
}

func splitHash(encoded string) []string {
	out := make([]string, 0, 4)
	start := 0
	for i := 0; i < len(encoded); i++ {
		if encoded[i] == '$' {
			out = append(out, encoded[start:i])
			start = i + 1
		}
	}
	out = append(out, encoded[start:])
	return out
}

func TestSessionIsActiveAt(t *testing.T) {
	now := mustParseTime(t, "2026-07-10T12:00:00Z")
	active := domain.Session{
		Status:    domain.SessionStatusActive,
		ExpiresAt: mustParseTime(t, "2026-07-10T13:00:00Z"),
	}
	if err := active.IsActiveAt(now); err != nil {
		t.Fatalf("active: %v", err)
	}
	if err := active.IsActiveAt(mustParseTime(t, "2026-07-10T13:00:00Z")); err != domain.ErrSessionExpired {
		t.Fatalf("expected expired at boundary, got %v", err)
	}
	revoked := domain.Session{Status: domain.SessionStatusInvalidated}
	if err := revoked.IsActiveAt(now); err != domain.ErrSessionInvalid {
		t.Fatalf("expected invalid, got %v", err)
	}
}
