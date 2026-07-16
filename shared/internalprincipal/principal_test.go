package internalprincipal

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

var testKey = []byte("0123456789abcdef0123456789abcdef")
var previousTestKey = []byte("abcdef0123456789abcdef0123456789")

func testBinding(t *testing.T) CommandBinding {
	t.Helper()
	binding, err := NewRoomCommandBinding("cmd-1", "PlayCard", "room-1", int64ptr(7), json.RawMessage(`{"roomId":"room-1","cardId":"c1"}`))
	if err != nil {
		t.Fatal(err)
	}
	return binding
}

func int64ptr(value int64) *int64 { return &value }

func testSignerVerifier(t *testing.T, now *time.Time) (*Signer, *Verifier) {
	t.Helper()
	clock := func() time.Time { return *now }
	signer, err := NewSigner(SignerConfig{ActiveKey: Key{ID: "current", Material: testKey}, Issuer: DefaultIssuer, Audience: DefaultRoomAudience, TTL: 15 * time.Second, Now: clock})
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := NewVerifier(VerifierConfig{CurrentKey: Key{ID: "current", Material: testKey}, Issuer: DefaultIssuer, Audience: DefaultRoomAudience, MaxTTL: 20 * time.Second, FutureSkew: DefaultFutureSkew, Now: clock})
	if err != nil {
		t.Fatal(err)
	}
	return signer, verifier
}

func TestIssueVerifyCanonicalPrincipal(t *testing.T) {
	now := time.Date(2026, 7, 16, 12, 0, 0, 123000000, time.UTC)
	signer, verifier := testSignerVerifier(t, &now)
	binding := testBinding(t)
	token, issued, err := signer.Issue(IssueInput{PlayerID: "p1", SessionID: "s1", Roles: []string{"player", "operator"}, Eligible: true, SessionNotAfter: now.Add(time.Hour), Command: binding})
	if err != nil {
		t.Fatal(err)
	}
	if len(token) > MaxTokenSize || !strings.HasPrefix(token, "v2.current.") || issued.Roles[0] != "operator" || issued.KeyID != "current" {
		t.Fatalf("token/claims=%q %+v", token, issued)
	}
	claims, err := verifier.Verify(token, "p1", "s1", binding)
	if err != nil || claims.ValidationMarker == "" || claims.ExpiresAtMillis-issued.IssuedAtMillis != 15000 {
		t.Fatalf("verify claims=%+v err=%v", claims, err)
	}
}

func TestVerifyRejectsTamperExpiryFutureAudienceAndWrongIDs(t *testing.T) {
	now := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	signer, verifier := testSignerVerifier(t, &now)
	binding := testBinding(t)
	token, _, err := signer.Issue(IssueInput{PlayerID: "p1", SessionID: "s1", Roles: []string{"player"}, Eligible: true, Command: binding})
	if err != nil {
		t.Fatal(err)
	}
	tampered := token[:len(token)-1] + "A"
	if _, err := verifier.Verify(tampered, "p1", "s1", binding); !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("tamper err=%v", err)
	}
	if _, err := verifier.Verify(token, "other", "s1", binding); !errors.Is(err, ErrSubjectMismatch) {
		t.Fatalf("player mismatch err=%v", err)
	}
	if _, err := verifier.Verify(token, "p1", "other", binding); !errors.Is(err, ErrSubjectMismatch) {
		t.Fatalf("session mismatch err=%v", err)
	}
	now = now.Add(15 * time.Second)
	if _, err := verifier.Verify(token, "p1", "s1", binding); !errors.Is(err, ErrExpired) {
		t.Fatalf("expiry err=%v", err)
	}

	futureNow := time.Date(2026, 7, 16, 13, 0, 0, 0, time.UTC)
	futureSigner, _ := testSignerVerifier(t, &futureNow)
	futureToken, _, _ := futureSigner.Issue(IssueInput{PlayerID: "p1", SessionID: "s1", Roles: []string{"player"}, Eligible: true, Command: binding})
	now = futureNow.Add(-10 * time.Second)
	if _, err := verifier.Verify(futureToken, "p1", "s1", binding); !errors.Is(err, ErrIssuedInFuture) {
		t.Fatalf("future err=%v", err)
	}

	now = futureNow
	wrongAudience, err := NewVerifier(VerifierConfig{CurrentKey: Key{ID: "current", Material: testKey}, Issuer: DefaultIssuer, Audience: "other", MaxTTL: 20 * time.Second, FutureSkew: 5 * time.Second, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrongAudience.Verify(futureToken, "p1", "s1", binding); !errors.Is(err, ErrWrongAudience) {
		t.Fatalf("audience err=%v", err)
	}
}

func TestIssueCapsExpiryAtSessionAndRejectsInvalidConfiguration(t *testing.T) {
	now := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	signer, verifier := testSignerVerifier(t, &now)
	binding := testBinding(t)
	token, claims, err := signer.Issue(IssueInput{PlayerID: "p1", SessionID: "s1", Roles: []string{"player"}, Eligible: true, SessionNotAfter: now.Add(2 * time.Second), Command: binding})
	if err != nil || claims.ExpiresAtMillis-claims.IssuedAtMillis != 2000 {
		t.Fatalf("session cap claims=%+v err=%v", claims, err)
	}
	now = now.Add(2 * time.Second)
	if _, err := verifier.Verify(token, "p1", "s1", binding); !errors.Is(err, ErrExpired) {
		t.Fatalf("session expiry err=%v", err)
	}
	if _, err := NewSigner(SignerConfig{ActiveKey: Key{ID: "current", Material: []byte("short")}, Issuer: DefaultIssuer, Audience: DefaultRoomAudience, TTL: time.Second}); err == nil {
		t.Fatal("short key accepted")
	}
	if _, err := verifier.Verify(strings.Repeat("x", MaxTokenSize+1), "p1", "s1", binding); !errors.Is(err, ErrMalformed) {
		t.Fatalf("oversize err=%v", err)
	}
}

func TestRoomCommandBindingCanonicalPayloadDigest(t *testing.T) {
	first, err := NewRoomCommandBinding("cmd-1", "PlayCard", "room-1", int64ptr(7), json.RawMessage(`{"roomId":"r1", "cardId":"c1"}`))
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewRoomCommandBinding("cmd-1", "PlayCard", "room-1", int64ptr(7), json.RawMessage("{\n  \"cardId\": \"c1\",\n  \"roomId\": \"r1\"\n}"))
	if err != nil {
		t.Fatal(err)
	}
	if first.PayloadSHA256 != "40264a468e393ba46f441bc50703b71b59d0bdd0238413ecf79bafa38918da13" || first != second {
		t.Fatalf("canonical bindings differ: first=%+v second=%+v", first, second)
	}
}

func TestVerifyRejectsAnyChangedRoomCommandBinding(t *testing.T) {
	now := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	signer, verifier := testSignerVerifier(t, &now)
	binding := testBinding(t)
	token, _, err := signer.Issue(IssueInput{PlayerID: "p1", SessionID: "s1", Roles: []string{"player"}, Eligible: true, Command: binding})
	if err != nil {
		t.Fatal(err)
	}

	changedPayload, err := NewRoomCommandBinding(binding.CommandID, binding.CommandType, binding.RoomID, int64ptr(binding.ExpectedSequenceNumber), json.RawMessage(`{"roomId":"room-1","cardId":"different"}`))
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]CommandBinding{
		"bounded context": func() CommandBinding { b := binding; b.BoundedContext = "other"; return b }(),
		"command id":      func() CommandBinding { b := binding; b.CommandID = "cmd-2"; return b }(),
		"command type":    func() CommandBinding { b := binding; b.CommandType = "DrawCard"; return b }(),
		"room id":         func() CommandBinding { b := binding; b.RoomID = "room-2"; return b }(),
		"sequence presence": func() CommandBinding {
			b := binding
			b.ExpectedSequencePresent = false
			b.ExpectedSequenceNumber = 0
			return b
		}(),
		"sequence value":           func() CommandBinding { b := binding; b.ExpectedSequenceNumber = 8; return b }(),
		"canonical payload digest": changedPayload,
	}
	for name, changed := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := verifier.Verify(token, "p1", "s1", changed); !errors.Is(err, ErrCommandMismatch) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestVerifierAcceptsPreviousKeyAndRejectsUnknownKid(t *testing.T) {
	now := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	oldSigner, err := NewSigner(SignerConfig{ActiveKey: Key{ID: "previous", Material: previousTestKey}, Issuer: DefaultIssuer, Audience: DefaultRoomAudience, TTL: 15 * time.Second, Now: clock})
	if err != nil {
		t.Fatal(err)
	}
	previous := Key{ID: "previous", Material: previousTestKey}
	verifier, err := NewVerifier(VerifierConfig{CurrentKey: Key{ID: "current", Material: testKey}, PreviousKey: &previous, Issuer: DefaultIssuer, Audience: DefaultRoomAudience, MaxTTL: 20 * time.Second, FutureSkew: DefaultFutureSkew, Now: clock})
	if err != nil {
		t.Fatal(err)
	}
	binding := testBinding(t)
	token, _, err := oldSigner.Issue(IssueInput{PlayerID: "p1", SessionID: "s1", Roles: []string{"player"}, Eligible: true, Command: binding})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.Verify(token, "p1", "s1", binding); err != nil {
		t.Fatalf("previous key rejected: %v", err)
	}
	unknown := strings.Replace(token, ".previous.", ".unknown.", 1)
	if _, err := verifier.Verify(unknown, "p1", "s1", binding); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("unknown kid error=%v", err)
	}
}

func TestVerifierRejectsDuplicateOrMalformedKeyring(t *testing.T) {
	current := Key{ID: "current", Material: testKey}
	duplicateID := Key{ID: "current", Material: previousTestKey}
	if _, err := NewVerifier(VerifierConfig{CurrentKey: current, PreviousKey: &duplicateID, Issuer: DefaultIssuer, Audience: DefaultRoomAudience, MaxTTL: 20 * time.Second}); err == nil {
		t.Fatal("duplicate key id accepted")
	}
	badID := Key{ID: "bad id", Material: previousTestKey}
	if _, err := NewVerifier(VerifierConfig{CurrentKey: badID, Issuer: DefaultIssuer, Audience: DefaultRoomAudience, MaxTTL: 20 * time.Second}); err == nil {
		t.Fatal("malformed key id accepted")
	}
	delimiterID := Key{ID: "bad.id", Material: previousTestKey}
	if _, err := NewVerifier(VerifierConfig{CurrentKey: delimiterID, Issuer: DefaultIssuer, Audience: DefaultRoomAudience, MaxTTL: 20 * time.Second}); err == nil {
		t.Fatal("token-delimiter key id accepted")
	}
	initialPrevious := Key{ID: "previous", Material: testKey}
	if _, err := NewVerifier(VerifierConfig{CurrentKey: current, PreviousKey: &initialPrevious, Issuer: DefaultIssuer, Audience: DefaultRoomAudience, MaxTTL: 20 * time.Second}); err != nil {
		t.Fatalf("initial overlap with distinct ids and same material rejected: %v", err)
	}
}
