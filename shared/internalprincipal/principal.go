// Package internalprincipal implements the compact signed principal from
// ADR-0021. The proof is an internal authorization artifact, never a public
// bearer token and never a substitute for Gateway's service identity.
package internalprincipal

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"time"
)

const (
	HeaderName          = "X-UnoArena-Internal-Principal"
	DefaultIssuer       = "unoarena.identity"
	DefaultRoomAudience = "unoarena.room-gameplay"
	TournamentContext   = "unoarena.tournament-orchestration"
	MaxTokenSize        = 4096
	MinKeyBytes         = 32
	MaxKeyBytes         = 1024
	MaxProofTTL         = time.Minute
	DefaultFutureSkew   = 5 * time.Second
)

var (
	ErrMalformed        = errors.New("malformed internal principal")
	ErrInvalidSignature = errors.New("invalid internal principal signature")
	ErrExpired          = errors.New("internal principal expired")
	ErrIssuedInFuture   = errors.New("internal principal issued in future")
	ErrWrongIssuer      = errors.New("wrong internal principal issuer")
	ErrWrongAudience    = errors.New("wrong internal principal audience")
	ErrSubjectMismatch  = errors.New("internal principal subject mismatch")
	ErrCommandMismatch  = errors.New("internal principal command mismatch")
	ErrUnknownKey       = errors.New("unknown internal principal key id")
	ErrIneligible       = errors.New("internal principal is not eligible")
)

// Key is one versioned HMAC key in the signed-principal trust domain. IDs are
// non-secret rollout identifiers; Material is always secret.
type Key struct {
	ID       string
	Material []byte
}

// CommandBinding is the exact Room mutation context authorized by Identity.
// ExpectedSequencePresent distinguishes an omitted sequence from a literal 0.
type CommandBinding struct {
	BoundedContext          string `json:"ctx"`
	CommandID               string `json:"cid"`
	CommandType             string `json:"ct"`
	RoomID                  string `json:"rid"`
	ExpectedSequencePresent bool   `json:"esp"`
	ExpectedSequenceNumber  int64  `json:"esn"`
	PayloadSHA256           string `json:"ph"`
}

type Claims struct {
	Version          int            `json:"v"`
	KeyID            string         `json:"kid"`
	Issuer           string         `json:"iss"`
	Audience         string         `json:"aud"`
	PlayerID         string         `json:"pid"`
	SessionID        string         `json:"sid"`
	Roles            []string       `json:"roles"`
	Eligible         bool           `json:"eligible"`
	IssuedAtMillis   int64          `json:"iat"`
	ExpiresAtMillis  int64          `json:"exp"`
	ValidationMarker string         `json:"vm"`
	Command          CommandBinding `json:"cmd"`
}

type IssueInput struct {
	PlayerID        string
	SessionID       string
	Roles           []string
	Eligible        bool
	SessionNotAfter time.Time
	Command         CommandBinding
}

type SignerConfig struct {
	ActiveKey Key
	Issuer    string
	Audience  string
	TTL       time.Duration
	Now       func() time.Time
}

type Signer struct {
	activeKey Key
	issuer    string
	audience  string
	ttl       time.Duration
	now       func() time.Time
}

func NewSigner(cfg SignerConfig) (*Signer, error) {
	if err := validateVersionedKey(cfg.ActiveKey); err != nil {
		return nil, err
	}
	if err := validateName("issuer", cfg.Issuer); err != nil {
		return nil, err
	}
	if err := validateName("audience", cfg.Audience); err != nil {
		return nil, err
	}
	if cfg.TTL < time.Second || cfg.TTL > MaxProofTTL {
		return nil, fmt.Errorf("principal TTL must be between 1s and %s", MaxProofTTL)
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	activeKey := Key{ID: cfg.ActiveKey.ID, Material: bytes.Clone(cfg.ActiveKey.Material)}
	return &Signer{activeKey: activeKey, issuer: cfg.Issuer, audience: cfg.Audience, ttl: cfg.TTL, now: cfg.Now}, nil
}

func (s *Signer) Issue(input IssueInput) (string, Claims, error) {
	if s == nil {
		return "", Claims{}, errors.New("nil internal principal signer")
	}
	playerID, sessionID := strings.TrimSpace(input.PlayerID), strings.TrimSpace(input.SessionID)
	if err := validateSubject(playerID, sessionID); err != nil {
		return "", Claims{}, err
	}
	roles, err := canonicalRoles(input.Roles)
	if err != nil {
		return "", Claims{}, err
	}
	if err := input.Command.validate(); err != nil {
		return "", Claims{}, err
	}
	if input.Command.BoundedContext != s.audience {
		return "", Claims{}, ErrWrongAudience
	}
	now := s.now().UTC()
	expires := now.Add(s.ttl)
	if !input.SessionNotAfter.IsZero() && input.SessionNotAfter.Before(expires) {
		expires = input.SessionNotAfter.UTC()
	}
	if !expires.After(now) {
		return "", Claims{}, ErrExpired
	}
	claims := Claims{
		Version: 2, KeyID: s.activeKey.ID, Issuer: s.issuer, Audience: s.audience,
		PlayerID: playerID, SessionID: sessionID, Roles: roles, Eligible: input.Eligible,
		IssuedAtMillis: now.UnixMilli(), ExpiresAtMillis: expires.UnixMilli(),
		ValidationMarker: fmt.Sprintf("identity-postgres-v1:%d", now.UnixMilli()),
		Command:          input.Command,
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", Claims{}, err
	}
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	signingInput := "v2." + s.activeKey.ID + "." + encoded
	sig := sign(s.activeKey.Material, signingInput)
	token := signingInput + "." + base64.RawURLEncoding.EncodeToString(sig)
	if len(token) > MaxTokenSize {
		return "", Claims{}, fmt.Errorf("%w: proof exceeds %d bytes", ErrMalformed, MaxTokenSize)
	}
	return token, claims, nil
}

type VerifierConfig struct {
	CurrentKey  Key
	PreviousKey *Key
	Issuer      string
	Audience    string
	MaxTTL      time.Duration
	FutureSkew  time.Duration
	Now         func() time.Time
}

type Verifier struct {
	keys       map[string][]byte
	issuer     string
	audience   string
	maxTTL     time.Duration
	futureSkew time.Duration
	now        func() time.Time
}

func NewVerifier(cfg VerifierConfig) (*Verifier, error) {
	if err := validateVersionedKey(cfg.CurrentKey); err != nil {
		return nil, err
	}
	keys := map[string][]byte{cfg.CurrentKey.ID: bytes.Clone(cfg.CurrentKey.Material)}
	if cfg.PreviousKey != nil {
		if err := validateVersionedKey(*cfg.PreviousKey); err != nil {
			return nil, err
		}
		if cfg.PreviousKey.ID == cfg.CurrentKey.ID {
			return nil, errors.New("principal key ids must be unique")
		}
		keys[cfg.PreviousKey.ID] = bytes.Clone(cfg.PreviousKey.Material)
	}
	if err := validateName("issuer", cfg.Issuer); err != nil {
		return nil, err
	}
	if err := validateName("audience", cfg.Audience); err != nil {
		return nil, err
	}
	if cfg.MaxTTL < time.Second || cfg.MaxTTL > MaxProofTTL {
		return nil, fmt.Errorf("principal max TTL must be between 1s and %s", MaxProofTTL)
	}
	if cfg.FutureSkew < 0 || cfg.FutureSkew > 30*time.Second {
		return nil, errors.New("principal future skew must be between 0s and 30s")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Verifier{keys: keys, issuer: cfg.Issuer, audience: cfg.Audience, maxTTL: cfg.MaxTTL, futureSkew: cfg.FutureSkew, now: cfg.Now}, nil
}

// Verify authenticates the compact proof before decoding claims, then enforces
// issuer, audience, freshness, eligibility, and exact body identity binding.
func (v *Verifier) Verify(token, expectedPlayerID, expectedSessionID string, expectedCommand CommandBinding) (Claims, error) {
	if v == nil {
		return Claims{}, errors.New("nil internal principal verifier")
	}
	if len(token) == 0 || len(token) > MaxTokenSize {
		return Claims{}, fmt.Errorf("%w: invalid proof size", ErrMalformed)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 4 || parts[0] != "v2" || parts[1] == "" || parts[2] == "" || parts[3] == "" {
		return Claims{}, ErrMalformed
	}
	if err := validateKeyID(parts[1]); err != nil {
		return Claims{}, ErrMalformed
	}
	key, ok := v.keys[parts[1]]
	if !ok {
		return Claims{}, ErrUnknownKey
	}
	signature, err := decodeCanonicalBase64(parts[3])
	if err != nil || len(signature) != sha256.Size {
		return Claims{}, ErrMalformed
	}
	want := sign(key, parts[0]+"."+parts[1]+"."+parts[2])
	if !hmac.Equal(signature, want) {
		return Claims{}, ErrInvalidSignature
	}
	payload, err := decodeCanonicalBase64(parts[2])
	if err != nil || len(payload) == 0 || len(payload) > MaxTokenSize {
		return Claims{}, ErrMalformed
	}
	var claims Claims
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&claims); err != nil {
		return Claims{}, ErrMalformed
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return Claims{}, ErrMalformed
	}
	if claims.Version != 2 || claims.KeyID != parts[1] || claims.ValidationMarker == "" || len(claims.ValidationMarker) > 128 {
		return Claims{}, ErrMalformed
	}
	if claims.Issuer != v.issuer {
		return Claims{}, ErrWrongIssuer
	}
	if claims.Audience != v.audience {
		return Claims{}, ErrWrongAudience
	}
	if err := validateSubject(claims.PlayerID, claims.SessionID); err != nil {
		return Claims{}, ErrMalformed
	}
	if claims.PlayerID != expectedPlayerID || claims.SessionID != expectedSessionID {
		return Claims{}, ErrSubjectMismatch
	}
	if err := expectedCommand.validate(); err != nil {
		return Claims{}, ErrCommandMismatch
	}
	if claims.Command != expectedCommand {
		return Claims{}, ErrCommandMismatch
	}
	if !claims.Eligible {
		return Claims{}, ErrIneligible
	}
	canonical, err := canonicalRoles(claims.Roles)
	if err != nil || !slices.Equal(canonical, claims.Roles) {
		return Claims{}, ErrMalformed
	}
	issued := time.UnixMilli(claims.IssuedAtMillis)
	expires := time.UnixMilli(claims.ExpiresAtMillis)
	if !expires.After(issued) || expires.Sub(issued) > v.maxTTL {
		return Claims{}, ErrMalformed
	}
	now := v.now().UTC()
	if issued.After(now.Add(v.futureSkew)) {
		return Claims{}, ErrIssuedInFuture
	}
	if !expires.After(now) {
		return Claims{}, ErrExpired
	}
	return claims, nil
}

// NewRoomCommandBinding canonicalizes the JSON payload and binds every field
// that can change the meaning or destination of a Room mutation.
func NewRoomCommandBinding(commandID, commandType, roomID string, expectedSequence *int64, payload json.RawMessage) (CommandBinding, error) {
	digest, err := CanonicalPayloadSHA256(payload)
	if err != nil {
		return CommandBinding{}, err
	}
	binding := CommandBinding{
		BoundedContext: DefaultRoomAudience,
		CommandID:      strings.TrimSpace(commandID),
		CommandType:    strings.TrimSpace(commandType),
		RoomID:         strings.TrimSpace(roomID),
		PayloadSHA256:  digest,
	}
	if expectedSequence != nil {
		binding.ExpectedSequencePresent = true
		binding.ExpectedSequenceNumber = *expectedSequence
	}
	if err := binding.validate(); err != nil {
		return CommandBinding{}, err
	}
	return binding, nil
}

// CanonicalPayloadSHA256 hashes a stable JSON representation. Object member
// order and insignificant whitespace therefore cannot change the binding.
func CanonicalPayloadSHA256(payload json.RawMessage) (string, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return "", fmt.Errorf("canonical payload: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return "", fmt.Errorf("canonical payload: %w", err)
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("canonical payload: %w", err)
	}
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:]), nil
}

func (b CommandBinding) validate() error {
	if b.BoundedContext != DefaultRoomAudience {
		return fmt.Errorf("%w: invalid bounded context", ErrMalformed)
	}
	for name, value := range map[string]string{
		"command id":   b.CommandID,
		"command type": b.CommandType,
	} {
		if value == "" || value != strings.TrimSpace(value) || len(value) > 256 {
			return fmt.Errorf("%w: invalid %s", ErrMalformed, name)
		}
	}
	if b.RoomID != strings.TrimSpace(b.RoomID) || len(b.RoomID) > 256 {
		return fmt.Errorf("%w: invalid room id", ErrMalformed)
	}
	if b.ExpectedSequencePresent {
		if b.ExpectedSequenceNumber < 0 {
			return fmt.Errorf("%w: invalid expected sequence", ErrMalformed)
		}
	} else if b.ExpectedSequenceNumber != 0 {
		return fmt.Errorf("%w: sequence value without presence", ErrMalformed)
	}
	if len(b.PayloadSHA256) != sha256.Size*2 {
		return fmt.Errorf("%w: invalid payload digest", ErrMalformed)
	}
	if decoded, err := hex.DecodeString(b.PayloadSHA256); err != nil || len(decoded) != sha256.Size || b.PayloadSHA256 != strings.ToLower(b.PayloadSHA256) {
		return fmt.Errorf("%w: invalid payload digest", ErrMalformed)
	}
	return nil
}

func validateVersionedKey(key Key) error {
	if err := validateKeyID(key.ID); err != nil {
		return err
	}
	return validateKey(key.Material)
}

func validateKeyID(id string) error {
	if id == "" || len(id) > 64 {
		return errors.New("principal key id must be between 1 and 64 characters")
	}
	for _, r := range id {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			continue
		}
		return errors.New("principal key id contains invalid characters")
	}
	return nil
}

func validateKey(key []byte) error {
	if len(key) < MinKeyBytes || len(key) > MaxKeyBytes {
		return fmt.Errorf("principal HMAC key must be %d..%d bytes", MinKeyBytes, MaxKeyBytes)
	}
	return nil
}

func validateName(kind, value string) error {
	if value == "" || strings.TrimSpace(value) != value || len(value) > 128 {
		return fmt.Errorf("invalid principal %s", kind)
	}
	return nil
}

func validateSubject(playerID, sessionID string) error {
	if playerID == "" || sessionID == "" || strings.TrimSpace(playerID) != playerID || strings.TrimSpace(sessionID) != sessionID || len(playerID) > 256 || len(sessionID) > 256 {
		return ErrSubjectMismatch
	}
	return nil
}

func canonicalRoles(roles []string) ([]string, error) {
	if len(roles) > 32 {
		return nil, ErrMalformed
	}
	out := append([]string{}, roles...)
	for _, role := range out {
		if role == "" || strings.TrimSpace(role) != role || len(role) > 64 {
			return nil, ErrMalformed
		}
	}
	sort.Strings(out)
	for i := 1; i < len(out); i++ {
		if out[i] == out[i-1] {
			return nil, ErrMalformed
		}
	}
	return out, nil
}

func sign(key []byte, input string) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(input))
	return mac.Sum(nil)
}

func decodeCanonicalBase64(value string) ([]byte, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || base64.RawURLEncoding.EncodeToString(decoded) != value {
		return nil, ErrMalformed
	}
	return decoded, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return ErrMalformed
	}
	return nil
}
