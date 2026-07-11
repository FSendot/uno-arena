package domain

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

// PasswordHasher isolates credential derivation so a later bcrypt/argon2
// adapter can replace the checkpoint implementation without domain changes.
// The checkpoint hasher is NOT production-grade password hashing.
type PasswordHasher interface {
	Hash(password string) (string, error)
	Compare(encodedHash, password string) bool
}

const (
	checkpointAlgo       = "chk-pbkdf2-sha256"
	checkpointIterations = 10000
	checkpointKeyLen     = 32
	checkpointSaltLen    = 16
)

// dummyPasswordHash equalizes missing-user login cost with a real Compare.
var dummyPasswordHash = mustDummyHash()

func mustDummyHash() string {
	salt := make([]byte, checkpointSaltLen)
	for i := range salt {
		salt[i] = byte(i + 1)
	}
	dk := pbkdf2SHA256([]byte("uno-arena-timing-dummy"), salt, checkpointIterations, checkpointKeyLen)
	return fmt.Sprintf("%s$%d$%s$%s",
		checkpointAlgo,
		checkpointIterations,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(dk),
	)
}

// DummyPasswordHash returns the fixed encoded hash used for missing-user login equalization.
func DummyPasswordHash() string { return dummyPasswordHash }

// CheckpointPasswordHasher derives keys with a stdlib-only PBKDF2-HMAC-SHA256.
// Suitable for offline checkpoint only; swap via PasswordHasher for production.
type CheckpointPasswordHasher struct {
	salts SaltGenerator
}

// NewCheckpointPasswordHasher builds a hasher. If salts is nil, crypto/rand is used.
func NewCheckpointPasswordHasher(salts SaltGenerator) *CheckpointPasswordHasher {
	return &CheckpointPasswordHasher{salts: salts}
}

func (h *CheckpointPasswordHasher) Hash(password string) (string, error) {
	var (
		salt []byte
		err  error
	)
	if h.salts != nil {
		salt, err = h.salts.NewSalt()
	} else {
		salt = make([]byte, checkpointSaltLen)
		_, err = rand.Read(salt)
	}
	if err != nil {
		return "", err
	}
	if len(salt) != checkpointSaltLen {
		return "", fmt.Errorf("salt length must be %d", checkpointSaltLen)
	}
	dk := pbkdf2SHA256([]byte(password), salt, checkpointIterations, checkpointKeyLen)
	return fmt.Sprintf("%s$%d$%s$%s",
		checkpointAlgo,
		checkpointIterations,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(dk),
	), nil
}

func (h *CheckpointPasswordHasher) Compare(encodedHash, password string) bool {
	algo, iters, salt, want, ok := parseCheckpointHash(encodedHash)
	if !ok || algo != checkpointAlgo {
		return false
	}
	if iters != checkpointIterations {
		return false
	}
	if len(salt) != checkpointSaltLen || len(want) != checkpointKeyLen {
		return false
	}
	got := pbkdf2SHA256([]byte(password), salt, iters, checkpointKeyLen)
	return subtle.ConstantTimeCompare(got, want) == 1
}

func parseCheckpointHash(encoded string) (algo string, iters int, salt, dk []byte, ok bool) {
	parts := strings.Split(encoded, "$")
	if len(parts) != 4 {
		return "", 0, nil, nil, false
	}
	if parts[0] != checkpointAlgo {
		return "", 0, nil, nil, false
	}
	iters, err := strconv.Atoi(parts[1])
	if err != nil || iters != checkpointIterations {
		return "", 0, nil, nil, false
	}
	// Reject empty encodings before decode to avoid treating "" as valid base64.
	if parts[2] == "" || parts[3] == "" {
		return "", 0, nil, nil, false
	}
	salt, err = base64.RawStdEncoding.DecodeString(parts[2])
	if err != nil || len(salt) != checkpointSaltLen {
		return "", 0, nil, nil, false
	}
	dk, err = base64.RawStdEncoding.DecodeString(parts[3])
	if err != nil || len(dk) != checkpointKeyLen {
		return "", 0, nil, nil, false
	}
	return parts[0], iters, salt, dk, true
}

// pbkdf2SHA256 is a minimal stdlib-only PBKDF2-HMAC-SHA256 (RFC 8018).
func pbkdf2SHA256(password, salt []byte, iterations, keyLen int) []byte {
	hashLen := sha256.Size
	numBlocks := (keyLen + hashLen - 1) / hashLen
	out := make([]byte, 0, numBlocks*hashLen)
	var blockIndex [4]byte
	for block := 1; block <= numBlocks; block++ {
		binary.BigEndian.PutUint32(blockIndex[:], uint32(block))
		mac := hmac.New(sha256.New, password)
		mac.Write(salt)
		mac.Write(blockIndex[:])
		u := mac.Sum(nil)
		t := make([]byte, len(u))
		copy(t, u)
		for i := 1; i < iterations; i++ {
			mac = hmac.New(sha256.New, password)
			mac.Write(u)
			u = mac.Sum(nil)
			for j := range t {
				t[j] ^= u[j]
			}
		}
		out = append(out, t...)
	}
	return out[:keyLen]
}

// HashToken returns a hex SHA-256 digest for durable token lookup (never store raw tokens).
func HashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return fmt.Sprintf("%x", sum[:])
}
