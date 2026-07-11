package domain

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// RandomIDGenerator issues cryptographically random opaque ids/tokens.
type RandomIDGenerator struct{}

func (RandomIDGenerator) NewID(prefix string) string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("rand: %v", err))
	}
	return prefix + "-" + hex.EncodeToString(b[:])
}

func (RandomIDGenerator) NewToken() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func (RandomIDGenerator) NewSalt() ([]byte, error) {
	salt := make([]byte, checkpointSaltLen)
	_, err := rand.Read(salt)
	return salt, err
}
