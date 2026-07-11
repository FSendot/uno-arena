package domain

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
)

// DeckSeed is an immutable seed used to derive the authoritative shuffle.
// After construction the seed bytes never change.
type DeckSeed struct {
	bytes []byte
}

// NewDeckSeed copies material into an immutable DeckSeed.
// Empty material is rejected.
func NewDeckSeed(material []byte) (DeckSeed, error) {
	if len(material) == 0 {
		return DeckSeed{}, errors.New("deck seed material must be non-empty")
	}
	cp := make([]byte, len(material))
	copy(cp, material)
	return DeckSeed{bytes: cp}, nil
}

// Bytes returns a defensive copy of the seed material.
func (s DeckSeed) Bytes() []byte {
	if len(s.bytes) == 0 {
		return nil
	}
	cp := make([]byte, len(s.bytes))
	copy(cp, s.bytes)
	return cp
}

// Valid reports whether the seed was constructed with non-empty material.
func (s DeckSeed) Valid() bool { return len(s.bytes) > 0 }

// Equal reports whether two seeds contain identical bytes.
func (s DeckSeed) Equal(other DeckSeed) bool {
	if len(s.bytes) != len(other.bytes) {
		return false
	}
	for i := range s.bytes {
		if s.bytes[i] != other.bytes[i] {
			return false
		}
	}
	return true
}

/*
SHA-256 counter PRNG (stable, documented)

Algorithm used by ShuffleCards / Fisher–Yates:

  Let seed = DeckSeed bytes.
  Let counter start at 0 (uint64, big-endian).

  nextBlock(counter) := SHA-256( seed || uint64_be(counter) )  // 32 bytes

  A stream of uint64 values is produced by consuming the 32-byte block as four
  big-endian uint64 words, then incrementing counter and hashing again when the
  block is exhausted.

  For Fisher–Yates index selection of j in [0, i] (inclusive), let n = i+1.
  Draw uint64 values with rejection sampling:

    limit = floor(2^64 / n) * n
    repeat:
      v = nextUint64()
      if v < limit: return v % n

  This avoids modulo bias and does not use math/rand or any global RNG.
  The same seed and input card multiset always produce the same permutation.
*/

// ShuffleCards returns a deterministic Fisher–Yates permutation of cards.
// The input slice is never mutated.
func ShuffleCards(seed DeckSeed, cards []Card) []Card {
	out := make([]Card, len(cards))
	copy(out, cards)
	if len(out) < 2 || !seed.Valid() {
		return out
	}
	rng := newSHA256CounterRNG(seed.bytes)
	for i := len(out) - 1; i >= 1; i-- {
		j := int(rng.uint64n(uint64(i + 1)))
		out[i], out[j] = out[j], out[i]
	}
	return out
}

type sha256CounterRNG struct {
	seed    []byte
	counter uint64
	block   [32]byte
	offset  int // next unread byte index in block; 32 means need refill
}

func newSHA256CounterRNG(seed []byte) *sha256CounterRNG {
	return &sha256CounterRNG{seed: seed, offset: 32}
}

func (r *sha256CounterRNG) refill() {
	h := sha256.New()
	_, _ = h.Write(r.seed)
	var ctr [8]byte
	binary.BigEndian.PutUint64(ctr[:], r.counter)
	_, _ = h.Write(ctr[:])
	sum := h.Sum(nil)
	copy(r.block[:], sum)
	r.counter++
	r.offset = 0
}

func (r *sha256CounterRNG) nextUint64() uint64 {
	if r.offset+8 > 32 {
		r.refill()
	}
	v := binary.BigEndian.Uint64(r.block[r.offset : r.offset+8])
	r.offset += 8
	return v
}

// uint64n returns a uniformly distributed value in [0, n).
func (r *sha256CounterRNG) uint64n(n uint64) uint64 {
	if n == 0 {
		return 0
	}
	limit := (^uint64(0) / n) * n
	for {
		v := r.nextUint64()
		if v < limit {
			return v % n
		}
	}
}
