package domain

import (
	"errors"
	"sort"
)

// AuthoritativeDeck owns the immutable seed, shuffled order, and draw pointer
// for one game. Pure stdlib; not goroutine-safe — callers serialize access.
type AuthoritativeDeck struct {
	gameID  GameID
	seed    DeckSeed
	order   []Card
	pointer int

	draws map[DrawOperationID]drawRecord
}

type drawRecord struct {
	count       int
	fromPointer int
	outcome     CommandOutcome
}

// DrawRestore is one confirmed draw operation reconstructed from durable state.
type DrawRestore struct {
	Count       int
	Cards       []Card
	FromPointer int
	Revision    Revision
}

// NewAuthoritativeDeck creates one deck for gameID, shuffling cards with seed.
// The seed is stored immutably; cards input is not mutated.
func NewAuthoritativeDeck(gameID GameID, seed DeckSeed, cards []Card) (*AuthoritativeDeck, error) {
	if !gameID.Valid() {
		return nil, errors.New("gameId required")
	}
	if !seed.Valid() {
		return nil, errors.New("deck seed required")
	}
	if len(cards) == 0 {
		return nil, errors.New("deck requires at least one card")
	}
	order := ShuffleCards(seed, cards)
	return &AuthoritativeDeck{
		gameID:  gameID,
		seed:    seed,
		order:   order,
		pointer: 0,
		draws:   map[DrawOperationID]drawRecord{},
	}, nil
}

// RestoreAuthoritativeDeck rebuilds a deck from durable snapshot material without reshuffling.
// Each restored draw must match the deterministic shuffled order at fromPointer/count/cards,
// and the pointer must equal the contiguous end of confirmed draws.
func RestoreAuthoritativeDeck(gameID GameID, seed DeckSeed, order []Card, pointer int, draws map[DrawOperationID]DrawRestore) (*AuthoritativeDeck, error) {
	if !gameID.Valid() {
		return nil, errors.New("gameId required")
	}
	if !seed.Valid() {
		return nil, errors.New("deck seed required")
	}
	if len(order) == 0 {
		return nil, errors.New("deck requires at least one card")
	}
	if pointer < 0 || pointer > len(order) {
		return nil, errors.New("deck pointer out of range")
	}
	if draws == nil {
		return nil, errors.New("draws map required")
	}
	d := &AuthoritativeDeck{
		gameID:  gameID,
		seed:    seed,
		order:   copyCards(order),
		pointer: 0,
		draws:   map[DrawOperationID]drawRecord{},
	}
	type span struct {
		op   DrawOperationID
		from int
		to   int
	}
	spans := make([]span, 0, len(draws))
	for opID, rec := range draws {
		if !opID.Valid() {
			return nil, errors.New("draw restore requires operationId")
		}
		if rec.Count <= 0 || len(rec.Cards) != rec.Count {
			return nil, errors.New("draw restore count/cards mismatch")
		}
		if rec.FromPointer < 0 || rec.FromPointer+rec.Count > len(order) {
			return nil, errors.New("draw restore range exceeds deck")
		}
		want := order[rec.FromPointer : rec.FromPointer+rec.Count]
		if !cardsEqual(want, rec.Cards) {
			return nil, errors.New("draw restore cards mismatch shuffled order")
		}
		rev := rec.Revision
		if rev == 0 {
			rev = Revision(rec.FromPointer + rec.Count)
		}
		if int64(rev) != int64(rec.FromPointer+rec.Count) {
			return nil, errors.New("draw restore revision mismatch")
		}
		fact := cardsDrawnFact(opID, rec.FromPointer, rec.Cards)
		d.draws[opID] = drawRecord{
			count:       rec.Count,
			fromPointer: rec.FromPointer,
			outcome:     acceptedDraw([]Fact{fact}, rec.Cards, rev),
		}
		spans = append(spans, span{op: opID, from: rec.FromPointer, to: rec.FromPointer + rec.Count})
	}
	// Contiguous coverage from 0 without gaps/overlaps.
	sort.Slice(spans, func(i, j int) bool { return spans[i].from < spans[j].from })
	cursor := 0
	for _, s := range spans {
		if s.from != cursor {
			return nil, errors.New("draw restore pointer coverage not contiguous from zero")
		}
		cursor = s.to
	}
	if pointer != cursor {
		return nil, errors.New("deck pointer must equal contiguous confirmed draw end")
	}
	d.pointer = pointer
	return d, nil
}

// EqualOrder reports whether the shuffled order matches other byte-for-byte.
func (d *AuthoritativeDeck) EqualOrder(other []Card) bool {
	if len(d.order) != len(other) {
		return false
	}
	for i := range d.order {
		if d.order[i] != other[i] {
			return false
		}
	}
	return true
}

// DrawIdempotencySnapshot returns a defensive copy of recorded draw outcomes for persistence.
func (d *AuthoritativeDeck) DrawIdempotencySnapshot() map[DrawOperationID]DrawRestore {
	out := make(map[DrawOperationID]DrawRestore, len(d.draws))
	for id, rec := range d.draws {
		out[id] = DrawRestore{
			Count:       rec.count,
			Cards:       copyCards(rec.outcome.Cards),
			FromPointer: rec.fromPointer,
			Revision:    rec.outcome.Revision,
		}
	}
	return out
}

// HasSameDrawIdempotency reports whether draw records match the provided snapshot.
func (d *AuthoritativeDeck) HasSameDrawIdempotency(other map[DrawOperationID]DrawRestore) bool {
	if len(d.draws) != len(other) {
		return false
	}
	for id, rec := range d.draws {
		o, ok := other[id]
		if !ok || o.Count != rec.count || !cardsEqual(o.Cards, rec.outcome.Cards) {
			return false
		}
	}
	return true
}

func cardsEqual(a, b []Card) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (d *AuthoritativeDeck) GameID() GameID { return d.gameID }

// Seed returns the immutable deck seed.
func (d *AuthoritativeDeck) Seed() DeckSeed { return d.seed }

func (d *AuthoritativeDeck) DrawPointer() int { return d.pointer }

func (d *AuthoritativeDeck) Remaining() int {
	rem := len(d.order) - d.pointer
	if rem < 0 {
		return 0
	}
	return rem
}

// ShuffledOrder returns a defensive copy of the full authoritative shuffled order.
func (d *AuthoritativeDeck) ShuffledOrder() []Card {
	return copyCards(d.order)
}

// Peek returns Count cards starting at pointer+offset without advancing the pointer.
// Rejects when the requested window exceeds remaining cards from that offset.
func (d *AuthoritativeDeck) Peek(offset, count int) ([]Card, *Rejection) {
	if count <= 0 {
		rej := Rejection{Code: RejectInvalidCommand, Message: "peek count must be positive"}
		return nil, &rej
	}
	if offset < 0 {
		rej := Rejection{Code: RejectInvalidCommand, Message: "peek offset must be non-negative"}
		return nil, &rej
	}
	start := d.pointer + offset
	if start+count > len(d.order) {
		rej := Rejection{Code: RejectInsufficientCards, Message: "not enough cards remaining"}
		return nil, &rej
	}
	return copyCards(d.order[start : start+count]), nil
}

// Draw advances the sequential pointer by Count cards.
// Same OperationID with same Count returns the prior cards (duplicate).
// Same OperationID with different Count is a conflicting duplicate rejection.
// Insufficient remaining cards reject without mutation.
func (d *AuthoritativeDeck) Draw(cmd DrawCommand) CommandOutcome {
	if prior, ok := d.draws[cmd.OperationID]; ok {
		if prior.count != cmd.Count {
			return rejectedOutcome(Revision(d.pointer), Rejection{
				Code:    RejectConflictingDuplicate,
				Message: "draw operationId reused with different count",
			})
		}
		return duplicateOutcome(prior.outcome)
	}

	if !cmd.OperationID.Valid() {
		return d.reject(Rejection{
			Code:    RejectInvalidIdentity,
			Message: "draw requires operationId",
		})
	}
	if cmd.Count <= 0 {
		return d.reject(Rejection{
			Code:    RejectInvalidCommand,
			Message: "draw count must be positive",
		})
	}
	if cmd.Count > d.Remaining() {
		return d.reject(Rejection{
			Code:    RejectInsufficientCards,
			Message: "not enough cards remaining",
		})
	}

	before := d.pointer
	drawn := copyCards(d.order[d.pointer : d.pointer+cmd.Count])
	d.pointer += cmd.Count

	fact := cardsDrawnFact(cmd.OperationID, before, drawn)
	rev := Revision(d.pointer)
	// Store and return independent defensive copies.
	d.draws[cmd.OperationID] = drawRecord{
		count:       cmd.Count,
		fromPointer: before,
		outcome:     acceptedDraw([]Fact{fact}, drawn, rev),
	}
	return acceptedDraw([]Fact{fact}, drawn, rev)
}

func (d *AuthoritativeDeck) reject(rej Rejection) CommandOutcome {
	return rejectedOutcome(Revision(d.pointer), rej)
}
