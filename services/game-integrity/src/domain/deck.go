package domain

import "errors"

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
	count   int
	outcome CommandOutcome
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
		count:   cmd.Count,
		outcome: acceptedDraw([]Fact{fact}, drawn, rev),
	}
	return acceptedDraw([]Fact{fact}, drawn, rev)
}

func (d *AuthoritativeDeck) reject(rej Rejection) CommandOutcome {
	return rejectedOutcome(Revision(d.pointer), rej)
}
