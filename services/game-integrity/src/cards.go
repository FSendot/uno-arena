package main

import (
	"fmt"
	"strings"

	"unoarena/services/game-integrity/domain"
)

// CardDTO is the structured card shape aligned with Room Gameplay DealSource.
type CardDTO struct {
	ID    string `json:"id"`
	Color string `json:"color"`
	Face  string `json:"face"`
}

// DealMaterial is authoritative initial deal material for Room StartMatch.
type DealMaterial struct {
	Hands           map[string][]CardDTO `json:"hands"`
	DiscardTop      CardDTO              `json:"discardTop"`
	ActiveColor     string               `json:"activeColor"`
	CurrentSeat     int                  `json:"currentSeat"`
	Direction       string               `json:"direction"`
	ApplyTopEffects bool                 `json:"applyTopEffects"`
}

const cardSep = "\x1f"

// EncodeCard packs a structured card into an opaque domain.Card.
func EncodeCard(c CardDTO) domain.Card {
	return domain.Card(c.ID + cardSep + c.Color + cardSep + c.Face)
}

// DecodeCard unpacks an opaque domain.Card into a structured CardDTO.
func DecodeCard(c domain.Card) (CardDTO, error) {
	parts := strings.Split(string(c), cardSep)
	if len(parts) != 3 {
		return CardDTO{}, fmt.Errorf("invalid card encoding")
	}
	return CardDTO{ID: parts[0], Color: parts[1], Face: parts[2]}, nil
}

func decodeCards(in []domain.Card) ([]CardDTO, error) {
	out := make([]CardDTO, len(in))
	for i, c := range in {
		dto, err := DecodeCard(c)
		if err != nil {
			return nil, err
		}
		out[i] = dto
	}
	return out, nil
}

// StandardDeckCards returns the canonical 108-card Uno multiset with stable IDs.
// Composition matches Room Gameplay StandardDeckComposition (IDs assigned here).
func StandardDeckCards() []domain.Card {
	colors := []string{"red", "yellow", "green", "blue"}
	faces := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "skip", "reverse", "draw_two"}
	out := make([]domain.Card, 0, 108)
	n := 0
	nextID := func() string {
		n++
		return fmt.Sprintf("c%d", n)
	}
	for _, color := range colors {
		out = append(out, EncodeCard(CardDTO{ID: nextID(), Color: color, Face: "0"}))
		for _, face := range faces {
			out = append(out, EncodeCard(CardDTO{ID: nextID(), Color: color, Face: face}))
			out = append(out, EncodeCard(CardDTO{ID: nextID(), Color: color, Face: face}))
		}
	}
	for i := 0; i < 4; i++ {
		out = append(out, EncodeCard(CardDTO{ID: nextID(), Color: "", Face: "wild"}))
		out = append(out, EncodeCard(CardDTO{ID: nextID(), Color: "", Face: "wild_draw_four"}))
	}
	return out
}

func isWildFace(face string) bool {
	return face == "wild" || face == "wild_draw_four"
}
