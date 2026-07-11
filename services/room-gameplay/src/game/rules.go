package game

// Optional-rule helpers and hand mutations for the pure Uno engine.

// canPlayOrdinary reports whether card may be played on discard with activeColor
// under ordinary turn legality (not jump-in, not stack-specific).
func canPlayOrdinary(card Card, discard Card, activeColor Color) bool {
	if card.Face == FaceWild || card.Face == FaceWildDrawFour {
		return true
	}
	if card.Color == activeColor {
		return true
	}
	if !discard.IsWild() && card.Face == discard.Face {
		return true
	}
	return false
}

// wildDrawFourLegal reports whether the player has no non-WD4 card matching activeColor.
func wildDrawFourLegal(hand []Card, activeColor Color, excludeID CardID) bool {
	for _, c := range hand {
		if c.ID == excludeID {
			continue
		}
		if c.Face == FaceWildDrawFour {
			continue
		}
		if !c.IsWild() && c.Color == activeColor {
			return false
		}
	}
	return true
}

// canStack reports whether the targeted player may stack card onto a pending penalty.
// Draw Two stacks under ordinary legality; Wild Draw Four when legally playable.
func canStack(card Card, hand []Card, activeColor Color, discard Card) bool {
	if card.Face == FaceDrawTwo {
		return canPlayOrdinary(card, discard, activeColor)
	}
	if card.Face == FaceWildDrawFour {
		return wildDrawFourLegal(hand, activeColor, card.ID)
	}
	return false
}

func hasLegalPlay(hand []Card, discard Card, activeColor Color) bool {
	for _, c := range hand {
		if c.Face == FaceWildDrawFour {
			if wildDrawFourLegal(hand, activeColor, c.ID) {
				return true
			}
			continue
		}
		if canPlayOrdinary(c, discard, activeColor) {
			return true
		}
	}
	return false
}

func cloneHand(hand []Card) []Card {
	if hand == nil {
		return nil
	}
	out := make([]Card, len(hand))
	copy(out, hand)
	return out
}

func removeCard(hand []Card, id CardID) ([]Card, Card, bool) {
	for i, c := range hand {
		if c.ID == id {
			card := c
			out := make([]Card, 0, len(hand)-1)
			out = append(out, hand[:i]...)
			out = append(out, hand[i+1:]...)
			return out, card, true
		}
	}
	return hand, Card{}, false
}

func findCard(hand []Card, id CardID) (Card, bool) {
	for _, c := range hand {
		if c.ID == id {
			return c, true
		}
	}
	return Card{}, false
}

func validPlayableColor(c Color) bool {
	switch c {
	case ColorRed, ColorYellow, ColorGreen, ColorBlue:
		return true
	default:
		return false
	}
}
