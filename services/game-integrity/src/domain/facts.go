package domain

import "strings"

// FactName identifies a named domain fact emitted by an accepted transition.
type FactName string

const (
	FactCardsDrawn           FactName = "CardsDrawn"
	FactGameLogEntryAppended FactName = "GameLogEntryAppended"
)

// Fact is a named domain fact produced by an accepted command.
// Rejected commands never produce facts.
type Fact struct {
	Name FactName
	Data map[string]string
}

func newFact(name FactName, data map[string]string) Fact {
	return Fact{Name: name, Data: copyStringMap(data)}
}

func cardsDrawnFact(op DrawOperationID, before int, cards []Card) Fact {
	parts := make([]string, len(cards))
	for i, c := range cards {
		parts[i] = string(c)
	}
	return newFact(FactCardsDrawn, map[string]string{
		"operationId": string(op),
		"cards":       strings.Join(parts, ","),
		"count":       itoa(len(cards)),
		"fromPointer": itoa(before),
	})
}

func logAppendedFact(eventID EventID, offset LogOffset, eventType string) Fact {
	return newFact(FactGameLogEntryAppended, map[string]string{
		"eventId":   string(eventID),
		"eventType": eventType,
		"offset":    u64toa(uint64(offset)),
	})
}

func copyFacts(in []Fact) []Fact {
	if len(in) == 0 {
		return []Fact{}
	}
	out := make([]Fact, len(in))
	for i, f := range in {
		out[i] = Fact{Name: f.Name, Data: copyStringMap(f.Data)}
	}
	return out
}

func copyStringMap(in map[string]string) map[string]string {
	if in == nil {
		return map[string]string{}
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func copyCards(in []Card) []Card {
	if len(in) == 0 {
		return []Card{}
	}
	out := make([]Card, len(in))
	copy(out, in)
	return out
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func u64toa(n uint64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
