package domain

import (
	"bytes"
	"encoding/json"
	"testing"
)

func mustSeed(t *testing.T, material string) DeckSeed {
	t.Helper()
	s, err := NewDeckSeed([]byte(material))
	if err != nil {
		t.Fatalf("NewDeckSeed: %v", err)
	}
	return s
}

func sampleCards() []Card {
	return []Card{"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7"}
}

func TestDeckSeed_ImmutableBytesCopy(t *testing.T) {
	s := mustSeed(t, "seed-a")
	b := s.Bytes()
	b[0] ^= 0xff
	if bytes.Equal(b, s.Bytes()) {
		t.Fatal("mutating Bytes() copy must not change seed")
	}
	s2 := mustSeed(t, "seed-a")
	if !s.Equal(s2) {
		t.Fatal("same material must yield equal seeds")
	}
	if mustSeed(t, "seed-b").Equal(s) {
		t.Fatal("different material must yield different seeds")
	}
}

func TestShuffle_SameSeedSameOrder(t *testing.T) {
	seed := mustSeed(t, "fair-seed")
	cards := sampleCards()
	a := ShuffleCards(seed, cards)
	b := ShuffleCards(seed, cards)
	if len(a) != len(cards) {
		t.Fatalf("len=%d", len(a))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("order mismatch at %d: %s vs %s", i, a[i], b[i])
		}
	}
	orig := sampleCards()
	for i := range cards {
		if cards[i] != orig[i] {
			t.Fatal("ShuffleCards mutated input")
		}
	}
}

func TestShuffle_DifferentSeedDifferentOrder(t *testing.T) {
	cards := sampleCards()
	a := ShuffleCards(mustSeed(t, "seed-1"), cards)
	b := ShuffleCards(mustSeed(t, "seed-2"), cards)
	same := true
	for i := range a {
		if a[i] != b[i] {
			same = false
			break
		}
	}
	if same {
		t.Fatal("different seeds unexpectedly produced identical order")
	}
}

func TestAuthoritativeDeck_DrawSequentialIdempotentAndConflict(t *testing.T) {
	seed := mustSeed(t, "deck-seed")
	d, err := NewAuthoritativeDeck("game-1", seed, sampleCards())
	if err != nil {
		t.Fatal(err)
	}
	if d.DrawPointer() != 0 || d.Remaining() != 8 {
		t.Fatalf("pointer=%d remaining=%d", d.DrawPointer(), d.Remaining())
	}

	first := d.Draw(DrawCommand{OperationID: "op-1", Count: 3})
	if !first.Accepted() || first.Kind != OutcomeAccepted {
		t.Fatalf("first draw: %+v", first)
	}
	if len(first.Cards) != 3 || len(first.Facts) != 1 || first.Facts[0].Name != FactCardsDrawn {
		t.Fatalf("cards/facts: %+v", first)
	}
	if first.Revision != 3 {
		t.Fatalf("revision=%d want 3", first.Revision)
	}
	if d.DrawPointer() != 3 {
		t.Fatalf("pointer=%d want 3", d.DrawPointer())
	}
	want := copyCards(first.Cards)

	dup := d.Draw(DrawCommand{OperationID: "op-1", Count: 3})
	if dup.Kind != OutcomeDuplicate || !dup.Accepted() {
		t.Fatalf("dup: %+v", dup)
	}
	if d.DrawPointer() != 3 {
		t.Fatal("duplicate mutated pointer")
	}
	for i := range want {
		if dup.Cards[i] != want[i] {
			t.Fatalf("dup cards mismatch: %v vs %v", dup.Cards, want)
		}
	}
	if dup.Revision != first.Revision {
		t.Fatalf("dup revision %d vs %d", dup.Revision, first.Revision)
	}

	conflict := d.Draw(DrawCommand{OperationID: "op-1", Count: 2})
	if !conflict.Rejected() || conflict.Rejection.Code != RejectConflictingDuplicate {
		t.Fatalf("conflict: %+v", conflict)
	}
	if d.DrawPointer() != 3 {
		t.Fatal("conflict mutated pointer")
	}

	second := d.Draw(DrawCommand{OperationID: "op-2", Count: 5})
	if !second.Accepted() || d.Remaining() != 0 {
		t.Fatalf("second: %+v remaining=%d", second, d.Remaining())
	}
	if second.Revision != 8 {
		t.Fatalf("revision=%d want 8", second.Revision)
	}

	insuff := d.Draw(DrawCommand{OperationID: "op-3", Count: 1})
	if !insuff.Rejected() || insuff.Rejection.Code != RejectInsufficientCards {
		t.Fatalf("insuff: %+v", insuff)
	}
	if len(insuff.Facts) != 0 {
		t.Fatal("rejection must not emit facts")
	}
}

func TestAuthoritativeDeck_RestorePreservesDrawOutcomeLiterals(t *testing.T) {
	seed := mustSeed(t, "restore-seed")
	d, err := NewAuthoritativeDeck("game-restore", seed, sampleCards())
	if err != nil {
		t.Fatal(err)
	}
	first := d.Draw(DrawCommand{OperationID: "op-restore", Count: 2})
	if !first.Accepted() {
		t.Fatalf("draw: %+v", first)
	}
	snap := d.DrawIdempotencySnapshot()
	rec, ok := snap["op-restore"]
	if !ok || rec.FromPointer != 0 || rec.Revision != 2 || rec.Count != 2 {
		t.Fatalf("snapshot missing original literals: %+v", rec)
	}

	restored, err := RestoreAuthoritativeDeck(d.GameID(), d.Seed(), d.ShuffledOrder(), d.DrawPointer(), snap)
	if err != nil {
		t.Fatal(err)
	}
	dup := restored.Draw(DrawCommand{OperationID: "op-restore", Count: 2})
	if dup.Kind != OutcomeDuplicate {
		t.Fatalf("dup kind: %+v", dup)
	}
	if dup.Revision != first.Revision {
		t.Fatalf("revision %d vs %d", dup.Revision, first.Revision)
	}
	if len(dup.Facts) != 1 || dup.Facts[0].Data["fromPointer"] != "0" {
		t.Fatalf("fromPointer fact: %+v", dup.Facts)
	}
	for i := range first.Cards {
		if dup.Cards[i] != first.Cards[i] {
			t.Fatalf("cards mismatch: %v vs %v", dup.Cards, first.Cards)
		}
	}
	if mustJSONFact(t, dup.Facts[0]) != mustJSONFact(t, first.Facts[0]) {
		t.Fatalf("fact mismatch: %+v vs %+v", dup.Facts[0], first.Facts[0])
	}
}

func mustJSONFact(t *testing.T, f Fact) string {
	t.Helper()
	b, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestAuthoritativeDeck_ShuffledOrderDefensiveCopy(t *testing.T) {
	d, err := NewAuthoritativeDeck("g", mustSeed(t, "s"), sampleCards())
	if err != nil {
		t.Fatal(err)
	}
	order := d.ShuffledOrder()
	order[0] = "MUTATED"
	if d.ShuffledOrder()[0] == "MUTATED" {
		t.Fatal("ShuffledOrder() must return defensive copy")
	}
	drawn := d.Draw(DrawCommand{OperationID: "d1", Count: 1})
	drawn.Cards[0] = "MUTATED"
	again := d.Draw(DrawCommand{OperationID: "d1", Count: 1})
	if again.Cards[0] == "MUTATED" {
		t.Fatal("draw outcome cards must be defensive copies")
	}
}
