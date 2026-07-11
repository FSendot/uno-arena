package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"unoarena/services/game-integrity/domain"
)

func validMinimalSnapshot(t *testing.T) *DeckStateSnapshotV1 {
	t.Helper()
	seedBytes := make([]byte, 32)
	for i := range seedBytes {
		seedBytes[i] = byte(i + 1)
	}
	ds, err := domain.NewDeckSeed(seedBytes)
	if err != nil {
		t.Fatal(err)
	}
	cards := StandardDeckCards()
	deck, err := domain.NewAuthoritativeDeck("g1", ds, cards)
	if err != nil {
		t.Fatal(err)
	}
	out := deck.Draw(domain.DrawCommand{OperationID: "op-draw", Count: 2})
	if out.Rejected() {
		t.Fatal(out.Rejection)
	}
	dtos, err := decodeCards(out.Cards)
	if err != nil {
		t.Fatal(err)
	}
	st := &DeckState{
		Deck:          deck,
		SeedHex:       hex.EncodeToString(seedBytes),
		SeedCommit:    sha256Hex(seedBytes),
		OrderCommit:   orderCommitment(deck.ShuffledOrder()),
		Pending:       map[string]*pendingReservation{},
		ByOp:          map[domain.DrawOperationID]*pendingReservation{},
		Confirmed:     map[domain.DrawOperationID]confirmedOp{},
		ConfirmedByID: map[string]confirmedOp{},
		Cancelled:     map[string]struct{}{},
		CancelledOps:  map[domain.DrawOperationID]struct{}{},
	}
	rec := confirmedOp{
		ReservationID: "res-1", OperationID: "op-draw", Kind: reservationDraw,
		Count: 2, Shape: drawShape(2), Cards: dtos, FromPointer: 0,
	}
	st.Confirmed["op-draw"] = rec
	st.ConfirmedByID["res-1"] = rec
	snap, err := snapshotFromDeckState("room-1", "g1", st)
	if err != nil {
		t.Fatal(err)
	}
	return snap
}

func windowCards(t *testing.T, snap *DeckStateSnapshotV1, count int) []CardDTO {
	t.Helper()
	if snap.Pointer+count > len(snap.Order) {
		t.Fatalf("window exceeds order: pointer=%d count=%d len=%d", snap.Pointer, count, len(snap.Order))
	}
	raw := make([]domain.Card, count)
	for i := 0; i < count; i++ {
		raw[i] = domain.Card(snap.Order[snap.Pointer+i])
	}
	dtos, err := decodeCards(raw)
	if err != nil {
		t.Fatal(err)
	}
	return dtos
}

func expectedDealMaterial(seats []string, perHand int, dtos []CardDTO) DealMaterial {
	hands := make(map[string][]CardDTO, len(seats))
	idx := 0
	for _, seat := range seats {
		hand := make([]CardDTO, perHand)
		copy(hand, dtos[idx:idx+perHand])
		hands[seat] = hand
		idx += perHand
	}
	discard := dtos[idx]
	active := discard.Color
	if isWildFace(discard.Face) {
		active = "red"
	}
	return DealMaterial{
		Hands: hands, DiscardTop: discard, ActiveColor: active,
		CurrentSeat: 0, Direction: "clockwise",
	}
}

func snapshotWithPendingDraw(t *testing.T) *DeckStateSnapshotV1 {
	t.Helper()
	snap := validMinimalSnapshot(t)
	count := 1
	dtos := windowCards(t, snap, count)
	id := "p-draw"
	op := "op-pend-draw"
	snap.Pending[id] = pendingSnapshot{
		ID: id, RoomID: snap.RoomID, GameID: snap.GameID, OperationID: op,
		Kind: string(reservationDraw), Count: count, CardCount: count,
		Cards: cloneCards(dtos), Shape: drawShape(count),
	}
	snap.ByOp[op] = id
	return snap
}

func snapshotWithPendingDeal(t *testing.T) *DeckStateSnapshotV1 {
	t.Helper()
	snap := validMinimalSnapshot(t)
	seats := []string{"seat-a", "seat-b"}
	perHand := 2
	need := len(seats)*perHand + 1
	dtos := windowCards(t, snap, need)
	material := expectedDealMaterial(seats, perHand, dtos)
	id := "p-deal"
	op := "op-pend-deal"
	snap.Pending[id] = pendingSnapshot{
		ID: id, RoomID: snap.RoomID, GameID: snap.GameID, OperationID: op,
		Kind: string(reservationDeal), Seats: append([]string(nil), seats...),
		CardsPerHand: perHand, Count: need, CardCount: need,
		Deal: cloneDealPtr(&material), Shape: dealShape(seats, perHand),
	}
	snap.ByOp[op] = id
	return snap
}

func TestDeckSnapshot_RejectsNilMaps(t *testing.T) {
	base := validMinimalSnapshot(t)
	cases := []struct {
		name string
		mut  func(*DeckStateSnapshotV1)
	}{
		{"nil draws", func(s *DeckStateSnapshotV1) { s.Draws = nil }},
		{"nil pending", func(s *DeckStateSnapshotV1) { s.Pending = nil }},
		{"nil confirmed", func(s *DeckStateSnapshotV1) { s.Confirmed = nil }},
		{"nil confirmedById", func(s *DeckStateSnapshotV1) { s.ConfirmedByID = nil }},
		{"nil cancelled", func(s *DeckStateSnapshotV1) { s.Cancelled = nil }},
		{"nil byOp", func(s *DeckStateSnapshotV1) { s.ByOp = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cp := *base
			tc.mut(&cp)
			if _, err := deckStateFromSnapshot(&cp); err == nil {
				t.Fatal("expected rejection of nil map/slice")
			}
		})
	}
}

func TestDeckSnapshot_EmptySeedAndOrderRejected(t *testing.T) {
	snap := &DeckStateSnapshotV1{
		RoomID: "r", GameID: "g",
		Draws: map[string]deckDrawSnapshot{}, Pending: map[string]pendingSnapshot{},
		ByOp: map[string]string{}, Confirmed: map[string]confirmedSnapshot{},
		ConfirmedByID: map[string]confirmedSnapshot{},
		Cancelled:     []string{}, CancelledOps: []string{},
	}
	_, err := deckStateFromSnapshot(snap)
	if err == nil {
		t.Fatal("empty seed/order must fail closed")
	}
	if !strings.Contains(err.Error(), "missing seed/order") {
		t.Fatalf("error should mention missing seed/order: %v", err)
	}
}

func TestDeckSnapshot_EmptyDeckStateIsEmptyPath(t *testing.T) {
	st := emptyDeckState()
	if st.Deck != nil || st.SeedHex != "" {
		t.Fatal("emptyDeckState must be the uninitialized empty path")
	}
}

func TestDeckSnapshot_RejectsClearedSeedAndOrder(t *testing.T) {
	snap := validMinimalSnapshot(t)
	snap.SeedHex = ""
	snap.Order = nil
	if _, err := deckStateFromSnapshot(snap); err == nil {
		t.Fatal("cleared seed/order on otherwise valid snapshot must fail")
	}
}

func TestDeckSnapshot_RejectsCorruptDrawAgainstOrder(t *testing.T) {
	snap := validMinimalSnapshot(t)
	d := snap.Draws["op-draw"]
	d.Cards = []string{
		string(EncodeCard(CardDTO{ID: "x", Color: "red", Face: "9"})),
		string(EncodeCard(CardDTO{ID: "y", Color: "red", Face: "8"})),
	}
	snap.Draws["op-draw"] = d
	if _, err := deckStateFromSnapshot(snap); err == nil {
		t.Fatal("corrupt draw cards must fail")
	}
}

func TestDeckSnapshot_RejectsPointerNotMatchingConfirmedEnd(t *testing.T) {
	snap := validMinimalSnapshot(t)
	snap.Pointer = 0
	if _, err := deckStateFromSnapshot(snap); err == nil {
		t.Fatal("pointer must equal contiguous confirmed draw end")
	}
}

func TestDeckSnapshot_RejectsByOpMismatch(t *testing.T) {
	snap := validMinimalSnapshot(t)
	snap.Pending = map[string]pendingSnapshot{
		"p1": {ID: "p1", RoomID: "room-1", GameID: "g1", OperationID: "op-pend", Kind: "draw", Count: 1, CardCount: 1, Shape: "draw:1"},
	}
	snap.ByOp = map[string]string{"op-pend": "missing"}
	if _, err := deckStateFromSnapshot(snap); err == nil {
		t.Fatal("byOp must align with pending")
	}
}

func TestDeckSnapshot_RejectsOmittedMapsInJSON(t *testing.T) {
	raw := `{"roomId":"r","gameId":"g","pointer":0,"order":[]}`
	var snap DeckStateSnapshotV1
	if err := json.Unmarshal([]byte(raw), &snap); err != nil {
		t.Fatal(err)
	}
	_, err := deckStateFromSnapshot(&snap)
	if err == nil {
		t.Fatal("omitted maps must fail closed")
	}
	if !strings.Contains(err.Error(), "required") && !strings.Contains(err.Error(), "nil") {
		t.Fatalf("error should mention missing maps: %v", err)
	}
}

func TestDeckSnapshot_RejectsTamperedSeedCommit(t *testing.T) {
	snap := validMinimalSnapshot(t)
	snap.SeedCommit = "deadbeef" + snap.SeedCommit
	if _, err := deckStateFromSnapshot(snap); err == nil {
		t.Fatal("tampered seed commitment must fail")
	}
}

func TestDeckSnapshot_RejectsTamperedOrderCommit(t *testing.T) {
	snap := validMinimalSnapshot(t)
	snap.OrderCommit = "deadbeef" + snap.OrderCommit
	if _, err := deckStateFromSnapshot(snap); err == nil {
		t.Fatal("tampered order commitment must fail")
	}
}

func TestDeckSnapshot_RejectsTamperedPendingDrawCards(t *testing.T) {
	snap := snapshotWithPendingDraw(t)
	p := snap.Pending["p-draw"]
	tampered := cloneCards(p.Cards)
	tampered[0] = CardDTO{ID: "tampered", Color: "red", Face: "9"}
	p.Cards = tampered
	snap.Pending["p-draw"] = p
	if _, err := deckStateFromSnapshot(snap); err == nil {
		t.Fatal("tampered pending draw cards must fail")
	}
}

func TestDeckSnapshot_RejectsTamperedPendingDealMaterial(t *testing.T) {
	snap := snapshotWithPendingDeal(t)
	p := snap.Pending["p-deal"]
	deal := cloneDealPtr(p.Deal)
	deal.DiscardTop = CardDTO{ID: "tampered", Color: "red", Face: "9"}
	p.Deal = deal
	snap.Pending["p-deal"] = p
	if _, err := deckStateFromSnapshot(snap); err == nil {
		t.Fatal("tampered pending deal material must fail")
	}

	snap = snapshotWithPendingDeal(t)
	p = snap.Pending["p-deal"]
	deal = cloneDealPtr(p.Deal)
	seat := p.Seats[0]
	hand := cloneCards(deal.Hands[seat])
	hand[0] = CardDTO{ID: "tampered-hand", Color: "blue", Face: "1"}
	deal.Hands[seat] = hand
	p.Deal = deal
	snap.Pending["p-deal"] = p
	if _, err := deckStateFromSnapshot(snap); err == nil {
		t.Fatal("tampered pending deal hands must fail")
	}
}

func TestDeckSnapshot_RestoresValidPendingDrawAndDeal(t *testing.T) {
	if _, err := deckStateFromSnapshot(snapshotWithPendingDraw(t)); err != nil {
		t.Fatalf("valid pending draw must restore: %v", err)
	}
	if _, err := deckStateFromSnapshot(snapshotWithPendingDeal(t)); err != nil {
		t.Fatalf("valid pending deal must restore: %v", err)
	}
}

func TestDeckSnapshot_RejectsOversizedPendingDrawCardCount(t *testing.T) {
	snap := snapshotWithPendingDraw(t)
	p := snap.Pending["p-draw"]
	// Huge CardCount must fail closed without panic/OOM (overflow-safe bounds).
	p.CardCount = int(^uint(0) >> 1) // MaxInt
	p.Count = p.CardCount
	p.Shape = ""
	snap.Pending["p-draw"] = p
	if _, err := deckStateFromSnapshot(snap); err == nil {
		t.Fatal("oversized pending draw cardCount must fail")
	}
}

func TestDeckSnapshot_RejectsPendingDrawExceedingRemainingDeck(t *testing.T) {
	snap := snapshotWithPendingDraw(t)
	remaining := len(snap.Order) - snap.Pointer
	p := snap.Pending["p-draw"]
	p.CardCount = remaining + 1
	p.Count = p.CardCount
	p.Shape = ""
	p.Cards = nil
	snap.Pending["p-draw"] = p
	if _, err := deckStateFromSnapshot(snap); err == nil {
		t.Fatal("pending draw exceeding remaining deck must fail")
	}
}

func TestDeckSnapshot_RejectsDealWithNonPositiveSeatsOrCardsPerHand(t *testing.T) {
	snap := snapshotWithPendingDeal(t)
	p := snap.Pending["p-deal"]
	p.Seats = nil
	p.CardsPerHand = 2
	p.CardCount = 1
	p.Count = 1
	p.Shape = ""
	snap.Pending["p-deal"] = p
	if _, err := deckStateFromSnapshot(snap); err == nil {
		t.Fatal("deal with empty seats must fail")
	}

	snap = snapshotWithPendingDeal(t)
	p = snap.Pending["p-deal"]
	p.CardsPerHand = 0
	p.Shape = ""
	snap.Pending["p-deal"] = p
	if _, err := deckStateFromSnapshot(snap); err == nil {
		t.Fatal("deal with non-positive cardsPerHand must fail")
	}
}

func TestDeckSnapshot_RejectsDealOverflowCardCount(t *testing.T) {
	snap := snapshotWithPendingDeal(t)
	p := snap.Pending["p-deal"]
	// Enormous seats * cardsPerHand must fail closed (checked multiply), not panic.
	p.Seats = make([]string, 1<<20)
	for i := range p.Seats {
		p.Seats[i] = "s"
	}
	p.CardsPerHand = 1 << 20
	p.CardCount = int(^uint(0) >> 1)
	p.Count = p.CardCount
	p.Shape = ""
	snap.Pending["p-deal"] = p
	if _, err := deckStateFromSnapshot(snap); err == nil {
		t.Fatal("deal with overflow-ish seats*cardsPerHand must fail")
	}
}

func snapshotWithConfirmedDeal(t *testing.T) *DeckStateSnapshotV1 {
	t.Helper()
	seedBytes := make([]byte, 32)
	for i := range seedBytes {
		seedBytes[i] = byte(i + 3)
	}
	ds, err := domain.NewDeckSeed(seedBytes)
	if err != nil {
		t.Fatal(err)
	}
	deck, err := domain.NewAuthoritativeDeck("g-deal", ds, StandardDeckCards())
	if err != nil {
		t.Fatal(err)
	}
	seats := []string{"seat-a", "seat-b"}
	perHand := 2
	need := len(seats)*perHand + 1
	out := deck.Draw(domain.DrawCommand{OperationID: "op-deal", Count: need})
	if out.Rejected() {
		t.Fatal(out.Rejection)
	}
	dtos, err := decodeCards(out.Cards)
	if err != nil {
		t.Fatal(err)
	}
	material := expectedDealMaterial(seats, perHand, dtos)
	st := &DeckState{
		Deck:          deck,
		SeedHex:       hex.EncodeToString(seedBytes),
		SeedCommit:    sha256Hex(seedBytes),
		OrderCommit:   orderCommitment(deck.ShuffledOrder()),
		Pending:       map[string]*pendingReservation{},
		ByOp:          map[domain.DrawOperationID]*pendingReservation{},
		Confirmed:     map[domain.DrawOperationID]confirmedOp{},
		ConfirmedByID: map[string]confirmedOp{},
		Cancelled:     map[string]struct{}{},
		CancelledOps:  map[domain.DrawOperationID]struct{}{},
	}
	rec := confirmedOp{
		ReservationID: "res-deal", OperationID: "op-deal", Kind: reservationDeal,
		Seats: append([]string(nil), seats...), CardsPerHand: perHand,
		Count: need, Shape: dealShape(seats, perHand),
		Deal: cloneDealPtr(&material), FromPointer: 0,
	}
	st.Confirmed["op-deal"] = rec
	st.ConfirmedByID["res-deal"] = rec
	snap, err := snapshotFromDeckState("room-deal", "g-deal", st)
	if err != nil {
		t.Fatal(err)
	}
	return snap
}

func TestDeckSnapshot_RestoresValidConfirmedDeal(t *testing.T) {
	if _, err := deckStateFromSnapshot(snapshotWithConfirmedDeal(t)); err != nil {
		t.Fatalf("valid confirmed deal must restore: %v", err)
	}
}

func TestDeckSnapshot_RejectsTamperedConfirmedDealMaterial(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*confirmedSnapshot)
	}{
		{"activeColor", func(c *confirmedSnapshot) { c.Deal.ActiveColor = "blue" }},
		{"direction", func(c *confirmedSnapshot) { c.Deal.Direction = "counterclockwise" }},
		{"currentSeat", func(c *confirmedSnapshot) { c.Deal.CurrentSeat = 1 }},
		{"applyTopEffects", func(c *confirmedSnapshot) { c.Deal.ApplyTopEffects = true }},
		{"discardTop", func(c *confirmedSnapshot) {
			c.Deal.DiscardTop = CardDTO{ID: "tampered", Color: "red", Face: "9"}
		}},
		{"hand grouping", func(c *confirmedSnapshot) {
			seat := c.Seats[0]
			hand := cloneCards(c.Deal.Hands[seat])
			hand[0] = CardDTO{ID: "tampered-hand", Color: "blue", Face: "1"}
			c.Deal.Hands[seat] = hand
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snap := snapshotWithConfirmedDeal(t)
			c := snap.Confirmed["op-deal"]
			c.Deal = cloneDealPtr(c.Deal)
			tc.mut(&c)
			snap.Confirmed["op-deal"] = c
			snap.ConfirmedByID["res-deal"] = c
			if _, err := deckStateFromSnapshot(snap); err == nil {
				t.Fatalf("tampered confirmed deal %s must fail", tc.name)
			}
		})
	}
}

func TestDeckSnapshot_RejectsBlankPendingAndConfirmedShapes(t *testing.T) {
	snap := snapshotWithPendingDraw(t)
	p := snap.Pending["p-draw"]
	p.Shape = ""
	snap.Pending["p-draw"] = p
	if _, err := deckStateFromSnapshot(snap); err == nil {
		t.Fatal("blank pending draw shape must fail")
	}

	snap = snapshotWithPendingDeal(t)
	p = snap.Pending["p-deal"]
	p.Shape = ""
	snap.Pending["p-deal"] = p
	if _, err := deckStateFromSnapshot(snap); err == nil {
		t.Fatal("blank pending deal shape must fail")
	}

	snap = validMinimalSnapshot(t)
	c := snap.Confirmed["op-draw"]
	c.Shape = ""
	snap.Confirmed["op-draw"] = c
	snap.ConfirmedByID["res-1"] = c
	if _, err := deckStateFromSnapshot(snap); err == nil {
		t.Fatal("blank confirmed draw shape must fail")
	}

	snap = snapshotWithConfirmedDeal(t)
	c = snap.Confirmed["op-deal"]
	c.Shape = ""
	snap.Confirmed["op-deal"] = c
	snap.ConfirmedByID["res-deal"] = c
	if _, err := deckStateFromSnapshot(snap); err == nil {
		t.Fatal("blank confirmed deal shape must fail")
	}
}

func TestDeckSnapshot_RejectsMultiplePendingReservations(t *testing.T) {
	snap := snapshotWithPendingDraw(t)
	// Second pending that also claims the same current deck window.
	count := 1
	dtos := windowCards(t, snap, count)
	id := "p-draw-2"
	op := "op-pend-draw-2"
	snap.Pending[id] = pendingSnapshot{
		ID: id, RoomID: snap.RoomID, GameID: snap.GameID, OperationID: op,
		Kind: string(reservationDraw), Count: count, CardCount: count,
		Cards: cloneCards(dtos), Shape: drawShape(count),
	}
	snap.ByOp[op] = id
	if _, err := deckStateFromSnapshot(snap); err == nil {
		t.Fatal("multiple pending reservations consuming same deck window must fail")
	}
}

func TestDeckSnapshot_RejectsConfirmedByIDMaterialMismatch(t *testing.T) {
	snap := snapshotWithConfirmedDeal(t)
	byID := snap.ConfirmedByID["res-deal"]
	byID.Deal = cloneDealPtr(byID.Deal)
	byID.Deal.ActiveColor = "yellow"
	snap.ConfirmedByID["res-deal"] = byID
	if _, err := deckStateFromSnapshot(snap); err == nil {
		t.Fatal("confirmedById material drift from confirmed must fail")
	}
}

func TestDealShape_IsInjectiveForCommaCollidingSeatVectors(t *testing.T) {
	a := dealShape([]string{"a,b", "c"}, 7)
	b := dealShape([]string{"a", "b,c"}, 7)
	if a == b {
		t.Fatalf("comma-joined shapes must not collide: %q", a)
	}
	if strings.Contains(a, "a,b,c") || strings.Contains(b, "a,b,c") {
		t.Fatalf("shape must not use bare comma-joined seats: a=%q b=%q", a, b)
	}
}

func TestDealShape_PreservesSeatOrder(t *testing.T) {
	fwd := dealShape([]string{"seat-a", "seat-b"}, 7)
	rev := dealShape([]string{"seat-b", "seat-a"}, 7)
	if fwd == rev {
		t.Fatal("deal shape must preserve seat order semantics")
	}
}

func TestDeckSnapshot_RejectsBlankDuplicateAndTooFewDealSeats(t *testing.T) {
	cases := []struct {
		name  string
		seats []string
	}{
		{"too few", []string{"only"}},
		{"blank", []string{"a", ""}},
		{"whitespace blank", []string{"a", "  "}},
		{"duplicate", []string{"a", "a"}},
	}
	for _, tc := range cases {
		t.Run(tc.name+" pending", func(t *testing.T) {
			snap := snapshotWithPendingDeal(t)
			p := snap.Pending["p-deal"]
			perHand := p.CardsPerHand
			if perHand <= 0 {
				perHand = 2
			}
			p.Seats = append([]string(nil), tc.seats...)
			need, ok := checkedDealNeed(len(p.Seats), perHand)
			if ok {
				p.CardsPerHand = perHand
				p.Count = need
				p.CardCount = need
				dtos := windowCards(t, snap, need)
				mat := expectedDealMaterial(p.Seats, perHand, dtos)
				p.Deal = &mat
			}
			p.Shape = dealShape(p.Seats, perHand)
			snap.Pending["p-deal"] = p
			if _, err := deckStateFromSnapshot(snap); err == nil {
				t.Fatalf("pending deal seats %v must fail", tc.seats)
			}
		})
		t.Run(tc.name+" confirmed", func(t *testing.T) {
			snap := snapshotWithConfirmedDeal(t)
			c := snap.Confirmed["op-deal"]
			perHand := c.CardsPerHand
			if perHand <= 0 {
				perHand = 2
			}
			c.Seats = append([]string(nil), tc.seats...)
			need, ok := checkedDealNeed(len(c.Seats), perHand)
			if ok {
				c.CardsPerHand = perHand
				c.Count = need
				from := c.FromPointer
				raw := make([]string, need)
				copy(raw, snap.Order[from:from+need])
				dtos := make([]CardDTO, need)
				for i, enc := range raw {
					d, err := decodeCards([]domain.Card{domain.Card(enc)})
					if err != nil {
						t.Fatal(err)
					}
					dtos[i] = d[0]
				}
				mat := expectedDealMaterial(c.Seats, perHand, dtos)
				c.Deal = &mat
				snap.Draws["op-deal"] = deckDrawSnapshot{
					Count: need, Cards: raw, FromPointer: from, Revision: int64(from + need),
				}
				snap.Pointer = from + need
			}
			c.Shape = dealShape(c.Seats, perHand)
			snap.Confirmed["op-deal"] = c
			snap.ConfirmedByID["res-deal"] = c
			if _, err := deckStateFromSnapshot(snap); err == nil {
				t.Fatalf("confirmed deal seats %v must fail", tc.seats)
			}
		})
	}
}

func TestDeckSnapshot_RejectsNonCanonicalDealShape(t *testing.T) {
	snap := snapshotWithPendingDeal(t)
	p := snap.Pending["p-deal"]
	p.Shape = fmt.Sprintf("deal|%s|%d", strings.Join(p.Seats, ","), p.CardsPerHand)
	snap.Pending["p-deal"] = p
	if _, err := deckStateFromSnapshot(snap); err == nil {
		t.Fatal("legacy comma-joined pending deal shape must fail exact canonical check")
	}

	snap = snapshotWithConfirmedDeal(t)
	c := snap.Confirmed["op-deal"]
	c.Shape = fmt.Sprintf("deal|%s|%d", strings.Join(c.Seats, ","), c.CardsPerHand)
	snap.Confirmed["op-deal"] = c
	snap.ConfirmedByID["res-deal"] = c
	if _, err := deckStateFromSnapshot(snap); err == nil {
		t.Fatal("legacy comma-joined confirmed deal shape must fail exact canonical check")
	}
}

func TestDeckSnapshot_RejectsExtraDrawWithoutConfirmed(t *testing.T) {
	snap := validMinimalSnapshot(t)
	// Contiguous extra draw at pointer end without confirmed material.
	from := snap.Pointer
	raw := []string{snap.Order[from], snap.Order[from+1]}
	snap.Draws["op-extra"] = deckDrawSnapshot{
		Count: 2, Cards: raw, FromPointer: from, Revision: int64(from + 2),
	}
	snap.Pointer = from + 2
	if _, err := deckStateFromSnapshot(snap); err == nil {
		t.Fatal("extra draws entry without confirmed op must fail")
	}
}

func TestDeckSnapshot_RejectsConfirmedMissingDraw(t *testing.T) {
	snap := validMinimalSnapshot(t)
	delete(snap.Draws, "op-draw")
	if _, err := deckStateFromSnapshot(snap); err == nil {
		t.Fatal("confirmed op missing draws entry must fail")
	}
}

func TestDeckSnapshot_RejectsPendingOperationAppearingInDraws(t *testing.T) {
	snap := snapshotWithPendingDraw(t)
	p := snap.Pending["p-draw"]
	from := snap.Pointer
	cards := make([]string, len(p.Cards))
	for i, c := range p.Cards {
		cards[i] = string(EncodeCard(c))
	}
	// Keep pointer at pending window start so RestoreAuthoritativeDeck would accept
	// draws coverage alone; pending+draws for same op must still fail closed.
	snap.Draws[p.OperationID] = deckDrawSnapshot{
		Count: p.Count, Cards: cards, FromPointer: from, Revision: int64(from + p.Count),
	}
	// Also add matching confirmed? No — pending must not be in Draws even without confirm.
	// Pointer stays at from so contiguous draws end at from+count would require pointer bump;
	// bump pointer to match draws so only the pending-in-draws rule should fire after 1:1 fix.
	snap.Pointer = from + p.Count
	// Remove confirmed mismatch path: ensure no confirmed for this op (already true for pending-only snap).
	if _, err := deckStateFromSnapshot(snap); err == nil {
		t.Fatal("pending reservation must not appear in Draws until confirmed")
	}
}

func TestDeckSnapshot_RejectsDrawsConfirmedCardinalityMismatch(t *testing.T) {
	snap := validMinimalSnapshot(t)
	from := snap.Pointer
	snap.Draws["op-orphan-draw"] = deckDrawSnapshot{
		Count: 1, Cards: []string{snap.Order[from]}, FromPointer: from, Revision: int64(from + 1),
	}
	snap.Pointer = from + 1
	if _, err := deckStateFromSnapshot(snap); err == nil {
		t.Fatal("draws without matching confirmed must fail one-to-one mapping")
	}
}
