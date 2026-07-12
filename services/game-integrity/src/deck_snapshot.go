package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"unoarena/services/game-integrity/domain"
)

const deckSnapshotEventType = "DeckStateSnapshotV1"

// DeckStateSnapshotV1 is the durable encrypted deck aggregate snapshot.
type DeckStateSnapshotV1 struct {
	RoomID        string                       `json:"roomId"`
	GameID        string                       `json:"gameId"`
	SeedHex       string                       `json:"seedHex"`
	SeedCommit    string                       `json:"seedCommitment"`
	OrderCommit   string                       `json:"orderCommitment"`
	Pointer       int                          `json:"pointer"`
	Order         []string                     `json:"order"`
	Draws         map[string]deckDrawSnapshot  `json:"draws"`
	Pending       map[string]pendingSnapshot   `json:"pending"`
	ByOp          map[string]string            `json:"byOp"`
	Confirmed     map[string]confirmedSnapshot `json:"confirmed"`
	ConfirmedByID map[string]confirmedSnapshot `json:"confirmedById"`
	Cancelled     []string                     `json:"cancelled"`
	CancelledOps  []string                     `json:"cancelledOps"`
}

type deckDrawSnapshot struct {
	Count       int      `json:"count"`
	Cards       []string `json:"cards"`
	FromPointer int      `json:"fromPointer"`
	Revision    int64    `json:"revision"`
}

type pendingSnapshot struct {
	ID             string        `json:"id"`
	RoomID         string        `json:"roomId"`
	GameID         string        `json:"gameId"`
	OperationID    string        `json:"operationId"`
	Kind           string        `json:"kind"`
	Seats          []string      `json:"seats,omitempty"`
	CardsPerHand   int           `json:"cardsPerHand,omitempty"`
	Count          int           `json:"count,omitempty"`
	CardCount      int           `json:"cardCount,omitempty"`
	Deal           *DealMaterial `json:"deal,omitempty"`
	Cards          []CardDTO     `json:"cards,omitempty"`
	Shape          string        `json:"shape"`
	RemainingAfter int           `json:"remainingAfter"`
}

type confirmedSnapshot struct {
	ReservationID  string        `json:"reservationId"`
	OperationID    string        `json:"operationId"`
	Kind           string        `json:"kind"`
	Seats          []string      `json:"seats,omitempty"`
	CardsPerHand   int           `json:"cardsPerHand,omitempty"`
	Count          int           `json:"count,omitempty"`
	Shape          string        `json:"shape"`
	Deal           *DealMaterial `json:"deal,omitempty"`
	Cards          []CardDTO     `json:"cards,omitempty"`
	FromPointer    int           `json:"fromPointer"`
	RemainingAfter int           `json:"remainingAfter"`
}

func snapshotFromDeckState(roomID domain.RoomID, gameID domain.GameID, st *DeckState) (*DeckStateSnapshotV1, error) {
	if st == nil {
		return nil, fmt.Errorf("nil deck state")
	}
	snap := &DeckStateSnapshotV1{
		RoomID:        string(roomID),
		GameID:        string(gameID),
		SeedHex:       st.SeedHex,
		SeedCommit:    st.SeedCommit,
		OrderCommit:   st.OrderCommit,
		Draws:         map[string]deckDrawSnapshot{},
		Pending:       map[string]pendingSnapshot{},
		ByOp:          map[string]string{},
		Confirmed:     map[string]confirmedSnapshot{},
		ConfirmedByID: map[string]confirmedSnapshot{},
		Cancelled:     make([]string, 0, len(st.Cancelled)),
		CancelledOps:  make([]string, 0, len(st.CancelledOps)),
	}
	for id := range st.Cancelled {
		snap.Cancelled = append(snap.Cancelled, id)
	}
	sort.Strings(snap.Cancelled)
	for op := range st.CancelledOps {
		snap.CancelledOps = append(snap.CancelledOps, string(op))
	}
	sort.Strings(snap.CancelledOps)
	if st.Deck != nil {
		snap.Pointer = st.Deck.DrawPointer()
		order := st.Deck.ShuffledOrder()
		snap.Order = make([]string, len(order))
		for i, c := range order {
			snap.Order[i] = string(c)
		}
		for opID, rec := range st.Deck.DrawIdempotencySnapshot() {
			cards := make([]string, len(rec.Cards))
			for i, c := range rec.Cards {
				cards[i] = string(c)
			}
			snap.Draws[string(opID)] = deckDrawSnapshot{
				Count: rec.Count, Cards: cards,
				FromPointer: rec.FromPointer, Revision: int64(rec.Revision),
			}
		}
	}
	for id, p := range st.Pending {
		snap.Pending[id] = pendingToSnapshot(p)
	}
	for op, p := range st.ByOp {
		snap.ByOp[string(op)] = p.ID
	}
	for op, c := range st.Confirmed {
		snap.Confirmed[string(op)] = confirmedToSnapshot(c)
	}
	for id, c := range st.ConfirmedByID {
		snap.ConfirmedByID[id] = confirmedToSnapshot(c)
	}
	return snap, nil
}

func pendingToSnapshot(p *pendingReservation) pendingSnapshot {
	return pendingSnapshot{
		ID: p.ID, RoomID: string(p.RoomID), GameID: string(p.GameID),
		OperationID: string(p.OperationID), Kind: string(p.Kind),
		Seats: append([]string(nil), p.Seats...), CardsPerHand: p.CardsPerHand,
		Count: p.Count, CardCount: p.CardCount, Deal: cloneDealPtr(p.Deal),
		Cards: cloneCards(p.Cards), Shape: p.Shape, RemainingAfter: p.RemainingAfter,
	}
}

func confirmedToSnapshot(c confirmedOp) confirmedSnapshot {
	return confirmedSnapshot{
		ReservationID: c.ReservationID, OperationID: string(c.OperationID), Kind: string(c.Kind),
		Seats: append([]string(nil), c.Seats...), CardsPerHand: c.CardsPerHand,
		Count: c.Count, Shape: c.Shape, Deal: cloneDealPtr(c.Deal),
		Cards: cloneCards(c.Cards), FromPointer: c.FromPointer, RemainingAfter: c.RemainingAfter,
	}
}

func deckStateFromSnapshot(snap *DeckStateSnapshotV1) (*DeckState, error) {
	if snap == nil {
		return nil, fmt.Errorf("nil snapshot")
	}
	if err := validateSnapshotMapsPresent(snap); err != nil {
		return nil, err
	}
	if snap.SeedHex == "" && len(snap.Order) == 0 {
		return nil, fmt.Errorf("deck snapshot missing seed/order")
	}
	st := &DeckState{
		SeedHex:       snap.SeedHex,
		SeedCommit:    snap.SeedCommit,
		OrderCommit:   snap.OrderCommit,
		Pending:       map[string]*pendingReservation{},
		ByOp:          map[domain.DrawOperationID]*pendingReservation{},
		Confirmed:     map[domain.DrawOperationID]confirmedOp{},
		ConfirmedByID: map[string]confirmedOp{},
		Cancelled:     map[string]struct{}{},
		CancelledOps:  map[domain.DrawOperationID]struct{}{},
	}
	for _, id := range snap.Cancelled {
		st.Cancelled[id] = struct{}{}
	}
	for _, op := range snap.CancelledOps {
		st.CancelledOps[domain.DrawOperationID(op)] = struct{}{}
	}
	if snap.SeedHex != "" || len(snap.Order) > 0 {
		seedBytes, err := hex.DecodeString(snap.SeedHex)
		if err != nil {
			return nil, fmt.Errorf("seed hex: %w", err)
		}
		if snap.SeedCommit != sha256Hex(seedBytes) {
			return nil, fmt.Errorf("seed commitment mismatch")
		}
		seed, err := domain.NewDeckSeed(seedBytes)
		if err != nil {
			return nil, err
		}
		authDeck, err := domain.NewAuthoritativeDeck(domain.GameID(snap.GameID), seed, StandardDeckCards())
		if err != nil {
			return nil, err
		}
		authOrder := authDeck.ShuffledOrder()
		if snap.OrderCommit != orderCommitment(authOrder) {
			return nil, fmt.Errorf("order commitment mismatch")
		}
		if len(snap.Order) != len(authOrder) {
			return nil, fmt.Errorf("order length mismatch standard deck")
		}
		for i, c := range authOrder {
			if snap.Order[i] != string(c) {
				return nil, fmt.Errorf("order mismatch authoritative shuffle")
			}
		}
		if snap.Pointer < 0 || snap.Pointer > len(authOrder) {
			return nil, fmt.Errorf("deck pointer out of range")
		}
		order := make([]domain.Card, len(authOrder))
		copy(order, authOrder)
		draws := make(map[domain.DrawOperationID]domain.DrawRestore, len(snap.Draws))
		for op, rec := range snap.Draws {
			cards := make([]domain.Card, len(rec.Cards))
			for i, c := range rec.Cards {
				cards[i] = domain.Card(c)
			}
			draws[domain.DrawOperationID(op)] = domain.DrawRestore{
				Count: rec.Count, Cards: cards,
				FromPointer: rec.FromPointer, Revision: domain.Revision(rec.Revision),
			}
		}
		deck, err := domain.RestoreAuthoritativeDeck(domain.GameID(snap.GameID), seed, order, snap.Pointer, draws)
		if err != nil {
			return nil, err
		}
		st.Deck = deck
	}
	if len(snap.Pending) > 1 {
		return nil, fmt.Errorf("multiple pending reservations not allowed")
	}
	for id, p := range snap.Pending {
		pending := snapshotToPending(p)
		if pending.ID != id {
			return nil, fmt.Errorf("pending map key/id mismatch")
		}
		if err := validatePendingAgainstDeck(snap, pending); err != nil {
			return nil, err
		}
		st.Pending[id] = pending
	}
	for op, resID := range snap.ByOp {
		pending, ok := st.Pending[resID]
		if !ok {
			return nil, fmt.Errorf("byOp references missing pending %s", resID)
		}
		if string(pending.OperationID) != op {
			return nil, fmt.Errorf("byOp identity mismatch for %s", op)
		}
		st.ByOp[domain.DrawOperationID(op)] = pending
	}
	if len(snap.ByOp) != len(snap.Pending) {
		return nil, fmt.Errorf("byOp/pending cardinality mismatch")
	}
	for _, p := range snap.Pending {
		if _, ok := snap.Draws[p.OperationID]; ok {
			return nil, fmt.Errorf("pending reservation must not appear in draws")
		}
	}
	for op, c := range snap.Confirmed {
		conf := snapshotToConfirmed(c)
		if string(conf.OperationID) != op {
			return nil, fmt.Errorf("confirmed operationId mismatch")
		}
		if err := validateConfirmedAgainstDraws(snap, conf); err != nil {
			return nil, err
		}
		st.Confirmed[domain.DrawOperationID(op)] = conf
	}
	for id, c := range snap.ConfirmedByID {
		conf := snapshotToConfirmed(c)
		if conf.ReservationID != id {
			return nil, fmt.Errorf("confirmedById key/reservation mismatch")
		}
		byOp, ok := st.Confirmed[conf.OperationID]
		if !ok {
			return nil, fmt.Errorf("confirmedById missing confirmed op %s", conf.OperationID)
		}
		if !confirmedOpsEqual(byOp, conf) {
			return nil, fmt.Errorf("confirmedById inconsistent with confirmed")
		}
		st.ConfirmedByID[id] = conf
	}
	if len(snap.Confirmed) != len(snap.ConfirmedByID) {
		return nil, fmt.Errorf("confirmed/confirmedById cardinality mismatch")
	}
	if len(snap.Draws) != len(snap.Confirmed) {
		return nil, fmt.Errorf("draws/confirmed cardinality mismatch")
	}
	for op := range snap.Draws {
		if _, ok := snap.Confirmed[op]; !ok {
			return nil, fmt.Errorf("draws entry missing confirmed op")
		}
	}
	return st, nil
}

func validatePendingAgainstDeck(snap *DeckStateSnapshotV1, pending *pendingReservation) error {
	if snap.Pointer < 0 || snap.Pointer > len(snap.Order) {
		return fmt.Errorf("deck pointer out of range")
	}
	remaining := len(snap.Order) - snap.Pointer
	switch pending.Kind {
	case reservationDraw:
		if pending.CardCount <= 0 {
			return fmt.Errorf("pending draw cardCount must be positive")
		}
		if pending.Count != pending.CardCount {
			return fmt.Errorf("pending draw count/cardCount mismatch")
		}
		if pending.Shape == "" || pending.Shape != drawShape(pending.Count) {
			return fmt.Errorf("pending draw shape mismatch")
		}
		if pending.CardCount > remaining {
			return fmt.Errorf("pending reservation exceeds deck")
		}
		wantRaw := make([]domain.Card, pending.CardCount)
		for i := 0; i < pending.CardCount; i++ {
			wantRaw[i] = domain.Card(snap.Order[snap.Pointer+i])
		}
		want, err := decodeCards(wantRaw)
		if err != nil {
			return fmt.Errorf("pending draw window decode: %w", err)
		}
		if !cardDTOsEqual(pending.Cards, want) {
			return fmt.Errorf("pending draw cards mismatch deck window")
		}
		if pending.RemainingAfter < 0 || pending.RemainingAfter != remaining-pending.CardCount {
			return fmt.Errorf("pending remainingAfter mismatch")
		}
	case reservationDeal:
		if err := validateDealSeatsList(pending.Seats); err != nil {
			return err
		}
		if pending.CardsPerHand <= 0 {
			return fmt.Errorf("pending deal cardsPerHand must be positive")
		}
		need, ok := checkedDealNeed(len(pending.Seats), pending.CardsPerHand)
		if !ok {
			return fmt.Errorf("pending deal cardCount overflow")
		}
		if pending.CardCount <= 0 {
			return fmt.Errorf("pending deal cardCount must be positive")
		}
		if pending.CardCount != need || pending.Count != need {
			return fmt.Errorf("pending deal cardCount mismatch")
		}
		if pending.Shape == "" || pending.Shape != dealShape(pending.Seats, pending.CardsPerHand) {
			return fmt.Errorf("pending deal shape mismatch")
		}
		if pending.CardCount > remaining {
			return fmt.Errorf("pending reservation exceeds deck")
		}
		if pending.Deal == nil {
			return fmt.Errorf("pending deal missing deal material")
		}
		wantRaw := make([]domain.Card, pending.CardCount)
		for i := 0; i < pending.CardCount; i++ {
			wantRaw[i] = domain.Card(snap.Order[snap.Pointer+i])
		}
		dtos, err := decodeCards(wantRaw)
		if err != nil {
			return fmt.Errorf("pending deal window decode: %w", err)
		}
		want := reconstructDealMaterial(pending.Seats, pending.CardsPerHand, dtos)
		if !dealMaterialsEqual(pending.Deal, &want) {
			return fmt.Errorf("pending deal material mismatch deck window")
		}
		if pending.RemainingAfter < 0 || pending.RemainingAfter != remaining-pending.CardCount {
			return fmt.Errorf("pending remainingAfter mismatch")
		}
	default:
		return fmt.Errorf("unknown pending kind %q", pending.Kind)
	}
	return nil
}

// checkedDealNeed computes seats*cardsPerHand+1 without overflowing int.
func checkedDealNeed(seats, cardsPerHand int) (int, bool) {
	if seats <= 0 || cardsPerHand <= 0 {
		return 0, false
	}
	if seats > (int(^uint(0)>>1))/cardsPerHand {
		return 0, false
	}
	product := seats * cardsPerHand
	if product > (int(^uint(0)>>1))-1 {
		return 0, false
	}
	return product + 1, true
}

func validateDealSeatsList(seats []string) error {
	if len(seats) < 2 {
		return fmt.Errorf("deal requires at least 2 seats")
	}
	seen := make(map[string]struct{}, len(seats))
	for _, seat := range seats {
		if strings.TrimSpace(seat) == "" {
			return fmt.Errorf("deal seats must be nonblank")
		}
		if _, ok := seen[seat]; ok {
			return fmt.Errorf("deal seats must be unique")
		}
		seen[seat] = struct{}{}
	}
	return nil
}

func reconstructDealMaterial(seats []string, perHand int, dtos []CardDTO) DealMaterial {
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

func cardDTOsEqual(a, b []CardDTO) bool {
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

func dealMaterialsEqual(a, b *DealMaterial) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.DiscardTop != b.DiscardTop || a.ActiveColor != b.ActiveColor ||
		a.CurrentSeat != b.CurrentSeat || a.Direction != b.Direction ||
		a.ApplyTopEffects != b.ApplyTopEffects {
		return false
	}
	if len(a.Hands) != len(b.Hands) {
		return false
	}
	for seat, hand := range a.Hands {
		other, ok := b.Hands[seat]
		if !ok || !cardDTOsEqual(hand, other) {
			return false
		}
	}
	return true
}

func validateSnapshotMapsPresent(snap *DeckStateSnapshotV1) error {
	if snap.Draws == nil {
		return fmt.Errorf("snapshot draws map required")
	}
	if snap.Pending == nil {
		return fmt.Errorf("snapshot pending map required")
	}
	if snap.ByOp == nil {
		return fmt.Errorf("snapshot byOp map required")
	}
	if snap.Confirmed == nil {
		return fmt.Errorf("snapshot confirmed map required")
	}
	if snap.ConfirmedByID == nil {
		return fmt.Errorf("snapshot confirmedById map required")
	}
	if snap.Cancelled == nil {
		return fmt.Errorf("snapshot cancelled list required")
	}
	if snap.CancelledOps == nil {
		return fmt.Errorf("snapshot cancelledOps list required")
	}
	return nil
}

func validateConfirmedAgainstDraws(snap *DeckStateSnapshotV1, conf confirmedOp) error {
	rec, ok := snap.Draws[string(conf.OperationID)]
	if !ok {
		return fmt.Errorf("confirmed op missing draws entry")
	}
	if rec.FromPointer != conf.FromPointer {
		return fmt.Errorf("confirmed fromPointer mismatch draws map")
	}
	switch conf.Kind {
	case reservationDraw:
		if rec.Count != conf.Count {
			return fmt.Errorf("confirmed draw count mismatch draws map")
		}
		if conf.Shape == "" || conf.Shape != drawShape(conf.Count) {
			return fmt.Errorf("confirmed draw shape mismatch")
		}
		if len(conf.Cards) != rec.Count {
			return fmt.Errorf("confirmed cards count mismatch")
		}
		for i, dto := range conf.Cards {
			if string(EncodeCard(dto)) != rec.Cards[i] {
				return fmt.Errorf("confirmed cards mismatch draws map")
			}
		}
	case reservationDeal:
		if conf.Deal == nil {
			return fmt.Errorf("confirmed deal missing deal material")
		}
		if err := validateDealSeatsList(conf.Seats); err != nil {
			return err
		}
		if conf.CardsPerHand <= 0 {
			return fmt.Errorf("confirmed deal cardsPerHand must be positive")
		}
		need, ok := checkedDealNeed(len(conf.Seats), conf.CardsPerHand)
		if !ok {
			return fmt.Errorf("confirmed deal cardCount overflow")
		}
		if conf.Count != rec.Count || conf.Count != need {
			return fmt.Errorf("confirmed deal count mismatch draws map")
		}
		if conf.Shape == "" || conf.Shape != dealShape(conf.Seats, conf.CardsPerHand) {
			return fmt.Errorf("confirmed deal shape mismatch")
		}
		if len(rec.Cards) != rec.Count {
			return fmt.Errorf("confirmed deal draws cards length mismatch")
		}
		raw := make([]domain.Card, len(rec.Cards))
		for i, c := range rec.Cards {
			raw[i] = domain.Card(c)
		}
		dtos, err := decodeCards(raw)
		if err != nil {
			return fmt.Errorf("confirmed deal draws decode: %w", err)
		}
		want := reconstructDealMaterial(conf.Seats, conf.CardsPerHand, dtos)
		if !dealMaterialsEqual(conf.Deal, &want) {
			return fmt.Errorf("confirmed deal material mismatch draws map")
		}
	default:
		return fmt.Errorf("unknown confirmed kind %q", conf.Kind)
	}
	return nil
}

func confirmedOpsEqual(a, b confirmedOp) bool {
	if a.ReservationID != b.ReservationID || a.OperationID != b.OperationID ||
		a.Kind != b.Kind || a.CardsPerHand != b.CardsPerHand || a.Count != b.Count ||
		a.Shape != b.Shape || a.FromPointer != b.FromPointer || a.RemainingAfter != b.RemainingAfter {
		return false
	}
	if len(a.Seats) != len(b.Seats) {
		return false
	}
	for i := range a.Seats {
		if a.Seats[i] != b.Seats[i] {
			return false
		}
	}
	if !cardDTOsEqual(a.Cards, b.Cards) {
		return false
	}
	return dealMaterialsEqual(a.Deal, b.Deal)
}

func snapshotToPending(p pendingSnapshot) *pendingReservation {
	return &pendingReservation{
		ID: p.ID, RoomID: domain.RoomID(p.RoomID), GameID: domain.GameID(p.GameID),
		OperationID: domain.DrawOperationID(p.OperationID), Kind: reservationKind(p.Kind),
		Seats: append([]string(nil), p.Seats...), CardsPerHand: p.CardsPerHand,
		Count: p.Count, CardCount: p.CardCount, Deal: cloneDealPtr(p.Deal),
		Cards: cloneCards(p.Cards), Shape: p.Shape, RemainingAfter: p.RemainingAfter,
	}
}

func snapshotToConfirmed(c confirmedSnapshot) confirmedOp {
	return confirmedOp{
		ReservationID: c.ReservationID, OperationID: domain.DrawOperationID(c.OperationID),
		Kind: reservationKind(c.Kind), Seats: append([]string(nil), c.Seats...),
		CardsPerHand: c.CardsPerHand, Count: c.Count, Shape: c.Shape,
		Deal: cloneDealPtr(c.Deal), Cards: cloneCards(c.Cards), FromPointer: c.FromPointer,
		RemainingAfter: c.RemainingAfter,
	}
}

func marshalDeckSnapshotCanonical(snap *DeckStateSnapshotV1) ([]byte, error) {
	// Normalize map key order via encoding/json (Go sorts map keys).
	return json.Marshal(snap)
}

func deckSnapshotsEqual(a, b *DeckStateSnapshotV1) bool {
	if a == nil || b == nil {
		return a == b
	}
	ab, err1 := marshalDeckSnapshotCanonical(a)
	bb, err2 := marshalDeckSnapshotCanonical(b)
	if err1 != nil || err2 != nil {
		return false
	}
	return bytes.Equal(ab, bb)
}

func emptyDeckState() *DeckState {
	return &DeckState{
		Pending:       map[string]*pendingReservation{},
		ByOp:          map[domain.DrawOperationID]*pendingReservation{},
		Confirmed:     map[domain.DrawOperationID]confirmedOp{},
		ConfirmedByID: map[string]confirmedOp{},
		Cancelled:     map[string]struct{}{},
		CancelledOps:  map[domain.DrawOperationID]struct{}{},
	}
}
