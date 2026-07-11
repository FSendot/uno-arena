package main

import (
	"context"
	"fmt"
	"sync"

	"unoarena/services/game-integrity/domain"
)

// RoomState holds the authoritative per-room append log.
type RoomState struct {
	Log *domain.GameLog
}

// reservationKind classifies a pending material reservation.
type reservationKind string

const (
	reservationDeal reservationKind = "deal"
	reservationDraw reservationKind = "draw"
)

// pendingReservation holds non-consuming reserved material until confirm/cancel.
// At most one pending reservation exists per deck at a time.
type pendingReservation struct {
	ID           string
	RoomID       domain.RoomID
	GameID       domain.GameID
	OperationID  domain.DrawOperationID
	Kind         reservationKind
	Seats        []string
	CardsPerHand int
	Count        int
	CardCount    int
	Deal         *DealMaterial
	Cards        []CardDTO
	Shape        string
}

// confirmedOp records a confirmed deal/draw for idempotency and audit.
// Retained under the original reservation ID so Confirm(originalID) is idempotent.
type confirmedOp struct {
	ReservationID string
	OperationID   domain.DrawOperationID
	Kind          reservationKind
	Seats         []string
	CardsPerHand  int
	Count         int
	Shape         string
	Deal          *DealMaterial
	Cards         []CardDTO
	FromPointer   int
}

// DeckState holds per room+game deck, reservations, and confirmed operations.
type DeckState struct {
	Deck          *domain.AuthoritativeDeck
	SeedHex       string // protected seed material (hex); never returned on room routes
	SeedCommit    string // sha256 hex commitment
	OrderCommit   string
	Pending       map[string]*pendingReservation // at most one entry
	ByOp          map[domain.DrawOperationID]*pendingReservation
	Confirmed     map[domain.DrawOperationID]confirmedOp
	ConfirmedByID map[string]confirmedOp // original reservation ID → confirmed record
	// Cancelled tombstones: a cancelled reservation/operation identity is never reused.
	Cancelled    map[string]struct{}
	CancelledOps map[domain.DrawOperationID]struct{}
}

// StreamRepository is the injectable backing store for offline Game Integrity.
type StreamRepository interface {
	WithRoom(ctx context.Context, roomID domain.RoomID, fn func(*RoomState) error) error
	WithExistingRoom(ctx context.Context, roomID domain.RoomID, fn func(*RoomState) error) error
	WithDeck(ctx context.Context, roomID domain.RoomID, gameID domain.GameID, create bool, fn func(*DeckState) error) error
	WithExistingDeck(ctx context.Context, roomID domain.RoomID, gameID domain.GameID, fn func(*DeckState) error) error
	FindByGameID(ctx context.Context, gameID domain.GameID) (roomID domain.RoomID, ok bool, err error)
}

// ErrStreamNotFound is returned when a room or deck stream has not been created.
var ErrStreamNotFound = fmt.Errorf("stream not found")

type deckKey struct {
	RoomID domain.RoomID
	GameID domain.GameID
}

// MemoryStreamRepository is a mutex-safe in-memory StreamRepository.
type MemoryStreamRepository struct {
	mu     sync.Mutex
	rooms  map[domain.RoomID]*RoomState
	decks  map[deckKey]*DeckState
	rLocks map[domain.RoomID]*sync.Mutex
	dLocks map[deckKey]*sync.Mutex
}

// NewMemoryStreamRepository creates an empty in-memory repository.
func NewMemoryStreamRepository() *MemoryStreamRepository {
	return &MemoryStreamRepository{
		rooms:  make(map[domain.RoomID]*RoomState),
		decks:  make(map[deckKey]*DeckState),
		rLocks: make(map[domain.RoomID]*sync.Mutex),
		dLocks: make(map[deckKey]*sync.Mutex),
	}
}

func (r *MemoryStreamRepository) roomLock(id domain.RoomID) *sync.Mutex {
	r.mu.Lock()
	defer r.mu.Unlock()
	lk, ok := r.rLocks[id]
	if !ok {
		lk = &sync.Mutex{}
		r.rLocks[id] = lk
	}
	return lk
}

func (r *MemoryStreamRepository) deckLock(k deckKey) *sync.Mutex {
	r.mu.Lock()
	defer r.mu.Unlock()
	lk, ok := r.dLocks[k]
	if !ok {
		lk = &sync.Mutex{}
		r.dLocks[k] = lk
	}
	return lk
}

func (r *MemoryStreamRepository) getOrCreateRoomLocked(id domain.RoomID) (*RoomState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.rooms[id]; ok {
		return s, nil
	}
	log, err := domain.NewGameLog(id)
	if err != nil {
		return nil, err
	}
	s := &RoomState{Log: log}
	r.rooms[id] = s
	return s, nil
}

func (r *MemoryStreamRepository) getRoomLocked(id domain.RoomID) (*RoomState, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.rooms[id]
	return s, ok
}

func (r *MemoryStreamRepository) getOrCreateDeckLocked(k deckKey) *DeckState {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.decks[k]; ok {
		return s
	}
	s := &DeckState{
		Pending:       map[string]*pendingReservation{},
		ByOp:          map[domain.DrawOperationID]*pendingReservation{},
		Confirmed:     map[domain.DrawOperationID]confirmedOp{},
		ConfirmedByID: map[string]confirmedOp{},
		Cancelled:     map[string]struct{}{},
		CancelledOps:  map[domain.DrawOperationID]struct{}{},
	}
	r.decks[k] = s
	return s
}

func (r *MemoryStreamRepository) getDeckLocked(k deckKey) (*DeckState, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.decks[k]
	return s, ok
}

// WithRoom implements StreamRepository.
func (r *MemoryStreamRepository) WithRoom(ctx context.Context, roomID domain.RoomID, fn func(*RoomState) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !roomID.Valid() {
		return fmt.Errorf("roomId required")
	}
	lk := r.roomLock(roomID)
	lk.Lock()
	defer lk.Unlock()
	s, err := r.getOrCreateRoomLocked(roomID)
	if err != nil {
		return err
	}
	return fn(s)
}

// WithExistingRoom implements StreamRepository.
func (r *MemoryStreamRepository) WithExistingRoom(ctx context.Context, roomID domain.RoomID, fn func(*RoomState) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !roomID.Valid() {
		return fmt.Errorf("roomId required")
	}
	lk := r.roomLock(roomID)
	lk.Lock()
	defer lk.Unlock()
	s, ok := r.getRoomLocked(roomID)
	if !ok {
		return ErrStreamNotFound
	}
	return fn(s)
}

// WithDeck implements StreamRepository.
func (r *MemoryStreamRepository) WithDeck(ctx context.Context, roomID domain.RoomID, gameID domain.GameID, create bool, fn func(*DeckState) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !roomID.Valid() || !gameID.Valid() {
		return fmt.Errorf("roomId and gameId required")
	}
	k := deckKey{RoomID: roomID, GameID: gameID}
	lk := r.deckLock(k)
	lk.Lock()
	defer lk.Unlock()
	var s *DeckState
	if create {
		s = r.getOrCreateDeckLocked(k)
	} else {
		var ok bool
		s, ok = r.getDeckLocked(k)
		if !ok {
			return ErrStreamNotFound
		}
	}
	return fn(s)
}

// WithExistingDeck implements StreamRepository.
func (r *MemoryStreamRepository) WithExistingDeck(ctx context.Context, roomID domain.RoomID, gameID domain.GameID, fn func(*DeckState) error) error {
	return r.WithDeck(ctx, roomID, gameID, false, fn)
}

// FindByGameID implements StreamRepository.
func (r *MemoryStreamRepository) FindByGameID(ctx context.Context, gameID domain.GameID) (domain.RoomID, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var found domain.RoomID
	var n int
	for k := range r.decks {
		if k.GameID == gameID {
			found = k.RoomID
			n++
		}
	}
	if n != 1 {
		return "", false, nil
	}
	return found, true, nil
}

func (d *DeckState) outstandingPending() *pendingReservation {
	for _, p := range d.Pending {
		return p
	}
	return nil
}
