package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"unoarena/services/game-integrity/domain"
)

const defaultCardsPerHand = 7

// Service is the offline Game Integrity application service.
type Service struct {
	repo StreamRepository
}

// NewService constructs a Service over an injectable stream repository.
func NewService(repo StreamRepository) *Service {
	return &Service{repo: repo}
}

// AppendRequest mirrors Room Gameplay app.AppendRequest.
type AppendRequest struct {
	RoomID           string
	GameID           string // optional metadata
	EventID          string
	ExpectedRevision int64
	EventType        string
	Payload          []byte
}

// AppendResult mirrors Room Gameplay app.AppendResult.
type AppendResult struct {
	LogOffset int64
	Revision  int64
	Kind      domain.OutcomeKind
}

// Append applies expected-revision append with eventId idempotency on the room log.
func (s *Service) Append(req AppendRequest) (AppendResult, *domain.Rejection, error) {
	if s.repo == nil {
		return AppendResult{}, nil, errors.New("repository unwired")
	}
	if !domain.IsAppendEventTypeAllowed(req.EventType) {
		return AppendResult{}, &domain.Rejection{
			Code:    domain.RejectInvalidCommand,
			Message: "eventType not on accepted gameplay/lifecycle allowlist",
		}, nil
	}
	var (
		result AppendResult
		rej    *domain.Rejection
	)
	err := s.repo.WithRoom(domain.RoomID(req.RoomID), func(st *RoomState) error {
		out := st.Log.Append(domain.AppendCommand{
			EventID:          domain.EventID(req.EventID),
			ExpectedRevision: domain.Revision(req.ExpectedRevision),
			EventType:        req.EventType,
			GameID:           domain.GameID(req.GameID),
			Payload:          append([]byte(nil), req.Payload...),
		})
		if out.Rejected() {
			rej = out.Rejection
			return nil
		}
		result = AppendResult{
			LogOffset: int64(out.Offset),
			Revision:  int64(out.Revision),
			Kind:      out.Kind,
		}
		return nil
	})
	return result, rej, err
}

// InitializeDeckRequest creates an AuthoritativeDeck for a room/game.
// Caller must not supply seed or cards — GI generates both.
type InitializeDeckRequest struct {
	RoomID string
	GameID string
}

// InitializeDeckResult is returned after deck creation.
type InitializeDeckResult struct {
	Kind           domain.OutcomeKind
	Remaining      int
	SeedCommitment string
}

// InitializeDeck creates a deterministic authoritative deck with an internal secure seed.
// Re-initialize of an existing deck is a conflicting duplicate rejection.
func (s *Service) InitializeDeck(req InitializeDeckRequest) (InitializeDeckResult, *domain.Rejection, error) {
	if s.repo == nil {
		return InitializeDeckResult{}, nil, errors.New("repository unwired")
	}
	if req.GameID == "" {
		return InitializeDeckResult{}, &domain.Rejection{
			Code: domain.RejectInvalidIdentity, Message: "gameId required",
		}, nil
	}
	var (
		result InitializeDeckResult
		rej    *domain.Rejection
	)
	err := s.repo.WithDeck(domain.RoomID(req.RoomID), domain.GameID(req.GameID), true, func(st *DeckState) error {
		if st.Deck != nil {
			rej = &domain.Rejection{
				Code:    domain.RejectConflictingDuplicate,
				Message: "deck already initialized",
			}
			return nil
		}
		seedBytes := make([]byte, 32)
		if _, err := rand.Read(seedBytes); err != nil {
			return err
		}
		seed, err := domain.NewDeckSeed(seedBytes)
		if err != nil {
			return err
		}
		cards := StandardDeckCards()
		deck, err := domain.NewAuthoritativeDeck(domain.GameID(req.GameID), seed, cards)
		if err != nil {
			return err
		}
		st.Deck = deck
		st.SeedHex = hex.EncodeToString(seedBytes)
		st.SeedCommit = sha256Hex(seedBytes)
		st.OrderCommit = orderCommitment(deck.ShuffledOrder())
		result = InitializeDeckResult{
			Kind:           domain.OutcomeAccepted,
			Remaining:      deck.Remaining(),
			SeedCommitment: st.SeedCommit,
		}
		return nil
	})
	return result, rej, err
}

// ReserveDealRequest reserves initial deal material without consuming the deck pointer.
type ReserveDealRequest struct {
	RoomID       string
	GameID       string
	OperationID  string
	Seats        []string
	CardsPerHand int
}

// ReserveDrawRequest reserves draw cards without consuming the deck pointer.
type ReserveDrawRequest struct {
	RoomID      string
	GameID      string
	OperationID string
	Count       int
}

// ReservationResult is returned from reserve operations.
type ReservationResult struct {
	Kind          domain.OutcomeKind
	ReservationID string
	Deal          *DealMaterial
	Cards         []CardDTO
}

// ReserveDeal peeks deal material without advancing the draw pointer.
// Reservation IDs are deterministic across room+game+operation+shape.
// At most one outstanding unconfirmed reservation is allowed per deck.
func (s *Service) ReserveDeal(req ReserveDealRequest) (ReservationResult, *domain.Rejection, error) {
	if s.repo == nil {
		return ReservationResult{}, nil, errors.New("repository unwired")
	}
	if req.OperationID == "" {
		return ReservationResult{}, &domain.Rejection{
			Code: domain.RejectInvalidIdentity, Message: "deal requires operationId",
		}, nil
	}
	if len(req.Seats) < 2 {
		return ReservationResult{}, &domain.Rejection{
			Code: domain.RejectInvalidCommand, Message: "deal requires at least 2 seats",
		}, nil
	}
	perHand := req.CardsPerHand
	if perHand <= 0 {
		perHand = defaultCardsPerHand
	}
	shape := dealShape(req.Seats, perHand)
	opID := domain.DrawOperationID(req.OperationID)
	resID := deterministicReservationID(req.RoomID, req.GameID, req.OperationID, shape)

	var (
		result ReservationResult
		rej    *domain.Rejection
	)
	err := s.repo.WithDeck(domain.RoomID(req.RoomID), domain.GameID(req.GameID), false, func(st *DeckState) error {
		if st.Deck == nil {
			rej = &domain.Rejection{Code: domain.RejectInvalidCommand, Message: "deck not initialized"}
			return nil
		}
		if prior, ok := st.Confirmed[opID]; ok {
			if prior.Kind != reservationDeal || prior.Shape != shape {
				rej = &domain.Rejection{
					Code:    domain.RejectConflictingDuplicate,
					Message: "operationId reused with different deal shape",
				}
				return nil
			}
			result = ReservationResult{
				Kind: domain.OutcomeDuplicate, ReservationID: prior.ReservationID,
				Deal: cloneDealPtr(prior.Deal),
			}
			return nil
		}
		if prior, ok := st.ByOp[opID]; ok {
			if prior.Shape != shape || prior.Kind != reservationDeal {
				rej = &domain.Rejection{
					Code:    domain.RejectConflictingDuplicate,
					Message: "operationId reused with different deal shape",
				}
				return nil
			}
			result = ReservationResult{
				Kind: domain.OutcomeDuplicate, ReservationID: prior.ID, Deal: cloneDealPtr(prior.Deal),
			}
			return nil
		}
		if outstanding := st.outstandingPending(); outstanding != nil {
			rej = &domain.Rejection{
				Code:    domain.RejectConflictingDuplicate,
				Message: "deck has outstanding unconfirmed reservation",
			}
			return nil
		}

		need := len(req.Seats)*perHand + 1
		raw, peekRej := st.Deck.Peek(0, need)
		if peekRej != nil {
			rej = peekRej
			return nil
		}
		dtos, err := decodeCards(raw)
		if err != nil {
			return err
		}
		hands := make(map[string][]CardDTO, len(req.Seats))
		idx := 0
		for _, seat := range req.Seats {
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
		material := DealMaterial{
			Hands: hands, DiscardTop: discard, ActiveColor: active,
			CurrentSeat: 0, Direction: "clockwise",
		}
		pending := &pendingReservation{
			ID: resID, RoomID: domain.RoomID(req.RoomID), GameID: domain.GameID(req.GameID),
			OperationID: opID, Kind: reservationDeal, Seats: append([]string(nil), req.Seats...),
			CardsPerHand: perHand, Count: need, CardCount: need,
			Deal: cloneDealPtr(&material), Shape: shape,
		}
		st.Pending[resID] = pending
		st.ByOp[opID] = pending
		result = ReservationResult{Kind: domain.OutcomeAccepted, ReservationID: resID, Deal: &material}
		return nil
	})
	if errors.Is(err, ErrStreamNotFound) {
		return ReservationResult{}, &domain.Rejection{
			Code: domain.RejectInvalidCommand, Message: "deck not initialized",
		}, nil
	}
	return result, rej, err
}

// ReserveDraw peeks draw cards without advancing the draw pointer.
// Reservation IDs are deterministic across room+game+operation+shape.
// At most one outstanding unconfirmed reservation is allowed per deck.
func (s *Service) ReserveDraw(req ReserveDrawRequest) (ReservationResult, *domain.Rejection, error) {
	if s.repo == nil {
		return ReservationResult{}, nil, errors.New("repository unwired")
	}
	if req.OperationID == "" {
		return ReservationResult{}, &domain.Rejection{
			Code: domain.RejectInvalidIdentity, Message: "draw requires operationId",
		}, nil
	}
	if req.Count <= 0 {
		return ReservationResult{}, &domain.Rejection{
			Code: domain.RejectInvalidCommand, Message: "draw count must be positive",
		}, nil
	}
	shape := drawShape(req.Count)
	opID := domain.DrawOperationID(req.OperationID)
	resID := deterministicReservationID(req.RoomID, req.GameID, req.OperationID, shape)

	var (
		result ReservationResult
		rej    *domain.Rejection
	)
	err := s.repo.WithDeck(domain.RoomID(req.RoomID), domain.GameID(req.GameID), false, func(st *DeckState) error {
		if st.Deck == nil {
			rej = &domain.Rejection{Code: domain.RejectInvalidCommand, Message: "deck not initialized"}
			return nil
		}
		if prior, ok := st.Confirmed[opID]; ok {
			if prior.Kind != reservationDraw || prior.Shape != shape {
				rej = &domain.Rejection{
					Code:    domain.RejectConflictingDuplicate,
					Message: "operationId reused with different draw shape",
				}
				return nil
			}
			result = ReservationResult{
				Kind: domain.OutcomeDuplicate, ReservationID: prior.ReservationID,
				Cards: cloneCards(prior.Cards),
			}
			return nil
		}
		if prior, ok := st.ByOp[opID]; ok {
			if prior.Shape != shape || prior.Kind != reservationDraw {
				rej = &domain.Rejection{
					Code:    domain.RejectConflictingDuplicate,
					Message: "operationId reused with different draw shape",
				}
				return nil
			}
			result = ReservationResult{
				Kind: domain.OutcomeDuplicate, ReservationID: prior.ID, Cards: cloneCards(prior.Cards),
			}
			return nil
		}
		if outstanding := st.outstandingPending(); outstanding != nil {
			rej = &domain.Rejection{
				Code:    domain.RejectConflictingDuplicate,
				Message: "deck has outstanding unconfirmed reservation",
			}
			return nil
		}

		raw, peekRej := st.Deck.Peek(0, req.Count)
		if peekRej != nil {
			rej = peekRej
			return nil
		}
		dtos, err := decodeCards(raw)
		if err != nil {
			return err
		}
		pending := &pendingReservation{
			ID: resID, RoomID: domain.RoomID(req.RoomID), GameID: domain.GameID(req.GameID),
			OperationID: opID, Kind: reservationDraw, Count: req.Count,
			CardCount: req.Count, Cards: cloneCards(dtos), Shape: shape,
		}
		st.Pending[resID] = pending
		st.ByOp[opID] = pending
		result = ReservationResult{Kind: domain.OutcomeAccepted, ReservationID: resID, Cards: dtos}
		return nil
	})
	if errors.Is(err, ErrStreamNotFound) {
		return ReservationResult{}, &domain.Rejection{
			Code: domain.RejectInvalidCommand, Message: "deck not initialized",
		}, nil
	}
	return result, rej, err
}

// ConfirmReservation atomically advances the deck pointer and records the operation.
// Confirm of an already-confirmed original reservation ID is idempotent (lost-response safe).
func (s *Service) ConfirmReservation(roomID, gameID, reservationID string) (domain.OutcomeKind, *domain.Rejection, error) {
	if s.repo == nil {
		return "", nil, errors.New("repository unwired")
	}
	var (
		kind domain.OutcomeKind
		rej  *domain.Rejection
	)
	err := s.repo.WithDeck(domain.RoomID(roomID), domain.GameID(gameID), false, func(st *DeckState) error {
		if st.Deck == nil {
			rej = &domain.Rejection{Code: domain.RejectInvalidCommand, Message: "deck not initialized"}
			return nil
		}
		if _, ok := st.ConfirmedByID[reservationID]; ok {
			kind = domain.OutcomeDuplicate
			return nil
		}
		pending, ok := st.Pending[reservationID]
		if !ok {
			rej = &domain.Rejection{Code: domain.RejectInvalidCommand, Message: "unknown reservation"}
			return nil
		}
		from := st.Deck.DrawPointer()
		out := st.Deck.Draw(domain.DrawCommand{OperationID: pending.OperationID, Count: pending.CardCount})
		if out.Rejected() {
			rej = out.Rejection
			return nil
		}
		rec := confirmedOp{
			ReservationID: reservationID, OperationID: pending.OperationID, Kind: pending.Kind,
			Seats: append([]string(nil), pending.Seats...), CardsPerHand: pending.CardsPerHand,
			Count: pending.Count, Shape: pending.Shape,
			Deal: cloneDealPtr(pending.Deal), Cards: cloneCards(pending.Cards),
			FromPointer: from,
		}
		st.Confirmed[pending.OperationID] = rec
		st.ConfirmedByID[reservationID] = rec
		delete(st.Pending, reservationID)
		delete(st.ByOp, pending.OperationID)
		kind = out.Kind
		return nil
	})
	if errors.Is(err, ErrStreamNotFound) {
		return "", &domain.Rejection{Code: domain.RejectInvalidCommand, Message: "deck not initialized"}, nil
	}
	return kind, rej, err
}

// CancelReservation releases a pending reservation without consuming cards.
// Cancellation does not shift or reinterpret any later returned material.
func (s *Service) CancelReservation(roomID, gameID, reservationID string) *domain.Rejection {
	if s.repo == nil {
		return &domain.Rejection{Code: domain.RejectInvalidCommand, Message: "repository unwired"}
	}
	var rej *domain.Rejection
	err := s.repo.WithDeck(domain.RoomID(roomID), domain.GameID(gameID), false, func(st *DeckState) error {
		pending, ok := st.Pending[reservationID]
		if !ok {
			// Cancel of unknown/already-released is idempotent success.
			return nil
		}
		delete(st.Pending, reservationID)
		delete(st.ByOp, pending.OperationID)
		return nil
	})
	if errors.Is(err, ErrStreamNotFound) {
		return nil // nothing to cancel
	}
	if err != nil {
		return &domain.Rejection{Code: domain.RejectInvalidCommand, Message: err.Error()}
	}
	return rej
}

// ResolveReservation looks up room/game for a reservation id across decks (confirm/cancel without gameId).
func (s *Service) ResolveReservation(reservationID string) (roomID, gameID string, ok bool) {
	// Memory repo does not index by reservation globally; callers must pass room/game.
	// Kept for API completeness — HTTP requires roomId path + gameId body.
	return "", "", false
}

// ReplayResult is a gapless replay slice of the room log.
type ReplayResult struct {
	RoomID   string
	Revision int64
	Entries  []ReplayEntry
}

// ReplayEntry is one exported log entry.
type ReplayEntry struct {
	Offset    uint64          `json:"offset"`
	EventID   string          `json:"eventId"`
	EventType string          `json:"eventType"`
	GameID    string          `json:"gameId,omitempty"`
	Payload   json.RawMessage `json:"payload"`
}

// Replay returns room log entries from the given offset.
func (s *Service) Replay(roomID string, from uint64) (ReplayResult, *domain.Rejection, error) {
	if s.repo == nil {
		return ReplayResult{}, nil, errors.New("repository unwired")
	}
	var (
		result ReplayResult
		rej    *domain.Rejection
	)
	err := s.repo.WithExistingRoom(domain.RoomID(roomID), func(st *RoomState) error {
		entries, r := st.Log.ReplayFrom(domain.LogOffset(from))
		if r != nil {
			rej = r
			return nil
		}
		result = ReplayResult{
			RoomID:   roomID,
			Revision: int64(st.Log.Revision()),
			Entries:  toReplayEntries(entries),
		}
		return nil
	})
	if errors.Is(err, ErrStreamNotFound) {
		return ReplayResult{}, &domain.Rejection{
			Code: domain.RejectInvalidCommand, Message: "stream not found",
		}, nil
	}
	return result, rej, err
}

// DeckAudit is the reconstructable deck audit section of an export.
type DeckAudit struct {
	GameID              string             `json:"gameId"`
	SeedCommitment      string             `json:"seedCommitment"`
	ProtectedSeed       string             `json:"protectedSeed,omitempty"`
	OrderCommitment     string             `json:"orderCommitment"`
	Pointer             int                `json:"pointer"`
	Remaining           int                `json:"remaining"`
	ConfirmedOperations []ConfirmedOpAudit `json:"confirmedOperations"`
	ShuffledOrder       []CardDTO          `json:"shuffledOrder,omitempty"`
}

// ConfirmedOpAudit is one confirmed deal/draw for reconstruction.
type ConfirmedOpAudit struct {
	OperationID  string        `json:"operationId"`
	Kind         string        `json:"kind"`
	Seats        []string      `json:"seats,omitempty"`
	CardsPerHand int           `json:"cardsPerHand,omitempty"`
	Count        int           `json:"count,omitempty"`
	FromPointer  int           `json:"fromPointer"`
	Deal         *DealMaterial `json:"deal,omitempty"`
	Cards        []CardDTO     `json:"cards,omitempty"`
}

// ExportResult is a full deterministic recovery export.
type ExportResult struct {
	RoomID   string        `json:"roomId"`
	GameID   string        `json:"gameId,omitempty"`
	Revision int64         `json:"revision"`
	Entries  []ReplayEntry `json:"entries"`
	Deck     *DeckAudit    `json:"deck,omitempty"`
}

// Export returns the full room log export, optionally scoped with deck audit for gameID.
// includeSeed exposes protected seed material (operator policy).
func (s *Service) Export(roomID, gameID string, includeSeed bool) (ExportResult, *domain.Rejection, error) {
	if s.repo == nil {
		return ExportResult{}, nil, errors.New("repository unwired")
	}
	var result ExportResult
	err := s.repo.WithExistingRoom(domain.RoomID(roomID), func(st *RoomState) error {
		exp := st.Log.ExportStateInput()
		result = ExportResult{
			RoomID:   string(exp.RoomID),
			GameID:   gameID,
			Revision: int64(exp.Revision),
			Entries:  toReplayEntries(exp.Entries),
		}
		return nil
	})
	if errors.Is(err, ErrStreamNotFound) {
		return ExportResult{}, &domain.Rejection{
			Code: domain.RejectInvalidCommand, Message: "stream not found",
		}, nil
	}
	if err != nil {
		return ExportResult{}, nil, err
	}
	if gameID != "" {
		_ = s.repo.WithExistingDeck(domain.RoomID(roomID), domain.GameID(gameID), func(st *DeckState) error {
			if st.Deck == nil {
				return nil
			}
			audit := &DeckAudit{
				GameID:          gameID,
				SeedCommitment:  st.SeedCommit,
				OrderCommitment: st.OrderCommit,
				Pointer:         st.Deck.DrawPointer(),
				Remaining:       st.Deck.Remaining(),
			}
			if includeSeed {
				audit.ProtectedSeed = st.SeedHex
				order, _ := decodeCards(st.Deck.ShuffledOrder())
				audit.ShuffledOrder = order
			}
			ops := make([]ConfirmedOpAudit, 0, len(st.Confirmed))
			for _, op := range st.Confirmed {
				ops = append(ops, ConfirmedOpAudit{
					OperationID: string(op.OperationID), Kind: string(op.Kind),
					Seats: append([]string(nil), op.Seats...), CardsPerHand: op.CardsPerHand,
					Count: op.Count, FromPointer: op.FromPointer,
					Deal: cloneDealPtr(op.Deal), Cards: cloneCards(op.Cards),
				})
			}
			sort.Slice(ops, func(i, j int) bool { return ops[i].FromPointer < ops[j].FromPointer })
			audit.ConfirmedOperations = ops
			result.Deck = audit
			return nil
		})
	}
	return result, nil, nil
}

// ExportByGameID resolves room via repository and exports.
func (s *Service) ExportByGameID(gameID string, includeSeed bool) (ExportResult, *domain.Rejection, error) {
	if s.repo == nil {
		return ExportResult{}, nil, errors.New("repository unwired")
	}
	roomID, ok := s.repo.FindByGameID(domain.GameID(gameID))
	if !ok {
		return ExportResult{}, &domain.Rejection{
			Code: domain.RejectInvalidCommand, Message: "stream not found for gameId",
		}, nil
	}
	return s.Export(string(roomID), gameID, includeSeed)
}

func dealShape(seats []string, perHand int) string {
	cp := append([]string(nil), seats...)
	return fmt.Sprintf("deal|%s|%d", strings.Join(cp, ","), perHand)
}

func drawShape(count int) string {
	return fmt.Sprintf("draw|%d", count)
}

// deterministicReservationID is globally unique and stable for room+game+operation+shape.
func deterministicReservationID(roomID, gameID, operationID, shape string) string {
	h := sha256.New()
	_, _ = h.Write([]byte(roomID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(gameID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(operationID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(shape))
	return "res-" + hex.EncodeToString(h.Sum(nil))
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func orderCommitment(cards []domain.Card) string {
	h := sha256.New()
	for _, c := range cards {
		_, _ = h.Write([]byte(c))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func toReplayEntries(in []domain.GameLogEntry) []ReplayEntry {
	if len(in) == 0 {
		return []ReplayEntry{}
	}
	out := make([]ReplayEntry, len(in))
	for i, e := range in {
		payload := json.RawMessage(append([]byte(nil), e.Payload...))
		if len(payload) == 0 {
			payload = json.RawMessage("null")
		} else if !json.Valid(payload) {
			b, _ := json.Marshal(string(e.Payload))
			payload = b
		}
		out[i] = ReplayEntry{
			Offset:    uint64(e.Offset),
			EventID:   string(e.EventID),
			EventType: e.EventType,
			GameID:    string(e.GameID),
			Payload:   payload,
		}
	}
	return out
}

func cloneDealPtr(in *DealMaterial) *DealMaterial {
	if in == nil {
		return nil
	}
	cp := cloneDealMaterial(*in)
	return &cp
}

func cloneDealMaterial(in DealMaterial) DealMaterial {
	hands := make(map[string][]CardDTO, len(in.Hands))
	for k, v := range in.Hands {
		cp := make([]CardDTO, len(v))
		copy(cp, v)
		hands[k] = cp
	}
	return DealMaterial{
		Hands: hands, DiscardTop: in.DiscardTop, ActiveColor: in.ActiveColor,
		CurrentSeat: in.CurrentSeat, Direction: in.Direction, ApplyTopEffects: in.ApplyTopEffects,
	}
}

func cloneCards(in []CardDTO) []CardDTO {
	if len(in) == 0 {
		return nil
	}
	out := make([]CardDTO, len(in))
	copy(out, in)
	return out
}

func payloadBytes(raw json.RawMessage) []byte {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil
	}
	return append([]byte(nil), raw...)
}

func rejectionHTTPStatus(code domain.RejectionCode) int {
	switch code {
	case domain.RejectRevisionMismatch, domain.RejectConflictingDuplicate:
		return 409
	case domain.RejectInsufficientCards, domain.RejectInvalidOffset:
		return 409
	case domain.RejectInvalidCommand, domain.RejectInvalidIdentity:
		return 400
	default:
		return 400
	}
}

func fmtRejection(rej *domain.Rejection) string {
	if rej == nil {
		return ""
	}
	if rej.Message != "" {
		return rej.Message
	}
	return string(rej.Code)
}
