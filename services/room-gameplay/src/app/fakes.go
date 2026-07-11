package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"unoarena/services/room-gameplay/domain"
	"unoarena/services/room-gameplay/game"
	"unoarena/shared/audit"
)

// SystemClock uses wall-clock UTC time for runtime wiring.
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }

// FixedClock is a mutable test-only clock. Runtime wiring must use SystemClock.
type FixedClock struct {
	mu  sync.Mutex
	now time.Time
}

func NewFixedClock(now time.Time) *FixedClock {
	return &FixedClock{now: now.UTC()}
}

func (c *FixedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *FixedClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func (c *FixedClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t.UTC()
}

// MemorySessionRepository is an in-memory Session store with atomic Commit + outbox.
type MemorySessionRepository struct {
	mu             sync.Mutex
	byID           map[domain.RoomID]*domain.Session
	outbox         []outboxRow
	provisions     map[ProvisionKey]string
	playerSess     map[string]string // roomID|playerID -> sessionID
	revisions      map[string]int64
	streamSeq      map[string]int64 // roomID -> last allocated player-feed sequence
	globalOutcomes map[string]domain.CommandOutcome
	pendingAudits  map[string]audit.RejectionRecord
	FailCommit     error
	FailMark       error
	FailMarkAudit  error
}

type outboxRow struct {
	Entry       OutboxEntry
	PublishedAt *time.Time
}

func NewMemorySessionRepository() *MemorySessionRepository {
	return &MemorySessionRepository{
		byID:           map[domain.RoomID]*domain.Session{},
		provisions:     map[ProvisionKey]string{},
		playerSess:     map[string]string{},
		revisions:      map[string]int64{},
		streamSeq:      map[string]int64{},
		globalOutcomes: map[string]domain.CommandOutcome{},
		pendingAudits:  map[string]audit.RejectionRecord{},
	}
}

func (r *MemorySessionRepository) Get(roomID domain.RoomID) (*domain.Session, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.byID[roomID]
	if !ok {
		return nil, false
	}
	// Copy-on-write: callers mutate a deep clone; live state changes only on Commit.
	// Rejected-outcome RememberOutcome must not leak into stored state if Commit fails.
	return s.Clone(), true
}

func (r *MemorySessionRepository) Commit(req CommitRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.FailCommit != nil {
		return r.FailCommit
	}
	if req.GlobalOutcome != nil && req.GlobalOutcome.CommandID.Valid() {
		r.globalOutcomes[string(req.GlobalOutcome.CommandID)] = *req.GlobalOutcome
	}
	if req.PendingAudit != nil && strings.TrimSpace(req.PendingAudit.CommandID) != "" {
		r.pendingAudits[req.PendingAudit.CommandID] = *req.PendingAudit
	}
	if req.Session == nil {
		if req.GlobalOutcome != nil || req.PendingAudit != nil {
			return nil
		}
		return errors.New("session required")
	}
	if req.Session.Room() == nil {
		return errors.New("session required")
	}
	roomID := string(req.Session.Room().ID())
	// Store a deep clone so post-commit caller mutations cannot alias live state.
	r.byID[req.Session.Room().ID()] = req.Session.Clone()
	// Empty outbox rows (CommandID set, no events) must not enqueue — they block drains.
	if len(req.Outbox.Events) > 0 {
		r.outbox = append(r.outbox, outboxRow{Entry: req.Outbox})
	}
	if req.ProvisionKey != nil && req.ProvisionRoomID != "" {
		r.provisions[*req.ProvisionKey] = req.ProvisionRoomID
	}
	if req.BindPlayerSession && req.PlayerID != "" && req.PlayerSessionID != "" {
		r.playerSess[roomID+"|"+req.PlayerID] = req.PlayerSessionID
	}
	if req.SetIntegrityRevision {
		r.revisions[roomID] = req.IntegrityRevision
	}
	if req.SetStreamSeq && req.StreamSeqHighWater > r.streamSeq[roomID] {
		r.streamSeq[roomID] = req.StreamSeqHighWater
	}
	return nil
}

func (r *MemorySessionRepository) Delete(roomID domain.RoomID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.byID, roomID)
	return nil
}

func (r *MemorySessionRepository) ListPendingOutbox(limit int) ([]OutboxEntry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if limit <= 0 {
		limit = len(r.outbox)
	}
	var out []OutboxEntry
	for _, row := range r.outbox {
		if row.PublishedAt != nil {
			continue
		}
		out = append(out, row.Entry)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (r *MemorySessionRepository) MarkOutboxPublished(eventID string, publishedAt time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.FailMark != nil {
		return r.FailMark
	}
	ts := publishedAt.UTC()
	for i := range r.outbox {
		if r.outbox[i].PublishedAt != nil {
			continue
		}
		for _, ev := range r.outbox[i].Entry.Events {
			if ev.EventID == eventID {
				r.outbox[i].PublishedAt = &ts
				return nil
			}
		}
	}
	return nil
}

// MarkOutboxEntryPublished marks an entire outbox entry published by command id.
func (r *MemorySessionRepository) MarkOutboxEntryPublished(commandID string, publishedAt time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.FailMark != nil {
		return r.FailMark
	}
	ts := publishedAt.UTC()
	for i := range r.outbox {
		if r.outbox[i].PublishedAt != nil {
			continue
		}
		if r.outbox[i].Entry.CommandID == commandID {
			r.outbox[i].PublishedAt = &ts
			return nil
		}
	}
	return nil
}

func (r *MemorySessionRepository) PlayerSession(roomID, playerID string) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.playerSess[roomID+"|"+playerID]
	return v, ok
}

func (r *MemorySessionRepository) GetProvision(key ProvisionKey) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.provisions[key]
	return v, ok
}

func (r *MemorySessionRepository) IntegrityRevision(roomID string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.revisions[roomID]
}

func (r *MemorySessionRepository) PeekStreamSeq(roomID string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.streamSeq[roomID]
}

func (r *MemorySessionRepository) GetGlobalOutcome(commandID string) (domain.CommandOutcome, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out, ok := r.globalOutcomes[commandID]
	return out, ok
}

func (r *MemorySessionRepository) GetPendingAudit(commandID string) (audit.RejectionRecord, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.pendingAudits[commandID]
	return rec, ok
}

func (r *MemorySessionRepository) MarkAuditComplete(commandID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.FailMarkAudit != nil {
		return r.FailMarkAudit
	}
	delete(r.pendingAudits, commandID)
	return nil
}

func (r *MemorySessionRepository) PendingOutboxLen() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, row := range r.outbox {
		if row.PublishedAt == nil {
			n++
		}
	}
	return n
}

// FakeGameIntegrity records appends and can fail on demand.
type FakeGameIntegrity struct {
	mu       sync.Mutex
	Appends  []AppendRequest
	FailNext error
	FailAll  error
}

func NewFakeGameIntegrity() *FakeGameIntegrity {
	return &FakeGameIntegrity{}
}

func (f *FakeGameIntegrity) Append(_ context.Context, req AppendRequest) (AppendResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.FailAll != nil {
		return AppendResult{}, f.FailAll
	}
	if f.FailNext != nil {
		err := f.FailNext
		f.FailNext = nil
		return AppendResult{}, err
	}
	f.Appends = append(f.Appends, req)
	rev := req.ExpectedRevision + 1
	return AppendResult{LogOffset: req.ExpectedRevision, Revision: rev}, nil
}

func (f *FakeGameIntegrity) Len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.Appends)
}

func (f *FakeGameIntegrity) Last() AppendRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.Appends) == 0 {
		return AppendRequest{}
	}
	return f.Appends[len(f.Appends)-1]
}

// FakeEventPublisher records published events one-at-a-time.
type FakeEventPublisher struct {
	mu       sync.Mutex
	Events   []PublishedEvent
	Fail     error
	FailNext error
	Calls    int
}

func NewFakeEventPublisher() *FakeEventPublisher { return &FakeEventPublisher{} }

func (f *FakeEventPublisher) Publish(_ context.Context, event PublishedEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls++
	if f.FailNext != nil {
		err := f.FailNext
		f.FailNext = nil
		return err
	}
	if f.Fail != nil {
		return f.Fail
	}
	f.Events = append(f.Events, event)
	return nil
}

// FakeAuditSink records rejection audits with commandId idempotency
// (exact duplicate no-op; conflicting same key errors).
type FakeAuditSink struct {
	mu         sync.Mutex
	Records    []audit.RejectionRecord
	seen       map[string]string
	FailRecord error
}

func NewFakeAuditSink() *FakeAuditSink {
	return &FakeAuditSink{seen: make(map[string]string)}
}

func (f *FakeAuditSink) Record(_ context.Context, rec audit.RejectionRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.FailRecord != nil {
		return f.FailRecord
	}
	if err := rec.Validate(); err != nil {
		return err
	}
	if f.seen == nil {
		f.seen = make(map[string]string)
	}
	canon, skip, err := checkAuditIdempotency(f.seen, rec)
	if err != nil {
		return err
	}
	if skip {
		return nil
	}
	f.seen[rec.CommandID] = canon
	f.Records = append(f.Records, rec)
	return nil
}

func (f *FakeAuditSink) Len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.Records)
}

// FakeDealSource returns deterministic deal/draw material with reservation semantics.
// Reservation IDs are stable for room+game+operation+shape; at most one pending per deck.
type FakeDealSource struct {
	mu           sync.Mutex
	DealFn       func(roomID, gameID string, seats []string) (game.DealMaterial, error)
	DrawFn       func(roomID, gameID string, count int) ([]game.Card, error)
	FailConfirm  error
	pending      map[string]MaterialReservation
	confirmed    map[string]MaterialReservation
	byOp         map[string]string // deckKey|op -> reservationID
	deckPending  map[string]string // deckKey -> reservationID
	resDeck      map[string]string // reservationID -> deckKey
	resDrawCount map[string]int    // reservationID -> cards to advance on confirm
	cursor       map[string]int    // deckKey -> next card index (advances only on confirm)
	DealCalls    int
	DrawCalls    int
	ConfirmCalls int
}

func NewFakeDealSource() *FakeDealSource {
	return &FakeDealSource{
		pending:      map[string]MaterialReservation{},
		confirmed:    map[string]MaterialReservation{},
		byOp:         map[string]string{},
		deckPending:  map[string]string{},
		resDeck:      map[string]string{},
		resDrawCount: map[string]int{},
		cursor:       map[string]int{},
	}
}

func deckKey(roomID, gameID string) string { return roomID + "/" + gameID }

func fakeReservationID(roomID, gameID, operationID, shape string) string {
	sum := sha256.Sum256([]byte(roomID + "\x00" + gameID + "\x00" + operationID + "\x00" + shape))
	return "res-" + hex.EncodeToString(sum[:])
}

func (f *FakeDealSource) ReserveDeal(_ context.Context, roomID, gameID, operationID string, seats []string) (MaterialReservation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.DealCalls++
	if operationID == "" {
		return MaterialReservation{}, errors.New("operationId required")
	}
	shape := "deal|" + strings.Join(seats, ",") + "|7"
	dk := deckKey(roomID, gameID)
	opKey := dk + "|" + operationID
	id := fakeReservationID(roomID, gameID, operationID, shape)

	if prior, ok := f.confirmed[id]; ok {
		return prior, nil
	}
	if priorID, ok := f.byOp[opKey]; ok {
		if priorID != id {
			return MaterialReservation{}, errors.New("operationId reused with different deal shape")
		}
		if res, ok := f.pending[priorID]; ok {
			return res, nil
		}
		if res, ok := f.confirmed[priorID]; ok {
			return res, nil
		}
	}
	if outstanding, ok := f.deckPending[dk]; ok && outstanding != "" {
		return MaterialReservation{}, errors.New("deck has outstanding unconfirmed reservation")
	}

	var deal game.DealMaterial
	var err error
	if f.DealFn != nil {
		deal, err = f.DealFn(roomID, gameID, seats)
	} else {
		deal = defaultTwoPlusDeal(seats)
	}
	if err != nil {
		return MaterialReservation{}, err
	}
	res := MaterialReservation{ID: id, Deal: &deal}
	f.pending[id] = res
	f.byOp[opKey] = id
	f.deckPending[dk] = id
	f.resDeck[id] = dk
	return res, nil
}

func (f *FakeDealSource) ReserveDraw(_ context.Context, roomID, gameID, operationID string, count int) (MaterialReservation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.DrawCalls++
	if operationID == "" {
		return MaterialReservation{}, errors.New("operationId required")
	}
	if count <= 0 {
		return MaterialReservation{}, errors.New("count must be positive")
	}
	shape := "draw|" + itoa(count)
	dk := deckKey(roomID, gameID)
	opKey := dk + "|" + operationID
	id := fakeReservationID(roomID, gameID, operationID, shape)

	if prior, ok := f.confirmed[id]; ok {
		return prior, nil
	}
	if priorID, ok := f.byOp[opKey]; ok {
		if priorID != id {
			return MaterialReservation{}, errors.New("operationId reused with different draw shape")
		}
		if res, ok := f.pending[priorID]; ok {
			return res, nil
		}
		if res, ok := f.confirmed[priorID]; ok {
			return res, nil
		}
	}
	if outstanding, ok := f.deckPending[dk]; ok && outstanding != "" {
		return MaterialReservation{}, errors.New("deck has outstanding unconfirmed reservation")
	}

	var cards []game.Card
	var err error
	if f.DrawFn != nil {
		cards, err = f.DrawFn(roomID, gameID, count)
	} else {
		start := f.cursor[dk]
		cards = make([]game.Card, count)
		for i := 0; i < count; i++ {
			cards[i] = game.Card{
				ID:    game.CardID(roomID + "/" + gameID + "/card-" + itoa(start+i)),
				Color: game.ColorRed,
				Face:  game.Face1,
			}
		}
	}
	if err != nil {
		return MaterialReservation{}, err
	}
	res := MaterialReservation{ID: id, Cards: cards}
	f.pending[id] = res
	f.byOp[opKey] = id
	f.deckPending[dk] = id
	f.resDeck[id] = dk
	f.resDrawCount[id] = count
	return res, nil
}

func (f *FakeDealSource) Confirm(_ context.Context, reservationID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ConfirmCalls++
	if f.FailConfirm != nil {
		return f.FailConfirm
	}
	if _, ok := f.confirmed[reservationID]; ok {
		return nil // idempotent
	}
	res, ok := f.pending[reservationID]
	if !ok {
		return errors.New("unknown reservation")
	}
	if dk := f.resDeck[reservationID]; dk != "" {
		if n := f.resDrawCount[reservationID]; n > 0 {
			f.cursor[dk] += n
		}
		if f.deckPending[dk] == reservationID {
			delete(f.deckPending, dk)
		}
	}
	delete(f.pending, reservationID)
	f.confirmed[reservationID] = res
	return nil
}

func (f *FakeDealSource) Cancel(_ context.Context, reservationID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.pending[reservationID]; !ok {
		return nil
	}
	delete(f.pending, reservationID)
	if dk := f.resDeck[reservationID]; dk != "" && f.deckPending[dk] == reservationID {
		delete(f.deckPending, dk)
	}
	for opKey, id := range f.byOp {
		if id == reservationID {
			delete(f.byOp, opKey)
			break
		}
	}
	delete(f.resDeck, reservationID)
	delete(f.resDrawCount, reservationID)
	return nil
}

func (f *FakeDealSource) ConfirmedLen() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.confirmed)
}

func (f *FakeDealSource) PendingLen() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.pending)
}

func defaultTwoPlusDeal(seats []string) game.DealMaterial {
	hands := make(map[game.PlayerID][]game.Card, len(seats))
	for i, s := range seats {
		pid := game.PlayerID(s)
		if i == 0 {
			hands[pid] = []game.Card{
				{ID: game.CardID(s + "-c1"), Color: game.ColorRed, Face: game.Face7},
				{ID: game.CardID(s + "-c2"), Color: game.ColorBlue, Face: game.Face3},
			}
		} else {
			hands[pid] = []game.Card{
				{ID: game.CardID(s + "-c1"), Color: game.ColorBlue, Face: game.Face2},
				{ID: game.CardID(s + "-c2"), Color: game.ColorYellow, Face: game.Face4},
			}
		}
	}
	return game.DealMaterial{
		Hands:       hands,
		DiscardTop:  game.Card{ID: "disc-top", Color: game.ColorRed, Face: game.Face5},
		ActiveColor: game.ColorRed,
		CurrentSeat: 0,
		Direction:   game.DirectionClockwise,
	}
}

// FakeSessionValidator validates session bindings; DenyAll rejects everything.
type FakeSessionValidator struct {
	mu    sync.Mutex
	Valid map[string]string // sessionID -> playerID
	Err   error
}

func NewFakeSessionValidator() *FakeSessionValidator {
	return &FakeSessionValidator{Valid: map[string]string{}}
}

func (v *FakeSessionValidator) Allow(sessionID, playerID string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.Valid[sessionID] = playerID
}

func (v *FakeSessionValidator) Validate(_ context.Context, sessionID, playerID string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.Err != nil {
		return v.Err
	}
	if sessionID == "" {
		return errors.New("session required")
	}
	want, ok := v.Valid[sessionID]
	if !ok || want != playerID {
		return errors.New("invalid or stale session")
	}
	return nil
}

// AllowAllSessionValidator accepts any non-empty session/player pair.
type AllowAllSessionValidator struct{}

func (AllowAllSessionValidator) Validate(_ context.Context, sessionID, playerID string) error {
	if sessionID == "" || playerID == "" {
		return errors.New("session and player required")
	}
	return nil
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// MustJSON is a test helper.
func MustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
