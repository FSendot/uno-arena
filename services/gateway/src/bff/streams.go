package bff

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

// StreamKind identifies an SSE subscription family.
type StreamKind string

const (
	StreamPlayer    StreamKind = "player"
	StreamSpectator StreamKind = "spectator"
	StreamControl   StreamKind = "control"
)

// DefaultStreamReplayBound caps per-stream buffered events for Last-Event-ID resume.
const DefaultStreamReplayBound = 64

// DefaultIngestIdempotencyBound caps remembered ingest eventIds.
const DefaultIngestIdempotencyBound = 4096

var (
	// ErrStreamDenied is returned when Subscribe is refused due to terminal/invalidated state.
	ErrStreamDenied = errors.New("stream subscription denied")
	// ErrSnapshotRequired is returned when Last-Event-ID is unknown or evicted from the bound.
	ErrSnapshotRequired = errors.New("snapshot_required")
	// ErrLiveFeedUnavailable is returned when Redis fails during BeginSession (HTTP 503).
	ErrLiveFeedUnavailable = errors.New("live_feed_unavailable")
	// ErrSequenceConflict is returned when an ingest eventId conflicts with a prior sequence.
	ErrSequenceConflict = errors.New("sequence_conflict")
	// ErrSequenceNotIncreasing is returned when an ingest sequence is not strictly increasing.
	ErrSequenceNotIncreasing = errors.New("sequence_not_increasing")
	// ErrSequenceRequired is returned when ingest omits a positive sequence.
	ErrSequenceRequired = errors.New("sequence_required")
	// ErrControlConflict is returned when a control eventId is rebound to a different
	// eventType/target than the canonical first acceptance.
	ErrControlConflict = errors.New("control_event_conflict")
)

// Canonical control-plane eventType values for idempotent binding.
const (
	ControlEventSessionInvalidated = "SessionInvalidated"
	ControlEventRoomTerminal       = "RoomTerminal"
)

// StreamEvent is one SSE frame the BFF can emit or proxy.
type StreamEvent struct {
	ID            string
	Event         string
	Data          json.RawMessage
	SchemaVersion int
}

// streamSub is an active SSE subscription tracked by Hub.
type streamSub struct {
	id        string
	kind      StreamKind
	roomID    string
	sessionID string
	playerID  string
	events    chan StreamEvent
	closed    bool
}

// Hub tracks live SSE subscriptions for offline control-plane behavior:
// session invalidation closes player/control/authenticated-spectator streams;
// terminal rooms close spectators. Player buffers/fanout are audience-scoped
// (room + player/session), never room-wide.
type Hub struct {
	mu                  sync.Mutex
	subs                map[string]*streamSub
	seq                 uint64
	eventSeq            map[string]uint64 // streamKey -> last published sequence
	buffers             map[string][]StreamEvent
	replayBound         int
	terminalRooms       map[string]struct{}
	invalidatedSessions map[string]struct{}
	seenEventIDs        map[string]uint64 // eventId -> sequence (stream ingest)
	seenEventOrder      []string
	ingestIdempotency   int
	// lastIngest retains the exact last accepted eventId+sequence per stream
	// so retries remain stable duplicates after idempotency-window eviction.
	lastIngest map[string]ingestMark
	// controlEvents binds control eventId -> canonical eventType+target so
	// session invalidation and room terminal cannot cross-collide or rebind.
	controlEvents map[string]controlBinding
}

type ingestMark struct {
	eventID  string
	sequence uint64
}

type controlBinding struct {
	eventType string
	targetID  string
}

// NewHub creates an empty stream hub.
func NewHub() *Hub {
	return &Hub{
		subs:                make(map[string]*streamSub),
		eventSeq:            make(map[string]uint64),
		buffers:             make(map[string][]StreamEvent),
		replayBound:         DefaultStreamReplayBound,
		terminalRooms:       make(map[string]struct{}),
		invalidatedSessions: make(map[string]struct{}),
		seenEventIDs:        make(map[string]uint64),
		seenEventOrder:      make([]string, 0),
		ingestIdempotency:   DefaultIngestIdempotencyBound,
		lastIngest:          make(map[string]ingestMark),
		controlEvents:       make(map[string]controlBinding),
	}
}

// SetReplayBound configures the per-stream replay buffer size (tests may lower it).
func (h *Hub) SetReplayBound(n int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if n < 1 {
		n = 1
	}
	h.replayBound = n
}

// SetIngestIdempotencyBound configures remembered ingest eventId capacity (tests may lower it).
func (h *Hub) SetIngestIdempotencyBound(n int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if n < 1 {
		n = 1
	}
	h.ingestIdempotency = n
}

func streamKey(kind StreamKind, roomID, sessionID, playerID string) string {
	switch kind {
	case StreamControl:
		return string(kind) + "|" + sessionID
	case StreamPlayer:
		return string(kind) + "|" + roomID + "|" + playerID + "|" + sessionID
	default:
		return string(kind) + "|" + roomID
	}
}

// Subscribe registers a new stream subscription after atomically checking
// terminal-room / invalidated-session state. lastEventID enables bounded replay.
// Unknown or evicted Last-Event-ID returns ErrSnapshotRequired (no false continuity).
func (h *Hub) Subscribe(kind StreamKind, roomID, sessionID, playerID, lastEventID string) (id string, events <-chan StreamEvent, replay []StreamEvent, cancel func(), err error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if kind == StreamSpectator {
		if _, terminal := h.terminalRooms[roomID]; terminal {
			return "", nil, nil, nil, fmt.Errorf("%w: room_terminal", ErrStreamDenied)
		}
	}
	if sessionID != "" {
		if _, inv := h.invalidatedSessions[sessionID]; inv {
			if kind == StreamPlayer || kind == StreamControl || kind == StreamSpectator {
				return "", nil, nil, nil, fmt.Errorf("%w: session_invalidated", ErrStreamDenied)
			}
		}
	}

	key := streamKey(kind, roomID, sessionID, playerID)
	replay, err = h.replayLocked(key, lastEventID)
	if err != nil {
		return "", nil, nil, nil, err
	}

	h.seq++
	id = fmt.Sprintf("sub_%d", h.seq)
	sub := &streamSub{
		id:        id,
		kind:      kind,
		roomID:    roomID,
		sessionID: sessionID,
		playerID:  playerID,
		events:    make(chan StreamEvent, 16),
	}
	h.subs[id] = sub

	cancel = func() {
		h.unsubscribe(id)
	}
	return id, sub.events, replay, cancel, nil
}

func (h *Hub) replayLocked(key, lastEventID string) ([]StreamEvent, error) {
	lastEventID = strings.TrimSpace(lastEventID)
	if lastEventID == "" {
		return nil, nil
	}
	buf := h.buffers[key]
	idx := -1
	for i, ev := range buf {
		if ev.ID == lastEventID {
			idx = i
			break
		}
	}
	if idx < 0 {
		// Unknown or evicted marker: never invent tail continuity.
		return nil, ErrSnapshotRequired
	}
	if idx+1 >= len(buf) {
		return nil, nil
	}
	out := make([]StreamEvent, len(buf)-idx-1)
	copy(out, buf[idx+1:])
	return out, nil
}

func (h *Hub) unsubscribe(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	sub, ok := h.subs[id]
	if !ok {
		return
	}
	delete(h.subs, id)
	h.closeSubLocked(sub)
}

func (h *Hub) closeSubLocked(sub *streamSub) {
	if sub.closed {
		return
	}
	sub.closed = true
	close(sub.events)
}

// Publish sends an event to a single subscription if still open (serialized with close).
func (h *Hub) Publish(id string, ev StreamEvent) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	sub, ok := h.subs[id]
	if !ok || sub.closed {
		return false
	}
	return h.sendLocked(sub, ev)
}

// PublishToStream appends to the audience-scoped replay buffer and fans out to matching live subs.
// Player streams are keyed by room + player + session (never room-wide).
// When ev.ID is empty, a strictly increasing per-audience sequence is assigned.
// When ev.ID is set to a numeric sequence, it must be strictly greater than the last
// published sequence for that audience or the publish is a no-op returning -1.
func (h *Hub) PublishToStream(kind StreamKind, roomID, sessionID, playerID string, ev StreamEvent) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.publishToStreamLocked(kind, roomID, sessionID, playerID, ev)
}

// IngestResult is the outcome of an idempotent stream ingest.
type IngestResult struct {
	Duplicate bool
	Sent      int
	Closed    int // spectator streams closed after terminal event families
}

// terminalSpectatorEvents are Room-authored spectator families that publish a
// final frame then mark the room terminal and close live spectator streams.
var terminalSpectatorEvents = map[string]struct{}{
	"RoomCompleted":         {},
	"RoomCancelled":         {},
	"SpectatorStreamsClose": {},
}

func isTerminalSpectatorEvent(event string) bool {
	_, ok := terminalSpectatorEvents[event]
	return ok
}

// IngestToStream atomically enforces eventId idempotency and strictly increasing
// per-audience sequence before any broadcast. Exact eventId+sequence duplicates are stable;
// conflicting eventId sequences or non-increasing sequences reject without fanout.
// After idempotency-window eviction, the exact last accepted eventId+sequence for the
// stream remains a stable duplicate (avoids sequence_not_increasing poisoning retries).
// For spectator RoomCompleted/RoomCancelled/SpectatorStreamsClose, the final event
// is published and CloseSpectatorRoom runs under the same lock (idempotent on duplicate).
func (h *Hub) IngestToStream(kind StreamKind, roomID, sessionID, playerID, eventID string, sequence int64, ev StreamEvent) (IngestResult, error) {
	if eventID == "" {
		return IngestResult{}, fmt.Errorf("eventId required")
	}
	if sequence < 1 {
		return IngestResult{}, ErrSequenceRequired
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	if prev, ok := h.seenEventIDs[eventID]; ok {
		if prev == uint64(sequence) {
			return IngestResult{Duplicate: true}, nil
		}
		return IngestResult{}, ErrSequenceConflict
	}

	key := streamKey(kind, roomID, sessionID, playerID)
	if mark, ok := h.lastIngest[key]; ok && mark.eventID == eventID && mark.sequence == uint64(sequence) {
		return IngestResult{Duplicate: true}, nil
	}

	last := h.eventSeq[key]
	if uint64(sequence) <= last {
		return IngestResult{}, ErrSequenceNotIncreasing
	}

	ev.ID = strconv.FormatInt(sequence, 10)
	sent := h.publishToStreamLocked(kind, roomID, sessionID, playerID, ev)
	h.rememberEventIDLocked(eventID, uint64(sequence))
	h.lastIngest[key] = ingestMark{eventID: eventID, sequence: uint64(sequence)}
	closed := 0
	if kind == StreamSpectator && isTerminalSpectatorEvent(ev.Event) {
		closed = h.closeSpectatorRoomLocked(roomID)
	}
	return IngestResult{Sent: sent, Closed: closed}, nil
}

func (h *Hub) publishToStreamLocked(kind StreamKind, roomID, sessionID, playerID string, ev StreamEvent) int {
	key := streamKey(kind, roomID, sessionID, playerID)
	if ev.ID == "" {
		h.eventSeq[key]++
		ev.ID = strconv.FormatUint(h.eventSeq[key], 10)
	} else if seq, err := strconv.ParseUint(ev.ID, 10, 64); err == nil {
		if seq <= h.eventSeq[key] {
			return -1
		}
		h.eventSeq[key] = seq
	}
	if ev.SchemaVersion < 1 {
		ev.SchemaVersion = 1
	}
	h.appendBufferLocked(key, ev)

	sent := 0
	for _, sub := range h.subs {
		if sub.kind != kind || sub.closed {
			continue
		}
		if !audienceMatch(kind, sub, roomID, sessionID, playerID) {
			continue
		}
		if h.sendLocked(sub, ev) {
			sent++
		}
	}
	return sent
}

func audienceMatch(kind StreamKind, sub *streamSub, roomID, sessionID, playerID string) bool {
	switch kind {
	case StreamControl:
		return sub.sessionID == sessionID
	case StreamPlayer:
		return sub.roomID == roomID && sub.playerID == playerID && sub.sessionID == sessionID
	default:
		return sub.roomID == roomID
	}
}

func (h *Hub) appendBufferLocked(key string, ev StreamEvent) {
	buf := append(h.buffers[key], ev)
	if len(buf) > h.replayBound {
		buf = buf[len(buf)-h.replayBound:]
	}
	h.buffers[key] = buf
}

func (h *Hub) sendLocked(sub *streamSub, ev StreamEvent) bool {
	if sub.closed {
		return false
	}
	select {
	case sub.events <- ev:
		return true
	default:
		// Full subscriber channel: signal resync then close — never silently drop.
		payload, _ := json.Marshal(map[string]string{
			"reason": "subscriber_buffer_full",
		})
		ctrl := StreamEvent{
			ID:            fmt.Sprintf("ctrl_%s", sub.id),
			Event:         "snapshot_required",
			Data:          payload,
			SchemaVersion: 1,
		}
		select {
		case <-sub.events: // make room for the resync signal
		default:
		}
		select {
		case sub.events <- ctrl:
		default:
		}
		delete(h.subs, sub.id)
		h.closeSubLocked(sub)
		return false
	}
}

// RememberEventID records an ingest/control eventId for idempotency.
// Returns true when the id was already seen.
func (h *Hub) RememberEventID(eventID string) bool {
	if eventID == "" {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.seenEventIDs[eventID]; ok {
		return true
	}
	h.rememberEventIDLocked(eventID, 0)
	return false
}

func (h *Hub) rememberEventIDLocked(eventID string, sequence uint64) {
	h.seenEventIDs[eventID] = sequence
	h.seenEventOrder = append(h.seenEventOrder, eventID)
	if len(h.seenEventOrder) > h.ingestIdempotency {
		old := h.seenEventOrder[0]
		h.seenEventOrder = h.seenEventOrder[1:]
		delete(h.seenEventIDs, old)
	}
}

// InvalidateSession marks the session invalidated and closes player/control/
// authenticated-spectator streams for that session.
func (h *Hub) InvalidateSession(sessionID string) int {
	if sessionID == "" {
		return 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.invalidateSessionLocked(sessionID)
}

// ApplySessionInvalidation atomically binds eventId to SessionInvalidated+session
// and applies the invalidation side effect under one lock.
// Exact replay returns duplicate=true with no second invalidation fanout.
// Same eventId rebound to a different type/target returns ErrControlConflict
// without applying a second invalidation.
func (h *Hub) ApplySessionInvalidation(eventID, sessionID string) (closed int, duplicate bool, err error) {
	if eventID == "" || sessionID == "" {
		return 0, false, fmt.Errorf("eventId and sessionId are required")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	dup, err := h.bindControlLocked(eventID, ControlEventSessionInvalidated, sessionID)
	if err != nil {
		return 0, false, err
	}
	if dup {
		return 0, true, nil
	}
	return h.invalidateSessionLocked(sessionID), false, nil
}

// ApplyRoomTerminal atomically binds eventId to RoomTerminal+room and closes
// spectator streams under one lock. Exact replay is a stable duplicate; rebound
// targets/types conflict without a second close.
func (h *Hub) ApplyRoomTerminal(eventID, roomID string) (closed int, duplicate bool, err error) {
	if eventID == "" || roomID == "" {
		return 0, false, fmt.Errorf("eventId and roomId are required")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	dup, err := h.bindControlLocked(eventID, ControlEventRoomTerminal, roomID)
	if err != nil {
		return 0, false, err
	}
	if dup {
		return 0, true, nil
	}
	return h.closeSpectatorRoomLocked(roomID), false, nil
}

func (h *Hub) bindControlLocked(eventID, eventType, targetID string) (duplicate bool, err error) {
	if h.controlEvents == nil {
		h.controlEvents = make(map[string]controlBinding)
	}
	if prev, ok := h.controlEvents[eventID]; ok {
		if prev.eventType == eventType && prev.targetID == targetID {
			return true, nil
		}
		return false, fmt.Errorf("%w: eventId %q already bound to %s/%s", ErrControlConflict, eventID, prev.eventType, prev.targetID)
	}
	h.controlEvents[eventID] = controlBinding{eventType: eventType, targetID: targetID}
	return false, nil
}

func (h *Hub) invalidateSessionLocked(sessionID string) int {
	h.invalidatedSessions[sessionID] = struct{}{}

	var toClose []*streamSub
	for _, sub := range h.subs {
		if sub.sessionID != sessionID {
			continue
		}
		switch sub.kind {
		case StreamPlayer, StreamControl, StreamSpectator:
			toClose = append(toClose, sub)
		}
	}
	for _, sub := range toClose {
		delete(h.subs, sub.id)
	}

	closed := 0
	for _, sub := range toClose {
		h.sendControlLocked(sub, "session_invalidated", map[string]string{
			"sessionId": sessionID,
			"reason":    "session_invalidated",
		})
		h.closeSubLocked(sub)
		closed++
	}
	return closed
}

// CloseSpectatorRoom marks the room terminal and closes spectator streams.
func (h *Hub) CloseSpectatorRoom(roomID string) int {
	if roomID == "" {
		return 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.closeSpectatorRoomLocked(roomID)
}

func (h *Hub) closeSpectatorRoomLocked(roomID string) int {
	h.terminalRooms[roomID] = struct{}{}

	var toClose []*streamSub
	for _, sub := range h.subs {
		if sub.kind == StreamSpectator && sub.roomID == roomID {
			toClose = append(toClose, sub)
		}
	}
	for _, sub := range toClose {
		delete(h.subs, sub.id)
	}

	closed := 0
	for _, sub := range toClose {
		h.sendControlLocked(sub, "room_terminal", map[string]string{
			"roomId": roomID,
			"reason": "room_terminal",
		})
		h.closeSubLocked(sub)
		closed++
	}
	return closed
}

// IsRoomTerminal reports whether the room is marked terminal.
func (h *Hub) IsRoomTerminal(roomID string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, ok := h.terminalRooms[roomID]
	return ok
}

// IsSessionInvalidated reports whether the session is marked invalidated.
func (h *Hub) IsSessionInvalidated(sessionID string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, ok := h.invalidatedSessions[sessionID]
	return ok
}

func (h *Hub) sendControlLocked(sub *streamSub, event string, data map[string]string) {
	payload, _ := json.Marshal(data)
	ev := StreamEvent{
		ID:            fmt.Sprintf("ctrl_%s", sub.id),
		Event:         event,
		Data:          payload,
		SchemaVersion: 1,
	}
	_ = h.sendLocked(sub, ev)
}

// ActiveCount returns the number of open subscriptions (test helper).
func (h *Hub) ActiveCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs)
}

// writeSSE writes a single SSE event to the response writer.
func writeSSE(w http.ResponseWriter, flusher http.Flusher, ev StreamEvent) error {
	if ev.SchemaVersion < 1 {
		ev.SchemaVersion = 1
	}
	data := ev.Data
	if len(data) == 0 {
		data = json.RawMessage(`{}`)
	}
	var envelope map[string]any
	if err := json.Unmarshal(data, &envelope); err != nil {
		envelope = map[string]any{"payload": json.RawMessage(data)}
	}
	envelope["schemaVersion"] = ev.SchemaVersion
	body, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	if ev.ID != "" {
		if _, err := fmt.Fprintf(w, "id: %s\n", ev.ID); err != nil {
			return err
		}
	}
	if ev.Event != "" {
		if _, err := fmt.Fprintf(w, "event: %s\n", ev.Event); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", body); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}
