package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"

	"unoarena/services/spectator-view/domain"
)

const defaultReplayBound = 64

// StreamEvent is one SSE frame for spectator subscribers.
type StreamEvent struct {
	ID       string
	Event    string
	Data     json.RawMessage
	Sequence uint64
}

type spectatorSub struct {
	id     string
	roomID domain.RoomID
	ch     chan StreamEvent
	closed bool
}

// StreamHub fans out projection updates and closes streams on terminal rooms.
type StreamHub struct {
	mu            sync.Mutex
	subs          map[string]*spectatorSub
	byRoom        map[domain.RoomID]map[string]*spectatorSub
	buffers       map[domain.RoomID][]StreamEvent
	terminalRooms map[domain.RoomID]struct{}
	replayBound   int
	seq           atomic.Uint64
}

// NewStreamHub creates an empty spectator SSE hub.
func NewStreamHub() *StreamHub {
	return &StreamHub{
		subs:          make(map[string]*spectatorSub),
		byRoom:        make(map[domain.RoomID]map[string]*spectatorSub),
		buffers:       make(map[domain.RoomID][]StreamEvent),
		terminalRooms: make(map[domain.RoomID]struct{}),
		replayBound:   defaultReplayBound,
	}
}

// IsTerminal reports whether the room has closed spectator streams.
func (h *StreamHub) IsTerminal(roomID domain.RoomID) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, ok := h.terminalRooms[roomID]
	return ok
}

// MarkTerminal closes existing room streams and denies new admissions via hub state.
func (h *StreamHub) MarkTerminal(roomID domain.RoomID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.terminalRooms[roomID] = struct{}{}
	for id, sub := range h.byRoom[roomID] {
		h.closeSubLocked(sub)
		delete(h.subs, id)
	}
	delete(h.byRoom, roomID)
}

// ActiveCount returns the number of open subscriptions (test/helper).
func (h *StreamHub) ActiveCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs)
}

// Subscribe registers an SSE subscriber. lastEventID enables bounded resume.
// Unknown/evicted Last-Event-ID still registers a fresh live subscriber (non-nil
// channel/cancel) and returns errSnapshotRequired so the handler can write a
// snapshot while live updates queue on the channel — no lost-update window.
// Returns errTerminalRoom when the room is already closed to spectators.
func (h *StreamHub) Subscribe(roomID domain.RoomID, lastEventID string) (id string, events <-chan StreamEvent, replay []StreamEvent, cancel func(), err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, term := h.terminalRooms[roomID]; term {
		return "", nil, nil, nil, errTerminalRoom
	}

	needSnapshot := false
	buf := h.buffers[roomID]
	if lastEventID != "" {
		found := false
		for i, ev := range buf {
			if ev.ID == lastEventID {
				replay = append([]StreamEvent(nil), buf[i+1:]...)
				found = true
				break
			}
		}
		if !found {
			needSnapshot = true
			replay = nil
		}
	}

	id = "sub_" + strconv.FormatUint(h.seq.Add(1), 10)
	sub := &spectatorSub{
		id:     id,
		roomID: roomID,
		ch:     make(chan StreamEvent, 16),
	}
	h.subs[id] = sub
	if h.byRoom[roomID] == nil {
		h.byRoom[roomID] = make(map[string]*spectatorSub)
	}
	h.byRoom[roomID][id] = sub

	cancel = func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if s, ok := h.subs[id]; ok {
			h.closeSubLocked(s)
			delete(h.subs, id)
			delete(h.byRoom[roomID], id)
		}
	}
	if needSnapshot {
		return id, sub.ch, nil, cancel, errSnapshotRequired
	}
	return id, sub.ch, replay, cancel, nil
}

// PublishUpdate broadcasts a projection update and buffers it for resume.
func (h *StreamHub) PublishUpdate(roomID domain.RoomID, sequence uint64, raw json.RawMessage) {
	ev := StreamEvent{
		ID:       "seq_" + strconv.FormatUint(sequence, 10),
		Event:    "projection_updated",
		Data:     raw,
		Sequence: sequence,
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.appendBufferLocked(roomID, ev)
	for _, sub := range h.byRoom[roomID] {
		h.sendLocked(sub, ev)
	}
}

// PublishSnapshot sends an initial snapshot frame id for resume anchoring.
func (h *StreamHub) PublishSnapshot(roomID domain.RoomID, sequence uint64, raw json.RawMessage) StreamEvent {
	ev := StreamEvent{
		ID:       "seq_" + strconv.FormatUint(sequence, 10),
		Event:    "snapshot",
		Data:     raw,
		Sequence: sequence,
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.appendBufferLocked(roomID, ev)
	return ev
}

// appendBufferLocked inserts by sequence to keep the shared replay buffer
// monotonically ordered. Same-sequence events are deduped (first wins) so a
// late snapshot cannot append behind a queued live update and poison resume.
func (h *StreamHub) appendBufferLocked(roomID domain.RoomID, ev StreamEvent) {
	buf := h.buffers[roomID]
	insertAt := len(buf)
	for i, existing := range buf {
		if existing.Sequence == ev.Sequence {
			return
		}
		if existing.Sequence > ev.Sequence {
			insertAt = i
			break
		}
	}
	if insertAt == len(buf) {
		buf = append(buf, ev)
	} else {
		buf = append(buf, StreamEvent{})
		copy(buf[insertAt+1:], buf[insertAt:])
		buf[insertAt] = ev
	}
	if len(buf) > h.replayBound {
		buf = buf[len(buf)-h.replayBound:]
	}
	h.buffers[roomID] = buf
}

func (h *StreamHub) sendLocked(sub *spectatorSub, ev StreamEvent) {
	if sub.closed {
		return
	}
	select {
	case sub.ch <- ev:
	default:
		// Full channel: signal resync then close — never silently drop.
		ctrl := StreamEvent{
			ID:       "ctrl_" + sub.id,
			Event:    "snapshot_required",
			Data:     json.RawMessage(`{"reason":"subscriber_buffer_full"}`),
			Sequence: ev.Sequence,
		}
		select {
		case <-sub.ch:
		default:
		}
		select {
		case sub.ch <- ctrl:
		default:
		}
		delete(h.subs, sub.id)
		delete(h.byRoom[sub.roomID], sub.id)
		h.closeSubLocked(sub)
	}
}

func (h *StreamHub) closeSubLocked(sub *spectatorSub) {
	if sub.closed {
		return
	}
	sub.closed = true
	close(sub.ch)
}

var (
	errTerminalRoom     = fmt.Errorf("room_terminal")
	errSnapshotRequired = fmt.Errorf("snapshot_required")
)

func writeSSE(w http.ResponseWriter, ev StreamEvent) error {
	if ev.ID != "" {
		if _, err := fmt.Fprintf(w, "id: %s\n", ev.ID); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Event, string(ev.Data)); err != nil {
		return err
	}
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	return nil
}
