package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"unoarena/services/spectator-view/domain"
)

// StreamEvent is one SSE frame sourced from the Redis spectator stream.
type StreamEvent struct {
	ID       string
	Event    string
	Data     json.RawMessage
	Sequence uint64
	RedisID  string
}

// BeginLiveSession opens a Redis-stream-backed SSE session.
// On missing/trimmed Last-Event-ID it returns ErrSnapshotRequired with a live session
// that will deliver new entries after the caller emits a snapshot.
func (s *RedisProjectionStore) BeginLiveSession(ctx context.Context, roomID domain.RoomID, lastEventID string) (*RedisLiveSession, error) {
	if err := ValidateRoomID(roomID); err != nil {
		return nil, err
	}
	load, err := s.loadRoom(ctx, roomID)
	if err != nil {
		return nil, err
	}
	if load.closed || load.proj.StreamClosed() {
		return nil, ErrTerminalRoom
	}
	gen := load.generation
	if gen == "" {
		gen = defaultGeneration
	}
	streamKey := s.keys.Stream(roomID, gen)

	sess := &RedisLiveSession{
		store:     s,
		roomID:    roomID,
		streamKey: streamKey,
		ch:        make(chan StreamEvent, 16),
		cancelCh:  make(chan struct{}),
	}

	needSnapshot := lastEventID == ""
	var replay []StreamEvent
	startID := "0-0"
	if lastEventID != "" {
		found, entries, cursor, err := s.readAfterLogicalID(ctx, streamKey, lastEventID)
		if err != nil {
			return nil, err
		}
		if !found {
			needSnapshot = true
		} else {
			replay = entries
			startID = cursor
		}
	}
	sess.replay = replay
	sess.snapshotRequired = needSnapshot
	sess.afterID = startID

	go sess.loop(ctx)

	if needSnapshot {
		return sess, ErrSnapshotRequired
	}
	return sess, nil
}

func (s *RedisProjectionStore) streamEntryExists(ctx context.Context, streamKey, redisID string) (bool, error) {
	msgs, err := s.rdb.XRange(ctx, streamKey, redisID, redisID).Result()
	if err != nil {
		return false, err
	}
	return len(msgs) > 0, nil
}

func (s *RedisProjectionStore) readAfterLogicalID(ctx context.Context, streamKey, lastEventID string) (found bool, replay []StreamEvent, cursor string, err error) {
	redisID, ok := ParseResumeStreamID(lastEventID)
	if !ok {
		return false, nil, "0-0", nil
	}
	exists, err := s.streamEntryExists(ctx, streamKey, redisID)
	if err != nil {
		return false, nil, "", err
	}
	if !exists {
		return false, nil, "0-0", nil
	}
	// Exclusive range after the resume ID — never XRANGE "-" "+".
	msgs, err := s.rdb.XRange(ctx, streamKey, "("+redisID, "+").Result()
	if err != nil {
		return false, nil, "", err
	}
	for _, m := range msgs {
		replay = append(replay, streamMsgToEvent(m))
	}
	cursor = redisID
	if len(msgs) > 0 {
		cursor = msgs[len(msgs)-1].ID
	}
	return true, replay, cursor, nil
}

func streamMsgToEvent(m redis.XMessage) StreamEvent {
	seq, _ := strconv.ParseUint(fieldString(m.Values, "sequence"), 10, 64)
	ev := fieldString(m.Values, "event")
	data := fieldString(m.Values, "data")
	logical := fieldString(m.Values, "id")
	if logical == "" {
		logical = m.ID
	}
	closed := fieldString(m.Values, "closed") == "1"
	if closed && ev != "stream_closed" {
		ev = "stream_closed"
	}
	return StreamEvent{
		ID:       logical,
		Event:    ev,
		Data:     json.RawMessage(data),
		Sequence: seq,
		RedisID:  m.ID,
	}
}

func fieldString(vals map[string]interface{}, key string) string {
	v, ok := vals[key]
	if !ok || v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	default:
		return fmt.Sprint(t)
	}
}

// RedisLiveSession is one Redis-backed SSE subscription.
type RedisLiveSession struct {
	store            *RedisProjectionStore
	roomID           domain.RoomID
	streamKey        string
	afterID          string
	ch               chan StreamEvent
	replay           []StreamEvent
	cancelCh         chan struct{}
	snapshotRequired bool
	closed           bool
}

// Events returns the live channel.
func (s *RedisLiveSession) Events() <-chan StreamEvent { return s.ch }

// Replay returns buffered entries after Last-Event-ID.
func (s *RedisLiveSession) Replay() []StreamEvent { return s.replay }

// SnapshotRequired reports whether the caller must emit a fresh snapshot.
func (s *RedisLiveSession) SnapshotRequired() bool { return s.snapshotRequired }

// Close stops the reader loop.
func (s *RedisLiveSession) Close() {
	if s.closed {
		return
	}
	s.closed = true
	close(s.cancelCh)
}

func (s *RedisLiveSession) loop(ctx context.Context) {
	defer close(s.ch)
	after := s.afterID
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.cancelCh:
			return
		default:
		}
		streams, err := s.store.rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{s.streamKey, after},
			Count:   16,
			Block:   2 * time.Second,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				// Check terminal between blocks.
				term, tErr := s.store.IsTerminal(ctx, s.roomID)
				if tErr == nil && term {
					s.emit(StreamEvent{Event: "stream_closed", Data: json.RawMessage(`{}`)})
					return
				}
				continue
			}
			return
		}
		for _, st := range streams {
			for _, m := range st.Messages {
				after = m.ID
				ev := streamMsgToEvent(m)
				if !s.emit(ev) {
					return
				}
				if ev.Event == "stream_closed" {
					return
				}
			}
		}
	}
}

func (s *RedisLiveSession) emit(ev StreamEvent) bool {
	select {
	case <-s.cancelCh:
		return false
	case s.ch <- ev:
		return true
	}
}
