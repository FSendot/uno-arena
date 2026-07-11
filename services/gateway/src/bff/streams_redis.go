package bff

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultRedisStreamReplayScanBound caps physical Redis stream entries scanned
// during Last-Event-ID resume. Aligned with Spectator View approximate MAXLEN
// (SPECTATOR_REDIS_STREAM_MAXLEN default 1024); overflow requires snapshot.
const DefaultRedisStreamReplayScanBound = 1024

// streamRedis is the Redis Streams surface used by RedisLiveFeed (unit-testable).
type streamRedis interface {
	XRange(ctx context.Context, stream, start, stop string) *redis.XMessageSliceCmd
	XRangeN(ctx context.Context, stream, start, stop string, count int64) *redis.XMessageSliceCmd
	XRead(ctx context.Context, a *redis.XReadArgs) *redis.XStreamSliceCmd
	HGetAll(ctx context.Context, key string) *redis.MapStringStringCmd
}

// RedisLiveFeed consumes Room player feeds and Spectator View streams directly.
// Hub remains the process-local control-state gate (invalidation/terminal) until
// Kafka SessionInvalidated consumption lands — not a cross-replica claim.
type RedisLiveFeed struct {
	playerRDB       streamRedis
	spectatorRDB    streamRedis
	spectatorKS     SpectatorKeySpace
	hub             *Hub
	replayScanBound int
}

// NewRedisLiveFeed constructs a durable LiveFeed.
func NewRedisLiveFeed(playerRDB, spectatorRDB redis.UniversalClient, spectatorPrefix string, hub *Hub) *RedisLiveFeed {
	return newRedisLiveFeed(playerRDB, spectatorRDB, spectatorPrefix, hub)
}

func newRedisLiveFeed(playerRDB, spectatorRDB streamRedis, spectatorPrefix string, hub *Hub) *RedisLiveFeed {
	if hub == nil {
		hub = NewHub()
	}
	return &RedisLiveFeed{
		playerRDB:       playerRDB,
		spectatorRDB:    spectatorRDB,
		spectatorKS:     NewSpectatorKeySpace(spectatorPrefix),
		hub:             hub,
		replayScanBound: DefaultRedisStreamReplayScanBound,
	}
}

// SetReplayScanBound configures the physical Redis XRANGE scan cap (tests may lower it).
func (f *RedisLiveFeed) SetReplayScanBound(n int) {
	if n < 1 {
		n = DefaultRedisStreamReplayScanBound
	}
	f.replayScanBound = n
}

// BeginSession implements LiveFeed.
func (f *RedisLiveFeed) BeginSession(ctx context.Context, kind StreamKind, roomID, sessionID, playerID, lastEventID string) (LiveSession, error) {
	switch kind {
	case StreamControl:
		return NewHubLiveFeed(f.hub).BeginSession(ctx, kind, roomID, sessionID, playerID, lastEventID)
	case StreamPlayer:
		return f.beginPlayer(ctx, roomID, sessionID, playerID, lastEventID)
	case StreamSpectator:
		return f.beginSpectator(ctx, roomID, sessionID, playerID, lastEventID)
	default:
		return nil, fmt.Errorf("unknown stream kind %q", kind)
	}
}

func (f *RedisLiveFeed) beginPlayer(ctx context.Context, roomID, sessionID, playerID, lastEventID string) (LiveSession, error) {
	if sessionID != "" && f.hub.IsSessionInvalidated(sessionID) {
		return nil, fmt.Errorf("%w: session_invalidated", ErrStreamDenied)
	}
	streamKey, err := PlayerFeedTargetStream(roomID)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrStreamDenied, err)
	}
	sess := f.newSession(f.playerRDB, StreamPlayer, roomID, sessionID, playerID, streamKey)
	lastEventID = strings.TrimSpace(lastEventID)
	if lastEventID == "" {
		sess.afterID = "$"
		go sess.loop(ctx)
		return sess, nil
	}
	found, replay, cursor, err := f.readAfterOpaqueID(ctx, f.playerRDB, streamKey, lastEventID, sess, true)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrSnapshotRequired
	}
	sess.replay = replay
	sess.afterID = cursor
	go sess.loop(ctx)
	return sess, nil
}

func (f *RedisLiveFeed) beginSpectator(ctx context.Context, roomID, sessionID, playerID, lastEventID string) (LiveSession, error) {
	if err := ValidateSpectatorRoomID(roomID); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrStreamDenied, err)
	}
	if f.hub.IsRoomTerminal(roomID) {
		return nil, fmt.Errorf("%w: room_terminal", ErrStreamDenied)
	}
	if sessionID != "" && f.hub.IsSessionInvalidated(sessionID) {
		return nil, fmt.Errorf("%w: session_invalidated", ErrStreamDenied)
	}
	gen, closed, err := f.loadSpectatorMeta(ctx, roomID)
	if err != nil {
		return nil, err
	}
	if closed {
		return nil, fmt.Errorf("%w: room_terminal", ErrStreamDenied)
	}
	streamKey := f.spectatorKS.Stream(roomID, gen)
	sess := f.newSession(f.spectatorRDB, StreamSpectator, roomID, sessionID, playerID, streamKey)
	lastEventID = strings.TrimSpace(lastEventID)
	if lastEventID == "" {
		sess.afterID = "$"
		go sess.loop(ctx)
		return sess, nil
	}
	found, replay, cursor, err := f.readAfterOpaqueID(ctx, f.spectatorRDB, streamKey, lastEventID, sess, false)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrSnapshotRequired
	}
	sess.replay = replay
	sess.afterID = cursor
	go sess.loop(ctx)
	return sess, nil
}

func (f *RedisLiveFeed) newSession(rdb streamRedis, kind StreamKind, roomID, sessionID, playerID, streamKey string) *redisLiveSession {
	return &redisLiveSession{
		feed:         f,
		rdb:          rdb,
		kind:         kind,
		roomID:       roomID,
		sessionID:    sessionID,
		playerID:     playerID,
		streamKey:    streamKey,
		ch:           make(chan StreamEvent, 16),
		cancelCh:     make(chan struct{}),
		seenEventIDs: make(map[string]uint64),
		seenOrder:    make([]string, 0),
		dedupeBound:  DefaultIngestIdempotencyBound,
	}
}

func (f *RedisLiveFeed) loadSpectatorMeta(ctx context.Context, roomID string) (generation string, closed bool, err error) {
	meta, err := f.spectatorRDB.HGetAll(ctx, f.spectatorKS.Meta(roomID)).Result()
	if err != nil {
		return "", false, fmt.Errorf("%w: %v", ErrLiveFeedUnavailable, err)
	}
	gen := meta["generation"]
	if gen == "" {
		gen = defaultSpectatorGeneration
	}
	if meta["terminal"] == "1" || meta["stream_closed"] == "1" {
		return gen, true, nil
	}
	return gen, false, nil
}

type entryFilter func(ev StreamEvent, values map[string]interface{}) (keep bool, stop bool)

func (f *RedisLiveFeed) readAfterOpaqueID(
	ctx context.Context,
	rdb streamRedis,
	streamKey, lastEventID string,
	sess *redisLiveSession,
	requirePlayerAudience bool,
) (found bool, replay []StreamEvent, cursor string, err error) {
	if !isOpaqueRedisStreamID(lastEventID) {
		return false, nil, "", nil
	}
	marker, exists, err := streamEntryGet(ctx, rdb, streamKey, lastEventID)
	if err != nil {
		return false, nil, "", fmt.Errorf("%w: %v", ErrLiveFeedUnavailable, err)
	}
	if !exists {
		return false, nil, "", nil
	}
	if requirePlayerAudience {
		if fieldString(marker.Values, "playerId") != sess.playerID ||
			fieldString(marker.Values, "sessionId") != sess.sessionID {
			return false, nil, "", nil
		}
	}
	sess.seedFromMarker(marker.Values)

	bound := f.replayScanBound
	if bound < 1 {
		bound = DefaultRedisStreamReplayScanBound
	}
	// Exclusive range after resume marker — never unbounded XRANGE to "+".
	msgs, err := rdb.XRangeN(ctx, streamKey, "("+lastEventID, "+", int64(bound)+1).Result()
	if err != nil {
		return false, nil, "", fmt.Errorf("%w: %v", ErrLiveFeedUnavailable, err)
	}
	if len(msgs) > bound {
		return false, nil, "", ErrSnapshotRequired
	}

	cursor = lastEventID
	var filter entryFilter
	switch sess.kind {
	case StreamPlayer:
		filter = sess.filterPlayer
	case StreamSpectator:
		filter = sess.filterSpectator
	}
	for _, m := range msgs {
		cursor = m.ID
		ev := streamMsgToEvent(m)
		if filter != nil {
			keep, stop := filter(ev, m.Values)
			if stop {
				if keep {
					replay = append(replay, ev)
				}
				break
			}
			if !keep {
				continue
			}
		}
		replay = append(replay, ev)
	}
	return true, replay, cursor, nil
}

// streamEntryGet loads a single physical Redis stream entry (exact ID bound).
func streamEntryGet(ctx context.Context, rdb streamRedis, streamKey, redisID string) (redis.XMessage, bool, error) {
	msgs, err := rdb.XRange(ctx, streamKey, redisID, redisID).Result()
	if err != nil {
		return redis.XMessage{}, false, err
	}
	if len(msgs) == 0 {
		return redis.XMessage{}, false, nil
	}
	return msgs[0], true, nil
}

// isOpaqueRedisStreamID accepts physical Redis stream IDs (N-M) only.
// Logical sequence / eventId values are payload fields, not resume markers.
func isOpaqueRedisStreamID(id string) bool {
	id = strings.TrimSpace(id)
	dash := strings.IndexByte(id, '-')
	if dash <= 0 || dash == len(id)-1 {
		return false
	}
	if _, err := strconv.ParseUint(id[:dash], 10, 64); err != nil {
		return false
	}
	if _, err := strconv.ParseUint(id[dash+1:], 10, 64); err != nil {
		return false
	}
	return true
}

func streamMsgToEvent(m redis.XMessage) StreamEvent {
	evName := fieldString(m.Values, "event")
	data := fieldString(m.Values, "data")
	if data == "" {
		data = "{}"
	}
	schema := 1
	if sv := fieldString(m.Values, "schemaVersion"); sv != "" {
		if n, err := strconv.Atoi(sv); err == nil && n > 0 {
			schema = n
		}
	}
	closed := fieldString(m.Values, "closed") == "1"
	if closed && evName != "stream_closed" {
		evName = "stream_closed"
	}
	// Physical Redis stream ID is the opaque SSE resume marker (Debezium sink owns IDs).
	return StreamEvent{
		ID:            m.ID,
		Event:         evName,
		Data:          json.RawMessage(data),
		SchemaVersion: schema,
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

type redisLiveSession struct {
	feed         *RedisLiveFeed
	rdb          streamRedis
	kind         StreamKind
	roomID       string
	sessionID    string
	playerID     string
	streamKey    string
	afterID      string
	ch           chan StreamEvent
	replay       []StreamEvent
	cancelCh     chan struct{}
	closed       bool
	seenEventIDs map[string]uint64
	seenOrder    []string
	dedupeBound  int
	lastSequence uint64
}

func (s *redisLiveSession) Events() <-chan StreamEvent { return s.ch }
func (s *redisLiveSession) Replay() []StreamEvent      { return s.replay }

func (s *redisLiveSession) Close() {
	if s.closed {
		return
	}
	s.closed = true
	close(s.cancelCh)
}

func (s *redisLiveSession) seedFromMarker(values map[string]interface{}) {
	eventID := fieldString(values, "eventId")
	seq, err := strconv.ParseUint(fieldString(values, "sequence"), 10, 64)
	if err != nil || seq == 0 {
		return
	}
	if eventID != "" {
		s.rememberEventFields(eventID, seq)
	}
	s.lastSequence = seq
}

// rememberEventFields records eventId→sequence within this subscription.
// Returns true when the eventId was already seen (exact duplicate or conflict);
// caller skips emission without poisoning lastSequence.
func (s *redisLiveSession) rememberEventFields(eventID string, sequence uint64) (duplicate bool) {
	if eventID == "" {
		return false
	}
	if s.seenEventIDs == nil {
		s.seenEventIDs = make(map[string]uint64)
	}
	if _, ok := s.seenEventIDs[eventID]; ok {
		return true
	}
	s.seenEventIDs[eventID] = sequence
	s.seenOrder = append(s.seenOrder, eventID)
	bound := s.dedupeBound
	if bound < 1 {
		bound = DefaultIngestIdempotencyBound
	}
	if len(s.seenOrder) > bound {
		old := s.seenOrder[0]
		s.seenOrder = s.seenOrder[1:]
		delete(s.seenEventIDs, old)
	}
	return false
}

// acceptPayload enforces positive monotonically increasing sequence, per-subscription
// eventId dedupe, and event name + JSON validity before emission. Gaps are allowed.
func (s *redisLiveSession) acceptPayload(ev StreamEvent, values map[string]interface{}) bool {
	if strings.TrimSpace(ev.Event) == "" {
		return false
	}
	if !json.Valid(ev.Data) {
		return false
	}
	seq, err := strconv.ParseUint(fieldString(values, "sequence"), 10, 64)
	if err != nil || seq == 0 {
		return false
	}
	if seq <= s.lastSequence {
		return false
	}
	eventID := fieldString(values, "eventId")
	if s.rememberEventFields(eventID, seq) {
		// Exact duplicate or conflicting eventId: skip without poisoning the stream.
		return false
	}
	s.lastSequence = seq
	return true
}

func (s *redisLiveSession) filterPlayer(ev StreamEvent, values map[string]interface{}) (keep bool, stop bool) {
	entryPlayer := fieldString(values, "playerId")
	entrySession := fieldString(values, "sessionId")
	if entryPlayer != s.playerID || entrySession != s.sessionID {
		return false, false
	}
	if !s.acceptPayload(ev, values) {
		return false, false
	}
	return true, false
}

func (s *redisLiveSession) filterSpectator(ev StreamEvent, values map[string]interface{}) (keep bool, stop bool) {
	terminal := ev.Event == "stream_closed" || ev.Event == "room_terminal"
	if !s.acceptPayload(ev, values) {
		if terminal {
			// Malformed/duplicate terminal still ends the subscription.
			return false, true
		}
		return false, false
	}
	if terminal {
		return true, true
	}
	return true, false
}

func (s *redisLiveSession) loop(ctx context.Context) {
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
		if s.kind == StreamPlayer || s.kind == StreamControl {
			if s.sessionID != "" && s.feed.hub.IsSessionInvalidated(s.sessionID) {
				s.emitControl("session_invalidated", map[string]string{
					"sessionId": s.sessionID,
					"reason":    "session_invalidated",
				})
				return
			}
		}
		if s.kind == StreamSpectator {
			if s.feed.hub.IsRoomTerminal(s.roomID) {
				s.emitControl("room_terminal", map[string]string{
					"roomId": s.roomID,
					"reason": "room_terminal",
				})
				return
			}
			if s.sessionID != "" && s.feed.hub.IsSessionInvalidated(s.sessionID) {
				s.emitControl("session_invalidated", map[string]string{
					"sessionId": s.sessionID,
					"reason":    "session_invalidated",
				})
				return
			}
		}
		streams, err := s.rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{s.streamKey, after},
			Count:   16,
			Block:   2 * time.Second,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			// Runtime Redis errors after SSE headers close the stream.
			return
		}
		for _, st := range streams {
			for _, m := range st.Messages {
				after = m.ID
				ev := streamMsgToEvent(m)
				var keep, stop bool
				switch s.kind {
				case StreamPlayer:
					keep, stop = s.filterPlayer(ev, m.Values)
				case StreamSpectator:
					keep, stop = s.filterSpectator(ev, m.Values)
				default:
					keep = true
				}
				if !keep {
					if stop {
						return
					}
					continue
				}
				if !s.emit(ev) {
					return
				}
				if stop || ev.Event == "stream_closed" || ev.Event == "session_invalidated" || ev.Event == "room_terminal" {
					return
				}
			}
		}
	}
}

func (s *redisLiveSession) emit(ev StreamEvent) bool {
	select {
	case <-s.cancelCh:
		return false
	case s.ch <- ev:
		return true
	}
}

func (s *redisLiveSession) emitControl(event string, data map[string]string) {
	payload, _ := json.Marshal(data)
	_ = s.emit(StreamEvent{
		ID:            fmt.Sprintf("ctrl_%s", event),
		Event:         event,
		Data:          payload,
		SchemaVersion: 1,
	})
}
