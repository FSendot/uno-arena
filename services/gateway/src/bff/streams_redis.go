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

// SessionInvalidationChecker is the durable Redis early-rejection surface.
// Identity remains authoritative for command authorization.
type SessionInvalidationChecker interface {
	IsInvalidated(ctx context.Context, sessionID string) (bool, error)
}

// RedisLiveFeed consumes Room player feeds and Spectator View streams directly.
// Hub provides process-local wake closure; durable Redis invalidation is checked
// on every player/control/authenticated-spectator admission and periodically while
// SSE is open so a replica that misses Pub/Sub cannot retain a stale stream.
// Anonymous spectator (empty sessionID) is unaffected.
type RedisLiveFeed struct {
	playerRDB       streamRedis
	spectatorRDB    streamRedis
	spectatorKS     SpectatorKeySpace
	hub             *Hub
	siStore         SessionInvalidationChecker
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

// WithSessionInvalidationStore attaches the durable Redis invalidation checker.
func (f *RedisLiveFeed) WithSessionInvalidationStore(store SessionInvalidationChecker) *RedisLiveFeed {
	if f == nil {
		return nil
	}
	f.siStore = store
	return f
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
		return f.beginControl(ctx, roomID, sessionID, playerID, lastEventID)
	case StreamPlayer:
		return f.beginPlayer(ctx, roomID, sessionID, playerID, lastEventID)
	case StreamSpectator:
		return f.beginSpectator(ctx, roomID, sessionID, playerID, lastEventID)
	default:
		return nil, fmt.Errorf("unknown stream kind %q", kind)
	}
}

func (f *RedisLiveFeed) denyIfSessionInvalidated(ctx context.Context, sessionID string) error {
	if sessionID == "" {
		return nil
	}
	if f.hub.IsSessionInvalidated(sessionID) {
		return fmt.Errorf("%w: session_invalidated", ErrStreamDenied)
	}
	if f.siStore == nil {
		return nil
	}
	ok, err := f.siStore.IsInvalidated(ctx, sessionID)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrLiveFeedUnavailable, err)
	}
	if ok {
		return fmt.Errorf("%w: session_invalidated", ErrStreamDenied)
	}
	return nil
}

func (f *RedisLiveFeed) beginControl(ctx context.Context, roomID, sessionID, playerID, lastEventID string) (LiveSession, error) {
	if err := f.denyIfSessionInvalidated(ctx, sessionID); err != nil {
		return nil, err
	}
	inner, err := NewHubLiveFeed(f.hub).BeginSession(ctx, StreamControl, roomID, sessionID, playerID, lastEventID)
	if err != nil {
		return nil, err
	}
	if f.siStore == nil || sessionID == "" {
		return inner, nil
	}
	return newDurableControlSession(ctx, f, inner, sessionID), nil
}

func (f *RedisLiveFeed) beginPlayer(ctx context.Context, roomID, sessionID, playerID, lastEventID string) (LiveSession, error) {
	if err := f.denyIfSessionInvalidated(ctx, sessionID); err != nil {
		return nil, err
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
	// Authenticated spectator only; anonymous (empty sessionID) skips invalidation.
	if err := f.denyIfSessionInvalidated(ctx, sessionID); err != nil {
		return nil, err
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
	markerValues := normalizeStreamValues(marker.Values)
	if requirePlayerAudience {
		if fieldString(markerValues, "playerId") != sess.playerID ||
			fieldString(markerValues, "sessionId") != sess.sessionID {
			return false, nil, "", nil
		}
	}
	sess.seedFromMarker(markerValues)

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
		values := normalizeStreamValues(m.Values)
		ev := streamValuesToEvent(m.ID, values)
		if filter != nil {
			keep, stop := filter(ev, values)
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
	return streamValuesToEvent(m.ID, normalizeStreamValues(m.Values))
}

func streamValuesToEvent(redisID string, values map[string]interface{}) StreamEvent {
	evName := fieldString(values, "event")
	data := fieldString(values, "data")
	if data == "" {
		data = "{}"
	}
	schema := 1
	if sv := fieldString(values, "schemaVersion"); sv != "" {
		if n, err := strconv.Atoi(sv); err == nil && n > 0 {
			schema = n
		}
	}
	closed := fieldString(values, "closed") == "1"
	if closed && evName != "stream_closed" {
		evName = "stream_closed"
	}
	// Physical Redis stream ID is the opaque SSE resume marker (Debezium sink owns IDs).
	return StreamEvent{
		ID:            redisID,
		Event:         evName,
		Data:          json.RawMessage(data),
		SchemaVersion: schema,
	}
}

// normalizeStreamValues expands Debezium Redis sink extended-format entries
// ({key, value} plus UPPERCASE headers) into the flat field map used by filters/SSE.
// Flat test-form entries (event/data/playerId/…) pass through unchanged.
func normalizeStreamValues(values map[string]interface{}) map[string]interface{} {
	if len(values) == 0 {
		return map[string]interface{}{}
	}
	rawValue, hasExtended := values["value"]
	if !hasExtended {
		return values
	}

	out := make(map[string]interface{}, len(values)+8)
	for k, v := range values {
		if k == "key" || k == "value" {
			continue
		}
		out[k] = v
		// Extended format stores Kafka headers as UPPERCASE stream fields (e.g. ID).
		if strings.EqualFold(k, "id") {
			if fieldString(out, "eventId") == "" {
				out["eventId"] = fmt.Sprint(v)
			}
		}
	}

	raw := strings.TrimSpace(fmt.Sprint(rawValue))
	if raw == "" || raw == "<nil>" {
		return out
	}
	envelope, ok := parseOutboxEnvelopeJSON(raw)
	if !ok {
		// Malformed extended value — fail closed (filters reject missing fields).
		return out
	}
	liftOutboxEnvelope(out, envelope)
	return out
}

func parseOutboxEnvelopeJSON(raw string) (map[string]interface{}, bool) {
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var top map[string]interface{}
	if err := dec.Decode(&top); err != nil || top == nil {
		return nil, false
	}
	// Connect JSON with schemas: { "schema": …, "payload": <envelope|row> }.
	if p, ok := top["payload"]; ok {
		if pm, ok := p.(map[string]interface{}); ok {
			if looksLikeOutboxEnvelope(pm) {
				return pm, true
			}
			// Raw CDC before/after envelope — unexpected after Outbox router.
			if _, hasAfter := pm["after"]; hasAfter {
				if _, hasOp := pm["op"]; hasOp {
					return nil, false
				}
			}
			if looksLikeOutboxEnvelope(top) {
				return top, true
			}
			return pm, true
		}
	}
	if looksLikeOutboxEnvelope(top) {
		return top, true
	}
	return top, true
}

func looksLikeOutboxEnvelope(m map[string]interface{}) bool {
	if m == nil {
		return false
	}
	_, hasPayload := m["payload"]
	_, hasData := m["data"]
	_, hasEvent := m["event"]
	_, hasEventID := m["eventId"]
	_, hasSeq := m["sequence"]
	_, hasSchema := m["schemaVersion"]
	_, hasPlayer := m["playerId"]
	return hasPayload || hasData || hasEvent || hasEventID || hasSeq || hasSchema || hasPlayer
}

func liftOutboxEnvelope(out, envelope map[string]interface{}) {
	if envelope == nil {
		return
	}
	copyScalarField(out, envelope, "eventId")
	copyScalarField(out, envelope, "event")
	copyScalarField(out, envelope, "schemaVersion")
	copyScalarField(out, envelope, "sequence")
	copyScalarField(out, envelope, "playerId")
	copyScalarField(out, envelope, "sessionId")
	copyScalarField(out, envelope, "closed")

	if data := coerceJSONPayload(envelope["data"]); data != "" {
		out["data"] = data
	} else if data := coerceJSONPayload(envelope["payload"]); data != "" {
		out["data"] = data
	}
}

func copyScalarField(dst, src map[string]interface{}, key string) {
	v, ok := src[key]
	if !ok || v == nil {
		return
	}
	switch t := v.(type) {
	case string:
		dst[key] = t
	case json.Number:
		dst[key] = t.String()
	case float64:
		if key == "schemaVersion" || key == "sequence" {
			dst[key] = strconv.FormatInt(int64(t), 10)
		} else {
			dst[key] = fmt.Sprint(t)
		}
	case bool:
		if t {
			dst[key] = "true"
		} else {
			dst[key] = "false"
		}
	default:
		dst[key] = fmt.Sprint(t)
	}
}

func coerceJSONPayload(v interface{}) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case json.RawMessage:
		return string(t)
	case map[string]interface{}, []interface{}:
		b, err := json.Marshal(t)
		if err != nil {
			return ""
		}
		return string(b)
	case json.Number:
		return t.String()
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return fmt.Sprint(t)
		}
		return string(b)
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
	case json.Number:
		return t.String()
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
			if closed, failClosed := s.checkSessionInvalidation(ctx); closed || failClosed {
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
			if closed, failClosed := s.checkSessionInvalidation(ctx); closed || failClosed {
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
				values := normalizeStreamValues(m.Values)
				ev := streamValuesToEvent(m.ID, values)
				var keep, stop bool
				switch s.kind {
				case StreamPlayer:
					keep, stop = s.filterPlayer(ev, values)
				case StreamSpectator:
					keep, stop = s.filterSpectator(ev, values)
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

// checkSessionInvalidation returns (closedWithEvent, failClosedWithoutEvent).
// Redis checker errors fail closed so a replica missing Pub/Sub cannot retain stale SSE.
func (s *redisLiveSession) checkSessionInvalidation(ctx context.Context) (closed, failClosed bool) {
	if s.sessionID == "" {
		return false, false
	}
	if s.feed.hub.IsSessionInvalidated(s.sessionID) {
		s.emitControl("session_invalidated", map[string]string{
			"sessionId": s.sessionID,
			"reason":    "session_invalidated",
		})
		return true, false
	}
	if s.feed.siStore == nil {
		return false, false
	}
	ok, err := s.feed.siStore.IsInvalidated(ctx, s.sessionID)
	if err != nil {
		return false, true
	}
	if ok {
		s.emitControl("session_invalidated", map[string]string{
			"sessionId": s.sessionID,
			"reason":    "session_invalidated",
		})
		return true, false
	}
	return false, false
}

// durableControlSession wraps Hub control SSE with periodic durable Redis rechecks.
type durableControlSession struct {
	inner     LiveSession
	feed      *RedisLiveFeed
	sessionID string
	out       chan StreamEvent
	cancelCh  chan struct{}
}

func newDurableControlSession(ctx context.Context, feed *RedisLiveFeed, inner LiveSession, sessionID string) *durableControlSession {
	s := &durableControlSession{
		inner:     inner,
		feed:      feed,
		sessionID: sessionID,
		out:       make(chan StreamEvent, 16),
		cancelCh:  make(chan struct{}),
	}
	go s.loop(ctx)
	return s
}

func (s *durableControlSession) Events() <-chan StreamEvent { return s.out }
func (s *durableControlSession) Replay() []StreamEvent      { return s.inner.Replay() }
func (s *durableControlSession) Close() {
	select {
	case <-s.cancelCh:
	default:
		close(s.cancelCh)
	}
	s.inner.Close()
}

func (s *durableControlSession) loop(ctx context.Context) {
	defer close(s.out)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	innerCh := s.inner.Events()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.cancelCh:
			return
		case <-ticker.C:
			if s.feed.hub.IsSessionInvalidated(s.sessionID) {
				s.emitInvalidated()
				return
			}
			ok, err := s.feed.siStore.IsInvalidated(ctx, s.sessionID)
			if err != nil {
				return
			}
			if ok {
				s.emitInvalidated()
				return
			}
		case ev, ok := <-innerCh:
			if !ok {
				return
			}
			select {
			case <-s.cancelCh:
				return
			case s.out <- ev:
				if ev.Event == "session_invalidated" {
					return
				}
			}
		}
	}
}

func (s *durableControlSession) emitInvalidated() {
	payload, _ := json.Marshal(map[string]string{
		"sessionId": s.sessionID,
		"reason":    "session_invalidated",
	})
	select {
	case <-s.cancelCh:
	case s.out <- StreamEvent{
		ID:            "ctrl_session_invalidated",
		Event:         "session_invalidated",
		Data:          payload,
		SchemaVersion: 1,
	}:
	default:
	}
}
