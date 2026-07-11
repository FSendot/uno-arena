package bff

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestStreamsKeys_PlayerParity(t *testing.T) {
	got, err := PlayerFeedTargetStream("room_abc")
	if err != nil {
		t.Fatal(err)
	}
	if got != "room:room_abc:player" {
		t.Fatalf("got %q", got)
	}
	if _, err := PlayerFeedTargetStream("bad:id"); err == nil {
		t.Fatal("colon in room id must fail")
	}
	if _, err := PlayerFeedTargetStream(" has space"); err == nil {
		t.Fatal("space must fail")
	}
}

func TestStreamsKeys_SpectatorLayout(t *testing.T) {
	ks := NewSpectatorKeySpace("spectator:")
	if err := ValidateSpectatorRoomID("room_1"); err != nil {
		t.Fatal(err)
	}
	if err := ValidateSpectatorRoomID("bad:id"); err == nil {
		t.Fatal("colon must fail")
	}
	meta := ks.Meta("room_1")
	wantMeta := "spectator:v1:room:{room_1}:meta"
	if meta != wantMeta {
		t.Fatalf("meta=%q want %q", meta, wantMeta)
	}
	stream := ks.Stream("room_1", "2")
	wantStream := "spectator:v1:room:{room_1}:stream:2"
	if stream != wantStream {
		t.Fatalf("stream=%q want %q", stream, wantStream)
	}
}

func TestOpaqueRedisStreamID(t *testing.T) {
	if !isOpaqueRedisStreamID("42-0") {
		t.Fatal("42-0 must be opaque redis id")
	}
	if isOpaqueRedisStreamID("42") {
		t.Fatal("bare sequence must not be resume marker")
	}
	if isOpaqueRedisStreamID("seq_42") {
		t.Fatal("logical seq_ form must not be resume marker")
	}
	if isOpaqueRedisStreamID("") {
		t.Fatal("empty must fail")
	}
}

func playerFilterSession(feed *RedisLiveFeed, playerID, sessionID string) *redisLiveSession {
	return &redisLiveSession{
		feed:         feed,
		kind:         StreamPlayer,
		playerID:     playerID,
		sessionID:    sessionID,
		seenEventIDs: make(map[string]uint64),
		dedupeBound:  DefaultIngestIdempotencyBound,
		ch:           make(chan StreamEvent, 4),
		cancelCh:     make(chan struct{}),
	}
}

func TestPlayerEntry_AudienceFilter(t *testing.T) {
	feed := newRedisLiveFeed(nil, nil, "spectator:", NewHub())
	sess := playerFilterSession(feed, "p1", "s1")
	values := map[string]interface{}{
		"playerId": "p2", "sessionId": "s1", "eventId": "e1", "sequence": "1",
		"event": "CardPlayed", "data": `{}`,
	}
	ev := StreamEvent{Event: "CardPlayed", Data: []byte(`{}`)}
	keep, _ := sess.filterPlayer(ev, values)
	if keep {
		t.Fatal("mismatched playerId must skip")
	}
	values["playerId"] = "p1"
	values["sessionId"] = "s2"
	keep, _ = sess.filterPlayer(ev, values)
	if keep {
		t.Fatal("mismatched sessionId must skip")
	}
	values["sessionId"] = "s1"
	keep, _ = sess.filterPlayer(ev, values)
	if !keep {
		t.Fatal("matching audience must keep")
	}
	keep, _ = sess.filterPlayer(ev, values)
	if keep {
		t.Fatal("duplicate eventId must skip")
	}
}

func TestRedisLiveFeed_TwoSubscribersIndependentlyReceive(t *testing.T) {
	feed := newRedisLiveFeed(nil, nil, "spectator:", NewHub())
	s1 := playerFilterSession(feed, "p1", "s1")
	s2 := playerFilterSession(feed, "p1", "s1")
	values := map[string]interface{}{
		"playerId": "p1", "sessionId": "s1", "eventId": "shared_e1", "sequence": "1",
		"event": "CardPlayed", "data": `{"n":1}`,
	}
	ev := StreamEvent{Event: "CardPlayed", Data: []byte(`{"n":1}`), ID: "10-0"}
	keep1, _ := s1.filterPlayer(ev, values)
	keep2, _ := s2.filterPlayer(ev, values)
	if !keep1 || !keep2 {
		t.Fatalf("both subscribers must accept the same player event independently, keep1=%v keep2=%v", keep1, keep2)
	}
	if !s1.emit(ev) || !s2.emit(ev) {
		t.Fatal("both must emit")
	}
	got1 := <-s1.Events()
	got2 := <-s2.Events()
	if got1.ID != "10-0" || got2.ID != "10-0" {
		t.Fatalf("ids=%q %q", got1.ID, got2.ID)
	}
}

func TestRedisLiveFeed_ConflictingDuplicateDoesNotPoison(t *testing.T) {
	feed := newRedisLiveFeed(nil, nil, "spectator:", NewHub())
	sess := playerFilterSession(feed, "p1", "s1")
	base := map[string]interface{}{
		"playerId": "p1", "sessionId": "s1", "event": "CardPlayed", "data": `{}`,
	}
	v1 := copyValues(base, "eventId", "e1", "sequence", "1")
	ev := StreamEvent{Event: "CardPlayed", Data: []byte(`{}`)}
	if keep, _ := sess.filterPlayer(ev, v1); !keep {
		t.Fatal("first accept")
	}
	// Conflicting eventId with different sequence: skip, do not poison.
	vConflict := copyValues(base, "eventId", "e1", "sequence", "2")
	if keep, _ := sess.filterPlayer(ev, vConflict); keep {
		t.Fatal("conflicting eventId must skip")
	}
	// Later distinct eventId with higher sequence must still deliver.
	vNext := copyValues(base, "eventId", "e2", "sequence", "3")
	if keep, _ := sess.filterPlayer(ev, vNext); !keep {
		t.Fatal("stream must continue after conflicting duplicate")
	}
}

func TestRedisLiveFeed_NonIncreasingAndMalformedSkipped(t *testing.T) {
	feed := newRedisLiveFeed(nil, nil, "spectator:", NewHub())
	sess := playerFilterSession(feed, "p1", "s1")
	base := map[string]interface{}{
		"playerId": "p1", "sessionId": "s1", "event": "CardPlayed", "data": `{}`,
	}
	ev := StreamEvent{Event: "CardPlayed", Data: []byte(`{}`)}
	if keep, _ := sess.filterPlayer(ev, copyValues(base, "eventId", "e1", "sequence", "5")); !keep {
		t.Fatal("seq 5 must keep")
	}
	if keep, _ := sess.filterPlayer(ev, copyValues(base, "eventId", "e2", "sequence", "3")); keep {
		t.Fatal("non-increasing sequence must skip")
	}
	if keep, _ := sess.filterPlayer(ev, copyValues(base, "eventId", "e3", "sequence", "0")); keep {
		t.Fatal("non-positive sequence must skip")
	}
	if keep, _ := sess.filterPlayer(ev, copyValues(base, "eventId", "e4", "sequence", "bad")); keep {
		t.Fatal("malformed sequence must skip")
	}
	badEv := StreamEvent{Event: "", Data: []byte(`{}`)}
	if keep, _ := sess.filterPlayer(badEv, copyValues(base, "eventId", "e5", "sequence", "6")); keep {
		t.Fatal("empty event name must skip")
	}
	badJSON := StreamEvent{Event: "CardPlayed", Data: []byte(`{`)}
	if keep, _ := sess.filterPlayer(badJSON, copyValues(base, "eventId", "e6", "sequence", "6")); keep {
		t.Fatal("invalid JSON data must skip")
	}
	// Gap from 5 → 7 is valid (other audiences share the room stream).
	if keep, _ := sess.filterPlayer(ev, copyValues(base, "eventId", "e7", "sequence", "7")); !keep {
		t.Fatal("gap after last accepted sequence must keep")
	}
}

func TestSpectatorFilter_SequencePerSubscription(t *testing.T) {
	sess := &redisLiveSession{
		kind:         StreamSpectator,
		seenEventIDs: make(map[string]uint64),
		dedupeBound:  DefaultIngestIdempotencyBound,
	}
	ev := StreamEvent{Event: "projection_updated", Data: []byte(`{}`), ID: "1-0"}
	vals := map[string]interface{}{"eventId": "s1", "sequence": "1"}
	keep, stop := sess.filterSpectator(ev, vals)
	if !keep || stop {
		t.Fatalf("first keep=%v stop=%v", keep, stop)
	}
	// Same physical stream can redeliver; sequence must still increase per subscription.
	keep, stop = sess.filterSpectator(ev, map[string]interface{}{"eventId": "s2", "sequence": "1"})
	if keep || stop {
		t.Fatalf("non-increasing spectator sequence must skip, keep=%v stop=%v", keep, stop)
	}
	keep, stop = sess.filterSpectator(StreamEvent{Event: "stream_closed", Data: []byte(`{}`)},
		map[string]interface{}{"eventId": "s3", "sequence": "2"})
	if !keep || !stop {
		t.Fatalf("terminal keep=%v stop=%v", keep, stop)
	}
}

func TestStreamMsgToEvent_UsesPhysicalRedisID(t *testing.T) {
	ev := streamMsgToEvent(redis.XMessage{
		ID: "99-7",
		Values: map[string]interface{}{
			"event": "CardPlayed", "data": `{"roomId":"r1"}`, "sequence": "5", "eventId": "evt_5", "id": "5",
		},
	})
	if ev.ID != "99-7" {
		t.Fatalf("SSE id must be physical redis id, got %q", ev.ID)
	}
	if ev.Event != "CardPlayed" {
		t.Fatalf("event=%q", ev.Event)
	}
}

func TestSpectatorFilter_ClosesOnStreamClosed(t *testing.T) {
	sess := &redisLiveSession{
		kind:         StreamSpectator,
		seenEventIDs: make(map[string]uint64),
		dedupeBound:  DefaultIngestIdempotencyBound,
	}
	keep, stop := sess.filterSpectator(StreamEvent{Event: "stream_closed", Data: []byte(`{}`)},
		map[string]interface{}{"eventId": "c1", "sequence": "1"})
	if !keep || !stop {
		t.Fatalf("keep=%v stop=%v", keep, stop)
	}
	sess2 := &redisLiveSession{
		kind:         StreamSpectator,
		seenEventIDs: make(map[string]uint64),
		dedupeBound:  DefaultIngestIdempotencyBound,
	}
	keep, stop = sess2.filterSpectator(StreamEvent{Event: "projection_updated", Data: []byte(`{}`)},
		map[string]interface{}{"eventId": "p1", "sequence": "1"})
	if !keep || stop {
		t.Fatalf("keep=%v stop=%v", keep, stop)
	}
}

func TestHubLiveFeed_UnknownLastEventID(t *testing.T) {
	hub := NewHub()
	hub.SetReplayBound(2)
	feed := NewHubLiveFeed(hub)
	_, err := feed.BeginSession(t.Context(), StreamSpectator, "room_1", "", "", "gone")
	if !errors.Is(err, ErrSnapshotRequired) {
		t.Fatalf("err=%v", err)
	}
}

func TestRedisLiveFeed_UnknownLastEventID(t *testing.T) {
	feed := newRedisLiveFeed(&stubStreamRedis{}, &stubStreamRedis{}, "spectator:", NewHub())
	_, err := feed.BeginSession(t.Context(), StreamPlayer, "room_1", "s1", "p1", "not-a-redis-id")
	if !errors.Is(err, ErrSnapshotRequired) {
		t.Fatalf("err=%v", err)
	}
}

func TestRedisLiveFeed_EmptyLastEventIDLiveOnly(t *testing.T) {
	stub := &stubStreamRedis{blockNil: true}
	feed := newRedisLiveFeed(stub, stub, "spectator:", NewHub())
	sess, err := feed.BeginSession(t.Context(), StreamPlayer, "room_1", "s1", "p1", "")
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	if len(sess.Replay()) != 0 {
		t.Fatalf("empty Last-Event-ID must not replay, got %d", len(sess.Replay()))
	}
}

func TestRedisLiveFeed_WrongAudienceResumeRequiresSnapshot(t *testing.T) {
	stream, err := PlayerFeedTargetStream("room_1")
	if err != nil {
		t.Fatal(err)
	}
	stub := &stubStreamRedis{
		entries: map[string][]redis.XMessage{
			stream: {{
				ID: "1-0",
				Values: map[string]interface{}{
					"event": "CardPlayed", "data": `{}`, "sequence": "1", "eventId": "e1",
					"playerId": "other", "sessionId": "other_s", "schemaVersion": "1",
				},
			}},
		},
		blockNil: true,
	}
	feed := newRedisLiveFeed(stub, stub, "spectator:", NewHub())
	_, err = feed.BeginSession(t.Context(), StreamPlayer, "room_1", "s1", "p1", "1-0")
	if !errors.Is(err, ErrSnapshotRequired) {
		t.Fatalf("wrong-audience marker must require snapshot, err=%v", err)
	}
}

func TestRedisLiveFeed_BoundedReplayOverflowRequiresSnapshot(t *testing.T) {
	stream, err := PlayerFeedTargetStream("room_1")
	if err != nil {
		t.Fatal(err)
	}
	msgs := []redis.XMessage{{
		ID: "1-0",
		Values: map[string]interface{}{
			"event": "CardPlayed", "data": `{}`, "sequence": "1", "eventId": "e1",
			"playerId": "p1", "sessionId": "s1", "schemaVersion": "1",
		},
	}}
	for i := 2; i <= 4; i++ {
		msgs = append(msgs, redis.XMessage{
			ID: fmt.Sprintf("%d-0", i),
			Values: map[string]interface{}{
				"event": "CardPlayed", "data": `{}`, "sequence": fmt.Sprintf("%d", i), "eventId": fmt.Sprintf("e%d", i),
				"playerId": "p1", "sessionId": "s1", "schemaVersion": "1",
			},
		})
	}
	stub := &stubStreamRedis{entries: map[string][]redis.XMessage{stream: msgs}, blockNil: true}
	feed := newRedisLiveFeed(stub, stub, "spectator:", NewHub())
	feed.SetReplayScanBound(2)
	_, err = feed.BeginSession(t.Context(), StreamPlayer, "room_1", "s1", "p1", "1-0")
	if !errors.Is(err, ErrSnapshotRequired) {
		t.Fatalf("replay beyond scan bound must require snapshot, err=%v", err)
	}
}

func TestRedisLiveFeed_RedisErrorMapsUnavailable(t *testing.T) {
	stub := &stubStreamRedis{xrangeErr: errors.New("redis down")}
	feed := newRedisLiveFeed(stub, stub, "spectator:", NewHub())
	_, err := feed.BeginSession(t.Context(), StreamPlayer, "room_1", "s1", "p1", "1-0")
	if !errors.Is(err, ErrLiveFeedUnavailable) {
		t.Fatalf("err=%v want ErrLiveFeedUnavailable", err)
	}
}

func TestSSE_LiveFeedUnavailableHTTP503(t *testing.T) {
	stub := &stubStreamRedis{xrangeErr: errors.New("redis down")}
	feed := newRedisLiveFeed(stub, stub, "spectator:", NewHub())
	identity := NewFakeIdentity()
	principal := Principal{PlayerID: "player_1", SessionID: "session_1", Username: "alice"}
	token := "tok_alice"
	identity.SeedSession(token, principal)
	srv := NewServer(Dependencies{
		Identity:  identity,
		Room:      NewFakeRoom(),
		Reads:     &FakeReads{},
		Audit:     NewMemoryAudit(),
		Spectator: NewFakeSpectatorGate(),
		Hub:       NewHub(),
		LiveFeed:  feed,
		Ready:     true,
		Clock:     func() time.Time { return time.Date(2026, 7, 10, 15, 0, 0, 0, time.UTC) },
		NewID:     func(prefix string) string { return prefix + "fixed" },
	})
	req := httptest.NewRequest(http.MethodGet, "/v1/streams/player?roomId=room_1", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Last-Event-ID", "1-0")
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "live_feed_unavailable") {
		t.Fatalf("body=%s", w.Body.String())
	}
}

func copyValues(base map[string]interface{}, kv ...string) map[string]interface{} {
	out := make(map[string]interface{}, len(base)+len(kv)/2)
	for k, v := range base {
		out[k] = v
	}
	for i := 0; i+1 < len(kv); i += 2 {
		out[kv[i]] = kv[i+1]
	}
	return out
}

// stubStreamRedis is an offline fake for resume/live-tail unit tests.
type stubStreamRedis struct {
	entries   map[string][]redis.XMessage // stream -> messages
	meta      map[string]map[string]string
	blockNil  bool
	xrangeErr error
	xreadErr  error
	hgetErr   error
}

func (s *stubStreamRedis) XRange(_ context.Context, stream, start, stop string) *redis.XMessageSliceCmd {
	cmd := redis.NewXMessageSliceCmd(context.Background())
	if start == "-" && stop == "+" {
		cmd.SetErr(errors.New("unbounded full-stream scan forbidden"))
		return cmd
	}
	if s.xrangeErr != nil {
		cmd.SetErr(s.xrangeErr)
		return cmd
	}
	msgs := s.entries[stream]
	var out []redis.XMessage
	for _, m := range msgs {
		if start == stop && m.ID == start {
			out = append(out, m)
		}
	}
	cmd.SetVal(out)
	return cmd
}

func (s *stubStreamRedis) XRangeN(_ context.Context, stream, start, stop string, count int64) *redis.XMessageSliceCmd {
	cmd := redis.NewXMessageSliceCmd(context.Background())
	if start == "-" && stop == "+" {
		cmd.SetErr(errors.New("unbounded full-stream scan forbidden"))
		return cmd
	}
	if s.xrangeErr != nil {
		cmd.SetErr(s.xrangeErr)
		return cmd
	}
	msgs := s.entries[stream]
	var out []redis.XMessage
	for _, m := range msgs {
		if len(start) > 0 && start[0] == '(' {
			after := start[1:]
			if m.ID > after {
				out = append(out, m)
			}
		}
	}
	if count > 0 && int64(len(out)) > count {
		out = out[:count]
	}
	cmd.SetVal(out)
	return cmd
}

func (s *stubStreamRedis) XRead(ctx context.Context, a *redis.XReadArgs) *redis.XStreamSliceCmd {
	cmd := redis.NewXStreamSliceCmd(ctx)
	if s.blockNil {
		cmd.SetErr(redis.Nil)
		return cmd
	}
	if s.xreadErr != nil {
		cmd.SetErr(s.xreadErr)
		return cmd
	}
	cmd.SetVal(nil)
	return cmd
}

func (s *stubStreamRedis) HGetAll(ctx context.Context, key string) *redis.MapStringStringCmd {
	cmd := redis.NewMapStringStringCmd(ctx)
	if s.hgetErr != nil {
		cmd.SetErr(s.hgetErr)
		return cmd
	}
	if s.meta != nil {
		if m, ok := s.meta[key]; ok {
			cmd.SetVal(m)
			return cmd
		}
	}
	cmd.SetVal(map[string]string{})
	return cmd
}
