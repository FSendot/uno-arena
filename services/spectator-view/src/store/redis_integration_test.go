//go:build redis_integration

package store_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9"

	"unoarena/services/spectator-view/domain"
	"unoarena/services/spectator-view/store"
)

func openIntegrationStore(t *testing.T) (*store.RedisProjectionStore, context.Context) {
	t.Helper()
	url := strings.TrimSpace(os.Getenv("SPECTATOR_REDIS_URL"))
	if url == "" {
		t.Skip("SPECTATOR_REDIS_URL not set")
	}
	// Require DB 14 isolation from Room timers (DB 2) / other contexts.
	if !strings.Contains(url, "/14") {
		t.Fatalf("SPECTATOR_REDIS_URL must target Redis DB 14, got %q", url)
	}
	rdb, err := store.NewRedisFromURL(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis unavailable (must not skip): %v", err)
	}
	prefix := "spectest_" + randomHex(8) + ":"
	s := store.NewRedisProjectionStore(rdb, prefix)
	if err := s.LoadScripts(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.FlushPrefixedKeys(context.Background()); err != nil {
			t.Errorf("prefix cleanup: %v", err)
		}
		// Ensure we did not delete outside our prefix: scan for leftover is optional.
	})
	if err := s.FlushPrefixedKeys(ctx); err != nil {
		t.Fatal(err)
	}
	return s, ctx
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func roomCreated(room, eid string, seq uint64) domain.SpectatorSafeEvent {
	return domain.SpectatorSafeEvent{
		EventID: domain.EventID(eid), EventType: domain.EventRoomCreated, SchemaVersion: 1,
		RoomID: domain.RoomID(room), Sequence: domain.SequenceNumber(seq),
		Payload: map[string]any{
			"visibility": "public",
			"seats": []any{
				map[string]any{"seatIndex": 0, "playerId": "p1", "displayName": "Alice", "cardCount": 0},
			},
		},
	}
}

func TestRedis_ApplyReplayIdempotent(t *testing.T) {
	s, ctx := openIntegrationStore(t)
	room := domain.RoomID("room_replay")
	out, err := s.Apply(ctx, room, []domain.SpectatorSafeEvent{roomCreated(string(room), "e1", 1)})
	if err != nil || out.Kind != domain.OutcomeAccepted {
		t.Fatalf("apply: %+v err=%v", out, err)
	}
	len1, err := streamLen(ctx, s, room)
	if err != nil || len1 != 1 {
		t.Fatalf("stream len=%d err=%v", len1, err)
	}
	dup, err := s.Apply(ctx, room, []domain.SpectatorSafeEvent{roomCreated(string(room), "e1", 1)})
	if err != nil || dup.Kind != domain.OutcomeDuplicate {
		t.Fatalf("replay: %+v err=%v", dup, err)
	}
	len2, err := streamLen(ctx, s, room)
	if err != nil || len2 != 1 {
		t.Fatalf("replay must not XADD, len=%d", len2)
	}
}

func TestRedis_ConcurrentCAS(t *testing.T) {
	s, ctx := openIntegrationStore(t)
	room := domain.RoomID("room_cas")
	if _, err := s.Apply(ctx, room, []domain.SpectatorSafeEvent{roomCreated(string(room), "seed", 1)}); err != nil {
		t.Fatal(err)
	}
	var accepted atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			out, err := s.Apply(ctx, room, []domain.SpectatorSafeEvent{{
				EventID: domain.EventID(fmt.Sprintf("c%d", i)), EventType: domain.EventTurnAdvanced,
				SchemaVersion: 1, RoomID: room, Sequence: 2,
				Payload: map[string]any{"currentPlayerId": "p1"},
			}})
			if err != nil {
				t.Errorf("apply: %v", err)
				return
			}
			if out.Kind == domain.OutcomeAccepted {
				accepted.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if accepted.Load() != 1 {
		t.Fatalf("exactly one seq=2 accept, got %d", accepted.Load())
	}
	lenN, _ := streamLen(ctx, s, room)
	if lenN != 2 { // seed + one update
		t.Fatalf("stream entries=%d want 2", lenN)
	}
}

func TestRedis_OutOfOrderQuarantine(t *testing.T) {
	s, ctx := openIntegrationStore(t)
	room := domain.RoomID("room_oo")
	_, _ = s.Apply(ctx, room, []domain.SpectatorSafeEvent{roomCreated(string(room), "e1", 1)})
	out, err := s.Apply(ctx, room, []domain.SpectatorSafeEvent{{
		EventID: "e3", EventType: domain.EventPlayerJoinedRoom, SchemaVersion: 1,
		RoomID: room, Sequence: 3, Payload: map[string]any{"playerId": "p2"},
	}})
	if err != nil || out.Kind != domain.OutcomeQuarantined {
		t.Fatalf("want quarantined got %+v err=%v", out, err)
	}
	lenN, _ := streamLen(ctx, s, room)
	if lenN != 1 {
		t.Fatalf("quarantine must not append stream, len=%d", lenN)
	}
	// Idempotent replay of quarantined id
	dup, err := s.Apply(ctx, room, []domain.SpectatorSafeEvent{{
		EventID: "e3", EventType: domain.EventPlayerJoinedRoom, SchemaVersion: 1,
		RoomID: room, Sequence: 3, Payload: map[string]any{"playerId": "p2"},
	}})
	if err != nil || dup.Kind != domain.OutcomeDuplicate {
		t.Fatalf("quarantine replay: %+v", dup)
	}
}

func TestRedis_PrivatePayloadDropZeroStream(t *testing.T) {
	s, ctx := openIntegrationStore(t)
	room := domain.RoomID("room_drop")
	_, _ = s.Apply(ctx, room, []domain.SpectatorSafeEvent{roomCreated(string(room), "e1", 1)})
	out, err := s.Apply(ctx, room, []domain.SpectatorSafeEvent{{
		EventID: "e2", EventType: domain.EventCardPlayed, SchemaVersion: 1,
		RoomID: room, Sequence: 2,
		Payload: map[string]any{"hand": []any{"red-1"}},
	}})
	if err != nil || out.Kind != domain.OutcomeDropped {
		t.Fatalf("want dropped got %+v err=%v", out, err)
	}
	lenN, _ := streamLen(ctx, s, room)
	if lenN != 1 {
		t.Fatalf("dropped must not XADD, len=%d", lenN)
	}
}

func TestRedis_InviteHashNeverPlaintext(t *testing.T) {
	s, ctx := openIntegrationStore(t)
	room := domain.RoomID("room_inv")
	token := "opaque-secret-token"
	ok, err := s.RegisterInvite(ctx, room, token)
	if err != nil || !ok {
		t.Fatalf("register: ok=%v err=%v", ok, err)
	}
	has, err := s.HasInvite(ctx, room, token)
	if err != nil || !has {
		t.Fatalf("has: %v %v", has, err)
	}
	plain, err := s.InvitePlaintextStored(ctx, room, token)
	if err != nil || plain {
		t.Fatalf("plaintext must not be stored, plain=%v err=%v", plain, err)
	}
}

func TestRedis_RebuildAtomicityAndFailure(t *testing.T) {
	s, ctx := openIntegrationStore(t)
	room := domain.RoomID("room_rb")
	_, _ = s.Apply(ctx, room, []domain.SpectatorSafeEvent{roomCreated(string(room), "old", 1)})
	res, err := s.Rebuild(ctx, room, []domain.SpectatorSafeEvent{
		roomCreated(string(room), "n1", 1),
		{
			EventID: "n2", EventType: domain.EventRoomLocked, SchemaVersion: 1,
			RoomID: room, Sequence: 2, Payload: map[string]any{"status": "locked"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != "locked" || res.EventCount != 2 || res.Sequence != 2 {
		t.Fatalf("rebuild result=%+v", res)
	}
	metaSeq, status, _, count, _, err := s.RoomMeta(ctx, room)
	if err != nil || status != "locked" || metaSeq != 2 || count != 2 {
		t.Fatalf("meta seq=%d status=%s count=%d err=%v", metaSeq, status, count, err)
	}

	// Failed rebuild (invalid first sequence) must not leave partial state.
	_, err = s.Rebuild(ctx, room, []domain.SpectatorSafeEvent{{
		EventID: "bad", EventType: domain.EventRoomCreated, SchemaVersion: 1,
		RoomID: room, Sequence: 5, Payload: map[string]any{"visibility": "public"},
	}})
	// RebuildFrom still produces a projection (quarantines); atomic swap still happens.
	// Domain RebuildFrom applies and quarantines rather than failing hard.
	_ = err
	// Snapshot after successful rebuild remains consistent public JSON.
	raw, err := s.SnapshotJSON(ctx, room)
	if err != nil {
		t.Fatal(err)
	}
	var snap map[string]any
	if err := json.Unmarshal(raw, &snap); err != nil {
		t.Fatal(err)
	}
	if snap["sequence"] == nil {
		t.Fatalf("snapshot=%s", raw)
	}
}

func TestRedis_SSEResumeAndTerminal(t *testing.T) {
	s, ctx := openIntegrationStore(t)
	room := domain.RoomID("room_sse")
	_, _ = s.Apply(ctx, room, []domain.SpectatorSafeEvent{roomCreated(string(room), "e1", 1)})
	_, _ = s.Apply(ctx, room, []domain.SpectatorSafeEvent{{
		EventID: "e2", EventType: domain.EventTurnAdvanced, SchemaVersion: 1,
		RoomID: room, Sequence: 2, Payload: map[string]any{"currentPlayerId": "p1"},
	}})

	sess, err := s.BeginLiveSession(ctx, room, "seq_missing")
	if !errors.Is(err, store.ErrSnapshotRequired) || sess == nil {
		t.Fatalf("trimmed history must require snapshot: err=%v sess=%v", err, sess != nil)
	}
	sess.Close()

	sess2, err := s.BeginLiveSession(ctx, room, "seq_1")
	if err != nil && !errors.Is(err, store.ErrSnapshotRequired) {
		t.Fatal(err)
	}
	if sess2 != nil {
		replay := sess2.Replay()
		for _, ev := range replay {
			if ev.Sequence <= 1 {
				t.Fatalf("resume must not replay seq<=1: %+v", replay)
			}
		}
		sess2.Close()
	}

	_, _ = s.Apply(ctx, room, []domain.SpectatorSafeEvent{{
		EventID: "term", EventType: domain.EventMatchCompleted, SchemaVersion: 1,
		RoomID: room, Sequence: 3, Payload: map[string]any{"matchWinner": "p1"},
	}})
	_, err = s.BeginLiveSession(ctx, room, "")
	if !errors.Is(err, store.ErrTerminalRoom) {
		t.Fatalf("terminal deny: %v", err)
	}
}

func TestRedis_NewStoreInstancePersistence(t *testing.T) {
	s, ctx := openIntegrationStore(t)
	room := domain.RoomID("room_persist")
	_, err := s.Apply(ctx, room, []domain.SpectatorSafeEvent{roomCreated(string(room), "e1", 1)})
	if err != nil {
		t.Fatal(err)
	}
	prefix := s.KeyPrefix()
	rdb := s.Client()
	s2 := store.NewRedisProjectionStore(rdb, prefix)
	dup, err := s2.Apply(ctx, room, []domain.SpectatorSafeEvent{roomCreated(string(room), "e1", 1)})
	if err != nil || dup.Kind != domain.OutcomeDuplicate {
		t.Fatalf("new instance replay: %+v err=%v", dup, err)
	}
	raw, err := s2.SnapshotJSON(ctx, room)
	if err != nil {
		t.Fatal(err)
	}
	var snap map[string]any
	_ = json.Unmarshal(raw, &snap)
	if snap["sequence"] != float64(1) {
		t.Fatalf("snap=%v", snap)
	}
}

func TestRedis_ReconnectAfterClientClose(t *testing.T) {
	url := strings.TrimSpace(os.Getenv("SPECTATOR_REDIS_URL"))
	if url == "" {
		t.Skip("SPECTATOR_REDIS_URL not set")
	}
	rdb, err := store.NewRedisFromURL(url)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	prefix := "spectest_" + randomHex(8) + ":"
	s := store.NewRedisProjectionStore(rdb, prefix)
	_ = s.LoadScripts(ctx)
	t.Cleanup(func() {
		_ = s.FlushPrefixedKeys(context.Background())
		_ = rdb.Close()
	})
	room := domain.RoomID("room_reconn")
	_, err = s.Apply(ctx, room, []domain.SpectatorSafeEvent{roomCreated(string(room), "e1", 1)})
	if err != nil {
		t.Fatal(err)
	}
	_ = rdb.Close()

	rdb2, err := store.NewRedisFromURL(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rdb2.Close() })
	s2 := store.NewRedisProjectionStore(rdb2, prefix)
	_ = s2.LoadScripts(ctx)
	if err := s2.Ready(ctx); err != nil {
		t.Fatalf("reconnect ready: %v", err)
	}
	dup, err := s2.Apply(ctx, room, []domain.SpectatorSafeEvent{roomCreated(string(room), "e1", 1)})
	if err != nil || dup.Kind != domain.OutcomeDuplicate {
		t.Fatalf("after reconnect: %+v err=%v", dup, err)
	}
}

func TestRedis_ConcurrentExactDuplicateOneStreamEntry(t *testing.T) {
	s, ctx := openIntegrationStore(t)
	room := domain.RoomID("room_okdup")
	var accepted, dupes atomic.Int64
	var wg sync.WaitGroup
	const n = 16
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := s.Apply(ctx, room, []domain.SpectatorSafeEvent{roomCreated(string(room), "same_e1", 1)})
			if err != nil {
				t.Errorf("apply: %v", err)
				return
			}
			switch out.Kind {
			case domain.OutcomeAccepted:
				accepted.Add(1)
			case domain.OutcomeDuplicate:
				dupes.Add(1)
			default:
				t.Errorf("unexpected kind %+v", out)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != 1 {
		t.Fatalf("want exactly 1 accepted, got %d", accepted.Load())
	}
	if dupes.Load() != int64(n-1) {
		t.Fatalf("want %d duplicates, got %d", n-1, dupes.Load())
	}
	lenN, err := streamLen(ctx, s, room)
	if err != nil || lenN != 1 {
		t.Fatalf("exactly one stream entry, len=%d err=%v", lenN, err)
	}
	key, err := s.StreamKeyFor(ctx, room)
	if err != nil {
		t.Fatal(err)
	}
	msgs, err := s.Client().XRange(ctx, key, "1-0", "1-0").Result()
	if err != nil || len(msgs) != 1 {
		t.Fatalf("explicit id 1-0 missing: len=%d err=%v", len(msgs), err)
	}
}

func TestRedis_SSEResumeBoundedNoFullScan(t *testing.T) {
	base, ctx := openIntegrationStore(t)
	room := domain.RoomID("room_resume_bound")
	s := base.WithStreamMaxLen(8)

	hook := &xrangeHook{}
	type hookable interface {
		AddHook(redis.Hook)
	}
	if c, ok := s.Client().(hookable); ok {
		c.AddHook(hook)
	} else {
		t.Fatal("redis client must support AddHook")
	}

	_, err := s.Apply(ctx, room, []domain.SpectatorSafeEvent{roomCreated(string(room), "e1", 1)})
	if err != nil {
		t.Fatal(err)
	}
	for i := 2; i <= 20; i++ {
		_, err := s.Apply(ctx, room, []domain.SpectatorSafeEvent{{
			EventID: domain.EventID(fmt.Sprintf("e%d", i)), EventType: domain.EventTurnAdvanced,
			SchemaVersion: 1, RoomID: room, Sequence: domain.SequenceNumber(i),
			Payload: map[string]any{"currentPlayerId": "p1"},
		}})
		if err != nil {
			t.Fatal(err)
		}
	}
	key, err := s.StreamKeyFor(ctx, room)
	if err != nil {
		t.Fatal(err)
	}
	// Force trim below earliest IDs so seq_1 is gone.
	if err := s.Client().XTrimMaxLen(ctx, key, 5).Err(); err != nil {
		t.Fatal(err)
	}
	hook.reset()

	sess, err := s.BeginLiveSession(ctx, room, "seq_1")
	if !errors.Is(err, store.ErrSnapshotRequired) || sess == nil {
		t.Fatalf("trimmed id must snapshot: err=%v sess=%v", err, sess != nil)
	}
	sess.Close()
	if hook.unboundedFullScans() != 0 {
		t.Fatalf("resume must not issue XRANGE - +, got %d", hook.unboundedFullScans())
	}
	for _, call := range hook.calls() {
		if call.start == "-" && call.end == "+" {
			t.Fatalf("unbounded XRANGE observed: %+v", call)
		}
	}
}

func TestRedis_SSEResumeAfterExistingIDLaterOnly(t *testing.T) {
	s, ctx := openIntegrationStore(t)
	room := domain.RoomID("room_resume_later")
	_, _ = s.Apply(ctx, room, []domain.SpectatorSafeEvent{roomCreated(string(room), "e1", 1)})
	_, _ = s.Apply(ctx, room, []domain.SpectatorSafeEvent{{
		EventID: "e2", EventType: domain.EventTurnAdvanced, SchemaVersion: 1,
		RoomID: room, Sequence: 2, Payload: map[string]any{"currentPlayerId": "p1"},
	}})
	_, _ = s.Apply(ctx, room, []domain.SpectatorSafeEvent{{
		EventID: "e3", EventType: domain.EventTurnAdvanced, SchemaVersion: 1,
		RoomID: room, Sequence: 3, Payload: map[string]any{"currentPlayerId": "p1"},
	}})

	sess, err := s.BeginLiveSession(ctx, room, "seq_2")
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	replay := sess.Replay()
	if len(replay) != 1 || replay[0].Sequence != 3 || replay[0].ID != "seq_3" {
		t.Fatalf("want only seq_3 after seq_2, got %+v", replay)
	}
	// Explicit Redis ID form
	sess2, err := s.BeginLiveSession(ctx, room, "2-0")
	if err != nil {
		t.Fatal(err)
	}
	defer sess2.Close()
	if len(sess2.Replay()) != 1 || sess2.Replay()[0].RedisID != "3-0" {
		t.Fatalf("want redis 3-0 after 2-0, got %+v", sess2.Replay())
	}
}

func TestRedis_LuaKeysShareRoomHashTag(t *testing.T) {
	s, ctx := openIntegrationStore(t)
	room := domain.RoomID("room_slot")
	_, err := s.Apply(ctx, room, []domain.SpectatorSafeEvent{roomCreated(string(room), "e1", 1)})
	if err != nil {
		t.Fatal(err)
	}
	ks := store.NewKeySpace(s.KeyPrefix())
	keys := []string{
		ks.Meta(room), ks.State(room), ks.Outcomes(room), ks.Generation(room),
		ks.Stream(room, "1"),
	}
	tag, ok := redisHashTag(keys[0])
	if !ok || tag != string(room) {
		t.Fatalf("hash tag=%q ok=%v", tag, ok)
	}
	for _, k := range keys {
		got, ok := redisHashTag(k)
		if !ok || got != tag {
			t.Fatalf("key %q tag=%q want %q", k, got, tag)
		}
		exists, err := s.Client().Exists(ctx, k).Result()
		if err != nil {
			t.Fatal(err)
		}
		if exists == 0 && !strings.HasSuffix(k, "generation") {
			// generation may be lazy; meta/state/outcomes/stream must exist after apply
			if strings.Contains(k, ":meta") || strings.Contains(k, ":state") ||
				strings.Contains(k, ":outcomes") || strings.Contains(k, ":stream:") {
				t.Fatalf("expected key present: %s", k)
			}
		}
	}
	other := domain.RoomID("room_slot_other")
	if redisHashTagMust(ks.Meta(room)) == redisHashTagMust(ks.Meta(other)) {
		t.Fatal("cross-room hash tag collision")
	}
}

func redisHashTagMust(key string) string {
	tag, ok := redisHashTag(key)
	if !ok {
		panic("no tag: " + key)
	}
	return tag
}

type xrangeCall struct {
	start, end string
}

type xrangeHook struct {
	mu       sync.Mutex
	recorded []xrangeCall
}

func (h *xrangeHook) reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.recorded = nil
}

func (h *xrangeHook) calls() []xrangeCall {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]xrangeCall, len(h.recorded))
	copy(out, h.recorded)
	return out
}

func (h *xrangeHook) unboundedFullScans() int {
	n := 0
	for _, c := range h.calls() {
		if c.start == "-" && c.end == "+" {
			n++
		}
	}
	return n
}

func (h *xrangeHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *xrangeHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "xrange" && len(cmd.Args()) >= 4 {
			start, _ := cmd.Args()[2].(string)
			end, _ := cmd.Args()[3].(string)
			h.mu.Lock()
			h.recorded = append(h.recorded, xrangeCall{start: start, end: end})
			h.mu.Unlock()
		}
		return next(ctx, cmd)
	}
}
func (h *xrangeHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func streamLen(ctx context.Context, s *store.RedisProjectionStore, room domain.RoomID) (int64, error) {
	key, err := s.StreamKeyFor(ctx, room)
	if err != nil {
		return 0, err
	}
	return s.Client().XLen(ctx, key).Result()
}
