package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"unoarena/services/analytics/domain"
)

func baseMeta(eventType, eventID string) map[string]any {
	return map[string]any{
		"schemaVersion": 1,
		"eventId":       eventID,
		"eventType":     eventType,
		"correlationId": "corr-" + eventID,
		"occurredAt":    time.Date(2026, 7, 11, 15, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
	}
}

func mustJSON(m map[string]any) []byte {
	b, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return b
}

func rec(topic, key string, value []byte, offset int64) ConsumerRecord {
	return ConsumerRecord{Topic: topic, Partition: 0, Offset: offset, Key: []byte(key), Value: value}
}

func TestParseAnalyticsRecord_AllTopicsIdempotency(t *testing.T) {
	cases := []struct {
		name   string
		topic  string
		key    string
		body   map[string]any
		wantID string
		wantET domain.EventType
		ignore bool
	}{
		{
			name:  "gameplay",
			topic: "room.gameplay.metrics",
			key:   "room-1",
			body: merge(baseMeta("GameplayMetric", "evt-g1"), map[string]any{
				"roomId": "room-1", "visibility": "anonymized_adhoc", "metricType": "turn_advanced",
			}),
			wantID: "evt-g1",
			wantET: domain.EventGameplayMetric,
		},
		{
			name:  "match_completed_tournament",
			topic: "room.match.completed",
			key:   "room-2",
			body: merge(baseMeta("MatchCompleted", "evt-mc1"), map[string]any{
				"roomId": "room-2", "tournamentId": "tour_1", "completionVersion": 3,
				"isAbandoned": false,
				"players":     []any{map[string]any{"playerId": "p1"}}, "forfeits": []any{"p2"},
			}),
			wantID: "room-2|3",
			wantET: domain.EventTournamentStatistic,
		},
		{
			name:  "match_completed_adhoc_ignore",
			topic: "room.match.completed",
			key:   "room-3",
			body: merge(baseMeta("MatchCompleted", "evt-mc2"), map[string]any{
				"roomId": "room-3", "completionVersion": 1, "isAbandoned": false, "players": []any{},
			}),
			wantID: "room-3|1",
			wantET: domain.EventTournamentStatistic,
			ignore: true,
		},
		{
			name:  "match_assigned",
			topic: "tournament.match.assigned",
			key:   "tour_1",
			body: merge(baseMeta("TournamentMatchAssigned", "evt-a1"), map[string]any{
				"tournamentId": "tour_1", "roundNumber": 2, "slotId": "s1", "roomId": "room-9",
			}),
			wantID: "tour_1|2|s1",
			wantET: domain.EventTournamentStatistic,
		},
		{
			name:  "match_result",
			topic: "tournament.match.result_recorded",
			key:   "tour_1",
			body: merge(baseMeta("TournamentMatchResultRecorded", "evt-r1"), map[string]any{
				"tournamentId": "tour_1", "roomId": "room-9", "completionVersion": 4, "slotId": "s1",
			}),
			wantID: "room-9|4",
			wantET: domain.EventTournamentStatistic,
		},
		{
			name:  "players_advanced",
			topic: "tournament.players.advanced",
			key:   "tour_1",
			body: merge(baseMeta("PlayersAdvanced", "evt-pa1"), map[string]any{
				"tournamentId": "tour_1", "roundNumber": 1, "sourceSlotId": "slot-a",
				"advancingPlayerIds": []any{"p1", "p2", "p3"},
			}),
			wantID: "tour_1|1|slot-a",
			wantET: domain.EventTournamentStatistic,
		},
		{
			name:  "round_completed",
			topic: "tournament.round.completed",
			key:   "tour_1",
			body: merge(baseMeta("TournamentRoundCompleted", "evt-rc1"), map[string]any{
				"tournamentId": "tour_1", "roundNumber": 3,
			}),
			wantID: "tour_1|3",
			wantET: domain.EventTournamentStatistic,
		},
		{
			name:  "tournament_completed",
			topic: "tournament.completed",
			key:   "tour_1",
			body: merge(baseMeta("TournamentCompleted", "evt-tc1"), map[string]any{
				"tournamentId": "tour_1", "finalStandings": []any{"p1", "p2", "p3"}, "completionReason": "finished",
			}),
			wantID: "evt-tc1",
			wantET: domain.EventTournamentStatistic,
		},
		{
			name:  "rating_game",
			topic: "ranking.player_rating_updated",
			key:   "player-1",
			body: merge(baseMeta("PlayerRatingUpdated", "evt-pr1"), map[string]any{
				"playerId": "player-1", "gameId": "game-9", "previousRating": 1000, "newRating": 1010,
			}),
			wantID: "player-1|game:game-9",
			wantET: domain.EventRatingStatistic,
		},
		{
			name:  "rating_placement",
			topic: "ranking.player_rating_updated",
			key:   "player-2",
			body: merge(baseMeta("PlayerRatingUpdated", "evt-pr2"), map[string]any{
				"playerId": "player-2", "tournamentId": "tour_1", "placementEventId": "pe-1",
				"previousRating": 900, "newRating": 920,
			}),
			wantID: "player-2|placement:tour_1|pe-1",
			wantET: domain.EventRatingStatistic,
		},
		{
			name:  "leaderboard",
			topic: "ranking.leaderboard_snapshot_published",
			key:   "casual_elo",
			body: merge(baseMeta("LeaderboardSnapshotPublished", "evt-lb1"), map[string]any{
				"snapshotId": "snap-1", "boardType": "casual_elo",
				"entries": []any{
					map[string]any{"playerId": "p1", "rating": 1200, "rank": 1},
				},
			}),
			wantID: "snap-1",
			wantET: domain.EventLeaderboardSnapshot,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			evt, err := ParseAnalyticsRecord(rec(tc.topic, tc.key, mustJSON(tc.body), 1))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if string(evt.Source) != tc.topic {
				t.Fatalf("source=%q", evt.Source)
			}
			if evt.EventType != tc.wantET {
				t.Fatalf("eventType=%q", evt.EventType)
			}
			if evt.IdempotencyKey != tc.wantID {
				t.Fatalf("idem=%q want %q", evt.IdempotencyKey, tc.wantID)
			}
			if evt.DurableIgnore != tc.ignore {
				t.Fatalf("ignore=%v", evt.DurableIgnore)
			}
			if evt.PayloadFingerprint == "" {
				t.Fatal("fingerprint required")
			}
			if _, ok := evt.Payload["players"]; ok {
				t.Fatal("players must not be forwarded")
			}
			if _, ok := evt.Payload["forfeits"]; ok {
				t.Fatal("forfeits must not be forwarded")
			}
			if _, ok := evt.Payload["advancingPlayerIds"]; ok {
				t.Fatal("advancingPlayerIds must not be forwarded")
			}
			if tc.name == "players_advanced" {
				if evt.Payload["advancingPlayerCount"] != float64(3) && evt.Payload["advancingPlayerCount"] != 3 {
					t.Fatalf("advancingPlayerCount=%v", evt.Payload["advancingPlayerCount"])
				}
			}
		})
	}
}

func TestParseAnalyticsRecord_LeaderboardEmptyEntries(t *testing.T) {
	body := merge(baseMeta("LeaderboardSnapshotPublished", "evt-lb-empty"), map[string]any{
		"snapshotId": "snap-empty", "boardType": "casual_elo",
	})
	evt, err := ParseAnalyticsRecord(rec("ranking.leaderboard_snapshot_published", "casual_elo", mustJSON(body), 1))
	if err != nil {
		t.Fatal(err)
	}
	entries, ok := evt.Payload["entries"].([]any)
	if !ok || len(entries) != 0 {
		t.Fatalf("entries=%v", evt.Payload["entries"])
	}
}

func TestParseAnalyticsRecord_KeyMismatchTerminal(t *testing.T) {
	body := merge(baseMeta("GameplayMetric", "evt-km"), map[string]any{
		"roomId": "room-1", "visibility": "anonymized_adhoc", "metricType": "turn_advanced",
	})
	_, err := ParseAnalyticsRecord(rec("room.gameplay.metrics", "wrong", mustJSON(body), 1))
	if err == nil || !IsTerminalKafkaConsumeError(err) {
		t.Fatalf("want terminal, got %v", err)
	}
}

func TestParseAnalyticsRecord_ForbiddenPrivacyTerminal(t *testing.T) {
	body := merge(baseMeta("GameplayMetric", "evt-priv"), map[string]any{
		"roomId": "room-1", "visibility": "anonymized_adhoc", "metricType": "turn_advanced",
		"hand": []any{"r1"},
	})
	_, err := ParseAnalyticsRecord(rec("room.gameplay.metrics", "room-1", mustJSON(body), 1))
	if err == nil || !IsTerminalKafkaConsumeError(err) {
		t.Fatalf("want terminal privacy, got %v", err)
	}
}

func TestParseAnalyticsRecord_InvalidRatingKey(t *testing.T) {
	cases := []map[string]any{
		merge(baseMeta("PlayerRatingUpdated", "evt-bad1"), map[string]any{
			"playerId": "p1", "previousRating": 1, "newRating": 2,
		}),
		merge(baseMeta("PlayerRatingUpdated", "evt-bad2"), map[string]any{
			"playerId": "p1", "tournamentId": "t1", "previousRating": 1, "newRating": 2,
		}),
		merge(baseMeta("PlayerRatingUpdated", "evt-bad3"), map[string]any{
			"playerId": "p1", "placementEventId": "pe", "previousRating": 1, "newRating": 2,
		}),
	}
	for i, body := range cases {
		t.Run(fmt.Sprintf("case_%d", i), func(t *testing.T) {
			_, err := ParseAnalyticsRecord(rec("ranking.player_rating_updated", "p1", mustJSON(body), 1))
			if err == nil || !IsTerminalKafkaConsumeError(err) {
				t.Fatalf("want terminal, got %v", err)
			}
		})
	}
}

func merge(a, b map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

type fakeIngester struct {
	mu    sync.Mutex
	calls []domain.UpstreamEvent
	fn    func(ctx context.Context, evt domain.UpstreamEvent) (domain.ApplyOutcome, error)
}

func (f *fakeIngester) Apply(ctx context.Context, evt domain.UpstreamEvent) (domain.ApplyOutcome, error) {
	f.mu.Lock()
	f.calls = append(f.calls, evt)
	f.mu.Unlock()
	if f.fn != nil {
		return f.fn(ctx, evt)
	}
	return domain.ApplyOutcome{Kind: domain.OutcomeAccepted, EventID: evt.EventID}, nil
}

type fakeSource struct {
	mu       sync.Mutex
	queue    [][]ConsumerRecord
	commits  []ConsumerRecord
	commitFn func(rec ConsumerRecord) error
}

func (f *fakeSource) Poll(ctx context.Context) ([]ConsumerRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.queue) == 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Millisecond):
			return nil, nil
		}
	}
	batch := f.queue[0]
	f.queue = f.queue[1:]
	return batch, nil
}

func (f *fakeSource) Commit(ctx context.Context, rec ConsumerRecord) error {
	_ = ctx
	if f.commitFn != nil {
		if err := f.commitFn(rec); err != nil {
			return err
		}
	}
	f.mu.Lock()
	f.commits = append(f.commits, rec)
	f.mu.Unlock()
	return nil
}

func (f *fakeSource) Close() error { return nil }

func (f *fakeSource) committedOffsets() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]int64, len(f.commits))
	for i, c := range f.commits {
		out[i] = c.Offset
	}
	return out
}

type fakeDLQ struct {
	mu   sync.Mutex
	pubs []dlqPublication
}

type dlqPublication struct {
	Original ConsumerRecord
	Meta     DLQFailureMeta
}

func (f *fakeDLQ) PublishDLQ(ctx context.Context, original ConsumerRecord, meta DLQFailureMeta) error {
	_ = ctx
	f.mu.Lock()
	f.pubs = append(f.pubs, dlqPublication{Original: original, Meta: meta})
	f.mu.Unlock()
	return nil
}

func (f *fakeDLQ) publications() []dlqPublication {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]dlqPublication, len(f.pubs))
	copy(out, f.pubs)
	return out
}

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

func gameplayBody() []byte {
	return mustJSON(merge(baseMeta("GameplayMetric", "evt-c1"), map[string]any{
		"roomId": "room-1", "visibility": "anonymized_adhoc", "metricType": "turn_advanced",
	}))
}

func newTestAnalyticsConsumer(src *fakeSource, dlq *fakeDLQ, h *fakeIngester) *AnalyticsKafkaConsumer {
	return &AnalyticsKafkaConsumer{
		source:  src,
		dlq:     dlq,
		handler: h,
		cfg: AnalyticsKafkaConfig{
			Group:               DefaultAnalyticsKafkaGroup,
			Topics:              append([]string(nil), DefaultAnalyticsTopics...),
			MaxAttempts:         3,
			RetryBackoff:        time.Millisecond,
			MaxPartitionWorkers: 4,
		},
		clock: fixedClock{now: time.Date(2026, 7, 11, 16, 0, 0, 0, time.UTC)},
		sleep: func(ctx context.Context, d time.Duration) error {
			_ = d
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				return nil
			}
		},
	}
}

func TestAnalyticsConsumer_CommitOutcomes(t *testing.T) {
	for _, kind := range []domain.OutcomeKind{
		domain.OutcomeAccepted, domain.OutcomeDuplicate, domain.OutcomeQuarantined, domain.OutcomeIgnored,
	} {
		t.Run(string(kind), func(t *testing.T) {
			src := &fakeSource{}
			dlq := &fakeDLQ{}
			h := &fakeIngester{fn: func(ctx context.Context, evt domain.UpstreamEvent) (domain.ApplyOutcome, error) {
				return domain.ApplyOutcome{Kind: kind, EventID: evt.EventID}, nil
			}}
			c := newTestAnalyticsConsumer(src, dlq, h)
			if err := c.ProcessBatch(context.Background(), []ConsumerRecord{rec("room.gameplay.metrics", "room-1", gameplayBody(), 10)}); err != nil {
				t.Fatal(err)
			}
			if got := src.committedOffsets(); len(got) != 1 || got[0] != 10 {
				t.Fatalf("commits=%v", got)
			}
			if len(dlq.publications()) != 0 {
				t.Fatal("unexpected dlq")
			}
		})
	}
}

func TestAnalyticsConsumer_RetryThenSuccess(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	var attempts atomic.Int32
	h := &fakeIngester{fn: func(ctx context.Context, evt domain.UpstreamEvent) (domain.ApplyOutcome, error) {
		n := attempts.Add(1)
		if n < 3 {
			return domain.ApplyOutcome{}, errors.New("clickhouse temporarily unavailable")
		}
		return domain.ApplyOutcome{Kind: domain.OutcomeAccepted, EventID: evt.EventID}, nil
	}}
	c := newTestAnalyticsConsumer(src, dlq, h)
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{rec("room.gameplay.metrics", "room-1", gameplayBody(), 3)}); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 3 {
		t.Fatalf("attempts=%d", attempts.Load())
	}
	if len(dlq.publications()) != 0 {
		t.Fatal("unexpected dlq")
	}
}

func TestAnalyticsConsumer_ExhaustedDLQBeforeCommit(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	var order []string
	src.commitFn = func(rec ConsumerRecord) error {
		order = append(order, "commit")
		return nil
	}
	h := &fakeIngester{fn: func(ctx context.Context, evt domain.UpstreamEvent) (domain.ApplyOutcome, error) {
		return domain.ApplyOutcome{}, errors.New("connection reset")
	}}
	c := newTestAnalyticsConsumer(src, dlq, h)
	origDLQ := c.dlq
	c.dlq = &orderedDLQ{inner: origDLQ.(*fakeDLQ), order: &order}
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{rec("room.gameplay.metrics", "room-1", gameplayBody(), 99)}); err != nil {
		t.Fatal(err)
	}
	if len(order) < 2 || order[0] != "dlq" || order[1] != "commit" {
		t.Fatalf("order=%v", order)
	}
	pubs := dlq.publications()
	if len(pubs) != 1 || pubs[0].Meta.SourceTopic != "room.gameplay.metrics" {
		t.Fatalf("pubs=%+v", pubs)
	}
}

type orderedDLQ struct {
	inner *fakeDLQ
	order *[]string
}

func (o *orderedDLQ) PublishDLQ(ctx context.Context, original ConsumerRecord, meta DLQFailureMeta) error {
	*o.order = append(*o.order, "dlq")
	return o.inner.PublishDLQ(ctx, original, meta)
}

func TestAnalyticsConsumer_PerTopicDLQSource(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	h := &fakeIngester{}
	c := newTestAnalyticsConsumer(src, dlq, h)
	bad := rec("tournament.round.completed", "tour_1", []byte(`{not-json`), 1)
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{bad}); err != nil {
		t.Fatal(err)
	}
	pubs := dlq.publications()
	if len(pubs) != 1 || pubs[0].Meta.SourceTopic != "tournament.round.completed" {
		t.Fatalf("pubs=%+v", pubs)
	}
}

func TestAnalyticsConsumer_TopicPartitionOrdering(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	var mu sync.Mutex
	seen := map[string][]int64{}
	h := &fakeIngester{fn: func(ctx context.Context, evt domain.UpstreamEvent) (domain.ApplyOutcome, error) {
		return domain.ApplyOutcome{Kind: domain.OutcomeAccepted, EventID: evt.EventID}, nil
	}}
	c := newTestAnalyticsConsumer(src, dlq, h)
	// Same partition number on different topics must not share a serial queue incorrectly;
	// within each topic-partition offsets must still commit in order.
	recs := []ConsumerRecord{
		{Topic: "room.gameplay.metrics", Partition: 0, Offset: 2, Key: []byte("room-1"), Value: gameplayBody()},
		{Topic: "room.match.completed", Partition: 0, Offset: 1, Key: []byte("room-2"), Value: mustJSON(merge(baseMeta("MatchCompleted", "evt-x"), map[string]any{
			"roomId": "room-2", "tournamentId": "t1", "completionVersion": 1,
			"isAbandoned": false, "players": []any{},
		}))},
		{Topic: "room.gameplay.metrics", Partition: 0, Offset: 1, Key: []byte("room-1"), Value: mustJSON(merge(baseMeta("GameplayMetric", "evt-c0"), map[string]any{
			"roomId": "room-1", "visibility": "anonymized_adhoc", "metricType": "turn_advanced",
		}))},
	}
	src.commitFn = func(rec ConsumerRecord) error {
		mu.Lock()
		seen[rec.Topic] = append(seen[rec.Topic], rec.Offset)
		mu.Unlock()
		return nil
	}
	if err := c.ProcessBatch(context.Background(), recs); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	g := seen["room.gameplay.metrics"]
	if len(g) != 2 || g[0] != 1 || g[1] != 2 {
		t.Fatalf("gameplay order=%v", g)
	}
	if m := seen["room.match.completed"]; len(m) != 1 || m[0] != 1 {
		t.Fatalf("match order=%v", m)
	}
}

func TestLoadAnalyticsKafkaConfig_Defaults(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "")
	_, enabled, err := LoadAnalyticsKafkaConfigFromEnv()
	if err != nil || enabled {
		t.Fatalf("empty brokers: enabled=%v err=%v", enabled, err)
	}

	t.Setenv("KAFKA_BROKERS", "kafka:9092")
	t.Setenv("KAFKA_CONSUMER_GROUP", "")
	os.Unsetenv("KAFKA_CONSUMER_GROUP")
	os.Unsetenv("KAFKA_TOPICS")
	cfg, enabled, err := LoadAnalyticsKafkaConfigFromEnv()
	if err != nil || !enabled {
		t.Fatalf("cfg err=%v enabled=%v", err, enabled)
	}
	if cfg.Group != DefaultAnalyticsKafkaGroup || len(cfg.Topics) != 9 {
		t.Fatalf("cfg=%+v", cfg)
	}
	if cfg.DLQTopic(cfg.Topics[0]) != cfg.Topics[0]+".analytics.dlq" {
		t.Fatalf("dlq=%q", cfg.DLQTopic(cfg.Topics[0]))
	}
}

func TestAnalyticsKafkaLifecycle_UnhealthyAfterUnexpectedStop(t *testing.T) {
	life := &analyticsKafkaLifecycle{}
	life.healthy.Store(true)
	life.healthy.Store(false)
	if life.Healthy() {
		t.Fatal("expected unhealthy")
	}
}

func TestParseAnalyticsRecord_TournamentCompletedFinalStandings(t *testing.T) {
	valid := merge(baseMeta("TournamentCompleted", "evt-tc-ok"), map[string]any{
		"tournamentId": "tour_1", "finalStandings": []any{"champ", "second", "third"},
		"completionReason": "finished",
	})
	evt, err := ParseAnalyticsRecord(rec("tournament.completed", "tour_1", mustJSON(valid), 1))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if evt.IdempotencyKey != "evt-tc-ok" {
		t.Fatalf("idem=%q", evt.IdempotencyKey)
	}
	pp, _ := evt.Payload["publicPayload"].(map[string]string)
	if pp == nil {
		if raw, ok := evt.Payload["publicPayload"].(map[string]any); ok {
			pp = map[string]string{}
			for k, v := range raw {
				pp[k] = fmt.Sprint(v)
			}
		}
	}
	if pp["result"] != "champ" {
		t.Fatalf("champion/result must derive from finalStandings[0], got %#v payload=%#v", pp, evt.Payload)
	}
	if _, ok := evt.Payload["finalStandings"]; ok {
		t.Fatal("full finalStandings must not be stored in public projection payload")
	}
	if evt.PayloadFingerprint == "" {
		t.Fatal("fingerprint required")
	}

	reversed := merge(baseMeta("TournamentCompleted", "evt-tc-rev"), map[string]any{
		"tournamentId": "tour_1", "finalStandings": []any{"third", "second", "champ"},
	})
	rev, err := ParseAnalyticsRecord(rec("tournament.completed", "tour_1", mustJSON(reversed), 2))
	if err != nil {
		t.Fatal(err)
	}
	if rev.PayloadFingerprint == evt.PayloadFingerprint {
		t.Fatal("finalStandings order must be fingerprint-sensitive")
	}

	terminalCases := []struct {
		name string
		body map[string]any
	}{
		{"missing", map[string]any{"tournamentId": "tour_1"}},
		{"empty", map[string]any{"tournamentId": "tour_1", "finalStandings": []any{}}},
		{"duplicates", map[string]any{"tournamentId": "tour_1", "finalStandings": []any{"p1", "p1"}}},
		{"too_many", map[string]any{
			"tournamentId":   "tour_1",
			"finalStandings": []any{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"},
		}},
		{"blank_entry", map[string]any{"tournamentId": "tour_1", "finalStandings": []any{"p1", "  "}}},
	}
	for _, tc := range terminalCases {
		t.Run(tc.name, func(t *testing.T) {
			body := merge(baseMeta("TournamentCompleted", "evt-bad-"+tc.name), tc.body)
			_, err := ParseAnalyticsRecord(rec("tournament.completed", "tour_1", mustJSON(body), 1))
			if err == nil || !IsTerminalKafkaConsumeError(err) {
				t.Fatalf("want terminal, got %v", err)
			}
		})
	}
}

func TestParseAnalyticsRecord_FingerprintDistinguishesBody(t *testing.T) {
	basePlayers := merge(baseMeta("PlayersAdvanced", "evt-pa-a"), map[string]any{
		"tournamentId": "tour_1", "roundNumber": 1, "sourceSlotId": "slot-a",
		"advancingPlayerIds": []any{"p1", "p2"}, "rule": "top_n",
	})
	a, err := ParseAnalyticsRecord(rec("tournament.players.advanced", "tour_1", mustJSON(basePlayers), 1))
	if err != nil {
		t.Fatal(err)
	}
	changedPlayers := merge(baseMeta("PlayersAdvanced", "evt-pa-b"), map[string]any{
		"tournamentId": "tour_1", "roundNumber": 1, "sourceSlotId": "slot-a",
		"advancingPlayerIds": []any{"p1", "p3"}, "rule": "top_n",
	})
	b, err := ParseAnalyticsRecord(rec("tournament.players.advanced", "tour_1", mustJSON(changedPlayers), 2))
	if err != nil {
		t.Fatal(err)
	}
	if a.IdempotencyKey != b.IdempotencyKey {
		t.Fatal("same business key expected")
	}
	if a.PayloadFingerprint == b.PayloadFingerprint {
		t.Fatal("changed advancingPlayerIds must change fingerprint")
	}
	if _, ok := a.Payload["advancingPlayerIds"]; ok {
		t.Fatal("advancingPlayerIds must stay stripped from projection payload")
	}

	matchA := merge(baseMeta("MatchCompleted", "evt-mc-a"), map[string]any{
		"roomId": "room-2", "tournamentId": "tour_1", "completionVersion": 3,
		"isAbandoned": false,
		"players": []any{
			map[string]any{"playerId": "p1", "matchWins": 1, "cumulativeCardPoints": 10},
		},
		"forfeits": []any{"p2"},
	})
	ma, err := ParseAnalyticsRecord(rec("room.match.completed", "room-2", mustJSON(matchA), 1))
	if err != nil {
		t.Fatal(err)
	}
	matchB := merge(baseMeta("MatchCompleted", "evt-mc-b"), map[string]any{
		"roomId": "room-2", "tournamentId": "tour_1", "completionVersion": 3,
		"isAbandoned": false,
		"players": []any{
			map[string]any{"playerId": "p9", "matchWins": 1, "cumulativeCardPoints": 10},
		},
		"forfeits": []any{"p2"},
	})
	mb, err := ParseAnalyticsRecord(rec("room.match.completed", "room-2", mustJSON(matchB), 2))
	if err != nil {
		t.Fatal(err)
	}
	if ma.PayloadFingerprint == mb.PayloadFingerprint {
		t.Fatal("changed match players must change fingerprint")
	}
	if _, ok := ma.Payload["players"]; ok {
		t.Fatal("players must stay stripped")
	}

	sameBody := merge(baseMeta("MatchCompleted", "evt-mc-dup-1"), map[string]any{
		"roomId": "room-2", "tournamentId": "tour_1", "completionVersion": 7,
		"isAbandoned": false,
		"players": []any{
			map[string]any{"playerId": "p1", "matchWins": 2, "cumulativeCardPoints": 20},
		},
	})
	d1, err := ParseAnalyticsRecord(rec("room.match.completed", "room-2", mustJSON(sameBody), 1))
	if err != nil {
		t.Fatal(err)
	}
	sameBody2 := merge(baseMeta("MatchCompleted", "evt-mc-dup-2"), map[string]any{
		"roomId": "room-2", "tournamentId": "tour_1", "completionVersion": 7,
		"isAbandoned": false, "correlationId": "corr-other",
		"players": []any{
			map[string]any{"playerId": "p1", "matchWins": 2, "cumulativeCardPoints": 20},
		},
	})
	d2, err := ParseAnalyticsRecord(rec("room.match.completed", "room-2", mustJSON(sameBody2), 2))
	if err != nil {
		t.Fatal(err)
	}
	if d1.EventID == d2.EventID {
		t.Fatal("event ids differ")
	}
	if d1.PayloadFingerprint != d2.PayloadFingerprint {
		t.Fatal("same body/different eventId must be duplicate fingerprint")
	}
}

func TestParseAnalyticsRecord_AdhocIgnoreFingerprintOmitsEventID(t *testing.T) {
	body1 := merge(baseMeta("MatchCompleted", "evt-adhoc-1"), map[string]any{
		"roomId": "room-3", "completionVersion": 1, "isAbandoned": false, "players": []any{},
	})
	a, err := ParseAnalyticsRecord(rec("room.match.completed", "room-3", mustJSON(body1), 1))
	if err != nil || !a.DurableIgnore {
		t.Fatalf("a=%+v err=%v", a, err)
	}
	body2 := merge(baseMeta("MatchCompleted", "evt-adhoc-2"), map[string]any{
		"roomId": "room-3", "completionVersion": 1, "isAbandoned": false, "players": []any{},
	})
	b, err := ParseAnalyticsRecord(rec("room.match.completed", "room-3", mustJSON(body2), 2))
	if err != nil || !b.DurableIgnore {
		t.Fatalf("b=%+v err=%v", b, err)
	}
	if a.PayloadFingerprint != b.PayloadFingerprint {
		t.Fatal("adhoc ignore fingerprint must not include eventId")
	}
	if strings.Contains(a.PayloadFingerprint, "evt-adhoc") {
		t.Fatal("fingerprint is a hash; eventId substring must not appear")
	}
}

func TestParseAnalyticsRecord_LifecyclePhaseLabels(t *testing.T) {
	cases := []struct {
		topic, key, eventType, phase string
		body                         map[string]any
	}{
		{"room.match.completed", "room-2", "MatchCompleted", "match_completed", map[string]any{
			"roomId": "room-2", "tournamentId": "tour_1", "completionVersion": 1,
			"isAbandoned": false, "players": []any{},
		}},
		{"tournament.match.assigned", "tour_1", "TournamentMatchAssigned", "assigned", map[string]any{
			"tournamentId": "tour_1", "roundNumber": 1, "slotId": "s1", "roomId": "r1",
		}},
		{"tournament.match.result_recorded", "tour_1", "TournamentMatchResultRecorded", "result_recorded", map[string]any{
			"tournamentId": "tour_1", "roomId": "r1", "completionVersion": 2,
		}},
		{"tournament.players.advanced", "tour_1", "PlayersAdvanced", "advanced", map[string]any{
			"tournamentId": "tour_1", "roundNumber": 1, "sourceSlotId": "s1",
			"advancingPlayerIds": []any{"p1"},
		}},
		{"tournament.round.completed", "tour_1", "TournamentRoundCompleted", "round_completed", map[string]any{
			"tournamentId": "tour_1", "roundNumber": 2,
		}},
		{"tournament.completed", "tour_1", "TournamentCompleted", "completed", map[string]any{
			"tournamentId": "tour_1", "finalStandings": []any{"champ"},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.phase, func(t *testing.T) {
			body := merge(baseMeta(tc.eventType, "evt-"+tc.phase), tc.body)
			evt, err := ParseAnalyticsRecord(rec(tc.topic, tc.key, mustJSON(body), 1))
			if err != nil {
				t.Fatal(err)
			}
			if evt.Payload["phase"] != tc.phase {
				t.Fatalf("phase=%v want %s", evt.Payload["phase"], tc.phase)
			}
		})
	}
}

func TestParseAnalyticsRecord_UseNumberRejectsNonIntegralAndTrailing(t *testing.T) {
	frac := []byte(`{"schemaVersion":1,"eventId":"evt-frac","eventType":"GameplayMetric","correlationId":"c","occurredAt":"2026-07-11T15:00:00Z","roomId":"room-1","visibility":"anonymized_adhoc","metricType":"turn_advanced","roomSequence":1.5}`)
	_, err := ParseAnalyticsRecord(rec("room.gameplay.metrics", "room-1", frac, 1))
	if err == nil || !IsTerminalKafkaConsumeError(err) {
		t.Fatalf("want terminal for non-integral, got %v", err)
	}

	body := mustJSON(merge(baseMeta("GameplayMetric", "evt-trail"), map[string]any{
		"roomId": "room-1", "visibility": "anonymized_adhoc", "metricType": "turn_advanced",
	}))
	trailing := append(append([]byte{}, body...), []byte(`{"extra":true}`)...)
	_, err = ParseAnalyticsRecord(rec("room.gameplay.metrics", "room-1", trailing, 1))
	if err == nil || !IsTerminalKafkaConsumeError(err) {
		t.Fatalf("want trailing token terminal, got %v", err)
	}

	overflow := []byte(`{"schemaVersion":1,"eventId":"evt-ov","eventType":"GameplayMetric","correlationId":"c","occurredAt":"2026-07-11T15:00:00Z","roomId":"room-1","visibility":"anonymized_adhoc","metricType":"turn_advanced","roomSequence":9223372036854775808}`)
	_, err = ParseAnalyticsRecord(rec("room.gameplay.metrics", "room-1", overflow, 1))
	if err == nil || !IsTerminalKafkaConsumeError(err) {
		t.Fatalf("want terminal for int64 overflow, got %v", err)
	}
}

func TestReady_DistinguishesKafkaConsumerStopped(t *testing.T) {
	store := NewMemoryAnalyticsStore()
	srv := NewServer(store, ProducerCredentials{
		Room: "r", Ranking: "k", Tournament: "t", Ops: "o",
	})
	srv.mode = "durable"
	srv.readyCheck = func(ctx context.Context) error {
		return fmt.Errorf("kafka_consumer_stopped")
	}
	w := doJSON(t, srv.routes(), http.MethodGet, "/ready", nil, nil)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("code=%d", w.Code)
	}
	var body map[string]string
	_ = json.NewDecoder(w.Body).Decode(&body)
	if body["reason"] != "kafka_consumer_stopped" {
		t.Fatalf("reason=%q", body["reason"])
	}

	srv.readyCheck = func(ctx context.Context) error {
		return fmt.Errorf("schema missing table analytics.processed_events")
	}
	w = doJSON(t, srv.routes(), http.MethodGet, "/ready", nil, nil)
	_ = json.NewDecoder(w.Body).Decode(&body)
	if body["reason"] != "clickhouse_schema" {
		t.Fatalf("schema reason=%q", body["reason"])
	}
}

func TestAnalyticsKafkaLifecycle_StopIdempotent(t *testing.T) {
	life := &analyticsKafkaLifecycle{}
	life.healthy.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	life.cancel = cancel
	life.done = make(chan struct{})
	close(life.done)
	life.stop()
	life.stop() // second stop must not panic
	if life.Healthy() {
		t.Fatal("stop must mark unhealthy")
	}
	_ = ctx
}
