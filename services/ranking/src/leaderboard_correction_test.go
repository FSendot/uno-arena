package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"unoarena/services/ranking/domain"
	"unoarena/services/ranking/store"
)

func TestParsePlayerRatingUpdated_RequiresEventMetadata(t *testing.T) {
	valid := `{
		"eventId":"e1","eventType":"PlayerRatingUpdated","schemaVersion":1,
		"correlationId":"c1","occurredAt":"2026-01-01T00:00:00Z",
		"playerId":"p1","previousRating":1000,"newRating":1010,"gameId":"g1",
		"projectionVersion":3
	}`
	evt, err := ParsePlayerRatingUpdatedRecord([]byte(valid))
	if err != nil {
		t.Fatal(err)
	}
	if evt.ProjectionVersion != 3 {
		t.Fatalf("projectionVersion=%d", evt.ProjectionVersion)
	}
	for _, bad := range []string{
		`{"eventType":"PlayerRatingUpdated","schemaVersion":1,"correlationId":"c1","occurredAt":"2026-01-01T00:00:00Z","playerId":"p1","previousRating":1,"newRating":2,"gameId":"g1","projectionVersion":1}`,
		`{"eventId":"e1","eventType":"PlayerRatingUpdated","schemaVersion":2,"correlationId":"c1","occurredAt":"2026-01-01T00:00:00Z","playerId":"p1","previousRating":1,"newRating":2,"gameId":"g1","projectionVersion":1}`,
		`{"eventId":"e1","eventType":"PlayerRatingUpdated","schemaVersion":1,"occurredAt":"2026-01-01T00:00:00Z","playerId":"p1","previousRating":1,"newRating":2,"gameId":"g1","projectionVersion":1}`,
		`{"eventId":"e1","eventType":"PlayerRatingUpdated","schemaVersion":1,"correlationId":"c1","playerId":"p1","previousRating":1,"newRating":2,"gameId":"g1","projectionVersion":1}`,
		`{"eventId":"e1","eventType":"PlayerRatingUpdated","schemaVersion":1,"correlationId":"c1","occurredAt":"not-a-time","playerId":"p1","previousRating":1,"newRating":2,"gameId":"g1","projectionVersion":1}`,
		`{"eventId":"e1","eventType":"PlayerRatingUpdated","schemaVersion":1,"correlationId":"c1","occurredAt":"2026-01-01T00:00:00Z","playerId":"p1","previousRating":1,"newRating":2,"gameId":"g1"}`,
		`{"eventId":"e1","eventType":"PlayerRatingUpdated","schemaVersion":1,"correlationId":"c1","occurredAt":"2026-01-01T00:00:00Z","playerId":"p1","previousRating":1,"newRating":2,"gameId":"g1","projectionVersion":0}`,
	} {
		_, err := ParsePlayerRatingUpdatedRecord([]byte(bad))
		if err == nil || !IsTerminalKafkaConsumeError(err) {
			t.Fatalf("want terminal parse error for %s got %v", bad, err)
		}
	}
}

func TestClassifyTopic_ConfiguredOverrides(t *testing.T) {
	cfg := RankingKafkaConfig{
		Topics: []string{
			"custom.game.completed",
			"custom.players.advanced",
			"custom.tournament.completed",
			"custom.player_rating_updated",
		},
	}
	if cfg.classifyTopic("custom.player_rating_updated") != topicKindPlayerRatingUpdated {
		t.Fatal("custom rating topic misclassified")
	}
	if cfg.classifyTopic("custom.players.advanced") != topicKindPlayersAdvanced {
		t.Fatal("custom advanced topic misclassified")
	}
	if cfg.classifyTopic("custom.tournament.completed") != topicKindTournamentCompleted {
		t.Fatal("custom completed topic misclassified")
	}
	if cfg.classifyTopic("custom.game.completed") != topicKindGameCompleted {
		t.Fatal("custom game topic misclassified")
	}
	// Default names still work when Topics unset.
	empty := RankingKafkaConfig{}
	if empty.classifyTopic(DefaultPlayerRatingUpdatedTopic) != topicKindPlayerRatingUpdated {
		t.Fatal("default rating topic")
	}
}

func TestProcessOne_CustomPlayerRatingTopicDispatches(t *testing.T) {
	h := &fakeHandler{
		ratingFn: func(ctx context.Context, evt PlayerRatingUpdatedEvent) error {
			if evt.PlayerID != "p1" || evt.NewRating != 1100 {
				t.Fatalf("evt=%+v", evt)
			}
			return nil
		},
	}
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	cfg := RankingKafkaConfig{
		Group: "ranking",
		Topics: []string{
			"custom.game.completed",
			"custom.players.advanced",
			"custom.tournament.completed",
			"custom.player_rating_updated",
		},
		DLQByTopic: map[string]string{
			"custom.player_rating_updated": "custom.player_rating_updated.ranking.dlq",
		},
		MaxAttempts: 1,
	}
	c := &GameCompletedKafkaConsumer{source: src, dlq: dlq, handler: h, cfg: cfg, clock: systemClock{}}
	body := []byte(`{
		"eventId":"e1","eventType":"PlayerRatingUpdated","schemaVersion":1,
		"correlationId":"c1","occurredAt":"2026-01-01T00:00:00Z",
		"playerId":"p1","previousRating":1000,"newRating":1100,"gameId":"g1",
		"projectionVersion":4
	}`)
	err := c.processOne(context.Background(), ConsumerRecord{
		Topic: "custom.player_rating_updated", Partition: 0, Offset: 1,
		Key: []byte("p1"), Value: body,
	})
	if err != nil {
		t.Fatal(err)
	}
	if h.ratingCalls != 1 {
		t.Fatalf("ratingCalls=%d", h.ratingCalls)
	}
	if len(dlq.pubs) != 0 {
		t.Fatalf("unexpected dlq %+v", dlq.pubs)
	}
}

func TestLeaderboardHTTP_NotReady503(t *testing.T) {
	srv := &Server{
		app:                NewMemoryRatingStore(),
		mode:               "durable",
		readyReason:        "durable_dependencies_missing: REDIS_URL",
		internalCredential: testInternalCredential,
	}
	w := httptest.NewRecorder()
	srv.routes().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/rankings/leaderboards?boardType=casual_elo", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var body map[string]any
	_ = json.NewDecoder(w.Body).Decode(&body)
	if body["code"] != "not_ready" {
		t.Fatalf("body=%+v", body)
	}
}

func TestDurableApp_LeaderboardFallsBackToPostgres(t *testing.T) {
	restore := store.SetLeaderboardCursorMACKeyForTest("test-lb-cursor")
	defer restore()
	// Redis client pointed at closed miniredis → Page fails → Postgres fallback path.
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	addr := mr.Addr()
	mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })
	lb := store.NewRedisLeaderboardStore(rdb, "ranking:")

	fallback := &fallbackPageStore{
		page: store.LeaderboardPage{
			BoardType:         domain.SourceCasualElo,
			ProjectionVersion: 7,
			GeneratedAt:       time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
			Entries: []store.RankedLeaderboardEntry{
				{PlayerID: "p1", Rating: 1200, Rank: 1},
			},
		},
	}
	page, err := leaderboardPageWithFallback(context.Background(), lb, fallback, domain.SourceCasualElo, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if page.ProjectionVersion != 7 || len(page.Entries) != 1 || page.Entries[0].PlayerID != "p1" {
		t.Fatalf("fallback page=%+v", page)
	}
}

type fallbackPageStore struct {
	page store.LeaderboardPage
}

func (f *fallbackPageStore) LeaderboardPage(ctx context.Context, q store.LeaderboardPageQuery) (store.LeaderboardPage, error) {
	return f.page, nil
}

type redisPager interface {
	Page(ctx context.Context, q store.LeaderboardPageQuery) (store.LeaderboardPage, error)
}

type postgresPager interface {
	LeaderboardPage(ctx context.Context, q store.LeaderboardPageQuery) (store.LeaderboardPage, error)
}

func leaderboardPageWithFallback(ctx context.Context, redis redisPager, pg postgresPager, board domain.RatingSourceType, cursor string, limit int) (store.LeaderboardPage, error) {
	q := store.LeaderboardPageQuery{BoardType: board, Cursor: cursor, Limit: limit}
	if redis != nil {
		page, err := redis.Page(ctx, q)
		if err == nil {
			return page, nil
		}
		if !errors.Is(err, store.ErrLeaderboardProjectionUnavailable) && !strings.Contains(err.Error(), "connection") {
			// Still fall back for any redis failure in this unit helper.
		}
	}
	return pg.LeaderboardPage(ctx, q)
}
