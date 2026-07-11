//go:build integration

package store_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"unoarena/services/analytics/domain"
	"unoarena/services/analytics/store"
)

func clickhouseURL(t *testing.T) (baseURL, user, pass, database string) {
	t.Helper()
	baseURL = os.Getenv("ANALYTICS_CLICKHOUSE_URL")
	if baseURL == "" {
		baseURL = os.Getenv("CLICKHOUSE_URL")
	}
	user = os.Getenv("ANALYTICS_CLICKHOUSE_USER")
	if user == "" {
		user = os.Getenv("CLICKHOUSE_USER")
	}
	pass = os.Getenv("ANALYTICS_CLICKHOUSE_PASSWORD")
	if pass == "" {
		pass = os.Getenv("CLICKHOUSE_PASSWORD")
	}
	database = os.Getenv("ANALYTICS_CLICKHOUSE_DATABASE")
	if database == "" {
		database = os.Getenv("CLICKHOUSE_DB")
	}
	if baseURL == "" || user == "" || pass == "" || database == "" {
		t.Skip("ANALYTICS_CLICKHOUSE_URL/USER/PASSWORD/DATABASE not set")
	}
	if err := store.ValidateSafeTestDatabase(database); err != nil {
		t.Fatalf("%v", err)
	}
	return baseURL, user, pass, database
}

func openStore(t *testing.T) *store.AnalyticsStore {
	t.Helper()
	ctx := context.Background()
	base, user, pass, db := clickhouseURL(t)
	adminUser := os.Getenv("ANALYTICS_CLICKHOUSE_ADMIN_USER")
	adminPass := os.Getenv("ANALYTICS_CLICKHOUSE_ADMIN_PASSWORD")
	if adminUser == "" {
		adminUser = user
	}
	if adminPass == "" {
		adminPass = pass
	}

	// Admin client on default DB creates/drops nothing of production analytics;
	// applies transformed migration into the generated test database only.
	admin, err := store.NewClient(store.Config{URL: base, User: adminUser, Password: adminPass, Database: "default"})
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	ident, err := store.QuoteIdent(db)
	if err != nil {
		t.Fatal(err)
	}
	_ = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+ident, nil)
	if err := admin.Exec(ctx, "CREATE DATABASE "+ident, nil); err != nil {
		t.Fatalf("create test db: %v", err)
	}
	t.Cleanup(func() {
		_ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+ident, nil)
	})

	raw, err := os.ReadFile(filepath.Join("..", "..", "migrations", "001_init.sql"))
	if err != nil {
		t.Fatalf("migration: %v", err)
	}
	transformed, err := store.TransformMigrationForDatabase(string(raw), db)
	if err != nil {
		t.Fatal(err)
	}
	migClient, err := store.NewClient(store.Config{URL: base, User: adminUser, Password: adminPass, Database: db})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyMigrationStatements(ctx, migClient, transformed); err != nil {
		t.Fatalf("apply migration: %v", err)
	}
	// Runtime DML grants for the ephemeral DB only (never analytics production DB).
	runtimeUser := user
	if err := admin.Exec(ctx, fmt.Sprintf("GRANT SELECT, INSERT ON %s.* TO %s", ident, runtimeUser), nil); err != nil {
		// ClickHouse may require quoting the user; try bare then fail.
		t.Fatalf("grant runtime on test db: %v", err)
	}

	runtime, err := store.NewClient(store.Config{URL: base, User: user, Password: pass, Database: db})
	if err != nil {
		t.Fatal(err)
	}
	s := store.NewAnalyticsStore(runtime)
	if err := s.Ready(ctx); err != nil {
		t.Fatalf("ready: %v", err)
	}
	return s
}

func gameplayEvt(id string) domain.UpstreamEvent {
	return domain.UpstreamEvent{
		EventID: domain.EventID(id), EventType: domain.EventGameplayMetric,
		Source: domain.SourceRoomGameplayMetrics, SchemaVersion: domain.CurrentSchemaVersion,
		CorrelationID: "c-" + id, OccurredAt: time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC),
		Payload: map[string]any{
			"visibility": "anonymized_adhoc", "metricType": "turn_advanced", "roomId": "room_1",
		},
	}
}

func tournamentEvt(id, tid string) domain.UpstreamEvent {
	return domain.UpstreamEvent{
		EventID: domain.EventID(id), EventType: domain.EventTournamentStatistic,
		Source: domain.SourceTournamentRoundCompleted, SchemaVersion: domain.CurrentSchemaVersion,
		CorrelationID: "c-" + id, OccurredAt: time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC),
		Payload: map[string]any{
			"tournamentId": tid, "phase": "final", "registeredCount": 8,
			"publicPayload": map[string]any{"bracketLabel": "F-1"},
		},
	}
}

func ratingEvt(id, pid string) domain.UpstreamEvent {
	return domain.UpstreamEvent{
		EventID: domain.EventID(id), EventType: domain.EventRatingStatistic,
		Source: domain.SourceRankingPlayerRatingUpdated, SchemaVersion: domain.CurrentSchemaVersion,
		CorrelationID: "c-" + id, OccurredAt: time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC),
		Payload: map[string]any{
			"playerId": pid, "sourceType": "casual_elo", "previousRating": 1000, "newRating": 1016,
		},
	}
}

func leaderboardEvt(id string) domain.UpstreamEvent {
	return domain.UpstreamEvent{
		EventID: domain.EventID(id), EventType: domain.EventLeaderboardSnapshot,
		Source: domain.SourceRankingLeaderboardSnapshot, SchemaVersion: domain.CurrentSchemaVersion,
		CorrelationID: "c-" + id, OccurredAt: time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC),
		Payload: map[string]any{
			"boardType": "casual_elo", "snapshotId": "snap-" + id, "sourceType": "casual_elo",
			"entries": []any{
				map[string]any{"playerId": "p1", "rating": 1200},
				map[string]any{"playerId": "p2", "rating": 1100},
				map[string]any{"playerId": "p3", "rating": 1000},
			},
		},
	}
}

func privacyBadEvt(id string) domain.UpstreamEvent {
	return domain.UpstreamEvent{
		EventID: domain.EventID(id), EventType: domain.EventTournamentStatistic,
		Source: domain.SourceTournamentRoundCompleted, SchemaVersion: domain.CurrentSchemaVersion,
		CorrelationID: "c-" + id, OccurredAt: time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC),
		Payload: map[string]any{
			"tournamentId": "tour_q", "phase": "final",
			"publicPayload": map[string]any{"bracketLabel": "F-1", "playerEmail": "leak@example.com"},
		},
	}
}

func TestIntegration_AcceptedGameplayTournamentRating(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	for _, evt := range []domain.UpstreamEvent{
		gameplayEvt("g1"), tournamentEvt("t1", "tour_1"), ratingEvt("r1", "p1"),
	} {
		out, err := s.Apply(ctx, evt)
		if err != nil || out.Kind != domain.OutcomeAccepted {
			t.Fatalf("apply %+v: out=%+v err=%v", evt.EventID, out, err)
		}
	}
	snap, err := s.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.GameplayMetrics) != 1 || len(snap.TournamentStats) != 1 || len(snap.RatingStats) != 1 {
		t.Fatalf("snap counts g=%d t=%d r=%d", len(snap.GameplayMetrics), len(snap.TournamentStats), len(snap.RatingStats))
	}
	if snap.Authoritative {
		t.Fatal("must be non-authoritative")
	}
}

func TestIntegration_LeaderboardMultiRow(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	out, err := s.Apply(ctx, leaderboardEvt("lb1"))
	if err != nil || out.Kind != domain.OutcomeAccepted {
		t.Fatalf("out=%+v err=%v", out, err)
	}
	snap, err := s.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.RatingStats) != 3 {
		t.Fatalf("want 3 rating rows, got %d", len(snap.RatingStats))
	}
	// One logical processed event.
	ver, err := s.ProjectionVersion(ctx)
	if err != nil || ver != 1 {
		t.Fatalf("version=%d err=%v", ver, err)
	}
}

func TestIntegration_PrivacyQuarantineZeroProjectionRows(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	out, err := s.Apply(ctx, privacyBadEvt("bad1"))
	if err != nil || out.Kind != domain.OutcomeQuarantined {
		t.Fatalf("out=%+v err=%v", out, err)
	}
	snap, err := s.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.GameplayMetrics)+len(snap.TournamentStats)+len(snap.RatingStats) != 0 {
		t.Fatalf("quarantine must leave zero projection rows: %+v", snap)
	}
}

func TestIntegration_ExactReplayAcrossNewClient(t *testing.T) {
	ctx := context.Background()
	s1 := openStore(t)
	evt := gameplayEvt("replay1")
	out1, err := s1.Apply(ctx, evt)
	if err != nil || out1.Kind != domain.OutcomeAccepted {
		t.Fatalf("first: %+v err=%v", out1, err)
	}
	base, user, pass, db := clickhouseURL(t)
	c2, err := store.NewClient(store.Config{URL: base, User: user, Password: pass, Database: db})
	if err != nil {
		t.Fatal(err)
	}
	s2 := store.NewAnalyticsStore(c2)
	out2, err := s2.Apply(ctx, evt)
	if err != nil || out2.Kind != domain.OutcomeDuplicate {
		t.Fatalf("replay: %+v err=%v", out2, err)
	}
	if out2.Rejection != nil {
		t.Fatalf("accepted replay must not carry rejection: %+v", out2.Rejection)
	}
	snap, err := s2.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.GameplayMetrics) != 1 {
		t.Fatalf("want 1 gameplay row after replay, got %d", len(snap.GameplayMetrics))
	}
}

func TestIntegration_ConcurrentDuplicateLogicalDedupe(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	evt := gameplayEvt("race1")
	const n = 8
	var wg sync.WaitGroup
	kinds := make(chan domain.OutcomeKind, n)
	errs := make(chan error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			out, err := s.Apply(ctx, evt)
			if err != nil {
				errs <- err
				return
			}
			kinds <- out.Kind
		}()
	}
	wg.Wait()
	close(kinds)
	close(errs)
	for err := range errs {
		t.Fatalf("apply: %v", err)
	}
	var accepted, dup int
	for k := range kinds {
		switch k {
		case domain.OutcomeAccepted:
			accepted++
		case domain.OutcomeDuplicate:
			dup++
		default:
			t.Fatalf("unexpected kind %s", k)
		}
	}
	if accepted < 1 || accepted+dup != n {
		t.Fatalf("accepted=%d dup=%d n=%d", accepted, dup, n)
	}
	snap, err := s.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.GameplayMetrics) != 1 {
		t.Fatalf("logical rows=%d", len(snap.GameplayMetrics))
	}
}

func TestIntegration_FailedRebuildPreservesOldSnapshot(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	if _, err := s.Apply(ctx, gameplayEvt("keep1")); err != nil {
		t.Fatal(err)
	}
	before, err := s.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(before.GameplayMetrics) != 1 {
		t.Fatalf("before=%d", len(before.GameplayMetrics))
	}

	// Simulate failed rebuild: building generation with rows, never activated.
	c := s.Client()
	building := "gen_building_fail"
	if err := c.Exec(ctx, `INSERT INTO projection_generations (generation_id, status, accepted_count) VALUES ({id:String}, {st:String}, 1)`,
		map[string]string{"id": building, "st": "building"}); err != nil {
		t.Fatal(err)
	}
	if err := c.Exec(ctx, `INSERT INTO gameplay_metrics (
		generation_id, event_id, schema_version, correlation_id, room_id, game_id, tournament_id,
		visibility, metric_type, occurred_at
	) VALUES (
		{gen:String}, 'ghost', 1, 'c', 'r', 'g', '', 'anonymized_adhoc', 'ghost', '2026-07-10 12:00:00.000'
	)`, map[string]string{"gen": building}); err != nil {
		t.Fatal(err)
	}

	after, err := s.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(after.GameplayMetrics) != 1 || after.GameplayMetrics[0].EventID != "keep1" {
		t.Fatalf("failed rebuild must preserve old snapshot: %+v", after.GameplayMetrics)
	}
}

func TestIntegration_RebuildActivation(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	if _, err := s.Apply(ctx, gameplayEvt("old1")); err != nil {
		t.Fatal(err)
	}
	outs, err := s.Rebuild(ctx, []domain.UpstreamEvent{gameplayEvt("new1"), tournamentEvt("newt", "tour_2")})
	if err != nil {
		t.Fatal(err)
	}
	if len(outs) != 2 {
		t.Fatalf("outs=%d", len(outs))
	}
	snap, err := s.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.GameplayMetrics) != 1 || snap.GameplayMetrics[0].EventID != "new1" {
		t.Fatalf("rebuild snap gameplay=%+v", snap.GameplayMetrics)
	}
	if len(snap.TournamentStats) != 1 {
		t.Fatalf("tournament=%d", len(snap.TournamentStats))
	}
	// Prior generation retained for inspection.
	cell, err := s.Client().QueryCell(ctx, `SELECT count() FROM projection_generations FINAL`, nil)
	if err != nil {
		t.Fatal(err)
	}
	var n int
	_, _ = fmt.Sscanf(cell, "%d", &n)
	if n < 2 {
		t.Fatalf("want retained generations >=2, got %d", n)
	}
}

func TestIntegration_ReadyAndReconnect(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	if err := s.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	base, user, pass, db := clickhouseURL(t)
	c2, err := store.NewClient(store.Config{URL: base, User: user, Password: pass, Database: db})
	if err != nil {
		t.Fatal(err)
	}
	s2 := store.NewAnalyticsStore(c2)
	if err := s2.Ready(ctx); err != nil {
		t.Fatalf("reconnect ready: %v", err)
	}
}
