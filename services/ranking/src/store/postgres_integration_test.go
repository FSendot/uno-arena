//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"unoarena/services/ranking/domain"
	"unoarena/services/ranking/store"
)

func postgresURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("RANKING_POSTGRES_URL")
	if url == "" {
		url = os.Getenv("DATABASE_URL")
	}
	if url == "" {
		t.Skip("RANKING_POSTGRES_URL not set")
	}
	if err := requireSafeRankingTestDatabase(url); err != nil {
		t.Fatalf("%v", err)
	}
	return url
}

func applyMigration(t *testing.T, ctx context.Context, pool *store.Pool) {
	t.Helper()
	sqlBytes, err := os.ReadFile(filepath.Join("..", "..", "migrations", "001_init.sql"))
	if err != nil {
		t.Fatalf("migration: %v", err)
	}
	if _, err := pool.Exec(ctx, string(sqlBytes)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS schema_bootstrap_meta`); err != nil {
		t.Fatalf("drop meta: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		CREATE TABLE schema_bootstrap_meta (
			version TEXT NOT NULL,
			checksum TEXT NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		t.Fatalf("meta table: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO schema_bootstrap_meta (version, checksum) VALUES ($1, $2)
	`, store.ExpectedBootstrapVersion, store.ExpectedSchemaChecksum); err != nil {
		t.Fatalf("meta insert: %v", err)
	}
}

func resetPublic(t *testing.T, ctx context.Context, pool *store.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		DROP TABLE IF EXISTS outbox_events CASCADE;
		DROP TABLE IF EXISTS ranking_command_responses CASCADE;
		DROP TABLE IF EXISTS leaderboard_snapshots CASCADE;
		DROP TABLE IF EXISTS processed_upstream_events CASCADE;
		DROP TABLE IF EXISTS processed_tournament_placement_keys CASCADE;
		DROP TABLE IF EXISTS processed_casual_elo_keys CASCADE;
		DROP TABLE IF EXISTS rating_history CASCADE;
		DROP TABLE IF EXISTS player_ratings CASCADE;
		DROP TABLE IF EXISTS schema_migrations CASCADE;
		DROP TABLE IF EXISTS schema_bootstrap_meta CASCADE;
	`); err != nil {
		t.Fatalf("reset: %v", err)
	}
}

func openStore(t *testing.T) (*store.Pool, *store.RankingStore) {
	t.Helper()
	ctx := context.Background()
	pool, err := store.NewPool(ctx, postgresURL(t))
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(func() { pool.Close() })
	resetPublic(t, ctx, pool)
	applyMigration(t, ctx, pool)
	return pool, store.NewRankingStore(pool.Pool)
}

func casualReq(cmd, evt, game string, parts []domain.RatedPlacement) store.GameCompletedRequest {
	return store.GameCompletedRequest{
		CommandID: domain.CommandID(cmd), EventID: domain.EventID(evt),
		GameID: domain.GameID(game), RoomID: "r1", RoomType: domain.RoomTypeAdHoc,
		Authoritative: true, Completed: true, Participants: parts,
	}
}

func TestIntegration_VerifySchemaExactAndDrift(t *testing.T) {
	ctx := context.Background()
	pool, _ := openStore(t)
	if err := store.VerifySchema(ctx, pool.Pool, store.DefaultSchemaExpectation()); err != nil {
		t.Fatalf("exact schema should pass: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE schema_bootstrap_meta SET checksum = 'deadbeef'`); err != nil {
		t.Fatal(err)
	}
	if err := store.VerifySchema(ctx, pool.Pool, store.DefaultSchemaExpectation()); err == nil {
		t.Fatal("drift checksum must fail")
	}
}

func TestIntegration_HydrateRoundTrip(t *testing.T) {
	ctx := context.Background()
	_, ts := openStore(t)
	out, err := ts.ApplyCasualGameCompleted(ctx, casualReq("c1", "e1", "g1", []domain.RatedPlacement{
		{PlayerID: "p1", Placement: 1},
		{PlayerID: "p2", Placement: 2},
	}))
	if err != nil || out.Kind != domain.OutcomeAccepted {
		t.Fatalf("apply: %+v err=%v", out, err)
	}
	c1, _, ok, err := ts.GetPlayerRating(ctx, "p1")
	if err != nil || !ok {
		t.Fatalf("hydrate p1: ok=%v err=%v", ok, err)
	}
	if c1 == 0 {
		t.Fatalf("unexpected zero casual rating")
	}
	hist, ok, err := ts.History(ctx, "p1")
	if err != nil || !ok || len(hist) != 1 {
		t.Fatalf("history: ok=%v len=%d err=%v", ok, len(hist), err)
	}
	board, err := ts.Leaderboard(ctx, domain.SourceCasualElo)
	if err != nil || len(board) != 2 {
		t.Fatalf("board=%+v err=%v", board, err)
	}
	n, err := ts.CountOutbox(ctx)
	if err != nil || n != 2 {
		t.Fatalf("outbox=%d err=%v", n, err)
	}
}

func TestIntegration_AtomicRollbackMultiParticipant(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)
	ts.FailNextCommits = 1
	_, err := ts.ApplyCasualGameCompleted(ctx, casualReq("c1", "e1", "g1", []domain.RatedPlacement{
		{PlayerID: "p1", Placement: 1},
		{PlayerID: "p2", Placement: 2},
		{PlayerID: "p3", Placement: 3},
	}))
	if err == nil {
		t.Fatal("expected injected failure")
	}
	nPlayers, _ := ts.CountPlayers(ctx)
	nHist, _ := ts.CountHistory(ctx)
	nOut, _ := ts.CountOutbox(ctx)
	var nDedupe, nResp, nCasualKeys int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM processed_upstream_events`).Scan(&nDedupe); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM ranking_command_responses`).Scan(&nResp); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM processed_casual_elo_keys`).Scan(&nCasualKeys); err != nil {
		t.Fatal(err)
	}
	if nPlayers != 0 || nHist != 0 || nOut != 0 || nDedupe != 0 || nResp != 0 || nCasualKeys != 0 {
		t.Fatalf("rollback must leave no state: players=%d hist=%d outbox=%d dedupe=%d resp=%d keys=%d",
			nPlayers, nHist, nOut, nDedupe, nResp, nCasualKeys)
	}
	// Retry succeeds atomically for all participants.
	out, err := ts.ApplyCasualGameCompleted(ctx, casualReq("c1", "e1", "g1", []domain.RatedPlacement{
		{PlayerID: "p1", Placement: 1},
		{PlayerID: "p2", Placement: 2},
		{PlayerID: "p3", Placement: 3},
	}))
	if err != nil || out.Kind != domain.OutcomeAccepted {
		t.Fatalf("retry: %+v err=%v", out, err)
	}
	nPlayers, _ = ts.CountPlayers(ctx)
	nHist, _ = ts.CountHistory(ctx)
	if nPlayers != 3 || nHist != 3 {
		t.Fatalf("players=%d hist=%d", nPlayers, nHist)
	}
}

func TestIntegration_ConcurrentSameEventReplay(t *testing.T) {
	ctx := context.Background()
	_, ts := openStore(t)
	const n = 8
	var wg sync.WaitGroup
	errs := make(chan error, n)
	results := make(chan domain.OutcomeKind, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			out, err := ts.ApplyCasualGameCompleted(ctx, casualReq(
				"c-same", "e-same", "g-same",
				[]domain.RatedPlacement{
					{PlayerID: "shared-a", Placement: 1},
					{PlayerID: "shared-b", Placement: 2},
				},
			))
			if err != nil {
				errs <- err
				return
			}
			results <- out.Kind
		}()
	}
	wg.Wait()
	close(errs)
	close(results)
	for err := range errs {
		t.Fatalf("goroutine error: %v", err)
	}
	accepted, dup := 0, 0
	for k := range results {
		switch k {
		case domain.OutcomeAccepted:
			accepted++
		case domain.OutcomeDuplicate:
			dup++
		default:
			t.Fatalf("unexpected kind %s", k)
		}
	}
	if accepted != 1 || dup != n-1 {
		t.Fatalf("accepted=%d dup=%d want 1/%d", accepted, dup, n-1)
	}
	nHist, _ := ts.CountHistory(ctx)
	nOut, _ := ts.CountOutbox(ctx)
	if nHist != 2 || nOut != 2 {
		t.Fatalf("hist=%d outbox=%d (no lost updates / no dup outbox)", nHist, nOut)
	}
}

func TestIntegration_ConcurrentOverlappingGames(t *testing.T) {
	ctx := context.Background()
	_, ts := openStore(t)
	const n = 8
	var wg sync.WaitGroup
	errs := make(chan error, n)
	results := make(chan domain.OutcomeKind, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			out, err := ts.ApplyCasualGameCompleted(ctx, casualReq(
				fmt.Sprintf("c-overlap-%d", i),
				fmt.Sprintf("e-overlap-%d", i),
				fmt.Sprintf("g-overlap-%d", i),
				[]domain.RatedPlacement{
					{PlayerID: "shared-a", Placement: 1},
					{PlayerID: "shared-b", Placement: 2},
				},
			))
			if err != nil {
				errs <- err
				return
			}
			results <- out.Kind
		}(i)
	}
	wg.Wait()
	close(errs)
	close(results)
	for err := range errs {
		t.Fatalf("goroutine error: %v", err)
	}
	accepted := 0
	for k := range results {
		if k != domain.OutcomeAccepted {
			t.Fatalf("overlapping distinct games must accept, got %s", k)
		}
		accepted++
	}
	if accepted != n {
		t.Fatalf("accepted=%d want %d", accepted, n)
	}
	nHist, _ := ts.CountHistory(ctx)
	nOut, _ := ts.CountOutbox(ctx)
	if nHist != n*2 || nOut != n*2 {
		t.Fatalf("hist=%d outbox=%d want %d (no deadlock / no lost updates)", nHist, nOut, n*2)
	}
}

func TestIntegration_DuplicateEventReplayNewInstance(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)
	first, err := ts.ApplyCasualGameCompleted(ctx, casualReq("c1", "e1", "g1", []domain.RatedPlacement{
		{PlayerID: "p1", Placement: 1},
		{PlayerID: "p2", Placement: 2},
	}))
	if err != nil || first.Kind != domain.OutcomeAccepted {
		t.Fatalf("first: %+v err=%v", first, err)
	}
	dsn := postgresURL(t)
	pool.Close()
	pool2, err := store.NewPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool2.Close()
	ts2 := store.NewRankingStore(pool2.Pool)
	second, err := ts2.ApplyCasualGameCompleted(ctx, casualReq("c1", "e1", "g1", []domain.RatedPlacement{
		{PlayerID: "p1", Placement: 1},
		{PlayerID: "p2", Placement: 2},
	}))
	if err != nil || second.Kind != domain.OutcomeDuplicate {
		t.Fatalf("replay: %+v err=%v", second, err)
	}
	if len(second.Facts) != len(first.Facts) {
		t.Fatalf("stable facts: first=%d second=%d", len(first.Facts), len(second.Facts))
	}
	nHist, _ := ts2.CountHistory(ctx)
	nOut, _ := ts2.CountOutbox(ctx)
	if nHist != 2 || nOut != 2 {
		t.Fatalf("replay must write nothing new: hist=%d outbox=%d", nHist, nOut)
	}
}

func TestIntegration_BusinessKeyDuplicateNewEventID(t *testing.T) {
	ctx := context.Background()
	_, ts := openStore(t)
	first, err := ts.ApplyCasualGameCompleted(ctx, casualReq("c1", "e1", "g1", []domain.RatedPlacement{
		{PlayerID: "p1", Placement: 1},
		{PlayerID: "p2", Placement: 2},
	}))
	if err != nil || first.Kind != domain.OutcomeAccepted {
		t.Fatal(first, err)
	}
	second, err := ts.ApplyCasualGameCompleted(ctx, casualReq("c2", "e2-new", "g1", []domain.RatedPlacement{
		{PlayerID: "p1", Placement: 1},
		{PlayerID: "p2", Placement: 2},
	}))
	if err != nil || second.Kind != domain.OutcomeDuplicate {
		t.Fatalf("gameId dedupe: %+v err=%v", second, err)
	}
	nHist, _ := ts.CountHistory(ctx)
	nOut, _ := ts.CountOutbox(ctx)
	if nHist != 2 || nOut != 2 {
		t.Fatalf("business-key dup must not write: hist=%d outbox=%d", nHist, nOut)
	}
}

func TestIntegration_PlacementDedupe(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)
	// Delimiter-like UTF-8-safe components (quotes/commas/brackets) — not raw NUL,
	// which PostgreSQL rejects in any TEXT column.
	cmd := domain.ApplyTournamentPlacementUpdateCommand{
		CommandID: "tp1", EventID: "te1", PlayerID: `p"1`,
		TournamentID: "t,1", PlacementEventID: `pe]1`, Placement: 1, Delta: 50,
	}
	first, err := ts.ApplyTournamentPlacement(ctx, store.TournamentPlacementRequest{
		Command: cmd, CorrelationID: "corr-place-1", CausationID: "tp1",
	})
	if err != nil || first.Kind != domain.OutcomeAccepted {
		t.Fatalf("first: %+v err=%v", first, err)
	}
	dsn := postgresURL(t)
	pool.Close()
	pool2, err := store.NewPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool2.Close()
	ts2 := store.NewRankingStore(pool2.Pool)
	second, err := ts2.ApplyTournamentPlacement(ctx, store.TournamentPlacementRequest{
		Command: cmd, CorrelationID: "corr-place-1", CausationID: "tp1",
	})
	if err != nil || second.Kind != domain.OutcomeDuplicate {
		t.Fatalf("event replay across new store: %+v err=%v", second, err)
	}
	third, err := ts2.ApplyTournamentPlacement(ctx, store.TournamentPlacementRequest{
		Command: domain.ApplyTournamentPlacementUpdateCommand{
			CommandID: "tp2", EventID: "te2-new", PlayerID: `p"1`,
			TournamentID: "t,1", PlacementEventID: `pe]1`, Placement: 1, Delta: 50,
		},
		CorrelationID: "corr-place-2", CausationID: "tp2",
	})
	if err != nil || third.Kind != domain.OutcomeDuplicate {
		t.Fatalf("biz-key dedupe: %+v err=%v", third, err)
	}
	nHist, _ := ts2.CountHistory(ctx)
	nOut, _ := ts2.CountOutbox(ctx)
	if nHist != 1 || nOut != 1 {
		t.Fatalf("hist=%d outbox=%d", nHist, nOut)
	}
	_, tour, ok, _ := ts2.GetPlayerRating(ctx, `p"1`)
	if !ok || tour != 50 {
		t.Fatalf("tournament rating=%d", tour)
	}
	casual, _, _, _ := ts2.GetPlayerRating(ctx, `p"1`)
	if casual != domain.DefaultInitialCasual {
		t.Fatalf("casual must be untouched: %d", casual)
	}
}

func TestIntegration_RejectionZeroWrites(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)
	cases := []store.GameCompletedRequest{
		{CommandID: "c", EventID: "e-abd", GameID: "g-abd", RoomType: domain.RoomTypeAdHoc,
			Authoritative: true, Completed: true, IsAbandoned: true,
			Participants: []domain.RatedPlacement{{PlayerID: "p1", Placement: 1}, {PlayerID: "p2", Placement: 2}}},
		{CommandID: "c", EventID: "e-tour", GameID: "g-tour", RoomType: domain.RoomTypeTournament,
			Authoritative: true, Completed: true,
			Participants: []domain.RatedPlacement{{PlayerID: "p1", Placement: 1}, {PlayerID: "p2", Placement: 2}}},
		{CommandID: "c", EventID: "e-na", GameID: "g-na", RoomType: domain.RoomTypeAdHoc,
			Authoritative: false, Completed: true,
			Participants: []domain.RatedPlacement{{PlayerID: "p1", Placement: 1}, {PlayerID: "p2", Placement: 2}}},
	}
	for _, req := range cases {
		out, err := ts.ApplyCasualGameCompleted(ctx, req)
		if err != nil || out.Kind != domain.OutcomeRejected {
			t.Fatalf("want rejected: %+v err=%v", out, err)
		}
	}
	nPlayers, _ := ts.CountPlayers(ctx)
	nHist, _ := ts.CountHistory(ctx)
	nOut, _ := ts.CountOutbox(ctx)
	var nDedupe, nResp, nCasualKeys int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM processed_upstream_events`).Scan(&nDedupe); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM ranking_command_responses`).Scan(&nResp); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM processed_casual_elo_keys`).Scan(&nCasualKeys); err != nil {
		t.Fatal(err)
	}
	if nPlayers != 0 || nHist != 0 || nOut != 0 || nDedupe != 0 || nResp != 0 || nCasualKeys != 0 {
		t.Fatalf("rejection must write nothing: players=%d hist=%d out=%d dedupe=%d resp=%d keys=%d",
			nPlayers, nHist, nOut, nDedupe, nResp, nCasualKeys)
	}
}

func TestIntegration_OutboxNoDuplication(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)
	req := casualReq("c1", "e1", "g1", []domain.RatedPlacement{
		{PlayerID: "p1", Placement: 1},
		{PlayerID: "p2", Placement: 2},
	})
	if _, err := ts.ApplyCasualGameCompleted(ctx, req); err != nil {
		t.Fatal(err)
	}
	if _, err := ts.ApplyCasualGameCompleted(ctx, req); err != nil {
		t.Fatal(err)
	}
	n, _ := ts.CountOutbox(ctx)
	if n != 2 {
		t.Fatalf("outbox=%d", n)
	}
	var publishedAtCol int
	err := pool.QueryRow(ctx, `
		SELECT count(*) FROM information_schema.columns
		WHERE table_name = 'outbox_events' AND column_name = 'published_at'
	`).Scan(&publishedAtCol)
	if err != nil || publishedAtCol != 0 {
		t.Fatalf("published_at must not exist: col=%d err=%v", publishedAtCol, err)
	}
}

func TestIntegration_OutboxCasualContractAndReplayStable(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)
	req := casualReq("c1", "e1", "g1", []domain.RatedPlacement{
		{PlayerID: "p1", Placement: 1},
		{PlayerID: "p2", Placement: 2},
	})
	req.CorrelationID = "corr-casual-1"
	req.CausationID = "c1"
	if _, err := ts.ApplyCasualGameCompleted(ctx, req); err != nil {
		t.Fatal(err)
	}
	before := loadOutboxRows(t, ctx, pool)
	if len(before) != 2 {
		t.Fatalf("outbox rows=%d", len(before))
	}
	for _, row := range before {
		if row.EventType != "PlayerRatingUpdated" {
			t.Fatalf("event_type=%q", row.EventType)
		}
		if row.Topic != "ranking.player_rating_updated" {
			t.Fatalf("topic=%q", row.Topic)
		}
		assertPersistedPlayerRatingPayload(t, row.Payload, false)
		if row.Payload["correlationId"] != "corr-casual-1" {
			t.Fatalf("correlationId=%v", row.Payload["correlationId"])
		}
		if row.Payload["causationId"] != "c1" {
			t.Fatalf("causationId=%v", row.Payload["causationId"])
		}
		if _, ok := row.Payload["gameId"]; !ok {
			t.Fatalf("casual payload missing gameId: %+v", row.Payload)
		}
	}

	req.CorrelationID = "corr-should-not-overwrite"
	req.CausationID = "c-other"
	dup, err := ts.ApplyCasualGameCompleted(ctx, req)
	if err != nil || dup.Kind != domain.OutcomeDuplicate {
		t.Fatalf("replay: %+v err=%v", dup, err)
	}
	after := loadOutboxRows(t, ctx, pool)
	if len(after) != len(before) {
		t.Fatalf("replay must not add outbox rows: before=%d after=%d", len(before), len(after))
	}
	for i := range before {
		if before[i].EventType != after[i].EventType {
			t.Fatalf("event_type changed on replay")
		}
		if string(mustJSON(t, before[i].Payload)) != string(mustJSON(t, after[i].Payload)) {
			t.Fatalf("payload altered on replay:\n before=%s\n after=%s",
				mustJSON(t, before[i].Payload), mustJSON(t, after[i].Payload))
		}
	}
}

func TestIntegration_OutboxPlacementContractAndReplayStable(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)
	req := store.TournamentPlacementRequest{
		Command: domain.ApplyTournamentPlacementUpdateCommand{
			CommandID: "tp1", EventID: "te1", PlayerID: "p1",
			TournamentID: "t1", PlacementEventID: "pe1", Placement: 1, Delta: 40,
		},
		CorrelationID: "corr-place-1",
		CausationID:   "tp1",
	}
	if _, err := ts.ApplyTournamentPlacement(ctx, req); err != nil {
		t.Fatal(err)
	}
	before := loadOutboxRows(t, ctx, pool)
	if len(before) != 1 {
		t.Fatalf("outbox rows=%d", len(before))
	}
	row := before[0]
	if row.EventType != "PlayerRatingUpdated" {
		t.Fatalf("placement must map to external PlayerRatingUpdated, got %q", row.EventType)
	}
	if row.Topic != "ranking.player_rating_updated" {
		t.Fatalf("topic=%q", row.Topic)
	}
	assertPersistedPlayerRatingPayload(t, row.Payload, true)
	if row.Payload["tournamentId"] != "t1" || row.Payload["placementEventId"] != "pe1" {
		t.Fatalf("placement fields: %+v", row.Payload)
	}
	if row.Payload["correlationId"] != "corr-place-1" || row.Payload["causationId"] != "tp1" {
		t.Fatalf("metadata: %+v", row.Payload)
	}

	req.CorrelationID = "corr-should-not-overwrite"
	dup, err := ts.ApplyTournamentPlacement(ctx, req)
	if err != nil || dup.Kind != domain.OutcomeDuplicate {
		t.Fatalf("replay: %+v err=%v", dup, err)
	}
	after := loadOutboxRows(t, ctx, pool)
	if len(after) != 1 {
		t.Fatalf("replay must not add outbox rows: %d", len(after))
	}
	if string(mustJSON(t, before[0].Payload)) != string(mustJSON(t, after[0].Payload)) {
		t.Fatalf("placement outbox payload altered on replay")
	}
	if before[0].EventType != after[0].EventType {
		t.Fatalf("event_type altered on replay")
	}
}

type outboxRow struct {
	EventType string
	Topic     string
	Payload   map[string]any
}

func loadOutboxRows(t *testing.T, ctx context.Context, pool *store.Pool) []outboxRow {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT event_type, topic, payload
		FROM outbox_events
		ORDER BY outbox_id
	`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []outboxRow
	for rows.Next() {
		var eventType, topic string
		var raw []byte
		if err := rows.Scan(&eventType, &topic, &raw); err != nil {
			t.Fatal(err)
		}
		var payload map[string]any
		if err := json.Unmarshal(raw, &payload); err != nil {
			t.Fatal(err)
		}
		out = append(out, outboxRow{EventType: eventType, Topic: topic, Payload: payload})
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func assertPersistedPlayerRatingPayload(t *testing.T, payload map[string]any, placement bool) {
	t.Helper()
	for _, key := range []string{"eventId", "eventType", "schemaVersion", "correlationId", "occurredAt", "playerId", "previousRating", "newRating"} {
		if _, ok := payload[key]; !ok {
			t.Fatalf("missing %q in %+v", key, payload)
		}
	}
	if payload["eventType"] != "PlayerRatingUpdated" {
		t.Fatalf("payload.eventType=%v", payload["eventType"])
	}
	if placement {
		if _, ok := payload["tournamentId"]; !ok {
			t.Fatalf("missing tournamentId")
		}
		if _, ok := payload["placementEventId"]; !ok {
			t.Fatalf("missing placementEventId")
		}
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestIntegration_DBReconnect(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)
	if err := ts.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	dsn := postgresURL(t)
	pool.Close()
	pool2, err := store.NewPool(ctx, dsn)
	if err != nil {
		t.Fatalf("reconnect pool: %v", err)
	}
	defer pool2.Close()
	ts2 := store.NewRankingStore(pool2.Pool)
	if err := ts2.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.VerifySchema(ctx, pool2.Pool, store.DefaultSchemaExpectation()); err != nil {
		t.Fatalf("schema after reconnect: %v", err)
	}
	_ = time.Second
}
