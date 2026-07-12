//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"unoarena/services/ranking/domain"
	"unoarena/services/ranking/store"
)

func TestIntegration_LeaderboardDirtyAndSnapshotPublish(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)

	// Seeded publication state.
	casual, err := ts.GetPublicationState(ctx, domain.SourceCasualElo)
	if err != nil {
		t.Fatal(err)
	}
	if casual.DirtyVersion != 0 || casual.PublishedVersion != 0 {
		t.Fatalf("seed=%+v", casual)
	}
	tour, err := ts.GetPublicationState(ctx, domain.SourceTournamentPlacement)
	if err != nil {
		t.Fatal(err)
	}
	if tour.DirtyVersion != 0 {
		t.Fatalf("tour seed=%+v", tour)
	}

	// Casual score change dirties casual_elo exactly once.
	req := casualReq("cmd-dirty-1", "evt-dirty-1", "game-dirty-1", []domain.RatedPlacement{
		{PlayerID: "p1", Placement: 1},
		{PlayerID: "p2", Placement: 2},
	})
	out, err := ts.ApplyCasualGameCompleted(ctx, req)
	if err != nil || out.Kind != domain.OutcomeAccepted {
		t.Fatalf("casual=%+v err=%v", out, err)
	}
	casual, err = ts.GetPublicationState(ctx, domain.SourceCasualElo)
	if err != nil {
		t.Fatal(err)
	}
	if casual.DirtyVersion != 1 {
		t.Fatalf("dirty want 1 got %d", casual.DirtyVersion)
	}
	if casual.LastDirtyAt == nil {
		t.Fatal("last_dirty_at must be stamped by Postgres now()")
	}
	tour, _ = ts.GetPublicationState(ctx, domain.SourceTournamentPlacement)
	if tour.DirtyVersion != 0 {
		t.Fatal("tournament board must stay clean")
	}

	// Publish claims dirty board, writes top-N entries outbox, checkpoints claimed version.
	pub, err := ts.PublishNextDirtyLeaderboardSnapshot(ctx, store.DefaultLeaderboardSnapshotCooldown)
	if err != nil || !pub.Published {
		t.Fatalf("publish=%+v err=%v", pub, err)
	}
	if pub.BoardType != domain.SourceCasualElo || pub.ClaimedDirty != 1 {
		t.Fatalf("pub=%+v", pub)
	}
	if pub.EntryCount != 2 {
		t.Fatalf("entries=%d", pub.EntryCount)
	}
	casual, _ = ts.GetPublicationState(ctx, domain.SourceCasualElo)
	if casual.PublishedVersion != 1 || casual.DirtyVersion != 1 {
		t.Fatalf("after publish=%+v", casual)
	}
	if casual.LastPublishedAt == nil {
		t.Fatal("last_published_at must be stamped by Postgres clock_timestamp()")
	}
	var generatedAt time.Time
	if err := pool.QueryRow(ctx, `
		SELECT generated_at FROM leaderboard_snapshots WHERE snapshot_id = $1
	`, pub.SnapshotID).Scan(&generatedAt); err != nil {
		t.Fatal(err)
	}
	if generatedAt.IsZero() {
		t.Fatal("generated_at must use DB clock")
	}
	// Snapshot metadata and publication checkpoint share one late clock_timestamp() value.
	if !generatedAt.Equal(*casual.LastPublishedAt) {
		t.Fatalf("generated_at=%v last_published_at=%v want identical authoritative publication stamp", generatedAt, *casual.LastPublishedAt)
	}

	var payload []byte
	err = pool.QueryRow(ctx, `
		SELECT payload FROM outbox_events WHERE event_type = 'LeaderboardSnapshotPublished'
	`).Scan(&payload)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(payload, &body); err != nil {
		t.Fatal(err)
	}
	entries, ok := body["entries"].([]any)
	if !ok || len(entries) != 2 {
		t.Fatalf("entries=%v", body["entries"])
	}
	first, _ := entries[0].(map[string]any)
	if first["rank"].(float64) != 1 || first["playerId"] == "" {
		t.Fatalf("first=%+v", first)
	}
	outboxGeneratedAt, ok := body["generatedAt"].(string)
	if !ok || outboxGeneratedAt == "" {
		t.Fatalf("outbox generatedAt missing: %v", body["generatedAt"])
	}
	parsedOutboxAt, err := time.Parse(time.RFC3339Nano, outboxGeneratedAt)
	if err != nil {
		t.Fatalf("outbox generatedAt parse: %v", err)
	}
	if !parsedOutboxAt.Equal(generatedAt) {
		t.Fatalf("outbox generatedAt=%v snapshot generated_at=%v want identical authoritative publication stamp", parsedOutboxAt, generatedAt)
	}

	// Cooldown blocks immediate republish even if dirtied again mid-window (DB clock).
	req2 := casualReq("cmd-dirty-2", "evt-dirty-2", "game-dirty-2", []domain.RatedPlacement{
		{PlayerID: "p1", Placement: 2},
		{PlayerID: "p2", Placement: 1},
	})
	if _, err := ts.ApplyCasualGameCompleted(ctx, req2); err != nil {
		t.Fatal(err)
	}
	casual, _ = ts.GetPublicationState(ctx, domain.SourceCasualElo)
	if casual.DirtyVersion != 2 || casual.PublishedVersion != 1 {
		t.Fatalf("after second dirty=%+v", casual)
	}
	blocked, err := ts.PublishNextDirtyLeaderboardSnapshot(ctx, store.DefaultLeaderboardSnapshotCooldown)
	if err != nil || blocked.Published {
		t.Fatalf("cooldown must block: %+v err=%v", blocked, err)
	}

	// Advance DB last_published_at past the coalesce window, then publish v2.
	if _, err := pool.Exec(ctx, `
		UPDATE leaderboard_publication_state
		SET last_published_at = now() - make_interval(secs => $1::double precision)
		WHERE board_type = 'casual_elo'
	`, store.DefaultLeaderboardSnapshotCooldown.Seconds()+1); err != nil {
		t.Fatal(err)
	}
	pub2, err := ts.PublishNextDirtyLeaderboardSnapshot(ctx, store.DefaultLeaderboardSnapshotCooldown)
	if err != nil || !pub2.Published || pub2.ClaimedDirty != 2 {
		t.Fatalf("pub2=%+v err=%v", pub2, err)
	}
	casual, _ = ts.GetPublicationState(ctx, domain.SourceCasualElo)
	if casual.PublishedVersion != 2 {
		t.Fatalf("published=%+v", casual)
	}
}

func TestIntegration_LeaderboardIndexesMatchTop100OrderBy(t *testing.T) {
	ctx := context.Background()
	pool, _ := openStore(t)

	cases := []struct {
		name string
		want string
	}{
		{"player_ratings_casual_elo_idx", "CREATE INDEX player_ratings_casual_elo_idx ON public.player_ratings USING btree (casual_elo DESC, player_id)"},
		{"player_ratings_tournament_idx", "CREATE INDEX player_ratings_tournament_idx ON public.player_ratings USING btree (tournament_placement_rating DESC, player_id)"},
	}
	for _, tc := range cases {
		var def string
		if err := pool.QueryRow(ctx, `SELECT pg_get_indexdef(c.oid)
			FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE c.relkind = 'i' AND n.nspname = 'public' AND c.relname = $1`, tc.name).Scan(&def); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if def != tc.want {
			t.Fatalf("%s indexdef=%q want %q", tc.name, def, tc.want)
		}
	}
}

func TestIntegration_TournamentPerformanceDirtiesWithoutInlineSnapshot(t *testing.T) {
	ctx := context.Background()
	_, ts := openStore(t)

	biz, err := store.PlayersAdvancedBusinessKey("t-snap", 1, "slot-1")
	if err != nil {
		t.Fatal(err)
	}
	req := store.TournamentPerformanceRequest{
		SourceTopic: "tournament.players.advanced", UpstreamEventID: "evt-snap-1",
		BusinessKey: biz, PayloadFingerprint: "fp-snap-1", TournamentID: "t-snap",
		Players: []store.TournamentPlayerPerformance{
			{PlayerID: "a", RoundNumber: 1, Reason: domain.ReasonTournamentAdvancement},
		},
	}
	first, err := ts.ApplyTournamentPerformance(ctx, req)
	if err != nil || !first.ScoreChanged {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	for _, f := range first.Facts {
		if f.Name == domain.FactLeaderboardSnapshotPublished {
			t.Fatal("ingest must not emit LeaderboardSnapshotPublished")
		}
	}
	st, err := ts.GetPublicationState(ctx, domain.SourceTournamentPlacement)
	if err != nil {
		t.Fatal(err)
	}
	if st.DirtyVersion != 1 {
		t.Fatalf("dirty=%d", st.DirtyVersion)
	}
	nOut, _ := ts.CountOutbox(ctx)
	// Only PlayerRatingUpdated — no snapshot from ingest.
	if nOut != 1 {
		t.Fatalf("outbox=%d want 1 rating fact", nOut)
	}
	// Zero-delta final standing: history+outbox, no dirty bump.
	zeroReq := store.TournamentPerformanceRequest{
		SourceTopic: "tournament.completed", UpstreamEventID: "evt-zero-snap",
		BusinessKey: "evt-zero-snap", PayloadFingerprint: "fp-zero-snap", TournamentID: "t-zero-snap",
		Players: []store.TournamentPlayerPerformance{
			{PlayerID: "z1", Placement: 10, Reason: domain.ReasonTournamentFinalStanding},
		},
	}
	zero, err := ts.ApplyTournamentPerformance(ctx, zeroReq)
	if err != nil || zero.ScoreChanged {
		t.Fatalf("zero=%+v err=%v", zero, err)
	}
	st2, _ := ts.GetPublicationState(ctx, domain.SourceTournamentPlacement)
	if st2.DirtyVersion != 1 {
		t.Fatalf("zero-delta must not dirty: %+v", st2)
	}
}

func TestIntegration_LegacyPlacementBridgeDirtiesOnce(t *testing.T) {
	ctx := context.Background()
	_, ts := openStore(t)

	cmd := domain.ApplyTournamentPlacementUpdateCommand{
		CommandID: "leg-1", EventID: "leg-evt-1", PlayerID: "lp1",
		TournamentID: "lt1", PlacementEventID: "pe-leg-1",
		RoundNumber: 1, Reason: domain.ReasonTournamentAdvancement,
	}
	out, err := ts.ApplyTournamentPlacement(ctx, store.TournamentPlacementRequest{Command: cmd})
	if err != nil || !out.Accepted() {
		t.Fatalf("out=%+v err=%v", out, err)
	}
	st, err := ts.GetPublicationState(ctx, domain.SourceTournamentPlacement)
	if err != nil {
		t.Fatal(err)
	}
	if st.DirtyVersion != 1 {
		t.Fatalf("legacy bridge dirty=%d", st.DirtyVersion)
	}

	// Tenth place zero award: accepted but no dirty.
	zero := domain.ApplyTournamentPlacementUpdateCommand{
		CommandID: "leg-2", EventID: "leg-evt-2", PlayerID: "lp2",
		TournamentID: "lt1", PlacementEventID: "pe-leg-2",
		Placement: 10, Reason: domain.ReasonTournamentFinalStanding,
	}
	zOut, err := ts.ApplyTournamentPlacement(ctx, store.TournamentPlacementRequest{Command: zero})
	if err != nil || !zOut.Accepted() {
		t.Fatalf("zero=%+v err=%v", zOut, err)
	}
	st2, _ := ts.GetPublicationState(ctx, domain.SourceTournamentPlacement)
	if st2.DirtyVersion != 1 {
		t.Fatalf("zero placement must not dirty: %+v", st2)
	}
}

func TestIntegration_SnapshotPublishClaimsSkipLockedAndCapsTop100(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)

	// Insert 105 players with descending tournament ratings.
	for i := 0; i < 105; i++ {
		id := "top-" + string(rune('a'+i%26)) + "-" + itoa(i)
		rating := 1000 - i
		if _, err := pool.Exec(ctx, `
			INSERT INTO player_ratings (
				player_id, casual_elo, casual_games_played,
				tournament_placement_rating, tournament_events_applied, rating_floor, updated_at
			) VALUES ($1, 1000, 0, $2, 0, 0, now())
		`, id, rating); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `
		UPDATE leaderboard_publication_state
		SET dirty_version = 1, last_dirty_at = now()
		WHERE board_type = 'tournament_placement'
	`); err != nil {
		t.Fatal(err)
	}

	pub, err := ts.PublishNextDirtyLeaderboardSnapshot(ctx, time.Millisecond)
	if err != nil || !pub.Published {
		t.Fatalf("pub=%+v err=%v", pub, err)
	}
	if pub.EntryCount != store.LeaderboardSnapshotTopN {
		t.Fatalf("entryCount=%d", pub.EntryCount)
	}
	var payload []byte
	if err := pool.QueryRow(ctx, `
		SELECT payload FROM outbox_events WHERE event_type = 'LeaderboardSnapshotPublished'
	`).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	_ = json.Unmarshal(payload, &body)
	entries := body["entries"].([]any)
	if len(entries) != 100 {
		t.Fatalf("entries=%d", len(entries))
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [16]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
