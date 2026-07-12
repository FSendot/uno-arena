//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/services/tournament-orchestration/store"
	"unoarena/shared/envelope"
)

func TestLoadStandingsProjection_BoundedSnapshot(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)

	players := []string{"a1", "a2", "a3", "a4"}
	_, slot := provisionedTournament(t, ts, "t-standings", players)

	page, err := ts.LoadStandingsProjection(ctx, "t-standings")
	if err != nil {
		t.Fatal(err)
	}
	if page.TournamentID != "t-standings" {
		t.Fatalf("id=%s", page.TournamentID)
	}
	if page.RegisteredCount != 4 {
		t.Fatalf("registeredCount=%d want 4 (shard SUM)", page.RegisteredCount)
	}
	var shardSum int
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(count),0)::int FROM tournament_registration_shards WHERE tournament_id=$1
	`, "t-standings").Scan(&shardSum); err != nil {
		t.Fatal(err)
	}
	if page.RegisteredCount != shardSum {
		t.Fatalf("registeredCount=%d shardSum=%d", page.RegisteredCount, shardSum)
	}
	if len(page.FinalStandings) != 0 {
		t.Fatalf("pre-final finalStandings=%v", page.FinalStandings)
	}
	if page.ProjectionVersion < 1 {
		t.Fatalf("projectionVersion=%d", page.ProjectionVersion)
	}

	standings := standingsFromSeeded(slot.SeededPlayers)
	// Deliberately reverse producer input order so persistence must not echo caller order.
	unsorted := make([]domain.PlayerMatchStanding, len(standings))
	for i := range standings {
		unsorted[i] = standings[len(standings)-1-i]
	}
	wantRanked, err := domain.RankStandings(unsorted)
	if err != nil {
		t.Fatal(err)
	}
	if wantRanked[0].PlayerID == unsorted[0].PlayerID {
		t.Fatal("test setup must use unsorted input")
	}
	uow, err := ts.BeginRoundMatch(ctx, "t-standings", string(slot.RoomID), 1, string(slot.SlotID), "ingest:st-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := uow.AttachPriorResult(string(slot.RoomID), 1); err != nil {
		t.Fatal(err)
	}
	cmd := domain.RecordMatchResultCommand{
		CommandID: "ingest:st-1", EventID: "st-1",
		RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
		CompletionVersion: 1, Standings: unsorted,
	}
	d := domain.DecideRecordMatchResult(uow.Loaded(), cmd)
	if err := uow.Commit(store.RoundMatchCommitRequest{
		TournamentID: "t-standings", CommandID: "ingest:st-1", CommandType: "RecordMatchResult",
		Outcome:  envelope.Accepted("ingest:st-1", "RecordMatchResult", nil, json.RawMessage(`{"facts":[]}`)),
		Decision: d, Command: cmd, ProjectionChanged: true,
		MatchResultSource: &store.MatchResultSource{EventID: "st-1", RoomID: string(slot.RoomID), CompletionVersion: 1},
	}); err != nil {
		t.Fatal(err)
	}

	page2, err := ts.LoadStandingsProjection(ctx, "t-standings")
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.FinalStandings) != 4 {
		t.Fatalf("finalStandings=%v", page2.FinalStandings)
	}
	// Public finalStandings follows Tournament ranking, not producer input order.
	for i := range wantRanked {
		if page2.FinalStandings[i] != string(wantRanked[i].PlayerID) {
			t.Fatalf("order[%d]=%s want %s (full=%v inputFirst=%s)",
				i, page2.FinalStandings[i], wantRanked[i].PlayerID, page2.FinalStandings, unsorted[0].PlayerID)
		}
	}
	if page2.FinalStandings[0] == string(unsorted[0].PlayerID) {
		t.Fatalf("finalStandings echoed unsorted input order: %v", page2.FinalStandings)
	}
	if len(page2.FinalStandings) > domain.FinalPlayerThreshold {
		t.Fatalf("max 10 exceeded: %d", len(page2.FinalStandings))
	}

	bracketPage, err := ts.LoadBracketPage(ctx, store.BracketPageQuery{TournamentID: "t-standings", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if page2.ProjectionVersion != bracketPage.ProjectionVersion {
		t.Fatalf("projectionVersion standings=%d bracket=%d", page2.ProjectionVersion, bracketPage.ProjectionVersion)
	}
	if !page2.GeneratedAt.Equal(bracketPage.GeneratedAt) {
		d := page2.GeneratedAt.Sub(bracketPage.GeneratedAt)
		if d < 0 {
			d = -d
		}
		if d > time.Second {
			t.Fatalf("generatedAt standings=%v bracket=%v", page2.GeneratedAt, bracketPage.GeneratedAt)
		}
	}

	_, err = ts.LoadStandingsProjection(ctx, "missing-tournament")
	if !errors.Is(err, store.ErrTournamentNotFound) {
		t.Fatalf("missing: %v", err)
	}

	if _, err := pool.Exec(ctx, `
		UPDATE match_results SET ranked_result = '{"standings":[]}'::jsonb
		WHERE tournament_id = $1 AND disposition = 'recorded'
	`, "t-standings"); err != nil {
		t.Fatal(err)
	}
	_, err = ts.LoadStandingsProjection(ctx, "t-standings")
	if !errors.Is(err, store.ErrMalformedStandings) {
		t.Fatalf("malformed: %v", err)
	}
}

func TestLoadStandingsProjection_MultipleFinalResultsFailClosed(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)

	players := []string{"b1", "b2", "b3", "b4"}
	_, slot := provisionedTournament(t, ts, "t-multi-final", players)
	standings := standingsFromSeeded(slot.SeededPlayers)
	uow, err := ts.BeginRoundMatch(ctx, "t-multi-final", string(slot.RoomID), 1, string(slot.SlotID), "ingest:mf-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := uow.AttachPriorResult(string(slot.RoomID), 1); err != nil {
		t.Fatal(err)
	}
	cmd := domain.RecordMatchResultCommand{
		CommandID: "ingest:mf-1", EventID: "mf-1",
		RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
		CompletionVersion: 1, Standings: standings,
	}
	d := domain.DecideRecordMatchResult(uow.Loaded(), cmd)
	if err := uow.Commit(store.RoundMatchCommitRequest{
		TournamentID: "t-multi-final", CommandID: "ingest:mf-1", CommandType: "RecordMatchResult",
		Outcome:  envelope.Accepted("ingest:mf-1", "RecordMatchResult", nil, json.RawMessage(`{"facts":[]}`)),
		Decision: d, Command: cmd, ProjectionChanged: true,
		MatchResultSource: &store.MatchResultSource{EventID: "mf-1", RoomID: string(slot.RoomID), CompletionVersion: 1},
	}); err != nil {
		t.Fatal(err)
	}

	// Inject a second recorded final-row (inconsistent state) on the same room.
	ranked, err := json.Marshal(map[string]any{
		"fingerprint": "conflict",
		"standings": []map[string]any{
			{"playerId": "x1", "matchWins": 1, "cumulativeCardPoints": 0, "finalGameCompletedAt": "2026-01-01T00:00:00Z", "forfeited": false},
			{"playerId": "x2", "matchWins": 0, "cumulativeCardPoints": 1, "finalGameCompletedAt": "2026-01-01T00:00:01Z", "forfeited": false},
			{"playerId": "x3", "matchWins": 0, "cumulativeCardPoints": 2, "finalGameCompletedAt": "2026-01-01T00:00:02Z", "forfeited": false},
			{"playerId": "x4", "matchWins": 0, "cumulativeCardPoints": 3, "finalGameCompletedAt": "2026-01-01T00:00:03Z", "forfeited": false},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO match_results (
			room_id, completion_version, tournament_id, round_number, slot_id,
			disposition, ranked_result, quarantine_reason, source_event_id, processed_at
		) VALUES ($1, 99, $2, 1, $3, 'recorded', $4::jsonb, NULL, 'mf-dup', NOW())
	`, string(slot.RoomID), "t-multi-final", string(slot.SlotID), string(ranked)); err != nil {
		t.Fatal(err)
	}

	_, err = ts.LoadStandingsProjection(ctx, "t-multi-final")
	if !errors.Is(err, store.ErrMalformedStandings) {
		t.Fatalf("want ErrMalformedStandings for multiple finals, got %v", err)
	}
}
