//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/services/tournament-orchestration/store"
	"unoarena/shared/envelope"
)

func serviceDurablePostgres(t *testing.T) (*store.Pool, *store.TournamentStore, *Service) {
	t.Helper()
	url := os.Getenv("TOURNAMENT_POSTGRES_URL")
	if url == "" {
		url = os.Getenv("DATABASE_URL")
	}
	if url == "" {
		t.Skip("TOURNAMENT_POSTGRES_URL not set")
	}
	if !strings.Contains(url, "unoarena_tournament_test_") {
		t.Fatalf("refusing unsafe DSN; require unoarena_tournament_test_ prefix")
	}
	ctx := context.Background()
	pool, err := store.NewPool(ctx, url)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(func() { pool.Close() })
	resetServiceTestDB(t, ctx, pool)
	applyServiceMigration(t, ctx, pool)
	ts := store.NewTournamentStore(pool.Pool)
	svc := NewService(ServiceDeps{
		Repo:           &durableRepo{store: ts},
		RoundMatches:   &durableRoundMatchRepo{store: ts},
		Registrations:  &durableRegistrationRepo{store: ts},
		CompleteRounds: &durableCompleteRoundRepo{store: ts},
		Lifecycle:      &durableLifecycleRepo{store: ts},
		Audit:          NoopAudit{},
		BracketPages:   ts,
		Standings:      ts,
	})
	return pool, ts, svc
}

func resetServiceTestDB(t *testing.T, ctx context.Context, pool *store.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		DROP TABLE IF EXISTS round_advancing_players CASCADE;
		DROP TABLE IF EXISTS tournament_round_slot_players CASCADE;
		DROP TABLE IF EXISTS advancement_records CASCADE;
		DROP TABLE IF EXISTS match_result_quarantines CASCADE;
		DROP TABLE IF EXISTS match_results CASCADE;
		DROP TABLE IF EXISTS assigned_matches CASCADE;
		DROP TABLE IF EXISTS bracket_slots CASCADE;
		DROP TABLE IF EXISTS round_seeding_batches CASCADE;
		DROP TABLE IF EXISTS round_seeding_jobs CASCADE;
		DROP TABLE IF EXISTS round_progress_shards CASCADE;
		DROP TABLE IF EXISTS provisioning_batches CASCADE;
		DROP TABLE IF EXISTS tournament_rounds CASCADE;
		DROP TABLE IF EXISTS tournament_registrations CASCADE;
		DROP TABLE IF EXISTS tournament_registration_shards CASCADE;
		DROP TABLE IF EXISTS bracket_projection_shards CASCADE;
		DROP TABLE IF EXISTS bracket_projection_versions CASCADE;
		DROP TABLE IF EXISTS outbox_events CASCADE;
		DROP TABLE IF EXISTS command_idempotency CASCADE;
		DROP TABLE IF EXISTS kafka_consumer_quarantine CASCADE;
		DROP TABLE IF EXISTS tournaments CASCADE;
		DROP TABLE IF EXISTS schema_migrations CASCADE;
		DROP TABLE IF EXISTS schema_bootstrap_meta CASCADE;
	`); err != nil {
		t.Fatalf("reset: %v", err)
	}
}

func applyServiceMigration(t *testing.T, ctx context.Context, pool *store.Pool) {
	t.Helper()
	sqlBytes, err := os.ReadFile(filepath.Join("..", "migrations", "001_init.sql"))
	if err != nil {
		t.Fatalf("migration: %v", err)
	}
	if _, err := pool.Exec(ctx, string(sqlBytes)); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS schema_bootstrap_meta`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		CREATE TABLE schema_bootstrap_meta (version TEXT NOT NULL, checksum TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO schema_bootstrap_meta (version, checksum) VALUES ($1,$2)`,
		store.ExpectedBootstrapVersion, store.ExpectedSchemaChecksum); err != nil {
		t.Fatal(err)
	}
}

func provisionViaService(t *testing.T, ts *store.TournamentStore, tid string, n int) domain.BracketSlot {
	t.Helper()
	players := make([]string, n)
	for i := 0; i < n; i++ {
		players[i] = fmt.Sprintf("sp%d", i)
	}
	tr, _ := domain.CreateTournament(domain.CreateTournamentCommand{
		CommandID: "c", TournamentID: domain.TournamentID(tid), Capacity: n,
	})
	for _, p := range players {
		_ = tr.RegisterPlayer(domain.RegisterPlayerCommand{CommandID: domain.CommandID("r-" + p), PlayerID: domain.PlayerID(p)})
	}
	_ = tr.CloseRegistration(domain.CloseRegistrationCommand{CommandID: "close"})
	_ = tr.SeedRound(domain.SeedRoundCommand{CommandID: "seed", RoundNumber: 1})
	_ = tr.ProvisionRoundMatches(domain.ProvisionRoundMatchesCommand{CommandID: "prov", RoundNumber: 1})
	if err := ts.Commit(context.Background(), store.CommitRequest{
		Tournament: tr, CommandID: "prov",
		Outcome:           envelope.Accepted("prov", "ProvisionRoundMatches", nil, json.RawMessage(`{}`)),
		ProjectionChanged: true,
	}); err != nil {
		t.Fatal(err)
	}
	got, ok := ts.Get(context.Background(), domain.TournamentID(tid))
	if !ok {
		t.Fatal("missing")
	}
	round, _ := got.Round(1)
	return round.Slots[0]
}

func TestIntegration_ServiceDurable_UnknownRoomAndHeld(t *testing.T) {
	pool, ts, svc := serviceDurablePostgres(t)
	ctx := context.Background()
	if !svc.UsesRoundMatchDifferential() {
		t.Fatal("expected differential RoundMatches wiring")
	}
	slot := provisionViaService(t, ts, "t-svc-d1", 12)
	standings := make([]MatchPlayerStanding, len(slot.SeededPlayers))
	now := time.Now().UTC()
	for i, p := range slot.SeededPlayers {
		standings[i] = MatchPlayerStanding{
			PlayerID: string(p), MatchWins: len(slot.SeededPlayers) - i,
			CumulativeCardPoints: 100 - i, FinalGameCompletedAt: now,
		}
	}

	// Unknown room via Service durable path: quarantine ledger only.
	unk, err := svc.IngestMatchCompleted(ctx, MatchCompletedEvent{
		EventID: "svc-unk", EventType: eventTypeMatchCompleted, SchemaVersion: envelope.CurrentSchemaVersion,
		RoomID: "ghost-room", TournamentID: "t-svc-d1", RoundNumber: 1, SlotID: string(slot.SlotID),
		CompletionVersion: 1, IsAbandoned: false, HasIsAbandoned: true, Players: standings,
	})
	if err != nil {
		t.Fatal(err)
	}
	if unk["disposition"] != "quarantined" {
		t.Fatalf("resp=%v", unk)
	}
	var nResults, nQ int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_results WHERE tournament_id=$1`, "t-svc-d1").Scan(&nResults)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_result_quarantines WHERE tournament_id=$1`, "t-svc-d1").Scan(&nQ)
	if nResults != 0 || nQ != 1 {
		t.Fatalf("results=%d quarantines=%d", nResults, nQ)
	}

	// Abandoned then held later version via Service.
	ab, err := svc.IngestMatchCompleted(ctx, MatchCompletedEvent{
		EventID: "svc-ab", EventType: eventTypeMatchCompleted, SchemaVersion: envelope.CurrentSchemaVersion,
		RoomID: string(slot.RoomID), TournamentID: "t-svc-d1", RoundNumber: 1, SlotID: string(slot.SlotID),
		CompletionVersion: 2, IsAbandoned: true, HasIsAbandoned: true, Players: standings,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ab["disposition"] != "quarantined" {
		t.Fatalf("abandon=%v", ab)
	}
	held, err := svc.IngestMatchCompleted(ctx, MatchCompletedEvent{
		EventID: "svc-held", EventType: eventTypeMatchCompleted, SchemaVersion: envelope.CurrentSchemaVersion,
		RoomID: string(slot.RoomID), TournamentID: "t-svc-d1", RoundNumber: 1, SlotID: string(slot.SlotID),
		CompletionVersion: 3, IsAbandoned: false, HasIsAbandoned: true, Players: standings,
	})
	if err != nil {
		t.Fatal(err)
	}
	if held["disposition"] != "quarantined" {
		t.Fatalf("held=%v", held)
	}
	var resAfter, qAfter int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_results WHERE tournament_id=$1`, "t-svc-d1").Scan(&resAfter)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_result_quarantines WHERE tournament_id=$1`, "t-svc-d1").Scan(&qAfter)
	if resAfter != 1 || qAfter != 2 { // unk + abandon; held writes neither
		t.Fatalf("after held results=%d quarantines=%d", resAfter, qAfter)
	}
}

func standingsFromSlotPlayers(slot domain.BracketSlot) []map[string]any {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	out := make([]map[string]any, len(slot.SeededPlayers))
	for i, p := range slot.SeededPlayers {
		out[i] = map[string]any{
			"playerId":             string(p),
			"matchWins":            len(slot.SeededPlayers) - i,
			"cumulativeCardPoints": 100 - i,
			"finalGameCompletedAt": now,
			"forfeited":            false,
		}
	}
	return out
}

func recordMatchHTTPPayload(tournamentID, roomID, slotID string, roundNumber int, completionVersion uint64, standings []map[string]any, opts map[string]any) json.RawMessage {
	payload := map[string]any{
		"tournamentId":      tournamentID,
		"roomId":            roomID,
		"roundNumber":       roundNumber,
		"slotId":            slotID,
		"completionVersion": completionVersion,
		"standings":         standings,
		"isAbandoned":       false,
	}
	for k, v := range opts {
		payload[k] = v
	}
	b, _ := json.Marshal(payload)
	return b
}

func TestIntegration_ServiceDurable_HTTPRecordMatchResult(t *testing.T) {
	pool, ts, svc := serviceDurablePostgres(t)
	ctx := context.Background()
	if !svc.UsesRoundMatchDifferential() {
		t.Fatal("expected differential RoundMatches wiring")
	}
	slot := provisionViaService(t, ts, "t-svc-http", 12)
	standings := standingsFromSlotPlayers(slot)

	// Optional eventId: source_event_id stays nullable; still records.
	res, err := svc.SubmitCommand(ctx, CommandRequest{
		CommandID: "http-no-event", Type: CmdRecordMatchResult, SchemaVersion: envelope.CurrentSchemaVersion,
		Payload: recordMatchHTTPPayload("t-svc-http", string(slot.RoomID), string(slot.SlotID), 1, 10, standings, nil),
	}, "corr-http")
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != envelope.StatusAccepted {
		t.Fatalf("status=%s reason=%s", res.Status, res.Reason)
	}
	var srcEvent *string
	if err := pool.QueryRow(ctx, `
		SELECT source_event_id FROM match_results
		WHERE room_id=$1 AND completion_version=10
	`, string(slot.RoomID)).Scan(&srcEvent); err != nil {
		t.Fatal(err)
	}
	if srcEvent != nil {
		t.Fatalf("expected nullable source_event_id, got %v", *srcEvent)
	}

	// Hint mismatch: claimed wrong slot → quarantine, recorded slot untouched for wrong claim path.
	other := findOtherProvisionedSlot(t, ts, "t-svc-http", slot.SlotID)
	hintMismatch, err := svc.SubmitCommand(ctx, CommandRequest{
		CommandID: "http-hint-mismatch", Type: CmdRecordMatchResult, SchemaVersion: envelope.CurrentSchemaVersion,
		Payload: recordMatchHTTPPayload("t-svc-http", string(slot.RoomID), string(other.SlotID), 1, 11, standings, map[string]any{
			"eventId": "hint-evt",
		}),
	}, "corr-hint")
	if err != nil {
		t.Fatal(err)
	}
	if hintMismatch.Status != envelope.StatusAccepted {
		t.Fatalf("hint mismatch status=%s reason=%s", hintMismatch.Status, hintMismatch.Reason)
	}
	var qHint int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_result_quarantines WHERE quarantine_id=$1`, "http-hint-mismatch").Scan(&qHint)
	if qHint != 1 {
		t.Fatalf("expected hint-mismatch quarantine, got %d", qHint)
	}

	// Exact duplicate different commandId: same room+version+fingerprint → exact duplicate, no second row.
	dupStandings := standings
	dup, err := svc.SubmitCommand(ctx, CommandRequest{
		CommandID: "http-exact-dup", Type: CmdRecordMatchResult, SchemaVersion: envelope.CurrentSchemaVersion,
		Payload: recordMatchHTTPPayload("t-svc-http", string(slot.RoomID), string(slot.SlotID), 1, 10, dupStandings, nil),
	}, "corr-dup")
	if err != nil {
		t.Fatal(err)
	}
	if dup.Status != envelope.StatusAccepted {
		t.Fatalf("dup status=%s reason=%s", dup.Status, dup.Reason)
	}
	var nVer10 int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_results WHERE room_id=$1 AND completion_version=10`, string(slot.RoomID)).Scan(&nVer10)
	if nVer10 != 1 {
		t.Fatalf("exact dup must not insert second row, got %d", nVer10)
	}

	// Conflict quarantine: same room+version, different fingerprint (change wins).
	conflictStandings := make([]map[string]any, len(standings))
	for i, row := range standings {
		cp := map[string]any{}
		for k, v := range row {
			cp[k] = v
		}
		if i == 0 {
			cp["matchWins"] = 1
		} else if i == 1 {
			cp["matchWins"] = len(standings)
		}
		conflictStandings[i] = cp
	}
	conflict, err := svc.SubmitCommand(ctx, CommandRequest{
		CommandID: "http-conflict", Type: CmdRecordMatchResult, SchemaVersion: envelope.CurrentSchemaVersion,
		Payload: recordMatchHTTPPayload("t-svc-http", string(slot.RoomID), string(slot.SlotID), 1, 10, conflictStandings, map[string]any{
			"eventId": "conflict-evt",
		}),
	}, "corr-conflict")
	if err != nil {
		t.Fatal(err)
	}
	if conflict.Status != envelope.StatusAccepted {
		t.Fatalf("conflict status=%s reason=%s", conflict.Status, conflict.Reason)
	}
	var qConflict int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_result_quarantines WHERE quarantine_id=$1`, "http-conflict").Scan(&qConflict)
	if qConflict != 1 {
		t.Fatalf("expected conflict quarantine, got %d", qConflict)
	}
	var disp string
	if err := pool.QueryRow(ctx, `
		SELECT disposition FROM match_results WHERE room_id=$1 AND completion_version=10
	`, string(slot.RoomID)).Scan(&disp); err != nil {
		t.Fatal(err)
	}
	if disp != "recorded" {
		t.Fatalf("conflict must preserve recorded disposition, got %s", disp)
	}

	// Projection/counter invariants after successful record (version 10).
	var resolvedSum, projSum int64
	_ = pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(resolved_count),0) FROM round_progress_shards WHERE tournament_id=$1
	`, "t-svc-http").Scan(&resolvedSum)
	_ = pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(version),0) FROM bracket_projection_shards WHERE tournament_id=$1
	`, "t-svc-http").Scan(&projSum)
	if resolvedSum < 1 {
		t.Fatalf("expected resolved_count bump, got %d", resolvedSum)
	}
	if projSum < 1 {
		t.Fatalf("expected projection shard bump, got %d", projSum)
	}
}

func TestIntegration_ServiceDurable_InvalidPayloadReplay(t *testing.T) {
	pool, _, svc := serviceDurablePostgres(t)
	ctx := context.Background()
	if !svc.UsesRoundMatchDifferential() {
		t.Fatal("expected differential RoundMatches wiring")
	}

	const cmdID = "http-invalid-payload"
	first, err := svc.SubmitCommand(ctx, CommandRequest{
		CommandID: cmdID, Type: CmdRecordMatchResult, SchemaVersion: envelope.CurrentSchemaVersion,
		Payload: json.RawMessage(`{not-json`),
	}, "corr-bad")
	if err != nil {
		t.Fatalf("must persist rejection, not Go error: %v", err)
	}
	if first.Status != envelope.StatusRejected || first.Reason != "invalid_payload" {
		t.Fatalf("got status=%s reason=%s", first.Status, first.Reason)
	}

	var nResults, nTournaments int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_results`).Scan(&nResults)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM tournaments`).Scan(&nTournaments)
	if nResults != 0 || nTournaments != 0 {
		t.Fatalf("invalid_payload must not mutate legacy repo rows: results=%d tournaments=%d", nResults, nTournaments)
	}

	var storedBody []byte
	if err := pool.QueryRow(ctx, `
		SELECT outcome_body FROM command_idempotency WHERE command_id=$1
	`, cmdID).Scan(&storedBody); err != nil {
		t.Fatalf("expected stored outcome: %v", err)
	}
	var stored envelope.Result
	if err := json.Unmarshal(storedBody, &stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status != first.Status || stored.Reason != first.Reason || stored.CommandID != first.CommandID {
		t.Fatalf("stored=%+v first=%+v", stored, first)
	}

	replay, err := svc.SubmitCommand(ctx, CommandRequest{
		CommandID: cmdID, Type: CmdRecordMatchResult, SchemaVersion: envelope.CurrentSchemaVersion,
		Payload: json.RawMessage(`{"tournamentId":"t-x","roomId":"r-x","completionVersion":1,"standings":[]}`),
	}, "corr-replay")
	if err != nil {
		t.Fatal(err)
	}
	if replay.Status != first.Status || replay.Reason != first.Reason || replay.CommandID != first.CommandID {
		t.Fatalf("replay mismatch first=%+v replay=%+v", first, replay)
	}
	if string(replay.Payload) != string(first.Payload) && string(replay.Payload) != string(stored.Payload) {
		// Canonical stored body wins; compare to stored.
		if replay.Reason != stored.Reason || replay.Status != stored.Status {
			t.Fatalf("replay must equal stored rejection")
		}
	}
}

func findOtherProvisionedSlot(t *testing.T, ts *store.TournamentStore, tid string, exclude domain.SlotID) domain.BracketSlot {
	t.Helper()
	got, ok := ts.Get(context.Background(), domain.TournamentID(tid))
	if !ok {
		t.Fatal("missing tournament")
	}
	round, _ := got.Round(1)
	for _, s := range round.Slots {
		if s.SlotID != exclude && s.RoomID.Valid() {
			return s
		}
	}
	t.Fatal("no other slot")
	return domain.BracketSlot{}
}
