//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/services/tournament-orchestration/store"
	"unoarena/shared/envelope"
)

func TestIntegration_VisibilityRoundTripAndPrivateReadAuth(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()

	createTournamentTReg(t, ts, "vis-pub", 4)
	var vis string
	if err := pool.QueryRow(ctx, `SELECT visibility FROM tournaments WHERE tournament_id=$1`, "vis-pub").Scan(&vis); err != nil {
		t.Fatal(err)
	}
	if vis != "public" {
		t.Fatalf("default visibility=%q", vis)
	}
	if err := ts.AuthorizeTournamentRead(ctx, "vis-pub", nil); err != nil {
		t.Fatalf("public anonymous: %v", err)
	}

	uow, err := ts.BeginCreateTournament(ctx, "vis-priv", "create-vis-priv")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = uow.Rollback() }()
	cmd := domain.CreateTournamentCommand{
		CommandID: "create-vis-priv", TournamentID: "vis-priv", Capacity: 4,
		Visibility: domain.TournamentVisibilityPrivate,
	}
	decision := domain.DecideCreateTournament(cmd)
	if decision.Kind != domain.RegistrationCreate {
		t.Fatalf("decision=%+v", decision)
	}
	res := envelope.Accepted("create-vis-priv", "CreateTournament", nil, json.RawMessage(`{"facts":[]}`))
	if err := uow.Commit(store.RegistrationCommitRequest{
		Op: store.RegistrationOpCreate, TournamentID: "vis-priv", CommandID: "create-vis-priv",
		CommandType: "CreateTournament", Outcome: res, Decision: decision, CreateCmd: cmd,
		RetryBudget: 3, BatchSize: 100, BumpBaseProjection: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT visibility FROM tournaments WHERE tournament_id=$1`, "vis-priv").Scan(&vis); err != nil {
		t.Fatal(err)
	}
	if vis != "private" {
		t.Fatalf("private visibility=%q", vis)
	}

	if err := ts.AuthorizeTournamentRead(ctx, "vis-priv", nil); err != store.ErrUnauthorizedRead {
		t.Fatalf("private anonymous want unauthorized, got %v", err)
	}
	if err := ts.AuthorizeTournamentRead(ctx, "vis-priv", &store.ReadPrincipal{PlayerID: "stranger"}); err != store.ErrForbiddenRead {
		t.Fatalf("nonparticipant want forbidden, got %v", err)
	}

	registerOneTReg(t, ts, "vis-priv", "p1", "reg-p1")
	if err := ts.AuthorizeTournamentRead(ctx, "vis-priv", &store.ReadPrincipal{PlayerID: "p1"}); err != nil {
		t.Fatalf("participant: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE tournament_registrations SET status='eliminated'
		WHERE tournament_id=$1 AND player_id=$2
	`, "vis-priv", "p1"); err != nil {
		t.Fatal(err)
	}
	if err := ts.AuthorizeTournamentRead(ctx, "vis-priv", &store.ReadPrincipal{PlayerID: "p1"}); err != nil {
		t.Fatalf("eliminated remains participant: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE tournament_registrations SET status='withdrawn'
		WHERE tournament_id=$1 AND player_id=$2
	`, "vis-priv", "p1"); err != nil {
		t.Fatal(err)
	}
	if err := ts.AuthorizeTournamentRead(ctx, "vis-priv", &store.ReadPrincipal{PlayerID: "p1"}); err != store.ErrForbiddenRead {
		t.Fatalf("withdrawn want forbidden, got %v", err)
	}
	if err := ts.AuthorizeTournamentRead(ctx, "vis-priv", &store.ReadPrincipal{OperatorScope: true}); err != nil {
		t.Fatalf("operator: %v", err)
	}

	bad := domain.DecideCreateTournament(domain.CreateTournamentCommand{
		CommandID: "bad-vis", TournamentID: "vis-bad", Capacity: 2,
		Visibility: domain.TournamentVisibility("secret"),
	})
	if bad.Kind != domain.RegistrationReject {
		t.Fatalf("invalid visibility want reject, got %+v", bad)
	}
}

func TestIntegration_PlayerAssignmentIndexRoundProgressionAndAuth(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	const tid = "asg-idx"
	players := []string{"p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8"}
	prepareSeedingTournament(t, ts, tid, players)
	res := seedRound1Kickoff(t, ts, tid, "seed-asg")
	if res.Status == envelope.StatusRejected {
		t.Fatalf("kickoff: %+v", res)
	}
	runSeedingToCompletion(t, ts, tid)

	var mapCount int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)::int FROM tournament_round_slot_players WHERE tournament_id=$1 AND round_number=1
	`, tid).Scan(&mapCount); err != nil {
		t.Fatal(err)
	}
	if mapCount != 8 {
		t.Fatalf("round1 mappings=%d want 8", mapCount)
	}

	view, err := ts.LoadPlayerAssignment(ctx, tid, "p1")
	if err != nil {
		t.Fatal(err)
	}
	if view.Assignment == nil || view.Assignment.RoundNumber != 1 || view.Assignment.SlotID == "" {
		t.Fatalf("assignment=%+v", view.Assignment)
	}
	if view.Assignment.RoomID != "" {
		t.Fatalf("pre-provision roomId should be empty, got %q", view.Assignment.RoomID)
	}

	if err := ts.AuthorizePlayerAssignment(ctx, tid, "p1", &store.ReadPrincipal{PlayerID: "p1"}); err != nil {
		t.Fatal(err)
	}
	if err := ts.AuthorizePlayerAssignment(ctx, tid, "p1", &store.ReadPrincipal{PlayerID: "p2"}); err != store.ErrForbiddenRead {
		t.Fatalf("wrong player: %v", err)
	}
	if err := ts.AuthorizePlayerAssignment(ctx, tid, "p1", &store.ReadPrincipal{PlayerID: "op", OperatorScope: true}); err != nil {
		t.Fatal(err)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO tournament_rounds (tournament_id, round_number, status, is_final)
		VALUES ($1, 2, 'seeded', false)
		ON CONFLICT DO NOTHING
	`, tid); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO bracket_slots (
			tournament_id, round_number, slot_id, slot_index, status, seeded_player_ids, created_at, updated_at
		) VALUES ($1, 2, 'slot-r2-0', 0, 'pending', ARRAY['p1','p2']::text[], now(), now())
	`, tid); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO tournament_round_slot_players (tournament_id, round_number, player_id, slot_id, seat_index)
		VALUES ($1, 2, 'p1', 'slot-r2-0', 0), ($1, 2, 'p2', 'slot-r2-0', 1)
	`, tid); err != nil {
		t.Fatal(err)
	}
	view, err = ts.LoadPlayerAssignment(ctx, tid, "p1")
	if err != nil {
		t.Fatal(err)
	}
	if view.Assignment == nil || view.Assignment.RoundNumber != 2 || view.Assignment.SlotID != "slot-r2-0" {
		t.Fatalf("latest mapping want round2, got %+v", view.Assignment)
	}

	const roomID = "room-asg-r2-0"
	if _, err := pool.Exec(ctx, `
		INSERT INTO assigned_matches (tournament_id, round_number, slot_id, room_id)
		VALUES ($1, 2, 'slot-r2-0', $2)
	`, tid, roomID); err != nil {
		t.Fatal(err)
	}
	view, err = ts.LoadPlayerAssignment(ctx, tid, "p1")
	if err != nil {
		t.Fatal(err)
	}
	if view.Assignment.RoomID != "" || view.Assignment.SlotStatus != "provisioning" {
		t.Fatalf("pre-ready assignment leaked room: %+v", view.Assignment)
	}
	roundTwo := 2
	page, err := ts.LoadBracketPage(ctx, store.BracketPageQuery{TournamentID: tid, RoundNumber: &roundTwo, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Slots) != 1 || page.Slots[0].RoomID != "" || page.Slots[0].Status != "provisioning" {
		t.Fatalf("pre-ready bracket leaked room: %+v", page.Slots)
	}
	projectionBefore, _, err := ts.LoadProjectionCheckpoint(ctx, tid)
	if err != nil {
		t.Fatal(err)
	}
	readyAt := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	applied, err := ts.ApplyRoomRuntimeReady(ctx, store.RoomRuntimeReady{
		EventID: "room-runtime-ready:" + roomID, RoomID: roomID, TournamentID: tid,
		RoundNumber: 2, SlotID: "slot-r2-0", Generation: 1, OccurredAt: readyAt,
	})
	if err != nil || !applied {
		t.Fatalf("apply readiness applied=%v err=%v", applied, err)
	}
	view, err = ts.LoadPlayerAssignment(ctx, tid, "p1")
	if err != nil || view.Assignment.RoomID != roomID {
		t.Fatalf("ready assignment=%+v err=%v", view.Assignment, err)
	}
	page, err = ts.LoadBracketPage(ctx, store.BracketPageQuery{TournamentID: tid, RoundNumber: &roundTwo, Limit: 10})
	if err != nil || len(page.Slots) != 1 || page.Slots[0].RoomID != roomID {
		t.Fatalf("ready bracket=%+v err=%v", page.Slots, err)
	}
	projectionReady, _, err := ts.LoadProjectionCheckpoint(ctx, tid)
	if err != nil || projectionReady != projectionBefore+1 {
		t.Fatalf("readiness projection before=%d after=%d err=%v", projectionBefore, projectionReady, err)
	}
	// A replacement generation is not a new visibility transition and cannot hide the room.
	applied, err = ts.ApplyRoomRuntimeReady(ctx, store.RoomRuntimeReady{
		EventID: "replacement-should-not-publish", RoomID: roomID, TournamentID: tid,
		RoundNumber: 2, SlotID: "slot-r2-0", Generation: 2, OccurredAt: readyAt.Add(time.Minute),
	})
	if err != nil || applied {
		t.Fatalf("replacement applied=%v err=%v", applied, err)
	}
	view, err = ts.LoadPlayerAssignment(ctx, tid, "p1")
	if err != nil || view.Assignment.RoomID != roomID {
		t.Fatalf("replacement hid room: %+v err=%v", view.Assignment, err)
	}
	projectionReplacement, _, err := ts.LoadProjectionCheckpoint(ctx, tid)
	if err != nil || projectionReplacement != projectionReady {
		t.Fatalf("replacement projection ready=%d after=%d err=%v", projectionReady, projectionReplacement, err)
	}

	var conflict int
	err = pool.QueryRow(ctx, `
		WITH incoming_players AS (
			SELECT 'p1'::text AS player_id, 'slot-r2-0'::text AS slot_id, 9::int AS seat_index
		)
		SELECT COUNT(*)::int
		FROM incoming_players ip
		INNER JOIN tournament_round_slot_players m
			ON m.tournament_id = $1 AND m.round_number = 2 AND m.player_id = ip.player_id
		WHERE m.seat_index IS DISTINCT FROM ip.seat_index
	`, tid).Scan(&conflict)
	if err != nil {
		t.Fatal(err)
	}
	if conflict != 1 {
		t.Fatalf("drift detect count=%d", conflict)
	}
}

func TestIntegration_PlayerAssignment_BoundedOver1000NoArrayScan(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	const tid = "asg-1k"
	const n = 1004
	createTournamentTReg(t, ts, tid, n)
	if _, err := pool.Exec(ctx, `
		INSERT INTO tournament_rounds (tournament_id, round_number, status, is_final, seeded_at)
		VALUES ($1, 1, 'seeded', false, now())
	`, tid); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	for i := 0; i < n; i += 4 {
		slotID := fmt.Sprintf("s-%d", i/4)
		p0, p1, p2, p3 := fmt.Sprintf("u%d", i), fmt.Sprintf("u%d", i+1), fmt.Sprintf("u%d", i+2), fmt.Sprintf("u%d", i+3)
		if _, err := pool.Exec(ctx, `
			INSERT INTO tournament_registrations (tournament_id, player_id, shard_id, registered_at, status)
			VALUES
				($1,$2,0,$6,'registered'),($1,$3,0,$6,'registered'),
				($1,$4,0,$6,'registered'),($1,$5,0,$6,'registered')
		`, tid, p0, p1, p2, p3, now.Add(time.Duration(i)*time.Microsecond)); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO bracket_slots (
				tournament_id, round_number, slot_id, slot_index, status, seeded_player_ids, created_at, updated_at
			) VALUES ($1, 1, $2, $3, 'pending', ARRAY[$4,$5,$6,$7]::text[], $8, $8)
		`, tid, slotID, i/4, p0, p1, p2, p3, now); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO tournament_round_slot_players (tournament_id, round_number, player_id, slot_id, seat_index)
			VALUES
				($1,1,$3,$2,0),($1,1,$4,$2,1),($1,1,$5,$2,2),($1,1,$6,$2,3)
		`, tid, slotID, p0, p1, p2, p3); err != nil {
			t.Fatal(err)
		}
	}
	target := "u1000"
	view, err := ts.LoadPlayerAssignment(ctx, tid, target)
	if err != nil {
		t.Fatal(err)
	}
	if view.Assignment == nil || view.Assignment.RoundNumber != 1 {
		t.Fatalf("bounded lookup failed: %+v", view)
	}
	var idx int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)::int FROM pg_indexes
		WHERE tablename='tournament_round_slot_players'
		  AND indexname='tournament_round_slot_players_player_round_idx'
	`).Scan(&idx); err != nil || idx != 1 {
		t.Fatalf("missing player_round index: idx=%d err=%v", idx, err)
	}
}
