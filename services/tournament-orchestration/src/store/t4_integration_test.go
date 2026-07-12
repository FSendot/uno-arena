//go:build integration

package store_test

import (
	"context"
	"testing"
	"time"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/services/tournament-orchestration/store"
	"unoarena/shared/envelope"
)

func TestIntegration_RoundMatchRecord_AdvancingPlayersSetBased(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	tr, slot := provisionedTournament(t, ts, "t-adv-norm", playersN(12))
	_ = tr
	standings := standingsFromSeeded(slot.SeededPlayers)
	cmd := domain.RecordMatchResultCommand{
		CommandID: "ingest:adv-norm-1", EventID: "adv-norm-1",
		RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
		CompletionVersion: 1, Standings: standings,
	}
	kind := ingestDifferentialKind(t, ts, "t-adv-norm", cmd)
	if kind != domain.RoundMatchRecord {
		t.Fatalf("kind=%s", kind)
	}

	ranked, err := domain.RankStandings(standings)
	if err != nil {
		t.Fatal(err)
	}
	wantAdv, err := domain.TopThree(ranked)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := pool.Query(ctx, `
		SELECT player_id, source_slot_id, advancement_rank
		FROM round_advancing_players
		WHERE tournament_id=$1 AND source_round_number=1
		ORDER BY advancement_rank ASC, player_id ASC
	`, "t-adv-norm")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := make([]string, 0, len(wantAdv))
	for rows.Next() {
		var pid, slotID string
		var rank int
		if err := rows.Scan(&pid, &slotID, &rank); err != nil {
			t.Fatal(err)
		}
		if slotID != string(slot.SlotID) {
			t.Fatalf("slot=%s want %s", slotID, slot.SlotID)
		}
		if rank != len(got) {
			t.Fatalf("rank=%d want %d", rank, len(got))
		}
		got = append(got, pid)
	}
	if len(got) != len(wantAdv) {
		t.Fatalf("rows=%d want %d", len(got), len(wantAdv))
	}
	for i := range wantAdv {
		if got[i] != string(wantAdv[i]) {
			t.Fatalf("ord %d: got %s want %s", i, got[i], wantAdv[i])
		}
	}

	// Exact same set-based insert is idempotent (conflictCount=0).
	adv := make([]string, len(wantAdv))
	for i, p := range wantAdv {
		adv[i] = string(p)
	}
	var conflictCount int
	err = pool.QueryRow(ctx, `
		WITH incoming AS (
			SELECT p.player_id, (p.ord - 1)::int AS advancement_rank
			FROM unnest($4::text[]) WITH ORDINALITY AS p(player_id, ord)
		),
		ins AS (
			INSERT INTO round_advancing_players (
				tournament_id, source_round_number, player_id, source_slot_id, advancement_rank
			)
			SELECT $1, $2, i.player_id, $3, i.advancement_rank
			FROM incoming i
			ON CONFLICT (tournament_id, source_round_number, player_id) DO NOTHING
			RETURNING player_id
		)
		SELECT COUNT(*)::int
		FROM incoming i
		INNER JOIN round_advancing_players r
			ON r.tournament_id = $1 AND r.source_round_number = $2 AND r.player_id = i.player_id
		WHERE NOT EXISTS (SELECT 1 FROM ins WHERE ins.player_id = i.player_id)
		  AND (
			r.source_slot_id IS DISTINCT FROM $3
			OR r.advancement_rank IS DISTINCT FROM i.advancement_rank
		  )
	`, "t-adv-norm", 1, string(slot.SlotID), adv).Scan(&conflictCount)
	if err != nil {
		t.Fatal(err)
	}
	if conflictCount != 0 {
		t.Fatalf("exact replay conflictCount=%d", conflictCount)
	}

	// Drift on rank/slot fails closed (conflictCount>0).
	wrongAdv := []string{adv[0]}
	err = pool.QueryRow(ctx, `
		WITH incoming AS (
			SELECT p.player_id, (p.ord - 1)::int AS advancement_rank
			FROM unnest($4::text[]) WITH ORDINALITY AS p(player_id, ord)
		),
		ins AS (
			INSERT INTO round_advancing_players (
				tournament_id, source_round_number, player_id, source_slot_id, advancement_rank
			)
			SELECT $1, $2, i.player_id, $3, i.advancement_rank
			FROM incoming i
			ON CONFLICT (tournament_id, source_round_number, player_id) DO NOTHING
			RETURNING player_id
		)
		SELECT COUNT(*)::int
		FROM incoming i
		INNER JOIN round_advancing_players r
			ON r.tournament_id = $1 AND r.source_round_number = $2 AND r.player_id = i.player_id
		WHERE NOT EXISTS (SELECT 1 FROM ins WHERE ins.player_id = i.player_id)
		  AND (
			r.source_slot_id IS DISTINCT FROM $3
			OR r.advancement_rank IS DISTINCT FROM i.advancement_rank
		  )
	`, "t-adv-norm", 1, "slot_other", wrongAdv).Scan(&conflictCount)
	if err != nil {
		t.Fatal(err)
	}
	if conflictCount == 0 {
		t.Fatal("expected fail-closed conflict on source_slot_id drift")
	}
}

func TestIntegration_CompleteRound_FinalCreatesNoNextJob(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	// 9 players → final (≤ FinalPlayerThreshold).
	_, wantAdv := prepareCompleteRoundReady(t, pool, ts, "t-cr-final", 9)
	d := completeRoundDifferential(t, ts, "t-cr-final", 1, domain.CompleteRoundCommandID("t-cr-final", 1))
	if d.Kind != domain.CompleteRoundSuccess || !d.IsFinal || d.NextRound != nil {
		t.Fatalf("want final success without NextRound, got %+v remaining=%d", d, wantAdv)
	}
	var n int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM round_seeding_jobs WHERE tournament_id=$1 AND round_number=2`, "t-cr-final").Scan(&n)
	if n != 0 {
		t.Fatalf("final must not create next job, got %d", n)
	}
	var r2 int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM tournament_rounds WHERE tournament_id=$1 AND round_number=2`, "t-cr-final").Scan(&r2)
	if r2 != 0 {
		t.Fatalf("final must not create round 2, got %d", r2)
	}
}

func TestIntegration_CompleteRound_PlanConflictRollsBack(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	_, wantAdv := prepareCompleteRoundReady(t, pool, ts, "t-cr-conflict", 12)
	_ = wantAdv

	// Pre-seed a conflicting next job (wrong player_count).
	if _, err := pool.Exec(ctx, `
		INSERT INTO tournament_rounds (tournament_id, round_number, status, is_final)
		VALUES ($1, 2, 'pending', false)
	`, "t-cr-conflict"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO round_seeding_jobs (
			tournament_id, round_number, source, source_round_number, status,
			player_count, slot_count, base_size, remainder,
			command_id, created_at, updated_at
		) VALUES (
			$1, 2, 'advancement', 1, 'pending',
			999, 100, 9, 99,
			'seed:t-cr-conflict:r2', now(), now()
		)
	`, "t-cr-conflict"); err != nil {
		t.Fatal(err)
	}

	uow, err := ts.BeginCompleteRound(ctx, "t-cr-conflict", 1, "cr-conflict-1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = uow.Rollback() }()
	d := domain.DecideCompleteRound(uow.Loaded(), domain.CompleteRoundCommand{
		CommandID: "cr-conflict-1", RoundNumber: 1,
	})
	if d.Kind != domain.CompleteRoundSuccess || d.NextRound == nil {
		t.Fatalf("decision=%+v", d)
	}
	err = uow.Commit(store.CompleteRoundCommitRequest{
		TournamentID: "t-cr-conflict", CommandID: "cr-conflict-1", CommandType: "CompleteRound",
		Outcome: envelope.Accepted("cr-conflict-1", "CompleteRound", nil, nil), Decision: d,
	})
	if err == nil {
		t.Fatal("expected plan conflict to fail commit")
	}

	var status string
	_ = pool.QueryRow(ctx, `SELECT status FROM tournament_rounds WHERE tournament_id=$1 AND round_number=1`, "t-cr-conflict").Scan(&status)
	if status == "completed" {
		t.Fatal("plan conflict must roll back completion")
	}
	var outcomeN int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM command_idempotency WHERE command_id=$1`, "cr-conflict-1").Scan(&outcomeN)
	if outcomeN != 0 {
		t.Fatalf("rolled-back commit must not persist outcome, got %d", outcomeN)
	}
}

func TestIntegration_SeedingRound2_FromAdvancementToProvisioning(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	_, wantAdv := prepareCompleteRoundReady(t, pool, ts, "t-seed-r2", 12)
	d := completeRoundDifferential(t, ts, "t-seed-r2", 1, domain.CompleteRoundCommandID("t-seed-r2", 1))
	if d.Kind != domain.CompleteRoundSuccess || d.NextRound == nil {
		t.Fatalf("complete=%+v", d)
	}
	if d.NextRound.PlayerCount != wantAdv {
		t.Fatalf("plan players=%d wantAdv=%d", d.NextRound.PlayerCount, wantAdv)
	}

	// Chunk order is player_id ASC independent of source slot order.
	var ordered []string
	rows, err := pool.Query(ctx, `
		SELECT player_id FROM round_advancing_players
		WHERE tournament_id=$1 AND source_round_number=1
		ORDER BY player_id ASC
	`, "t-seed-r2")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var pid string
		if err := rows.Scan(&pid); err != nil {
			t.Fatal(err)
		}
		ordered = append(ordered, pid)
	}
	rows.Close()
	if len(ordered) != wantAdv {
		t.Fatalf("advancing rows=%d want %d", len(ordered), wantAdv)
	}

	now := time.Now().UTC()
	owner := "seed-r2-worker"
	var completed bool
	for i := 0; i < 64; i++ {
		job, err := ts.ClaimNextSeedingJob(ctx, owner, now, store.DefaultSeedingLease)
		if err != nil {
			t.Fatal(err)
		}
		if job == nil {
			t.Fatal("expected claimable round-2 job")
		}
		if job.RoundNumber != 2 || job.Source != domain.SeedingSourceAdvancement {
			t.Fatalf("job=%+v", job)
		}
		done, err := ts.ProcessSeedingChunk(ctx, *job, owner, now)
		if err != nil {
			t.Fatalf("chunk: %v", err)
		}
		if done {
			completed = true
			break
		}
	}
	if !completed {
		t.Fatal("seeding round 2 did not complete")
	}

	var r2status string
	_ = pool.QueryRow(ctx, `SELECT status FROM tournament_rounds WHERE tournament_id=$1 AND round_number=2`, "t-seed-r2").Scan(&r2status)
	if r2status != "provisioning" {
		t.Fatalf("round 2 status=%s want provisioning", r2status)
	}
	var batchN, shardN int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM provisioning_batches WHERE tournament_id=$1 AND round_number=2`, "t-seed-r2").Scan(&batchN)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM round_progress_shards WHERE tournament_id=$1 AND round_number=2`, "t-seed-r2").Scan(&shardN)
	if batchN < 1 {
		t.Fatalf("expected provisioning batches, got %d", batchN)
	}
	if shardN != domain.ProgressShardCount {
		t.Fatalf("shards=%d want %d", shardN, domain.ProgressShardCount)
	}

	// First seeded slot players follow player_id ASC (not slot-order of source).
	var firstSlotPlayers []string
	_ = pool.QueryRow(ctx, `
		SELECT seeded_player_ids FROM bracket_slots
		WHERE tournament_id=$1 AND round_number=2 AND slot_id='slot_0'
	`, "t-seed-r2").Scan(&firstSlotPlayers)
	plan, err := domain.ComputeRoundSlotPlan(wantAdv)
	if err != nil {
		t.Fatal(err)
	}
	wantFirst := ordered[:plan.SizeForSlot(0)]
	if len(firstSlotPlayers) != len(wantFirst) {
		t.Fatalf("slot_0 len=%d want %d", len(firstSlotPlayers), len(wantFirst))
	}
	for i := range wantFirst {
		if firstSlotPlayers[i] != wantFirst[i] {
			t.Fatalf("slot_0[%d]=%s want %s (player_id ASC)", i, firstSlotPlayers[i], wantFirst[i])
		}
	}

	claimed, err := ts.ClaimNextProvisioningBatch(ctx, "prov-r2", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if claimed == nil || claimed.RoundNumber != 2 || claimed.TournamentID != "t-seed-r2" {
		t.Fatalf("provisioning claim=%+v", claimed)
	}
}

func TestIntegration_SeedRound_ManualRecoveryLaterRound(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	_, wantAdv := prepareCompleteRoundReady(t, pool, ts, "t-seed-recover", 12)
	d := completeRoundDifferential(t, ts, "t-seed-recover", 1, domain.CompleteRoundCommandID("t-seed-recover", 1))
	if d.Kind != domain.CompleteRoundSuccess {
		t.Fatalf("complete=%+v", d)
	}
	// Simulate missing job after completed round (ops recovery via SeedRound).
	if _, err := pool.Exec(ctx, `DELETE FROM round_seeding_batches WHERE tournament_id=$1 AND round_number=2`, "t-seed-recover"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM round_seeding_jobs WHERE tournament_id=$1 AND round_number=2`, "t-seed-recover"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM tournament_rounds WHERE tournament_id=$1 AND round_number=2`, "t-seed-recover"); err != nil {
		t.Fatal(err)
	}

	cmdID := domain.SeedRoundCommandID(domain.TournamentID("t-seed-recover"), 2)
	uow, err := ts.BeginSeedRound(ctx, "t-seed-recover", 2, cmdID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = uow.Rollback() }()
	kick := uow.KickoffContext()
	if kick.SourcePlayerCount != wantAdv {
		t.Fatalf("source count=%d want %d", kick.SourcePlayerCount, wantAdv)
	}
	decision := domain.DecideSeedRoundKickoff(kick, domain.SeedRoundCommand{
		CommandID: domain.CommandID(cmdID), RoundNumber: 2,
	})
	if decision.Kind != domain.SeedKickoffSchedule {
		t.Fatalf("decision=%+v", decision)
	}
	if err := uow.Commit(store.SeedingCommitRequest{
		TournamentID: "t-seed-recover", CommandID: cmdID, CommandType: "SeedRound",
		Decision: decision,
		Outcome:  envelope.Accepted(cmdID, "SeedRound", nil, nil),
	}); err != nil {
		t.Fatal(err)
	}
	job, ok, err := ts.LoadSeedingJob(ctx, "t-seed-recover", 2)
	if err != nil || !ok || job.Status != "pending" || job.Source != domain.SeedingSourceAdvancement {
		t.Fatalf("recovered job ok=%v job=%+v err=%v", ok, job, err)
	}
}
