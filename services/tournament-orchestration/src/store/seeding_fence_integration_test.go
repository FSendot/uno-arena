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

func TestIntegration_SeedingLeaseVersion_IncrementsOnReclaimSameOwner(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)
	tid := "seed-fence-bump"
	players := make([]string, 8)
	for i := range players {
		players[i] = fmt.Sprintf("sfb%02d", i)
	}
	prepareSeedingTournament(t, ts, tid, players)
	seedRound1Kickoff(t, ts, tid, "seed-"+tid)

	now := time.Now().UTC()
	owner := "worker-same"
	first, err := ts.ClaimNextSeedingJob(ctx, owner, now, store.DefaultSeedingLease)
	if err != nil || first == nil || first.TournamentID != tid {
		t.Fatalf("claim1: %v %+v", err, first)
	}
	if first.LeaseVersion < 1 {
		t.Fatalf("lease_version=%d", first.LeaseVersion)
	}
	old := first.LeaseVersion

	if _, err := pool.Exec(ctx, `
		UPDATE round_seeding_jobs SET lease_expires_at = $1
		WHERE tournament_id = $2 AND round_number = 1
	`, now.Add(-time.Second), tid); err != nil {
		t.Fatal(err)
	}
	second, err := ts.ClaimNextSeedingJob(ctx, owner, now, store.DefaultSeedingLease)
	if err != nil || second == nil || second.TournamentID != tid {
		t.Fatalf("claim2: %v %+v", err, second)
	}
	if second.LeaseVersion <= old {
		t.Fatalf("lease_version did not bump: old=%d new=%d", old, second.LeaseVersion)
	}
	var stored int64
	if err := pool.QueryRow(ctx, `
		SELECT lease_version FROM round_seeding_jobs WHERE tournament_id=$1 AND round_number=1
	`, tid).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != second.LeaseVersion {
		t.Fatalf("stored version=%d claimed=%d", stored, second.LeaseVersion)
	}
}

func TestIntegration_SeedingLeaseFence_StaleSameOwnerCannotMutate(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)
	tid := "seed-fence-stale"
	players := make([]string, 8)
	for i := range players {
		players[i] = fmt.Sprintf("sfs%02d", i)
	}
	prepareSeedingTournament(t, ts, tid, players)
	seedRound1Kickoff(t, ts, tid, "seed-"+tid)

	now := time.Now().UTC()
	owner := "worker-same"
	stale, err := ts.ClaimNextSeedingJob(ctx, owner, now, store.DefaultSeedingLease)
	if err != nil || stale == nil {
		t.Fatalf("claim1: %v %+v", err, stale)
	}
	staleVersion := stale.LeaseVersion

	if _, err := pool.Exec(ctx, `
		UPDATE round_seeding_jobs SET lease_expires_at = $1
		WHERE tournament_id = $2 AND round_number = 1
	`, now.Add(-time.Second), tid); err != nil {
		t.Fatal(err)
	}
	current, err := ts.ClaimNextSeedingJob(ctx, owner, now, store.DefaultSeedingLease)
	if err != nil || current == nil {
		t.Fatalf("claim2: %v %+v", err, current)
	}
	if current.LeaseVersion <= staleVersion {
		t.Fatalf("reclaim must bump version: stale=%d current=%d", staleVersion, current.LeaseVersion)
	}

	var slotsBefore, batchesBefore int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM bracket_slots WHERE tournament_id=$1 AND round_number=1`, tid).Scan(&slotsBefore)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM round_seeding_batches WHERE tournament_id=$1 AND round_number=1`, tid).Scan(&batchesBefore)
	var nextBefore, processedBefore int
	var lastBefore, statusBefore, qBefore string
	_ = pool.QueryRow(ctx, `
		SELECT next_slot_index, processed_player_count, last_player_id, status, COALESCE(quarantine_reason,'')
		FROM round_seeding_jobs WHERE tournament_id=$1 AND round_number=1
	`, tid).Scan(&nextBefore, &processedBefore, &lastBefore, &statusBefore, &qBefore)

	done, err := ts.ProcessSeedingChunk(ctx, *stale, owner, now)
	if err != nil {
		t.Fatalf("stale chunk must be no-op err=nil, got %v", err)
	}
	if done {
		t.Fatal("stale chunk must not finalize")
	}

	var slotsAfter, batchesAfter int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM bracket_slots WHERE tournament_id=$1 AND round_number=1`, tid).Scan(&slotsAfter)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM round_seeding_batches WHERE tournament_id=$1 AND round_number=1`, tid).Scan(&batchesAfter)
	if slotsAfter != slotsBefore || batchesAfter != batchesBefore {
		t.Fatalf("stale must not insert slots/batches: slots %d→%d batches %d→%d",
			slotsBefore, slotsAfter, batchesBefore, batchesAfter)
	}
	var nextAfter, processedAfter int
	var lastAfter, statusAfter, qAfter string
	var versionAfter int64
	_ = pool.QueryRow(ctx, `
		SELECT next_slot_index, processed_player_count, last_player_id, status,
			COALESCE(quarantine_reason,''), lease_version
		FROM round_seeding_jobs WHERE tournament_id=$1 AND round_number=1
	`, tid).Scan(&nextAfter, &processedAfter, &lastAfter, &statusAfter, &qAfter, &versionAfter)
	if nextAfter != nextBefore || processedAfter != processedBefore || lastAfter != lastBefore {
		t.Fatal("stale must not advance checkpoint")
	}
	if statusAfter != statusBefore || qAfter != qBefore {
		t.Fatalf("stale must not quarantine/cancel: status %s→%s q %q→%q", statusBefore, statusAfter, qBefore, qAfter)
	}
	if versionAfter != current.LeaseVersion {
		t.Fatalf("current lease_version mutated: %d", versionAfter)
	}

	// Force quarantine attempt via stale claim after poisoning phase — still must not stick.
	if _, err := pool.Exec(ctx, `UPDATE tournaments SET phase='registration' WHERE tournament_id=$1`, tid); err != nil {
		t.Fatal(err)
	}
	if _, err := ts.ProcessSeedingChunk(ctx, *stale, owner, now); err != nil {
		t.Fatalf("stale quarantine attempt must no-op: %v", err)
	}
	_ = pool.QueryRow(ctx, `
		SELECT status, COALESCE(quarantine_reason,''), lease_version
		FROM round_seeding_jobs WHERE tournament_id=$1 AND round_number=1
	`, tid).Scan(&statusAfter, &qAfter, &versionAfter)
	if statusAfter == string(domain.SeedingJobQuarantined) || statusAfter == string(domain.SeedingJobCancelled) {
		t.Fatalf("stale must not quarantine/cancel current generation: %s q=%q", statusAfter, qAfter)
	}
	if versionAfter != current.LeaseVersion {
		t.Fatalf("version drifted: %d", versionAfter)
	}

	// Restore phase; current generation must still complete.
	if _, err := pool.Exec(ctx, `UPDATE tournaments SET phase='seeding' WHERE tournament_id=$1`, tid); err != nil {
		t.Fatal(err)
	}
	// `current` still holds the lease; drive that generation to completion.
	job := current
	for i := 0; i < 50; i++ {
		if job == nil {
			var err error
			job, err = ts.ClaimNextSeedingJob(ctx, owner, now, store.DefaultSeedingLease)
			if err != nil {
				t.Fatal(err)
			}
			if job == nil {
				loaded, ok, err := ts.LoadSeedingJob(ctx, tid, 1)
				if err != nil || !ok {
					t.Fatalf("load: %v ok=%v", err, ok)
				}
				if loaded.Status == string(domain.SeedingJobCompleted) {
					return
				}
				continue
			}
		}
		if job.TournamentID != tid {
			job = nil
			continue
		}
		done, err := ts.ProcessSeedingChunk(ctx, *job, owner, now)
		if err != nil {
			t.Fatal(err)
		}
		job = nil
		if done {
			loaded, ok, err := ts.LoadSeedingJob(ctx, tid, 1)
			if err != nil || !ok || loaded.Status != string(domain.SeedingJobCompleted) {
				t.Fatalf("want completed got ok=%v %+v", ok, loaded)
			}
			return
		}
	}
	t.Fatal("current generation did not complete")
}

func TestIntegration_SeedingQuarantine_UnknownReasonOnly(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)
	tid := "seed-q-unknown"
	players := make([]string, 4)
	for i := range players {
		players[i] = fmt.Sprintf("squ%02d", i)
	}
	prepareSeedingTournament(t, ts, tid, players)
	seedRound1Kickoff(t, ts, tid, "seed-"+tid)

	now := time.Now().UTC()
	job, err := ts.ClaimNextSeedingJob(ctx, "owner-q", now, time.Minute)
	if err != nil || job == nil {
		t.Fatalf("claim: %v %+v", err, job)
	}
	// Conflicting slot forces immutable_slot_conflict — allowlisted.
	if _, err := pool.Exec(ctx, `
		INSERT INTO bracket_slots (
			tournament_id, round_number, slot_id, slot_index, status, seeded_player_ids, created_at, updated_at
		) VALUES ($1, 1, 'slot_0', 0, 'pending', ARRAY['other'], $2, $2)
	`, tid, now); err != nil {
		t.Fatal(err)
	}
	_, err = ts.ProcessSeedingChunk(ctx, *job, "owner-q", now)
	if err == nil {
		t.Fatal("expected quarantine error")
	}
	var reason, status string
	if err := pool.QueryRow(ctx, `
		SELECT status, quarantine_reason FROM round_seeding_jobs
		WHERE tournament_id=$1 AND round_number=1
	`, tid).Scan(&status, &reason); err != nil {
		t.Fatal(err)
	}
	if status != string(domain.SeedingJobQuarantined) {
		t.Fatalf("status=%s", status)
	}
	if reason != "immutable_slot_conflict" {
		t.Fatalf("reason=%q", reason)
	}

	// phase_drift is an allowlisted operational code (unit test covers unknown→unknown).
	if _, err := pool.Exec(ctx, `
		UPDATE round_seeding_jobs
		SET status='in_progress', quarantine_reason=NULL, lease_owner='owner-q',
		    lease_expires_at=$2, lease_version=lease_version+1, completed_at=NULL
		WHERE tournament_id=$1 AND round_number=1
	`, tid, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	loaded, ok, err := ts.LoadSeedingJob(ctx, tid, 1)
	if err != nil || !ok {
		t.Fatalf("load: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE tournaments SET phase='registration' WHERE tournament_id=$1`, tid); err != nil {
		t.Fatal(err)
	}
	_, _ = ts.ProcessSeedingChunk(ctx, *loaded, "owner-q", now)
	if err := pool.QueryRow(ctx, `
		SELECT status, COALESCE(quarantine_reason,'') FROM round_seeding_jobs
		WHERE tournament_id=$1 AND round_number=1
	`, tid).Scan(&status, &reason); err != nil {
		t.Fatal(err)
	}
	if status != string(domain.SeedingJobQuarantined) || reason != "phase_drift" {
		t.Fatalf("want phase_drift quarantine got status=%s reason=%q", status, reason)
	}
}

func TestIntegration_CompleteRound_WrongStatusOrCommandIDRollsBack(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()

	for _, tc := range []struct {
		name   string
		status string
		cmdID  string
	}{
		{name: "wrong-status", status: "in_progress", cmdID: ""},
		{name: "wrong-command", status: "pending", cmdID: "seed:wrong-cmd"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tid := "t-cr-id-" + tc.name
			_, wantAdv := prepareCompleteRoundReady(t, pool, ts, tid, 12)
			_ = wantAdv
			uowPeek, err := ts.BeginCompleteRound(ctx, tid, 1, "peek-"+tc.name)
			if err != nil {
				t.Fatal(err)
			}
			dPeek := domain.DecideCompleteRound(uowPeek.Loaded(), domain.CompleteRoundCommand{
				CommandID: domain.CommandID("peek-" + tc.name), RoundNumber: 1,
			})
			_ = uowPeek.Rollback()
			if dPeek.NextRound == nil {
				t.Fatalf("need next round plan: %+v", dPeek)
			}
			wantCmd := dPeek.NextRound.JobCommandID
			cmd := tc.cmdID
			if cmd == "" {
				cmd = wantCmd
			}
			if tc.name == "wrong-command" && cmd == wantCmd {
				cmd = wantCmd + "-other"
			}

			if _, err := pool.Exec(ctx, `
				INSERT INTO tournament_rounds (tournament_id, round_number, status, is_final)
				VALUES ($1, 2, 'pending', false)
			`, tid); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, `
				INSERT INTO round_seeding_jobs (
					tournament_id, round_number, source, source_round_number, status,
					player_count, slot_count, base_size, remainder,
					command_id, created_at, updated_at
				) VALUES (
					$1, 2, 'advancement', 1, $2,
					$3, $4, $5, $6,
					$7, now(), now()
				)
			`, tid, tc.status,
				dPeek.NextRound.PlayerCount, dPeek.NextRound.SlotCount,
				dPeek.NextRound.BaseSize, dPeek.NextRound.Remainder,
				cmd); err != nil {
				t.Fatal(err)
			}

			uow, err := ts.BeginCompleteRound(ctx, tid, 1, "cr-"+tc.name)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = uow.Rollback() }()
			d := domain.DecideCompleteRound(uow.Loaded(), domain.CompleteRoundCommand{
				CommandID: domain.CommandID("cr-" + tc.name), RoundNumber: 1,
			})
			if d.Kind != domain.CompleteRoundSuccess || d.NextRound == nil {
				t.Fatalf("decision=%+v", d)
			}
			err = uow.Commit(store.CompleteRoundCommitRequest{
				TournamentID: tid, CommandID: "cr-" + tc.name, CommandType: "CompleteRound",
				Outcome: envelope.Accepted("cr-"+tc.name, "CompleteRound", nil, json.RawMessage(`{"facts":[]}`)), Decision: d,
			})
			if err == nil {
				t.Fatal("expected identity conflict to fail commit")
			}
			var status string
			_ = pool.QueryRow(ctx, `SELECT status FROM tournament_rounds WHERE tournament_id=$1 AND round_number=1`, tid).Scan(&status)
			if status == "completed" {
				t.Fatal("identity conflict must roll back completion")
			}
			var outcomeN int
			_ = pool.QueryRow(ctx, `SELECT count(*) FROM command_idempotency WHERE command_id=$1`, "cr-"+tc.name).Scan(&outcomeN)
			if outcomeN != 0 {
				t.Fatalf("rolled-back commit must not persist outcome, got %d", outcomeN)
			}
		})
	}
}
