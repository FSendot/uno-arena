//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/services/tournament-orchestration/store"
	"unoarena/shared/envelope"
)

func TestIntegration_LeaseVersionFence_StaleSameOwnerFails(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)
	players := make([]string, 8)
	for i := range players {
		players[i] = "fence" + itoa(i)
	}
	seedProvisionedBatches(t, ts, "fence-t1", players, 4)

	now := time.Now().UTC()
	first, err := ts.ClaimNextProvisioningBatch(ctx, "worker-same", now, time.Minute)
	if err != nil || first == nil {
		t.Fatalf("claim1: %v %+v", err, first)
	}
	if first.LeaseVersion < 1 {
		t.Fatalf("lease_version=%d", first.LeaseVersion)
	}
	oldVersion := first.LeaseVersion

	if _, err := pool.Exec(ctx, `
		UPDATE provisioning_batches SET lease_expires_at = $1
		WHERE tournament_id = $2 AND batch_id = $3
	`, now.Add(-time.Second), first.TournamentID, first.BatchID); err != nil {
		t.Fatal(err)
	}
	second, err := ts.ClaimNextProvisioningBatch(ctx, "worker-same", now, time.Minute)
	if err != nil || second == nil {
		t.Fatalf("claim2: %v %+v", err, second)
	}
	if second.BatchID != first.BatchID {
		t.Fatalf("expected same batch reclaim got %s", second.BatchID)
	}
	if second.LeaseVersion <= oldVersion {
		t.Fatalf("lease_version did not bump: old=%d new=%d", oldVersion, second.LeaseVersion)
	}

	cmdID := "process:" + first.TournamentID + ":1:" + first.BatchID + ":0"
	_, err = ts.PrepareProvisioningBatch(ctx, store.PrepareProvisioningBatchInput{
		CommandID: cmdID, TournamentID: first.TournamentID, RoundNumber: first.RoundNumber,
		BatchID: first.BatchID, SlotFrom: first.SlotFrom, SlotTo: first.SlotTo,
		SlotSize: first.SlotSize, RetryAttempt: first.RetryAttempt,
		LeaseOwner: "worker-same", LeaseVersion: oldVersion,
	})
	if !errors.Is(err, store.ErrProvisioningFence) {
		t.Fatalf("stale version want fence got %v", err)
	}
	var outcomeCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM command_idempotency WHERE command_id = $1`, cmdID).Scan(&outcomeCount); err != nil {
		t.Fatal(err)
	}
	if outcomeCount != 0 {
		t.Fatal("stale prepare must not poison command_idempotency")
	}

	prepared, err := ts.PrepareProvisioningBatch(ctx, store.PrepareProvisioningBatchInput{
		CommandID: cmdID, TournamentID: second.TournamentID, RoundNumber: second.RoundNumber,
		BatchID: second.BatchID, SlotFrom: second.SlotFrom, SlotTo: second.SlotTo,
		SlotSize: second.SlotSize, RetryAttempt: second.RetryAttempt,
		LeaseOwner: second.LeaseOwner, LeaseVersion: second.LeaseVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	if prepared == nil || prepared.Rejected || prepared.Quarantined || len(prepared.Slots) == 0 {
		t.Fatalf("fenced claimant prepare failed: %+v", prepared)
	}
}

func TestIntegration_KickoffReplay_MoreThanTenBatchesNumericOrder(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)
	tid := "kickoff-11batches"
	const slotCount = 1001
	const batchSize = 100 // → 11 batches; lexical batch_10 < batch_2 would drift

	now := time.Now().UTC()
	rules, _ := json.Marshal(map[string]any{"retryBudget": 3, "batchSize": batchSize, "currentRound": 1})
	if _, err := pool.Exec(ctx, `
		INSERT INTO tournaments (tournament_id, phase, capacity, registered_count, rules, created_at, updated_at)
		VALUES ($1, 'seeding', $2, $2, $3::jsonb, $4, $4)
	`, tid, slotCount, rules, now); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO bracket_projection_versions (tournament_id, projection_version, generated_at)
		VALUES ($1, 0, $2)
	`, tid, now); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO tournament_rounds (tournament_id, round_number, status, is_final, seeded_at)
		VALUES ($1, 1, 'seeded', false, $2)
	`, tid, now); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO bracket_slots (tournament_id, round_number, slot_id, slot_index, status, seeded_player_ids, created_at, updated_at)
		SELECT $1, 1, 'slot_' || g.i::text, g.i, 'pending', ARRAY['p'||g.i::text], $2, $2
		FROM generate_series(0, $3::int - 1) AS g(i)
	`, tid, now, slotCount); err != nil {
		t.Fatal(err)
	}

	uow1, err := ts.BeginProvisionRound(ctx, tid, 1, "prov-1")
	if err != nil {
		t.Fatal(err)
	}
	d1 := domain.DecideProvisionKickoff(uow1.KickoffContext(), domain.ProvisionRoundMatchesCommand{
		CommandID: "prov-1", RoundNumber: 1,
	})
	if d1.Kind != domain.ProvisionKickoffSchedule {
		_ = uow1.Rollback()
		t.Fatalf("schedule1: %+v ctx=%+v", d1, uow1.KickoffContext())
	}
	if d1.Plan.BatchCount < 11 {
		_ = uow1.Rollback()
		t.Fatalf("want >=11 batches got %d", d1.Plan.BatchCount)
	}
	if err := uow1.Commit(store.ProvisioningCommitRequest{
		TournamentID: tid, CommandID: "prov-1", CommandType: "ProvisionRoundMatches",
		Outcome:  envelope.Accepted("prov-1", "ProvisionRoundMatches", nil, json.RawMessage(`{"facts":[]}`)),
		Decision: d1, RoundNumber: 1,
	}); err != nil {
		t.Fatal(err)
	}

	uow2, err := ts.BeginProvisionRound(ctx, tid, 1, "prov-2")
	if err != nil {
		t.Fatal(err)
	}
	d2 := domain.DecideProvisionKickoff(uow2.KickoffContext(), domain.ProvisionRoundMatchesCommand{
		CommandID: "prov-2", RoundNumber: 1,
	})
	if d2.Kind != domain.ProvisionKickoffAlreadyDone {
		_ = uow2.Rollback()
		t.Fatalf("second kickoff must be already-done no-op, got %+v fp=%q", d2, uow2.KickoffContext().ExistingBatchPlanFingerprint)
	}
	if err := uow2.Commit(store.ProvisioningCommitRequest{
		TournamentID: tid, CommandID: "prov-2", CommandType: "ProvisionRoundMatches",
		Outcome:  envelope.Accepted("prov-2", "ProvisionRoundMatches", nil, json.RawMessage(`{"facts":[]}`)),
		Decision: d2, RoundNumber: 1,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestIntegration_PrepareConflict_QuarantinesWithoutRawSecret(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)
	players := make([]string, 4)
	for i := range players {
		players[i] = "qsec" + itoa(i)
	}
	seedProvisionedBatches(t, ts, "q-secret", players, 4)
	now := time.Now().UTC()
	claimed, err := ts.ClaimNextProvisioningBatch(ctx, "worker-q", now, time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("claim: %v", err)
	}
	slotID := claimed.SlotFrom
	if _, err := pool.Exec(ctx, `
		INSERT INTO assigned_matches (tournament_id, round_number, slot_id, room_id, assigned_at, provisioning_batch_id)
		VALUES ($1, $2, $3, 'wrong-room-SECRET', $4, 'other-batch')
		ON CONFLICT (tournament_id, round_number, slot_id) DO UPDATE
		SET room_id = 'wrong-room-SECRET', provisioning_batch_id = 'other-batch'
	`, claimed.TournamentID, claimed.RoundNumber, slotID, now); err != nil {
		t.Fatal(err)
	}
	cmdID := "process:" + claimed.TournamentID + ":1:" + claimed.BatchID + ":0"
	res, err := ts.PrepareProvisioningBatch(ctx, store.PrepareProvisioningBatchInput{
		CommandID: cmdID, TournamentID: claimed.TournamentID, RoundNumber: claimed.RoundNumber,
		BatchID: claimed.BatchID, SlotFrom: claimed.SlotFrom, SlotTo: claimed.SlotTo,
		SlotSize: claimed.SlotSize, RetryAttempt: claimed.RetryAttempt,
		LeaseOwner: claimed.LeaseOwner, LeaseVersion: claimed.LeaseVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res == nil || !res.Quarantined {
		t.Fatalf("want quarantined got %+v", res)
	}
	var lastErr, qReason string
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(last_error,''), COALESCE(quarantine_reason,'')
		FROM provisioning_batches WHERE tournament_id=$1 AND batch_id=$2
	`, claimed.TournamentID, claimed.BatchID).Scan(&lastErr, &qReason); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(lastErr, "SECRET") || strings.Contains(qReason, "SECRET") ||
		strings.Contains(lastErr, "{") || strings.Contains(qReason, "{") {
		t.Fatalf("secret/raw leaked: last=%q q=%q", lastErr, qReason)
	}
}

func TestIntegration_Heartbeat_WrongRetryAttemptCannotRenew(t *testing.T) {
	ctx := context.Background()
	_, ts := openStore(t)
	players := make([]string, 4)
	for i := range players {
		players[i] = "hb" + itoa(i)
	}
	seedProvisionedBatches(t, ts, "hb-retry", players, 4)
	now := time.Now().UTC()
	claimed, err := ts.ClaimNextProvisioningBatch(ctx, "worker-hb", now, time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("claim: %v %+v", err, claimed)
	}
	ok, err := ts.HeartbeatProvisioningLease(ctx, claimed.TournamentID, claimed.RoundNumber, claimed.BatchID,
		claimed.LeaseOwner, claimed.LeaseVersion, claimed.RetryAttempt, now, time.Minute)
	if err != nil || !ok {
		t.Fatalf("matching fence must renew: ok=%v err=%v", ok, err)
	}
	wrongRetry := claimed.RetryAttempt + 1
	ok, err = ts.HeartbeatProvisioningLease(ctx, claimed.TournamentID, claimed.RoundNumber, claimed.BatchID,
		claimed.LeaseOwner, claimed.LeaseVersion, wrongRetry, now, time.Minute)
	if err != nil {
		t.Fatalf("wrong retry must return ok=false not err: %v", err)
	}
	if ok {
		t.Fatal("same owner/version but wrong retryAttempt must not renew")
	}
}

func TestIntegration_StaleTerminal_PrepareFinalizeNoCommandPoison(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)
	players := make([]string, 4)
	for i := range players {
		players[i] = "term" + itoa(i)
	}
	seedProvisionedBatches(t, ts, "stale-term", players, 4)
	now := time.Now().UTC()
	claimed, err := ts.ClaimNextProvisioningBatch(ctx, "worker-a", now, time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("claim: %v %+v", err, claimed)
	}
	owner, version, retry := claimed.LeaseOwner, claimed.LeaseVersion, claimed.RetryAttempt
	winnerCmd := "process:" + claimed.TournamentID + ":1:" + claimed.BatchID + ":a0"
	prepared, err := ts.PrepareProvisioningBatch(ctx, store.PrepareProvisioningBatchInput{
		CommandID: winnerCmd, TournamentID: claimed.TournamentID, RoundNumber: claimed.RoundNumber,
		BatchID: claimed.BatchID, SlotFrom: claimed.SlotFrom, SlotTo: claimed.SlotTo,
		SlotSize: claimed.SlotSize, RetryAttempt: retry,
		LeaseOwner: owner, LeaseVersion: version,
	})
	if err != nil || prepared == nil || len(prepared.Slots) == 0 {
		t.Fatalf("prepare: %v %+v", err, prepared)
	}
	if _, err := ts.FinalizeProvisioningBatchSuccess(ctx, store.FinalizeProvisioningBatchInput{
		CommandID: winnerCmd, TournamentID: claimed.TournamentID, RoundNumber: claimed.RoundNumber,
		BatchID: claimed.BatchID, SlotFrom: claimed.SlotFrom, SlotTo: claimed.SlotTo,
		SlotSize: claimed.SlotSize, RetryAttempt: retry,
		LeaseOwner: owner, LeaseVersion: version,
	}); err != nil {
		t.Fatalf("finalize success: %v", err)
	}

	staleCmd := "process:" + claimed.TournamentID + ":1:" + claimed.BatchID + ":stale"
	staleIn := store.PrepareProvisioningBatchInput{
		CommandID: staleCmd, TournamentID: claimed.TournamentID, RoundNumber: claimed.RoundNumber,
		BatchID: claimed.BatchID, SlotFrom: claimed.SlotFrom, SlotTo: claimed.SlotTo,
		SlotSize: claimed.SlotSize, RetryAttempt: retry,
		LeaseOwner: owner, LeaseVersion: version,
	}
	if _, err := ts.PrepareProvisioningBatch(ctx, staleIn); !errors.Is(err, store.ErrProvisioningFence) {
		t.Fatalf("stale prepare on completed want fence got %v", err)
	}
	var outcomeCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM command_idempotency WHERE command_id = $1`, staleCmd).Scan(&outcomeCount); err != nil {
		t.Fatal(err)
	}
	if outcomeCount != 0 {
		t.Fatal("stale prepare must not poison command_idempotency")
	}

	finIn := store.FinalizeProvisioningBatchInput{
		CommandID: staleCmd, TournamentID: claimed.TournamentID, RoundNumber: claimed.RoundNumber,
		BatchID: claimed.BatchID, SlotFrom: claimed.SlotFrom, SlotTo: claimed.SlotTo,
		SlotSize: claimed.SlotSize, RetryAttempt: retry,
		LeaseOwner: owner, LeaseVersion: version,
	}
	if _, err := ts.FinalizeProvisioningBatchSuccess(ctx, finIn); !errors.Is(err, store.ErrProvisioningFence) {
		t.Fatalf("stale finalize success on completed want fence got %v", err)
	}
	if _, err := ts.FinalizeProvisioningBatchFailure(ctx, finIn); !errors.Is(err, store.ErrProvisioningFence) {
		t.Fatalf("stale finalize failure on completed want fence got %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM command_idempotency WHERE command_id = $1`, staleCmd).Scan(&outcomeCount); err != nil {
		t.Fatal(err)
	}
	if outcomeCount != 0 {
		t.Fatal("stale finalize must not poison command_idempotency")
	}

	// Genuine idempotency: prior outcome for the winner command still replays.
	preparedAgain, err := ts.PrepareProvisioningBatch(ctx, store.PrepareProvisioningBatchInput{
		CommandID: winnerCmd, TournamentID: claimed.TournamentID, RoundNumber: claimed.RoundNumber,
		BatchID: claimed.BatchID, SlotFrom: claimed.SlotFrom, SlotTo: claimed.SlotTo,
		SlotSize: claimed.SlotSize, RetryAttempt: retry,
		LeaseOwner: owner, LeaseVersion: version,
	})
	if err != nil {
		t.Fatal(err)
	}
	if preparedAgain == nil || preparedAgain.PriorOutcome == nil {
		t.Fatalf("winner prepare must replay prior outcome: %+v", preparedAgain)
	}
}

func TestIntegration_StaleQuarantined_FinalizeFailureNoCommandPoison(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)
	players := make([]string, 4)
	for i := range players {
		players[i] = "qterm" + itoa(i)
	}
	seedProvisionedBatches(t, ts, "stale-q", players, 4)
	now := time.Now().UTC()
	claimed, err := ts.ClaimNextProvisioningBatch(ctx, "worker-q", now, time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE provisioning_batches
		SET status = 'quarantined', lease_owner = NULL, lease_expires_at = NULL, quarantine_reason = 'test'
		WHERE tournament_id = $1 AND batch_id = $2
	`, claimed.TournamentID, claimed.BatchID); err != nil {
		t.Fatal(err)
	}
	staleCmd := "process:" + claimed.TournamentID + ":1:" + claimed.BatchID + ":q-stale"
	_, err = ts.PrepareProvisioningBatch(ctx, store.PrepareProvisioningBatchInput{
		CommandID: staleCmd, TournamentID: claimed.TournamentID, RoundNumber: claimed.RoundNumber,
		BatchID: claimed.BatchID, SlotFrom: claimed.SlotFrom, SlotTo: claimed.SlotTo,
		SlotSize: claimed.SlotSize, RetryAttempt: claimed.RetryAttempt,
		LeaseOwner: claimed.LeaseOwner, LeaseVersion: claimed.LeaseVersion,
	})
	if !errors.Is(err, store.ErrProvisioningFence) {
		t.Fatalf("prepare on quarantined want fence got %v", err)
	}
	_, err = ts.FinalizeProvisioningBatchFailure(ctx, store.FinalizeProvisioningBatchInput{
		CommandID: staleCmd, TournamentID: claimed.TournamentID, RoundNumber: claimed.RoundNumber,
		BatchID: claimed.BatchID, SlotFrom: claimed.SlotFrom, SlotTo: claimed.SlotTo,
		SlotSize: claimed.SlotSize, RetryAttempt: claimed.RetryAttempt,
		LeaseOwner: claimed.LeaseOwner, LeaseVersion: claimed.LeaseVersion,
		LastError: "stale",
	})
	if !errors.Is(err, store.ErrProvisioningFence) {
		t.Fatalf("finalize failure on quarantined want fence got %v", err)
	}
	var outcomeCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM command_idempotency WHERE command_id = $1`, staleCmd).Scan(&outcomeCount); err != nil {
		t.Fatal(err)
	}
	if outcomeCount != 0 {
		t.Fatal("quarantined shortcut must not poison command_idempotency")
	}
}
