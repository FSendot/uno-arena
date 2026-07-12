//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/services/tournament-orchestration/store"
	"unoarena/shared/envelope"
)

func seedProvisionedBatches(t *testing.T, ts *store.TournamentStore, tid string, players []string, batchSize int) {
	t.Helper()
	ctx := context.Background()
	cmdID := "prov-" + tid
	tr, _ := domain.CreateTournament(domain.CreateTournamentCommand{
		CommandID: domain.CommandID("c-" + tid), TournamentID: domain.TournamentID(tid), Capacity: len(players), BatchSize: batchSize,
	})
	for _, p := range players {
		_ = tr.RegisterPlayer(domain.RegisterPlayerCommand{
			CommandID: domain.CommandID("r-" + tid + "-" + p), PlayerID: domain.PlayerID(p),
		})
	}
	_ = tr.CloseRegistration(domain.CloseRegistrationCommand{CommandID: domain.CommandID("close-" + tid)})
	_ = tr.SeedRound(domain.SeedRoundCommand{CommandID: domain.CommandID("seed-" + tid), RoundNumber: 1})
	_ = tr.ProvisionRoundMatches(domain.ProvisionRoundMatchesCommand{CommandID: domain.CommandID(cmdID), RoundNumber: 1})
	if err := ts.Commit(ctx, store.CommitRequest{
		Tournament: tr, CommandID: cmdID,
		Outcome:           envelope.Accepted(cmdID, "ProvisionRoundMatches", nil, json.RawMessage(`{}`)),
		ProjectionChanged: true,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestIntegration_ClaimNextProvisioningBatch_SkipLockedAndReap(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)
	players := make([]string, 40)
	for i := range players {
		players[i] = "p" + itoa(i)
	}
	seedProvisionedBatches(t, ts, "claim-t1", players, 2)

	now := time.Now().UTC()
	first, err := ts.ClaimNextProvisioningBatch(ctx, "worker-1", now, time.Minute)
	if err != nil || first == nil {
		t.Fatalf("first claim: %v %+v", err, first)
	}
	if first.LeaseOwner != "worker-1" {
		t.Fatalf("owner=%q", first.LeaseOwner)
	}
	if first.SlotSize <= 0 || first.SlotSize > domain.MaxProvisioningBatchSize {
		t.Fatalf("slotSize=%d", first.SlotSize)
	}

	second, err := ts.ClaimNextProvisioningBatch(ctx, "worker-2", now, time.Minute)
	if err != nil || second == nil {
		t.Fatalf("second claim: %v %+v", err, second)
	}
	if second.BatchID == first.BatchID {
		t.Fatal("SKIP LOCKED failed: claimed same batch")
	}

	if _, err := pool.Exec(ctx, `
		UPDATE provisioning_batches
		SET status = 'completed', lease_owner = NULL, lease_expires_at = NULL
		WHERE tournament_id = $1 AND batch_id = $2
	`, first.TournamentID, first.BatchID); err != nil {
		t.Fatal(err)
	}
	for {
		c, err := ts.ClaimNextProvisioningBatch(ctx, "drain", now, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if c == nil {
			break
		}
		if c.BatchID == first.BatchID {
			t.Fatal("completed batch was claimable")
		}
		if _, err := pool.Exec(ctx, `
			UPDATE provisioning_batches
			SET status = 'completed', lease_owner = NULL, lease_expires_at = NULL
			WHERE tournament_id = $1 AND round_number = $2 AND batch_id = $3
		`, c.TournamentID, c.RoundNumber, c.BatchID); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO provisioning_batches (
			tournament_id, round_number, batch_id, shard_key, status, retry_attempt,
			slot_id_from, slot_id_to, lease_owner, lease_expires_at, created_at, updated_at
		) VALUES (
			$1, 1, 'batch_reap', 'batch_reap', 'in_progress', 0,
			'slot_0', 'slot_0', 'dead-worker', $2, $3, $3
		)
		ON CONFLICT (tournament_id, round_number, batch_id) DO UPDATE
		SET status = 'in_progress', retry_attempt = 0,
		    lease_owner = 'dead-worker', lease_expires_at = EXCLUDED.lease_expires_at,
		    slot_id_from = 'slot_0', slot_id_to = 'slot_0', updated_at = EXCLUDED.updated_at
	`, first.TournamentID, now.Add(-time.Minute), now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	n, err := ts.ReapExpiredProvisioningLeases(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if n < 1 {
		t.Fatalf("reaped=%d", n)
	}
	reclaimed, err := ts.ClaimNextProvisioningBatch(ctx, "worker-reap", now, time.Minute)
	if err != nil || reclaimed == nil {
		t.Fatalf("reclaim after reap: %v %+v", err, reclaimed)
	}
	if reclaimed.BatchID != "batch_reap" {
		t.Fatalf("batch=%s", reclaimed.BatchID)
	}

	for _, status := range []string{"quarantined", "cancelled"} {
		bid := "batch_" + status
		var qReason any
		if status == "quarantined" {
			qReason = "test"
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO provisioning_batches (
				tournament_id, round_number, batch_id, shard_key, status, retry_attempt,
				slot_id_from, slot_id_to, quarantine_reason, created_at, updated_at
			) VALUES ($1, 1, $2, $2, $3, 0, 'slot_0', 'slot_0', $4, now(), now())
			ON CONFLICT DO NOTHING
		`, first.TournamentID, bid, status, qReason); err != nil {
			t.Fatal(err)
		}
	}
	c, err := ts.ClaimNextProvisioningBatch(ctx, "worker-term", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if c != nil && (c.BatchID == "batch_quarantined" || c.BatchID == "batch_cancelled") {
		t.Fatalf("terminal claimed: %+v", c)
	}
}

func TestIntegration_ClaimNextProvisioningBatch_ConcurrentExclusive(t *testing.T) {
	ctx := context.Background()
	_, ts := openStore(t)
	players := make([]string, 100)
	for i := range players {
		players[i] = "cp" + itoa(i)
	}
	seedProvisionedBatches(t, ts, "claim-race", players, 1)

	now := time.Now().UTC()
	var mu sync.Mutex
	seen := map[string]string{}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		owner := "w" + itoa(i)
		go func(owner string) {
			defer wg.Done()
			for {
				c, err := ts.ClaimNextProvisioningBatch(ctx, owner, now, time.Minute)
				if err != nil {
					t.Errorf("claim: %v", err)
					return
				}
				if c == nil {
					return
				}
				mu.Lock()
				if prev, ok := seen[c.BatchID]; ok {
					t.Errorf("batch %s claimed by %s and %s", c.BatchID, prev, owner)
				}
				seen[c.BatchID] = owner
				mu.Unlock()
			}
		}(owner)
	}
	wg.Wait()
	if len(seen) == 0 {
		t.Fatal("expected claims")
	}
}

func TestIntegration_PersistPreservesSiblingInProgressLease(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)
	players := make([]string, 40)
	for i := range players {
		players[i] = "lp" + itoa(i)
	}
	seedProvisionedBatches(t, ts, "lease-preserve", players, 2)

	now := time.Now().UTC()
	first, err := ts.ClaimNextProvisioningBatch(ctx, "worker-a", now, time.Minute)
	if err != nil || first == nil {
		t.Fatalf("first: %v %+v", err, first)
	}
	second, err := ts.ClaimNextProvisioningBatch(ctx, "worker-b", now, time.Minute)
	if err != nil || second == nil {
		t.Fatalf("second: %v %+v", err, second)
	}
	if first.BatchID == second.BatchID {
		t.Fatal("expected distinct batches")
	}

	uow, err := ts.BeginExisting(ctx, domain.TournamentID(first.TournamentID))
	if err != nil {
		t.Fatal(err)
	}
	tr := uow.Loaded()
	out := tr.CompleteTournamentProvisioningBatch(domain.CompleteTournamentProvisioningBatchCommand{
		CommandID: "complete-a", RoundNumber: first.RoundNumber, BatchID: domain.BatchID(first.BatchID),
	})
	if out.Kind != domain.OutcomeAccepted {
		_ = uow.Rollback()
		t.Fatalf("complete: %+v", out)
	}
	if err := uow.Commit(store.CommitRequest{
		Tournament: tr, CommandID: "complete-a",
		Outcome:           envelope.Accepted("complete-a", "CompleteTournamentProvisioningBatch", nil, json.RawMessage(`{}`)),
		ProjectionChanged: len(out.Facts) > 0,
	}); err != nil {
		t.Fatal(err)
	}

	var (
		aOwner, bOwner   *string
		aExp, bExp       *time.Time
		aStatus, bStatus string
	)
	if err := pool.QueryRow(ctx, `
		SELECT status, lease_owner, lease_expires_at FROM provisioning_batches
		WHERE tournament_id = $1 AND batch_id = $2
	`, first.TournamentID, first.BatchID).Scan(&aStatus, &aOwner, &aExp); err != nil {
		t.Fatal(err)
	}
	if aStatus != "completed" || aOwner != nil || aExp != nil {
		t.Fatalf("completed batch lease must clear: status=%s owner=%v exp=%v", aStatus, aOwner, aExp)
	}
	if err := pool.QueryRow(ctx, `
		SELECT status, lease_owner, lease_expires_at FROM provisioning_batches
		WHERE tournament_id = $1 AND batch_id = $2
	`, second.TournamentID, second.BatchID).Scan(&bStatus, &bOwner, &bExp); err != nil {
		t.Fatal(err)
	}
	if bStatus != "in_progress" || bOwner == nil || *bOwner != "worker-b" || bExp == nil {
		t.Fatalf("sibling lease erased: status=%s owner=%v exp=%v", bStatus, bOwner, bExp)
	}

	// Expired sibling lease remains reclaimable after rewrite.
	if _, err := pool.Exec(ctx, `
		UPDATE provisioning_batches
		SET lease_expires_at = $1
		WHERE tournament_id = $2 AND batch_id = $3
	`, now.Add(-time.Second), second.TournamentID, second.BatchID); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := ts.ClaimNextProvisioningBatch(ctx, "worker-reclaim", now, time.Minute)
	if err != nil || reclaimed == nil {
		t.Fatalf("reclaim: %v %+v", err, reclaimed)
	}
	if reclaimed.BatchID != second.BatchID || reclaimed.LeaseOwner != "worker-reclaim" {
		t.Fatalf("reclaim mismatch: %+v", reclaimed)
	}
}

func TestIntegration_ClaimNext_SkipsTerminalTournamentAndNonProvisioningRound(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)
	players := make([]string, 20)
	for i := range players {
		players[i] = "term" + itoa(i)
	}
	seedProvisionedBatches(t, ts, "term-skip", players, 2)

	if _, err := pool.Exec(ctx, `
		UPDATE tournaments SET phase = 'cancelled' WHERE tournament_id = 'term-skip'
	`); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	c, err := ts.ClaimNextProvisioningBatch(ctx, "worker-term", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if c != nil {
		t.Fatalf("cancelled tournament must not claim: %+v", c)
	}

	seedProvisionedBatches(t, ts, "round-skip", players, 2)
	if _, err := pool.Exec(ctx, `
		UPDATE tournament_rounds SET status = 'in_progress'
		WHERE tournament_id = 'round-skip' AND round_number = 1
	`); err != nil {
		t.Fatal(err)
	}
	c, err = ts.ClaimNextProvisioningBatch(ctx, "worker-round", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if c != nil {
		t.Fatalf("non-provisioning round must not claim: %+v", c)
	}
}

func TestIntegration_ReaperBoundedAndSkipsLockedTournament(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)
	players := make([]string, 20)
	for i := range players {
		players[i] = "rp" + itoa(i)
	}
	seedProvisionedBatches(t, ts, "reap-bound", players, 1)
	now := time.Now().UTC()
	expired := now.Add(-time.Minute)

	rows, err := pool.Query(ctx, `
		SELECT batch_id FROM provisioning_batches WHERE tournament_id = 'reap-bound' ORDER BY batch_id
	`)
	if err != nil {
		t.Fatal(err)
	}
	var batchIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		batchIDs = append(batchIDs, id)
	}
	rows.Close()
	if len(batchIDs) < 2 {
		t.Fatalf("need >=2 batches, got %d", len(batchIDs))
	}
	for _, id := range batchIDs {
		if _, err := pool.Exec(ctx, `
			UPDATE provisioning_batches
			SET status = 'in_progress', lease_owner = 'dead', lease_expires_at = $1
			WHERE tournament_id = 'reap-bound' AND batch_id = $2
		`, expired, id); err != nil {
			t.Fatal(err)
		}
	}

	uow, err := ts.BeginExisting(ctx, "reap-bound")
	if err != nil {
		t.Fatal(err)
	}
	n, err := ts.ReapExpiredProvisioningLeasesBounded(ctx, now, 1)
	if err != nil {
		_ = uow.Rollback()
		t.Fatal(err)
	}
	if n != 0 {
		_ = uow.Rollback()
		t.Fatalf("reaper raced locked tournament: reaped=%d", n)
	}
	_ = uow.Rollback()

	n, err = ts.ReapExpiredProvisioningLeasesBounded(ctx, now, 1)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("bounded reap want 1 got %d", n)
	}
	var remaining int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM provisioning_batches
		WHERE tournament_id = 'reap-bound' AND status = 'in_progress' AND lease_owner IS NOT NULL
	`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != len(batchIDs)-1 {
		t.Fatalf("remaining leased=%d want %d", remaining, len(batchIDs)-1)
	}
}
