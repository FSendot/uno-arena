//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"testing"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/shared/envelope"
)

func TestIntegration_ManualRetry_StrictSequencingAndProjection(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)
	players := make([]string, 16)
	for i := range players {
		players[i] = "retry" + itoa(i)
	}
	const tid = "retry-seq"
	seedProvisionedBatches(t, ts, tid, players, 2)

	var batchID string
	var curAttempt int
	if err := pool.QueryRow(ctx, `
		SELECT batch_id, retry_attempt FROM provisioning_batches
		WHERE tournament_id = $1 AND round_number = 1
		ORDER BY batch_id LIMIT 1
	`, tid).Scan(&batchID, &curAttempt); err != nil {
		t.Fatal(err)
	}
	var projBefore int64
	_ = pool.QueryRow(ctx, `
		SELECT COALESCE((SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1), 0)
	`, tid).Scan(&projBefore)

	// Auto-next (retryAttempt=0) applies current+1.
	res, err := ts.ManualRetryProvisioningBatch(ctx, "retry-auto", tid, 1, batchID, 0, "corr")
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != envelope.StatusAccepted {
		t.Fatalf("auto retry: %+v", res)
	}
	assertFactNames(t, res, string(domain.FactTournamentProvisioningBatchRetried))
	var attemptAfter int
	_ = pool.QueryRow(ctx, `
		SELECT retry_attempt FROM provisioning_batches
		WHERE tournament_id=$1 AND round_number=1 AND batch_id=$2
	`, tid, batchID).Scan(&attemptAfter)
	if attemptAfter != curAttempt+1 {
		t.Fatalf("attempt after auto=%d want %d", attemptAfter, curAttempt+1)
	}
	var projAfterRetry int64
	_ = pool.QueryRow(ctx, `
		SELECT COALESCE((SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1), 0)
	`, tid).Scan(&projAfterRetry)
	if projAfterRetry != projBefore {
		t.Fatalf("retry must not bump projection: before=%d after=%d", projBefore, projAfterRetry)
	}

	// Exact no-op: status=retried + explicit == current, different commandId.
	noop, err := ts.ManualRetryProvisioningBatch(ctx, "retry-noop", tid, 1, batchID, attemptAfter, "corr")
	if err != nil {
		t.Fatal(err)
	}
	if noop.Status != envelope.StatusAccepted {
		t.Fatalf("exact no-op: %+v", noop)
	}
	assertEmptyFacts(t, noop)
	_ = pool.QueryRow(ctx, `
		SELECT COALESCE((SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1), 0)
	`, tid).Scan(&projAfterRetry)
	if projAfterRetry != projBefore {
		t.Fatalf("exact no-op must not bump: before=%d after=%d", projBefore, projAfterRetry)
	}

	// Same commandId returns stored canonical outcome.
	replay, err := ts.ManualRetryProvisioningBatch(ctx, "retry-auto", tid, 1, batchID, 99, "corr")
	if err != nil {
		t.Fatal(err)
	}
	if !outcomesEqual(res, replay) {
		t.Fatalf("same commandId must return prior\nfirst=%s\nreplay=%s", res.Payload, replay.Payload)
	}

	// Skip attempt rejects.
	skip, err := ts.ManualRetryProvisioningBatch(ctx, "retry-skip", tid, 1, batchID, attemptAfter+2, "corr")
	if err != nil {
		t.Fatal(err)
	}
	if skip.Status != envelope.StatusRejected || skip.Reason != string(domain.RejectInvalidCommand) {
		t.Fatalf("skip want invalid_command got %+v", skip)
	}

	// Apply next sequential attempt so we can assert a lower attempt rejects.
	nextOK := attemptAfter + 1
	seq, err := ts.ManualRetryProvisioningBatch(ctx, "retry-seq-2", tid, 1, batchID, nextOK, "corr")
	if err != nil {
		t.Fatal(err)
	}
	if seq.Status != envelope.StatusAccepted {
		t.Fatalf("sequential retry: %+v", seq)
	}
	_ = pool.QueryRow(ctx, `
		SELECT retry_attempt FROM provisioning_batches
		WHERE tournament_id=$1 AND round_number=1 AND batch_id=$2
	`, tid, batchID).Scan(&attemptAfter)

	lower, err := ts.ManualRetryProvisioningBatch(ctx, "retry-lower", tid, 1, batchID, attemptAfter-1, "corr")
	if err != nil {
		t.Fatal(err)
	}
	if lower.Status != envelope.StatusRejected || lower.Reason != string(domain.RejectInvalidCommand) {
		t.Fatalf("lower want invalid_command got %+v", lower)
	}

	// Advance to budget exhaustion quarantine.
	budget := domain.DefaultRetryBudget
	for attemptAfter < budget {
		next := attemptAfter + 1
		cmd := "retry-next-" + itoa(next)
		r, err := ts.ManualRetryProvisioningBatch(ctx, cmd, tid, 1, batchID, next, "corr")
		if err != nil {
			t.Fatal(err)
		}
		if r.Status != envelope.StatusAccepted {
			t.Fatalf("retry %d: %+v", next, r)
		}
		_ = pool.QueryRow(ctx, `
			SELECT retry_attempt FROM provisioning_batches
			WHERE tournament_id=$1 AND round_number=1 AND batch_id=$2
		`, tid, batchID).Scan(&attemptAfter)
	}
	q, err := ts.ManualRetryProvisioningBatch(ctx, "retry-budget", tid, 1, batchID, attemptAfter+1, "corr")
	if err != nil {
		t.Fatal(err)
	}
	if q.Status != envelope.StatusAccepted {
		t.Fatalf("budget quarantine: %+v", q)
	}
	assertFactNames(t, q, string(domain.FactTournamentProvisioningBatchQuarantined))
	var roundStatus, batchStatus, reason string
	_ = pool.QueryRow(ctx, `
		SELECT r.status, b.status, COALESCE(b.quarantine_reason,'')
		FROM tournament_rounds r
		JOIN provisioning_batches b ON b.tournament_id=r.tournament_id AND b.round_number=r.round_number
		WHERE r.tournament_id=$1 AND r.round_number=1 AND b.batch_id=$2
	`, tid, batchID).Scan(&roundStatus, &batchStatus, &reason)
	if roundStatus != string(domain.RoundBlocked) || batchStatus != string(domain.BatchQuarantined) {
		t.Fatalf("round=%s batch=%s", roundStatus, batchStatus)
	}
	if reason != "retry_budget_exhausted" {
		t.Fatalf("reason=%q want retry_budget_exhausted", reason)
	}
	var projAfterQ int64
	_ = pool.QueryRow(ctx, `
		SELECT COALESCE((SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1), 0)
	`, tid).Scan(&projAfterQ)
	if projAfterQ != projBefore+1 {
		t.Fatalf("first quarantine→blocked must bump once: before=%d after=%d", projBefore, projAfterQ)
	}

	// Already quarantined: factless no-op, no further bump.
	again, err := ts.ManualRetryProvisioningBatch(ctx, "retry-already-q", tid, 1, batchID, attemptAfter+1, "corr")
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != envelope.StatusAccepted {
		t.Fatalf("already quarantined: %+v", again)
	}
	assertEmptyFacts(t, again)
	_ = pool.QueryRow(ctx, `
		SELECT COALESCE((SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1), 0)
	`, tid).Scan(&projAfterQ)
	if projAfterQ != projBefore+1 {
		t.Fatalf("already-quarantined retry must not bump again: %d", projAfterQ)
	}
}

func TestIntegration_ManualQuarantine_BumpOnlyOnRoundBlockedTransition(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)
	players := make([]string, 16)
	for i := range players {
		players[i] = "qvis" + itoa(i)
	}
	const tid = "q-vis"
	seedProvisionedBatches(t, ts, tid, players, 1)

	var batches []string
	rows, err := pool.Query(ctx, `
		SELECT batch_id FROM provisioning_batches
		WHERE tournament_id=$1 AND round_number=1 ORDER BY batch_id
	`, tid)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		batches = append(batches, id)
	}
	rows.Close()
	if len(batches) < 2 {
		t.Fatalf("need >=2 batches, got %d", len(batches))
	}

	var projBefore int64
	_ = pool.QueryRow(ctx, `
		SELECT COALESCE((SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1), 0)
	`, tid).Scan(&projBefore)

	first, err := ts.ManualQuarantineProvisioningBatch(ctx, "q-first", tid, 1, batches[0], "operator secret leak http 500 body", "corr")
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != envelope.StatusAccepted {
		t.Fatalf("first quarantine: %+v", first)
	}
	assertFactNames(t, first, string(domain.FactTournamentProvisioningBatchQuarantined))
	var reason string
	_ = pool.QueryRow(ctx, `
		SELECT quarantine_reason FROM provisioning_batches
		WHERE tournament_id=$1 AND batch_id=$2
	`, tid, batches[0]).Scan(&reason)
	if reason != "quarantined" && reason != "room_provision_http_5xx" {
		// "operator..." sanitizes to quarantined; "http 5" substring may map to 5xx depending on text.
		if reason != "quarantined" {
			t.Fatalf("sanitized reason=%q", reason)
		}
	}
	var projMid int64
	_ = pool.QueryRow(ctx, `
		SELECT COALESCE((SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1), 0)
	`, tid).Scan(&projMid)
	if projMid != projBefore+1 {
		t.Fatalf("first quarantine→blocked must bump: before=%d after=%d", projBefore, projMid)
	}

	// Second batch quarantine while round already blocked: no projection bump.
	second, err := ts.ManualQuarantineProvisioningBatch(ctx, "q-second", tid, 1, batches[1], "another operator note", "corr")
	if err != nil {
		t.Fatal(err)
	}
	if second.Status != envelope.StatusAccepted {
		t.Fatalf("second quarantine: %+v", second)
	}
	assertFactNames(t, second, string(domain.FactTournamentProvisioningBatchQuarantined))
	var projAfter int64
	_ = pool.QueryRow(ctx, `
		SELECT COALESCE((SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1), 0)
	`, tid).Scan(&projAfter)
	if projAfter != projMid {
		t.Fatalf("already-blocked quarantine must not bump: mid=%d after=%d", projMid, projAfter)
	}

	// Exact already-quarantined no-op.
	dup, err := ts.ManualQuarantineProvisioningBatch(ctx, "q-dup", tid, 1, batches[0], "again", "corr")
	if err != nil {
		t.Fatal(err)
	}
	assertEmptyFacts(t, dup)
}

func TestIntegration_ManualRetry_BudgetQuarantineBlocksInProgressRound(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)
	players := make([]string, 16)
	for i := range players {
		players[i] = "rinp" + itoa(i)
	}
	const tid = "retry-inprog"
	seedProvisionedBatches(t, ts, tid, players, 1)

	var batchID string
	var attempt int
	if err := pool.QueryRow(ctx, `
		SELECT batch_id, retry_attempt FROM provisioning_batches
		WHERE tournament_id = $1 AND round_number = 1
		ORDER BY batch_id LIMIT 1
	`, tid).Scan(&batchID, &attempt); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE tournament_rounds SET status = 'in_progress'
		WHERE tournament_id = $1 AND round_number = 1
	`, tid); err != nil {
		t.Fatal(err)
	}

	var projBefore int64
	_ = pool.QueryRow(ctx, `
		SELECT COALESCE((SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1), 0)
	`, tid).Scan(&projBefore)

	budget := domain.DefaultRetryBudget
	for attempt < budget {
		next := attempt + 1
		r, err := ts.ManualRetryProvisioningBatch(ctx, "rinp-"+itoa(next), tid, 1, batchID, next, "corr")
		if err != nil {
			t.Fatal(err)
		}
		if r.Status != envelope.StatusAccepted {
			t.Fatalf("retry %d: %+v", next, r)
		}
		_ = pool.QueryRow(ctx, `
			SELECT retry_attempt FROM provisioning_batches
			WHERE tournament_id=$1 AND round_number=1 AND batch_id=$2
		`, tid, batchID).Scan(&attempt)
	}

	q, err := ts.ManualRetryProvisioningBatch(ctx, "rinp-budget", tid, 1, batchID, attempt+1, "corr")
	if err != nil {
		t.Fatal(err)
	}
	if q.Status != envelope.StatusAccepted {
		t.Fatalf("budget quarantine: %+v", q)
	}
	assertFactNames(t, q, string(domain.FactTournamentProvisioningBatchQuarantined))

	var roundStatus string
	_ = pool.QueryRow(ctx, `
		SELECT status FROM tournament_rounds WHERE tournament_id=$1 AND round_number=1
	`, tid).Scan(&roundStatus)
	if roundStatus != string(domain.RoundBlocked) {
		t.Fatalf("in_progress round must become blocked, got %s", roundStatus)
	}
	var projAfter int64
	_ = pool.QueryRow(ctx, `
		SELECT COALESCE((SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1), 0)
	`, tid).Scan(&projAfter)
	if projAfter != projBefore+1 {
		t.Fatalf("in_progress→blocked must bump once: before=%d after=%d", projBefore, projAfter)
	}

	// Already blocked: quarantine another batch must not bump again.
	var otherBatch string
	if err := pool.QueryRow(ctx, `
		SELECT batch_id FROM provisioning_batches
		WHERE tournament_id=$1 AND round_number=1 AND batch_id<>$2
		ORDER BY batch_id LIMIT 1
	`, tid, batchID).Scan(&otherBatch); err != nil {
		t.Fatal(err)
	}
	already, err := ts.ManualQuarantineProvisioningBatch(ctx, "rinp-already-blocked", tid, 1, otherBatch, "operator note", "corr")
	if err != nil {
		t.Fatal(err)
	}
	if already.Status != envelope.StatusAccepted {
		t.Fatalf("already-blocked quarantine: %+v", already)
	}
	assertFactNames(t, already, string(domain.FactTournamentProvisioningBatchQuarantined))
	_ = pool.QueryRow(ctx, `
		SELECT COALESCE((SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1), 0)
	`, tid).Scan(&projAfter)
	if projAfter != projBefore+1 {
		t.Fatalf("already-blocked must not bump again: want %d got %d", projBefore+1, projAfter)
	}
}

func assertFactNames(t *testing.T, res envelope.Result, names ...string) {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(res.Payload, &payload); err != nil {
		t.Fatalf("payload: %v", err)
	}
	facts, _ := payload["facts"].([]any)
	got := map[string]bool{}
	for _, f := range facts {
		m, _ := f.(map[string]any)
		name, _ := m["name"].(string)
		got[name] = true
		data, _ := m["data"].(map[string]any)
		if data["tournamentId"] == nil || data["tournamentId"] == "" {
			t.Fatalf("fact %s missing tournamentId: %+v", name, data)
		}
		if data["batchId"] == nil || data["batchId"] == "" {
			t.Fatalf("fact %s missing batchId: %+v", name, data)
		}
	}
	for _, n := range names {
		if !got[n] {
			t.Fatalf("missing fact %s in %s", n, res.Payload)
		}
	}
}

func assertEmptyFacts(t *testing.T, res envelope.Result) {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(res.Payload, &payload); err != nil {
		t.Fatalf("payload: %v", err)
	}
	facts, _ := payload["facts"].([]any)
	if len(facts) != 0 {
		t.Fatalf("want empty facts, got %s", res.Payload)
	}
}
