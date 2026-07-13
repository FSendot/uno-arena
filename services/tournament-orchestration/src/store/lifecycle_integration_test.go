//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/services/tournament-orchestration/store"
	"unoarena/shared/envelope"
)

func prepareFinalCompletedTournament(t *testing.T, pool *store.Pool, ts *store.TournamentStore, tid string, n int) []domain.PlayerID {
	t.Helper()
	if n < 1 || n > domain.FinalPlayerThreshold {
		t.Fatalf("final tournament needs 1..%d players, got %d", domain.FinalPlayerThreshold, n)
	}
	tr, _ := prepareCompleteRoundReady(t, pool, ts, tid, n)
	round, ok := tr.Round(1)
	if !ok || !round.IsFinal {
		t.Fatal("expected final round 1")
	}
	// Model a pre-autonomous-policy / recovery state: the final round is durable
	// and complete while tournament lifecycle still needs CompleteTournament.
	if _, err := pool.Exec(context.Background(), `
		UPDATE tournament_rounds
		SET status='completed', completed_at=now()
		WHERE tournament_id=$1 AND round_number=1 AND status='in_progress'
	`, tid); err != nil {
		t.Fatal(err)
	}
	got, ok := ts.Get(context.Background(), domain.TournamentID(tid))
	if !ok {
		t.Fatal("missing")
	}
	r, _ := got.Round(1)
	if !r.Completed || !r.IsFinal {
		t.Fatalf("round completed=%v final=%v", r.Completed, r.IsFinal)
	}
	return append([]domain.PlayerID(nil), r.Slots[0].Advancing...)
}

func commitCompleteTournament(t *testing.T, ts *store.TournamentStore, tid, cmdID string, d domain.CompleteTournamentDecision) envelope.Result {
	t.Helper()
	ctx := context.Background()
	uow, err := ts.BeginCompleteTournament(ctx, tid, cmdID)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = uow.Rollback() }()
	if prior, ok := uow.LookupOutcome(cmdID); ok {
		return prior
	}
	if d.Kind == domain.CompleteTournamentKind("") {
		d = domain.DecideCompleteTournament(uow.CompleteContext(), domain.CompleteTournamentCommand{CommandID: domain.CommandID(cmdID)})
	}
	factsPayload := make([]map[string]any, 0, len(d.Outcome.Facts))
	for _, f := range d.Outcome.Facts {
		factsPayload = append(factsPayload, map[string]any{"name": string(f.Name), "data": f.Data})
	}
	var events []store.OutboxEvent
	if d.Kind == domain.CompleteTournamentSuccess {
		now := time.Now().UTC()
		for i, f := range d.Outcome.Facts {
			events = append(events, store.OutboxEvent{
				EventID: fmt.Sprintf("%s:%s:%d", cmdID, f.Name, i), EventType: string(f.Name),
				TournamentID: tid, Topic: "tournament.completed", PartitionKey: tid, SchemaVersion: 1,
				Payload: map[string]any{
					"schemaVersion": 1, "eventType": string(f.Name),
					"tournamentId": tid, "finalStandings": splitCSVTest(f.Data["finalStandings"]),
				},
				CreatedAt: now,
			})
		}
	}
	out := envelope.Accepted(cmdID, "CompleteTournament", nil, mustJSON(map[string]any{"facts": factsPayload}))
	if d.Outcome.Rejected() {
		out = envelope.Rejected(cmdID, "CompleteTournament", string(d.Outcome.Rejection.Code), nil)
		events = nil
	}
	if err := uow.Commit(store.LifecycleCommitRequest{
		TournamentID: tid, CommandID: cmdID, CommandType: "CompleteTournament",
		Outcome: out, Events: events, Complete: d,
	}); err != nil {
		if prior, ok := store.AsPriorCommandOutcome(err); ok {
			return prior
		}
		t.Fatalf("commit: %v", err)
	}
	return out
}

func commitCancelTournament(t *testing.T, ts *store.TournamentStore, tid, cmdID string, d domain.CancelTournamentDecision) envelope.Result {
	t.Helper()
	ctx := context.Background()
	uow, err := ts.BeginCancelTournament(ctx, tid, cmdID)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = uow.Rollback() }()
	if prior, ok := uow.LookupOutcome(cmdID); ok {
		return prior
	}
	if d.Kind == domain.CancelTournamentKind("") {
		d = domain.DecideCancelTournament(uow.CancelContext(), domain.CancelTournamentCommand{CommandID: domain.CommandID(cmdID)})
	}
	factsPayload := make([]map[string]any, 0, len(d.Outcome.Facts))
	for _, f := range d.Outcome.Facts {
		factsPayload = append(factsPayload, map[string]any{"name": string(f.Name), "data": f.Data})
	}
	out := envelope.Accepted(cmdID, "CancelTournament", nil, mustJSON(map[string]any{"facts": factsPayload}))
	if d.Outcome.Rejected() {
		out = envelope.Rejected(cmdID, "CancelTournament", string(d.Outcome.Rejection.Code), nil)
	}
	if err := uow.Commit(store.LifecycleCommitRequest{
		TournamentID: tid, CommandID: cmdID, CommandType: "CancelTournament",
		Outcome: out, Cancel: d,
	}); err != nil {
		if prior, ok := store.AsPriorCommandOutcome(err); ok {
			return prior
		}
		t.Fatalf("commit: %v", err)
	}
	return out
}

func mustJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func splitCSVTest(s string) []string {
	if s == "" {
		return nil
	}
	parts := make([]string, 0)
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			p := s[start:i]
			if p != "" {
				parts = append(parts, p)
			}
			start = i + 1
		}
	}
	return parts
}

func TestIntegration_CompleteTournament_SuccessPayloadOrderNoChampionId(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	standings := prepareFinalCompletedTournament(t, pool, ts, "t-ct-ok", 4)
	var projBefore int64
	_ = pool.QueryRow(ctx, `SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1`, "t-ct-ok").Scan(&projBefore)

	out := commitCompleteTournament(t, ts, "t-ct-ok", "ct-ok", domain.CompleteTournamentDecision{})
	if out.Status != envelope.StatusAccepted {
		t.Fatalf("out=%+v", out)
	}

	var phase string
	var completedAt *time.Time
	var champ string
	if err := pool.QueryRow(ctx, `
		SELECT phase, completed_at, rules->>'championId' FROM tournaments WHERE tournament_id=$1
	`, "t-ct-ok").Scan(&phase, &completedAt, &champ); err != nil {
		t.Fatal(err)
	}
	if phase != "completed" || completedAt == nil {
		t.Fatalf("phase=%s completed_at=%v", phase, completedAt)
	}
	if champ != string(standings[0]) {
		t.Fatalf("rules.championId=%s want %s", champ, standings[0])
	}

	var payload []byte
	var eventType string
	if err := pool.QueryRow(ctx, `
		SELECT event_type, payload FROM outbox_events
		WHERE tournament_id=$1 AND event_type='TournamentCompleted'
	`, "t-ct-ok").Scan(&eventType, &payload); err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(payload, &body); err != nil {
		t.Fatal(err)
	}
	if _, has := body["championId"]; has {
		t.Fatalf("championId must be absent: %s", payload)
	}
	rawStandings, ok := body["finalStandings"].([]any)
	if !ok || len(rawStandings) != len(standings) {
		t.Fatalf("finalStandings=%v want %v", body["finalStandings"], standings)
	}
	for i, p := range standings {
		if rawStandings[i] != string(p) {
			t.Fatalf("order[%d]=%v want %s", i, rawStandings[i], p)
		}
	}

	var projAfter int64
	_ = pool.QueryRow(ctx, `SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1`, "t-ct-ok").Scan(&projAfter)
	if projAfter != projBefore+1 {
		t.Fatalf("projection bump want +1: %d -> %d", projBefore, projAfter)
	}
}

func TestIntegration_CompleteTournament_Rejections(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()

	t.Run("cancelled", func(t *testing.T) {
		_ = prepareFinalCompletedTournament(t, pool, ts, "t-ct-can", 4)
		_, _ = pool.Exec(ctx, `UPDATE tournaments SET phase='cancelled' WHERE tournament_id=$1`, "t-ct-can")
		out := commitCompleteTournament(t, ts, "t-ct-can", "ct-can", domain.CompleteTournamentDecision{})
		if out.Status != envelope.StatusRejected || out.Reason != string(domain.RejectAlreadyTerminal) {
			t.Fatalf("got %+v", out)
		}
	})

	t.Run("not_final", func(t *testing.T) {
		prepareCompleteRoundReady(t, pool, ts, "t-ct-nf", 12)
		completeRoundDifferential(t, ts, "t-ct-nf", 1, "cr-nf")
		out := commitCompleteTournament(t, ts, "t-ct-nf", "ct-nf", domain.CompleteTournamentDecision{})
		if out.Status != envelope.StatusRejected || out.Reason != string(domain.RejectNotFinal) {
			t.Fatalf("got %+v", out)
		}
	})

	t.Run("round_incomplete", func(t *testing.T) {
		tr, _ := provisionedTournament(t, ts, "t-ct-ri", playersN(4))
		_ = tr
		markRoundMatchingReady(t, pool, "t-ct-ri", 1)
		out := commitCompleteTournament(t, ts, "t-ct-ri", "ct-ri", domain.CompleteTournamentDecision{})
		if out.Status != envelope.StatusRejected || out.Reason != string(domain.RejectRoundIncomplete) {
			t.Fatalf("got %+v", out)
		}
	})

	t.Run("invalid_standings", func(t *testing.T) {
		_ = prepareFinalCompletedTournament(t, pool, ts, "t-ct-bad", 4)
		_, err := pool.Exec(ctx, `
			UPDATE advancement_records
			SET advancing_player_ids = ARRAY['p0','p0','p1']
			WHERE tournament_id=$1 AND round_number=1
		`, "t-ct-bad")
		if err != nil {
			t.Fatal(err)
		}
		var phaseBefore string
		_ = pool.QueryRow(ctx, `SELECT phase FROM tournaments WHERE tournament_id=$1`, "t-ct-bad").Scan(&phaseBefore)
		out := commitCompleteTournament(t, ts, "t-ct-bad", "ct-bad", domain.CompleteTournamentDecision{})
		if out.Status != envelope.StatusRejected || out.Reason != string(domain.RejectRoundIncomplete) {
			t.Fatalf("got %+v", out)
		}
		var phaseAfter string
		_ = pool.QueryRow(ctx, `SELECT phase FROM tournaments WHERE tournament_id=$1`, "t-ct-bad").Scan(&phaseAfter)
		if phaseAfter != phaseBefore || phaseAfter == "completed" {
			t.Fatalf("invalid standings must not mutate phase: %s -> %s", phaseBefore, phaseAfter)
		}
	})

	t.Run("multiple_final_slots", func(t *testing.T) {
		_ = prepareFinalCompletedTournament(t, pool, ts, "t-ct-multi", 4)
		now := time.Now().UTC()
		_, err := pool.Exec(ctx, `
			INSERT INTO bracket_slots (
				tournament_id, round_number, slot_id, slot_index, status, seeded_player_ids, created_at, updated_at
			) VALUES ($1, 1, 'slot_corrupt_1', 1, 'advanced', ARRAY['x0','x1'], $2, $2)
		`, "t-ct-multi", now)
		if err != nil {
			t.Fatal(err)
		}
		assertCompleteRejectsWithoutMutation(t, pool, ts, "t-ct-multi", "ct-multi")
	})

	t.Run("nonzero_only_final_slot", func(t *testing.T) {
		_ = prepareFinalCompletedTournament(t, pool, ts, "t-ct-nz", 4)
		_, err := pool.Exec(ctx, `
			UPDATE bracket_slots SET slot_index = 1
			WHERE tournament_id=$1 AND round_number=1
		`, "t-ct-nz")
		if err != nil {
			t.Fatal(err)
		}
		assertCompleteRejectsWithoutMutation(t, pool, ts, "t-ct-nz", "ct-nz")
	})

	t.Run("missing_advancement", func(t *testing.T) {
		_ = prepareFinalCompletedTournament(t, pool, ts, "t-ct-miss-adv", 4)
		_, err := pool.Exec(ctx, `
			DELETE FROM advancement_records WHERE tournament_id=$1 AND round_number=1
		`, "t-ct-miss-adv")
		if err != nil {
			t.Fatal(err)
		}
		assertCompleteRejectsWithoutMutation(t, pool, ts, "t-ct-miss-adv", "ct-miss-adv")
	})

	t.Run("quarantined_match_result", func(t *testing.T) {
		_ = prepareFinalCompletedTournament(t, pool, ts, "t-ct-quar", 4)
		_, err := pool.Exec(ctx, `
			UPDATE match_results
			SET disposition = 'quarantined', quarantine_reason = 'test_corrupt'
			WHERE tournament_id=$1 AND round_number=1 AND disposition='recorded'
		`, "t-ct-quar")
		if err != nil {
			t.Fatal(err)
		}
		assertCompleteRejectsWithoutMutation(t, pool, ts, "t-ct-quar", "ct-quar")
	})
}

func assertCompleteRejectsWithoutMutation(t *testing.T, pool *store.Pool, ts *store.TournamentStore, tid, cmdID string) {
	t.Helper()
	ctx := context.Background()
	var phaseBefore string
	var projBefore, outboxBefore int64
	_ = pool.QueryRow(ctx, `SELECT phase FROM tournaments WHERE tournament_id=$1`, tid).Scan(&phaseBefore)
	_ = pool.QueryRow(ctx, `SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1`, tid).Scan(&projBefore)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE tournament_id=$1 AND event_type='TournamentCompleted'`, tid).Scan(&outboxBefore)

	out := commitCompleteTournament(t, ts, tid, cmdID, domain.CompleteTournamentDecision{})
	if out.Status != envelope.StatusRejected || out.Reason != string(domain.RejectRoundIncomplete) {
		t.Fatalf("got %+v", out)
	}
	var phaseAfter string
	var projAfter, outboxAfter int64
	_ = pool.QueryRow(ctx, `SELECT phase FROM tournaments WHERE tournament_id=$1`, tid).Scan(&phaseAfter)
	_ = pool.QueryRow(ctx, `SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1`, tid).Scan(&projAfter)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE tournament_id=$1 AND event_type='TournamentCompleted'`, tid).Scan(&outboxAfter)
	if phaseAfter != phaseBefore || phaseAfter == "completed" {
		t.Fatalf("must not mutate phase: %s -> %s", phaseBefore, phaseAfter)
	}
	if projAfter != projBefore {
		t.Fatalf("must not bump projection: %d -> %d", projBefore, projAfter)
	}
	if outboxAfter != outboxBefore {
		t.Fatalf("must not write TournamentCompleted outbox: %d -> %d", outboxBefore, outboxAfter)
	}
}

func TestIntegration_CompleteTournament_IdempotentReplayAndConcurrent(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	_ = prepareFinalCompletedTournament(t, pool, ts, "t-ct-idem", 4)
	var projBefore int64
	_ = pool.QueryRow(ctx, `SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1`, "t-ct-idem").Scan(&projBefore)

	var wg sync.WaitGroup
	results := make([]envelope.Result, 2)
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func(i int) {
			defer wg.Done()
			results[i] = commitCompleteTournament(t, ts, "t-ct-idem", "ct-idem", domain.CompleteTournamentDecision{})
		}(i)
	}
	wg.Wait()
	for i, r := range results {
		if r.Status != envelope.StatusAccepted {
			t.Fatalf("result %d: %+v", i, r)
		}
	}
	var outbox, proj int64
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE tournament_id=$1 AND event_type='TournamentCompleted'`, "t-ct-idem").Scan(&outbox)
	_ = pool.QueryRow(ctx, `SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1`, "t-ct-idem").Scan(&proj)
	if outbox != 1 {
		t.Fatalf("outbox=%d want 1", outbox)
	}
	if proj != projBefore+1 {
		t.Fatalf("proj %d -> %d want exactly +1", projBefore, proj)
	}

	// Already completed factless: no second bump/outbox.
	out := commitCompleteTournament(t, ts, "t-ct-idem", "ct-idem-2", domain.CompleteTournamentDecision{})
	if out.Status != envelope.StatusAccepted {
		t.Fatalf("already done %+v", out)
	}
	var outbox2, proj2 int64
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE tournament_id=$1 AND event_type='TournamentCompleted'`, "t-ct-idem").Scan(&outbox2)
	_ = pool.QueryRow(ctx, `SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1`, "t-ct-idem").Scan(&proj2)
	if outbox2 != 1 || proj2 != proj {
		t.Fatalf("already-done must be factless: outbox=%d proj=%d", outbox2, proj2)
	}
}

func TestIntegration_CompleteTournament_CommitRollback(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	_ = prepareFinalCompletedTournament(t, pool, ts, "t-ct-rb", 4)
	ts.FailNextCommits = 1
	uow, err := ts.BeginCompleteTournament(ctx, "t-ct-rb", "ct-rb")
	if err != nil {
		t.Fatal(err)
	}
	d := domain.DecideCompleteTournament(uow.CompleteContext(), domain.CompleteTournamentCommand{CommandID: "ct-rb"})
	err = uow.Commit(store.LifecycleCommitRequest{
		TournamentID: "t-ct-rb", CommandID: "ct-rb", CommandType: "CompleteTournament",
		Outcome: envelope.Accepted("ct-rb", "CompleteTournament", nil, nil),
		Events: []store.OutboxEvent{{
			EventID: "ct-rb:TournamentCompleted:0", EventType: "TournamentCompleted",
			TournamentID: "t-ct-rb", Topic: "tournament.completed", PartitionKey: "t-ct-rb",
			SchemaVersion: 1, Payload: map[string]any{"tournamentId": "t-ct-rb"}, CreatedAt: time.Now().UTC(),
		}},
		Complete: d,
	})
	if err == nil {
		t.Fatal("expected injected failure")
	}
	var phase string
	var nOut, nCmd int
	_ = pool.QueryRow(ctx, `SELECT phase FROM tournaments WHERE tournament_id=$1`, "t-ct-rb").Scan(&phase)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE tournament_id=$1 AND event_type='TournamentCompleted'`, "t-ct-rb").Scan(&nOut)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM command_idempotency WHERE command_id=$1`, "ct-rb").Scan(&nCmd)
	if phase == "completed" || nOut != 0 || nCmd != 0 {
		t.Fatalf("rollback leaked: phase=%s completedOutbox=%d cmd=%d", phase, nOut, nCmd)
	}
}

func TestIntegration_CancelTournament_SuccessO1AndWorkersSkip(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	tr, _ := provisionedTournament(t, ts, "t-cancel-o1", playersN(12))
	_ = tr
	markRoundMatchingReady(t, pool, "t-cancel-o1", 1)

	var slotsBefore, batchesBefore, regsBefore int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM bracket_slots WHERE tournament_id=$1`, "t-cancel-o1").Scan(&slotsBefore)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM provisioning_batches WHERE tournament_id=$1`, "t-cancel-o1").Scan(&batchesBefore)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM tournament_registrations WHERE tournament_id=$1`, "t-cancel-o1").Scan(&regsBefore)
	var slotStatusSample, batchStatusSample string
	_ = pool.QueryRow(ctx, `SELECT status FROM bracket_slots WHERE tournament_id=$1 ORDER BY slot_index LIMIT 1`, "t-cancel-o1").Scan(&slotStatusSample)
	_ = pool.QueryRow(ctx, `SELECT status FROM provisioning_batches WHERE tournament_id=$1 ORDER BY batch_id LIMIT 1`, "t-cancel-o1").Scan(&batchStatusSample)

	var projBefore int64
	_ = pool.QueryRow(ctx, `SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1`, "t-cancel-o1").Scan(&projBefore)

	out := commitCancelTournament(t, ts, "t-cancel-o1", "cancel-o1", domain.CancelTournamentDecision{})
	if out.Status != envelope.StatusAccepted {
		t.Fatalf("cancel %+v", out)
	}
	var phase string
	_ = pool.QueryRow(ctx, `SELECT phase FROM tournaments WHERE tournament_id=$1`, "t-cancel-o1").Scan(&phase)
	if phase != "cancelled" {
		t.Fatalf("phase=%s", phase)
	}
	var slotsAfter, batchesAfter, regsAfter int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM bracket_slots WHERE tournament_id=$1`, "t-cancel-o1").Scan(&slotsAfter)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM provisioning_batches WHERE tournament_id=$1`, "t-cancel-o1").Scan(&batchesAfter)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM tournament_registrations WHERE tournament_id=$1`, "t-cancel-o1").Scan(&regsAfter)
	if slotsAfter != slotsBefore || batchesAfter != batchesBefore || regsAfter != regsBefore {
		t.Fatalf("cancel must not rewrite child rows: slots %d→%d batches %d→%d regs %d→%d",
			slotsBefore, slotsAfter, batchesBefore, batchesAfter, regsBefore, regsAfter)
	}
	var slotStatusAfter, batchStatusAfter string
	_ = pool.QueryRow(ctx, `SELECT status FROM bracket_slots WHERE tournament_id=$1 ORDER BY slot_index LIMIT 1`, "t-cancel-o1").Scan(&slotStatusAfter)
	_ = pool.QueryRow(ctx, `SELECT status FROM provisioning_batches WHERE tournament_id=$1 ORDER BY batch_id LIMIT 1`, "t-cancel-o1").Scan(&batchStatusAfter)
	if slotStatusAfter != slotStatusSample || batchStatusAfter != batchStatusSample {
		t.Fatalf("cancel must not mutate child statuses: slot %s→%s batch %s→%s",
			slotStatusSample, slotStatusAfter, batchStatusSample, batchStatusAfter)
	}
	var projAfter int64
	_ = pool.QueryRow(ctx, `SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1`, "t-cancel-o1").Scan(&projAfter)
	if projAfter != projBefore+1 {
		t.Fatalf("projection bump want +1: %d -> %d", projBefore, projAfter)
	}

	// Bounded workers skip cancelled tournaments.
	cand, err := ts.FindReadyRoundCandidate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cand != nil && cand.TournamentID == "t-cancel-o1" {
		t.Fatalf("completion worker must skip cancelled: %+v", cand)
	}
	work, err := ts.ClaimNextProvisioningBatch(ctx, "owner-cancel", time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if work != nil && work.TournamentID == "t-cancel-o1" {
		t.Fatalf("provisioning claim must skip cancelled: %+v", work)
	}

	// Already cancelled factless.
	out2 := commitCancelTournament(t, ts, "t-cancel-o1", "cancel-o1-b", domain.CancelTournamentDecision{})
	if out2.Status != envelope.StatusAccepted {
		t.Fatalf("already cancelled %+v", out2)
	}
	var proj2 int64
	_ = pool.QueryRow(ctx, `SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1`, "t-cancel-o1").Scan(&proj2)
	if proj2 != projAfter {
		t.Fatalf("already-cancelled must not bump proj: %d -> %d", projAfter, proj2)
	}
}

func TestIntegration_CancelTournament_CompletedRejects(t *testing.T) {
	pool, ts := openStore(t)
	_ = prepareFinalCompletedTournament(t, pool, ts, "t-cancel-done", 4)
	_ = commitCompleteTournament(t, ts, "t-cancel-done", "ct-done", domain.CompleteTournamentDecision{})
	out := commitCancelTournament(t, ts, "t-cancel-done", "cancel-done", domain.CancelTournamentDecision{})
	if out.Status != envelope.StatusRejected || out.Reason != string(domain.RejectAlreadyTerminal) {
		t.Fatalf("got %+v", out)
	}
}

func TestIntegration_Lifecycle_CrossCommandElection(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	_ = prepareFinalCompletedTournament(t, pool, ts, "t-elect", 4)

	var wg sync.WaitGroup
	var completeOut, cancelOut envelope.Result
	wg.Add(2)
	go func() {
		defer wg.Done()
		completeOut = commitCompleteTournament(t, ts, "t-elect", "elect-ct", domain.CompleteTournamentDecision{})
	}()
	go func() {
		defer wg.Done()
		cancelOut = commitCancelTournament(t, ts, "t-elect", "elect-cancel", domain.CancelTournamentDecision{})
	}()
	wg.Wait()

	var phase string
	_ = pool.QueryRow(ctx, `SELECT phase FROM tournaments WHERE tournament_id=$1`, "t-elect").Scan(&phase)
	switch phase {
	case "completed":
		if completeOut.Status != envelope.StatusAccepted {
			t.Fatalf("complete winner %+v", completeOut)
		}
		if cancelOut.Status != envelope.StatusRejected || cancelOut.Reason != string(domain.RejectAlreadyTerminal) {
			t.Fatalf("cancel loser %+v", cancelOut)
		}
	case "cancelled":
		if cancelOut.Status != envelope.StatusAccepted {
			t.Fatalf("cancel winner %+v", cancelOut)
		}
		if completeOut.Status != envelope.StatusRejected || completeOut.Reason != string(domain.RejectAlreadyTerminal) {
			t.Fatalf("complete loser %+v", completeOut)
		}
	default:
		t.Fatalf("phase=%s complete=%+v cancel=%+v", phase, completeOut, cancelOut)
	}
}
