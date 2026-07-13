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

func resolveAllSlots(t *testing.T, ts *store.TournamentStore, tid string, tr *domain.Tournament) int {
	t.Helper()
	round, ok := tr.Round(1)
	if !ok {
		t.Fatal("round 1 missing")
	}
	advTotal := 0
	for i, slot := range round.Slots {
		if !slot.RoomID.Valid() {
			continue
		}
		cmd := domain.RecordMatchResultCommand{
			CommandID: domain.CommandID(fmt.Sprintf("ingest:cr-%s-%d", tid, i)),
			EventID:   domain.EventID(fmt.Sprintf("cr-%s-%d", tid, i)),
			RoomID:    slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
			CompletionVersion: domain.CompletionVersion(i + 1),
			Standings:         standingsFromSeeded(slot.SeededPlayers),
		}
		kind := ingestDifferentialKind(t, ts, tid, cmd)
		if kind != domain.RoundMatchRecord {
			t.Fatalf("slot %d kind=%s", i, kind)
		}
		ranked, err := domain.RankStandings(standingsFromSeeded(slot.SeededPlayers))
		if err != nil {
			t.Fatal(err)
		}
		if round.IsFinal {
			advTotal += len(ranked)
		} else {
			adv, err := domain.TopThree(ranked)
			if err != nil {
				t.Fatal(err)
			}
			advTotal += len(adv)
		}
	}
	return advTotal
}

func prepareCompleteRoundReady(t *testing.T, pool *store.Pool, ts *store.TournamentStore, tid string, n int) (*domain.Tournament, int) {
	t.Helper()
	tr, _ := provisionedTournament(t, ts, tid, playersN(n))
	markRoundMatchingReady(t, pool, tid, 1)
	wantAdv := resolveAllSlots(t, ts, tid, tr)
	return tr, wantAdv
}

func completeRoundDifferential(t *testing.T, ts *store.TournamentStore, tid string, rn int, cmdID string) domain.CompleteRoundDecision {
	t.Helper()
	ctx := context.Background()
	uow, err := ts.BeginCompleteRound(ctx, tid, rn, cmdID)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = uow.Rollback() }()
	if prior, ok := uow.LookupOutcome(cmdID); ok {
		_ = prior
		return domain.CompleteRoundDecision{Kind: domain.CompleteRoundAlreadyDone}
	}
	d := domain.DecideCompleteRound(uow.Loaded(), domain.CompleteRoundCommand{
		CommandID: domain.CommandID(cmdID), RoundNumber: rn,
	})
	factsPayload := make([]map[string]any, 0, len(d.Outcome.Facts))
	for _, f := range d.Outcome.Facts {
		factsPayload = append(factsPayload, map[string]any{"name": string(f.Name), "data": f.Data})
	}
	payload, _ := json.Marshal(map[string]any{"facts": factsPayload})
	var events []store.OutboxEvent
	if d.Kind == domain.CompleteRoundSuccess {
		now := time.Now().UTC()
		for i, f := range d.Outcome.Facts {
			topic := "tournament.round.completed"
			if f.Name == domain.FactTournamentCompleted {
				topic = "tournament.completed"
			}
			events = append(events, store.OutboxEvent{
				EventID: fmt.Sprintf("%s:%s:%d", cmdID, f.Name, i), EventType: string(f.Name),
				TournamentID: tid, Topic: topic, PartitionKey: tid, SchemaVersion: 1,
				Payload: map[string]any{"schemaVersion": 1, "eventType": string(f.Name)}, CreatedAt: now,
			})
		}
	}
	out := envelope.Accepted(cmdID, "CompleteRound", nil, payload)
	if d.Outcome.Rejected() {
		out = envelope.Rejected(cmdID, "CompleteRound", string(d.Outcome.Rejection.Code), nil)
	}
	if err := uow.Commit(store.CompleteRoundCommitRequest{
		TournamentID: tid, CommandID: cmdID, CommandType: "CompleteRound",
		Outcome: out, Events: events, Decision: d,
	}); err != nil {
		if prior, ok := store.AsPriorCommandOutcome(err); ok {
			_ = prior
			return domain.CompleteRoundDecision{Kind: domain.CompleteRoundAlreadyDone}
		}
		t.Fatalf("commit: %v", err)
	}
	return d
}

func TestIntegration_CompleteFinalRoundAtomicallyCompletesTournament(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	_, _ = prepareCompleteRoundReady(t, pool, ts, "t-cr-final-auto", 2)

	cmdID := domain.CompleteRoundCommandID("t-cr-final-auto", 1)
	d := completeRoundDifferential(t, ts, "t-cr-final-auto", 1, cmdID)
	if d.Kind != domain.CompleteRoundSuccess || !d.IsFinal || d.TournamentCompletion == nil {
		t.Fatalf("final completion decision=%+v", d)
	}

	var phase, champion string
	var completedAt *time.Time
	if err := pool.QueryRow(ctx, `
		SELECT phase, completed_at, COALESCE(rules->>'championId','')
		FROM tournaments WHERE tournament_id=$1
	`, "t-cr-final-auto").Scan(&phase, &completedAt, &champion); err != nil {
		t.Fatal(err)
	}
	if phase != string(domain.PhaseCompleted) || completedAt == nil || champion != string(d.TournamentCompletion.ChampionID) {
		t.Fatalf("tournament completion not atomic: phase=%s completedAt=%v champion=%s decision=%+v",
			phase, completedAt, champion, d.TournamentCompletion)
	}
	var roundEvents, tournamentEvents int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE event_type='TournamentRoundCompleted')::int,
		       count(*) FILTER (WHERE event_type='TournamentCompleted')::int
		FROM outbox_events WHERE tournament_id=$1
	`, "t-cr-final-auto").Scan(&roundEvents, &tournamentEvents); err != nil {
		t.Fatal(err)
	}
	if roundEvents != 1 || tournamentEvents != 1 {
		t.Fatalf("atomic contracts missing: round=%d tournament=%d", roundEvents, tournamentEvents)
	}
}

func TestIntegration_CompleteRound_NotReady(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	provisionedTournament(t, ts, "t-cr-nr", playersN(12))
	markRoundMatchingReady(t, pool, "t-cr-nr", 1)
	ready, err := ts.LoadRoundProgressReadiness(ctx, "t-cr-nr", 1)
	if err != nil {
		t.Fatal(err)
	}
	if ready.Ready {
		t.Fatalf("expected not ready before results: %+v", ready)
	}
	d := completeRoundDifferential(t, ts, "t-cr-nr", 1, "cr-nr")
	if d.Kind != domain.CompleteRoundReject || d.Outcome.Rejection.Code != domain.RejectRoundIncomplete {
		t.Fatalf("want incomplete reject, got %+v", d)
	}
}

func TestIntegration_CompleteRound_ReadySuccessAndIdempotent(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	_, wantAdv := prepareCompleteRoundReady(t, pool, ts, "t-cr-ok", 12)

	ready, err := ts.LoadRoundProgressReadiness(ctx, "t-cr-ok", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !ready.Ready || ready.AdvancingCount != wantAdv {
		t.Fatalf("ready=%+v wantAdv=%d", ready, wantAdv)
	}
	cand, err := ts.FindReadyRoundCandidate(ctx)
	if err != nil || cand == nil || cand.TournamentID != "t-cr-ok" {
		t.Fatalf("candidate=%v err=%v", cand, err)
	}

	var projBefore int64
	_ = pool.QueryRow(ctx, `SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1`, "t-cr-ok").Scan(&projBefore)
	var outboxBefore int64
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE tournament_id=$1 AND event_type='TournamentRoundCompleted'`, "t-cr-ok").Scan(&outboxBefore)

	cmdID := domain.CompleteRoundCommandID("t-cr-ok", 1)
	d := completeRoundDifferential(t, ts, "t-cr-ok", 1, cmdID)
	if d.Kind != domain.CompleteRoundSuccess || d.RemainingPlayers != wantAdv {
		t.Fatalf("success remaining=%d want=%d d=%+v", d.RemainingPlayers, wantAdv, d)
	}

	var status string
	var completedAt *time.Time
	if err := pool.QueryRow(ctx, `
		SELECT status, completed_at FROM tournament_rounds WHERE tournament_id=$1 AND round_number=1
	`, "t-cr-ok").Scan(&status, &completedAt); err != nil {
		t.Fatal(err)
	}
	if status != "completed" || completedAt == nil {
		t.Fatalf("status=%s completed_at=%v", status, completedAt)
	}
	var projAfter, outboxAfter int64
	_ = pool.QueryRow(ctx, `SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1`, "t-cr-ok").Scan(&projAfter)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE tournament_id=$1 AND event_type='TournamentRoundCompleted'`, "t-cr-ok").Scan(&outboxAfter)
	if projAfter != projBefore+1 {
		t.Fatalf("projection bump want +1: %d -> %d", projBefore, projAfter)
	}
	if outboxAfter != outboxBefore+1 {
		t.Fatalf("outbox want +1: %d -> %d", outboxBefore, outboxAfter)
	}

	var nextJobs int
	var nextSource string
	_ = pool.QueryRow(ctx, `
		SELECT count(*), COALESCE(max(source),'') FROM round_seeding_jobs
		WHERE tournament_id=$1 AND round_number=2
	`, "t-cr-ok").Scan(&nextJobs, &nextSource)
	if nextJobs != 1 || nextSource != domain.SeedingSourceAdvancement {
		t.Fatalf("non-final complete must schedule one advancement job: n=%d source=%s", nextJobs, nextSource)
	}
	var r2 string
	_ = pool.QueryRow(ctx, `SELECT status FROM tournament_rounds WHERE tournament_id=$1 AND round_number=2`, "t-cr-ok").Scan(&r2)
	if r2 != "pending" {
		t.Fatalf("round 2 status=%s", r2)
	}

	// Replay / already complete: factless no-op, no second bump/outbox.
	d2 := completeRoundDifferential(t, ts, "t-cr-ok", 1, cmdID)
	if d2.Kind != domain.CompleteRoundAlreadyDone && d2.Kind != domain.CompleteRoundSuccess {
		t.Fatalf("replay kind=%s", d2.Kind)
	}
	var projReplay, outboxReplay int64
	_ = pool.QueryRow(ctx, `SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1`, "t-cr-ok").Scan(&projReplay)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE tournament_id=$1 AND event_type='TournamentRoundCompleted'`, "t-cr-ok").Scan(&outboxReplay)
	if projReplay != projAfter || outboxReplay != outboxAfter {
		t.Fatalf("replay must not bump/outbox again")
	}

	// Different command after complete → factless already-done.
	d3 := completeRoundDifferential(t, ts, "t-cr-ok", 1, "cr-manual-other")
	if d3.Kind != domain.CompleteRoundAlreadyDone {
		t.Fatalf("want already_done, got %+v", d3)
	}
	_ = pool.QueryRow(ctx, `SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1`, "t-cr-ok").Scan(&projReplay)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE tournament_id=$1 AND event_type='TournamentRoundCompleted'`, "t-cr-ok").Scan(&outboxReplay)
	if projReplay != projAfter || outboxReplay != outboxAfter {
		t.Fatalf("already-done must not bump/outbox")
	}

	ready, _ = ts.LoadRoundProgressReadiness(ctx, "t-cr-ok", 1)
	if ready.Ready {
		t.Fatalf("completed round must not be ready: %+v", ready)
	}
	cand, _ = ts.FindReadyRoundCandidate(ctx)
	if cand != nil {
		t.Fatalf("no candidate after complete, got %+v", cand)
	}
}

func TestIntegration_CompleteRound_ConcurrentExactlyOneOutbox(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	_, _ = prepareCompleteRoundReady(t, pool, ts, "t-cr-conc", 12)

	cmdID := domain.CompleteRoundCommandID("t-cr-conc", 1)
	var wg sync.WaitGroup
	kinds := make([]domain.CompleteRoundKind, 2)
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func(i int) {
			defer wg.Done()
			kinds[i] = completeRoundDifferential(t, ts, "t-cr-conc", 1, cmdID).Kind
		}(i)
	}
	wg.Wait()

	var outbox int64
	_ = pool.QueryRow(ctx, `
		SELECT count(*) FROM outbox_events
		WHERE tournament_id=$1 AND event_type='TournamentRoundCompleted'
	`, "t-cr-conc").Scan(&outbox)
	if outbox != 1 {
		t.Fatalf("exactly one outbox, got %d kinds=%v", outbox, kinds)
	}
	var proj int64
	_ = pool.QueryRow(ctx, `SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1`, "t-cr-conc").Scan(&proj)
	if proj < 1 {
		t.Fatalf("projection=%d", proj)
	}
	var jobN int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM round_seeding_jobs WHERE tournament_id=$1 AND round_number=2`, "t-cr-conc").Scan(&jobN)
	if jobN != 1 {
		t.Fatalf("concurrent CompleteRound must create exactly one next seeding job, got %d", jobN)
	}
}

func TestIntegration_CompleteRound_QuarantineGates(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	_, _ = prepareCompleteRoundReady(t, pool, ts, "t-cr-q", 12)

	if _, err := pool.Exec(ctx, `
		UPDATE provisioning_batches SET status='quarantined', quarantine_reason='test'
		WHERE tournament_id=$1 AND round_number=1
	`, "t-cr-q"); err != nil {
		t.Fatal(err)
	}
	ready, _ := ts.LoadRoundProgressReadiness(ctx, "t-cr-q", 1)
	if ready.Ready {
		t.Fatalf("quarantined batch must block: %+v", ready)
	}
	d := completeRoundDifferential(t, ts, "t-cr-q", 1, "cr-q")
	if d.Kind != domain.CompleteRoundReject || d.Outcome.Rejection.Code != domain.RejectQuarantined {
		t.Fatalf("want quarantined, got %+v", d)
	}
}

func TestIntegration_CompleteRound_AdvancingCountRebuildParity(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	_, wantAdv := prepareCompleteRoundReady(t, pool, ts, "t-cr-adv", 12)

	var sumAdv int
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(advancing_count),0) FROM round_progress_shards WHERE tournament_id=$1 AND round_number=1
	`, "t-cr-adv").Scan(&sumAdv); err != nil {
		t.Fatal(err)
	}
	if sumAdv != wantAdv {
		t.Fatalf("shard advancing_count=%d want=%d", sumAdv, wantAdv)
	}
	var cardSum int
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(cardinality(advancing_player_ids)),0) FROM advancement_records
		WHERE tournament_id=$1 AND round_number=1
	`, "t-cr-adv").Scan(&cardSum); err != nil {
		t.Fatal(err)
	}
	if cardSum != wantAdv {
		t.Fatalf("advancement_records card=%d want=%d", cardSum, wantAdv)
	}

	// Legacy rebuild must preserve exact advancing_count.
	uow, err := ts.BeginExisting(ctx, domain.TournamentID("t-cr-adv"))
	if err != nil {
		t.Fatal(err)
	}
	loaded := uow.Loaded()
	if err := uow.Commit(store.CommitRequest{
		Tournament: loaded, CommandID: "legacy-rewrite-adv",
		Outcome:           envelope.Accepted("legacy-rewrite-adv", "Rewrite", nil, json.RawMessage(`{}`)),
		ProjectionChanged: false,
	}); err != nil {
		t.Fatal(err)
	}
	var sumAfter int
	_ = pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(advancing_count),0) FROM round_progress_shards WHERE tournament_id=$1 AND round_number=1
	`, "t-cr-adv").Scan(&sumAfter)
	if sumAfter != wantAdv {
		t.Fatalf("rebuild advancing_count=%d want=%d", sumAfter, wantAdv)
	}
}

func TestIntegration_CompleteRound_FinalRemainingPlayers(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	_, wantAdv := prepareCompleteRoundReady(t, pool, ts, "t-cr-final", 8)
	d := completeRoundDifferential(t, ts, "t-cr-final", 1, domain.CompleteRoundCommandID("t-cr-final", 1))
	if d.Kind != domain.CompleteRoundSuccess || !d.IsFinal || d.RemainingPlayers != wantAdv {
		t.Fatalf("final complete: %+v want remaining=%d", d, wantAdv)
	}
	var isFinal bool
	_ = pool.QueryRow(ctx, `SELECT is_final FROM tournament_rounds WHERE tournament_id=$1 AND round_number=1`, "t-cr-final").Scan(&isFinal)
	if !isFinal {
		t.Fatal("expected final round")
	}
}

func TestIntegration_CompleteRound_LockOrderBarrier(t *testing.T) {
	if !store.RewriteBarrierUsesHashtextextended() {
		t.Fatal("barrier must use hashtextextended")
	}
	pool, ts := openStore(t)
	ctx := context.Background()
	_, _ = prepareCompleteRoundReady(t, pool, ts, "t-cr-lock", 12)

	uow, err := ts.BeginCompleteRound(ctx, "t-cr-lock", 1, "cr-lock-hold")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = uow.Rollback() }()

	createCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err = ts.BeginCreate(createCtx, "t-cr-lock")
	elapsed := time.Since(started)
	if err == nil {
		t.Fatal("BeginCreate must block/fail while CompleteRound shared barrier held")
	}
	if elapsed < 50*time.Millisecond {
		t.Fatalf("BeginCreate returned too quickly (%s); expected wait on exclusive barrier", elapsed)
	}
}

func TestIntegration_CompleteRound_MissingShardsZeroInitThenReject(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	provisionedTournament(t, ts, "t-cr-noshard", playersN(12))
	markRoundMatchingReady(t, pool, "t-cr-noshard", 1)
	if _, err := pool.Exec(ctx, `
		DELETE FROM round_progress_shards WHERE tournament_id=$1 AND round_number=1
	`, "t-cr-noshard"); err != nil {
		t.Fatal(err)
	}
	var before int
	_ = pool.QueryRow(ctx, `
		SELECT count(*) FROM round_progress_shards WHERE tournament_id=$1 AND round_number=1
	`, "t-cr-noshard").Scan(&before)
	if before != 0 {
		t.Fatalf("expected zero shards before begin, got %d", before)
	}

	uow, err := ts.BeginCompleteRound(ctx, "t-cr-noshard", 1, "cr-noshard")
	if err != nil {
		t.Fatalf("BeginCompleteRound must not return infra error for missing shards: %v", err)
	}
	defer func() { _ = uow.Rollback() }()
	loaded := uow.Loaded()
	if !loaded.RoundFound || loaded.AssignedCount != 0 {
		t.Fatalf("loaded=%+v", loaded)
	}
	d := domain.DecideCompleteRound(loaded, domain.CompleteRoundCommand{
		CommandID: "cr-noshard", RoundNumber: 1,
	})
	if d.Kind != domain.CompleteRoundReject || d.Outcome.Rejection.Code != domain.RejectRoundIncomplete {
		t.Fatalf("want round_incomplete, got %+v", d)
	}
	res := envelope.Rejected("cr-noshard", "CompleteRound", string(d.Outcome.Rejection.Code), nil)
	if err := uow.Commit(store.CompleteRoundCommitRequest{
		TournamentID: "t-cr-noshard", CommandID: "cr-noshard", CommandType: "CompleteRound",
		Outcome: res, Decision: d,
	}); err != nil {
		t.Fatal(err)
	}
	var shards int
	_ = pool.QueryRow(ctx, `
		SELECT count(*) FROM round_progress_shards WHERE tournament_id=$1 AND round_number=1
	`, "t-cr-noshard").Scan(&shards)
	if shards != domain.ProgressShardCount {
		t.Fatalf("expected %d zero-init shards after begin/commit, got %d", domain.ProgressShardCount, shards)
	}
	prior, ok := ts.LookupCompleteRoundOutcome(ctx, "cr-noshard")
	if !ok || prior.Status != envelope.StatusRejected || prior.Reason != string(domain.RejectRoundIncomplete) {
		t.Fatalf("manual reject must persist for replay, prior=%+v ok=%v", prior, ok)
	}
	// Replay same command id → prior reject (stable).
	d2 := completeRoundDifferential(t, ts, "t-cr-noshard", 1, "cr-noshard")
	if d2.Kind != domain.CompleteRoundAlreadyDone {
		// Helper maps prior outcome to already_done; either way prior must remain rejected.
		prior2, ok2 := ts.LookupCompleteRoundOutcome(ctx, "cr-noshard")
		if !ok2 || prior2.Reason != string(domain.RejectRoundIncomplete) {
			t.Fatalf("replay must keep prior reject; d2=%+v prior=%+v", d2, prior2)
		}
	}
}

func TestIntegration_FindReadyRoundCandidate_SkipsCancelledSortedBeforeHealthy(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	// Lexicographically earlier cancelled id would starve the healthy candidate without the phase filter.
	_, _ = prepareCompleteRoundReady(t, pool, ts, "t-cr-starve-a-cancel", 12)
	_, wantAdv := prepareCompleteRoundReady(t, pool, ts, "t-cr-starve-b-ok", 12)
	if _, err := pool.Exec(ctx, `
		UPDATE tournaments SET phase = 'cancelled' WHERE tournament_id = $1
	`, "t-cr-starve-a-cancel"); err != nil {
		t.Fatal(err)
	}

	cand, err := ts.FindReadyRoundCandidate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cand == nil || cand.TournamentID != "t-cr-starve-b-ok" || cand.RoundNumber != 1 {
		t.Fatalf("want healthy candidate t-cr-starve-b-ok, got %+v", cand)
	}

	d := completeRoundDifferential(t, ts, "t-cr-starve-b-ok", 1, domain.CompleteRoundCommandID("t-cr-starve-b-ok", 1))
	if d.Kind != domain.CompleteRoundSuccess || d.RemainingPlayers != wantAdv {
		t.Fatalf("healthy complete: %+v want remaining=%d", d, wantAdv)
	}
	cand, err = ts.FindReadyRoundCandidate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cand != nil {
		t.Fatalf("cancelled tournament must not starve later polls; got %+v", cand)
	}
	var cancelStatus string
	_ = pool.QueryRow(ctx, `
		SELECT status FROM tournament_rounds WHERE tournament_id=$1 AND round_number=1
	`, "t-cr-starve-a-cancel").Scan(&cancelStatus)
	if cancelStatus != "in_progress" {
		t.Fatalf("cancelled tournament round should remain in_progress (not claimed), got %s", cancelStatus)
	}
}

func TestIntegration_FindReadyRoundCandidate_SkipsAdvancingDriftSortedBeforeHealthy(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	_, _ = prepareCompleteRoundReady(t, pool, ts, "t-cr-drift-a", 12)
	_, wantAdv := prepareCompleteRoundReady(t, pool, ts, "t-cr-drift-b-ok", 12)
	if _, err := pool.Exec(ctx, `
		UPDATE round_progress_shards
		SET advancing_count = advancing_count + 1
		WHERE tournament_id = $1 AND round_number = 1 AND shard_id = 0
	`, "t-cr-drift-a"); err != nil {
		t.Fatal(err)
	}

	cand, err := ts.FindReadyRoundCandidate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cand == nil || cand.TournamentID != "t-cr-drift-b-ok" || cand.RoundNumber != 1 {
		t.Fatalf("want healthy candidate t-cr-drift-b-ok, got %+v", cand)
	}

	d := completeRoundDifferential(t, ts, "t-cr-drift-b-ok", 1, domain.CompleteRoundCommandID("t-cr-drift-b-ok", 1))
	if d.Kind != domain.CompleteRoundSuccess || d.RemainingPlayers != wantAdv {
		t.Fatalf("healthy complete: %+v want remaining=%d", d, wantAdv)
	}
	cand, err = ts.FindReadyRoundCandidate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cand != nil {
		t.Fatalf("drift candidate must not starve later polls; got %+v", cand)
	}
	var driftStatus string
	_ = pool.QueryRow(ctx, `
		SELECT status FROM tournament_rounds WHERE tournament_id=$1 AND round_number=1
	`, "t-cr-drift-a").Scan(&driftStatus)
	if driftStatus != "in_progress" {
		t.Fatalf("drift round should remain in_progress (not completed), got %s", driftStatus)
	}
}

func TestIntegration_CompleteRound_MoreThanTenBatchesNumericLockOrder(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	provisionedTournament(t, ts, "t-cr-batches", playersN(12))
	markRoundMatchingReady(t, pool, "t-cr-batches", 1)

	// Force >10 batches with lexical trap ids (slot_10 before slot_2 textually).
	if _, err := pool.Exec(ctx, `
		DELETE FROM provisioning_batches WHERE tournament_id=$1 AND round_number=1
	`, "t-cr-batches"); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	for i := 0; i < 12; i++ {
		from := fmt.Sprintf("slot_%d", i)
		batchID := fmt.Sprintf("batch_%d", i)
		shardKey := fmt.Sprintf("t-cr-batches:1:%s", batchID)
		if _, err := pool.Exec(ctx, `
			INSERT INTO provisioning_batches (
				tournament_id, round_number, batch_id, shard_key, status, retry_attempt,
				slot_id_from, slot_id_to, created_at, updated_at
			) VALUES ($1, 1, $2, $3, 'completed', 0, $4, $4, $5, $5)
		`, "t-cr-batches", batchID, shardKey, from, now); err != nil {
			t.Fatal(err)
		}
	}
	var batchCount int
	_ = pool.QueryRow(ctx, `
		SELECT count(*) FROM provisioning_batches WHERE tournament_id=$1 AND round_number=1
	`, "t-cr-batches").Scan(&batchCount)
	if batchCount <= 10 {
		t.Fatalf("need >10 batches for lexical trap guard, got %d", batchCount)
	}

	uow, err := ts.BeginCompleteRound(ctx, "t-cr-batches", 1, "cr-batches-lock")
	if err != nil {
		t.Fatalf("BeginCompleteRound with >10 batches: %v", err)
	}
	defer func() { _ = uow.Rollback() }()
	if !uow.Loaded().RoundFound {
		t.Fatal("expected round found")
	}
}
