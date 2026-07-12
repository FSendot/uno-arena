//go:build integration

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/shared/envelope"
)

func TestIntegration_ServiceDurable_CompleteRoundWorkerAndManual(t *testing.T) {
	pool, ts, svc := serviceDurablePostgres(t)
	ctx := context.Background()
	if svc.completeRounds == nil {
		t.Fatal("expected CompleteRounds differential wiring")
	}

	slot := provisionViaService(t, ts, "t-svc-cr", 12)
	_ = slot
	if _, err := pool.Exec(ctx, `
		UPDATE provisioning_batches
		SET status = 'completed', completed_at = COALESCE(completed_at, now()), updated_at = now()
		WHERE tournament_id = $1 AND round_number = 1 AND status <> 'quarantined'
	`, "t-svc-cr"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE tournament_rounds SET status = 'in_progress'
		WHERE tournament_id = $1 AND round_number = 1 AND status IN ('provisioning', 'seeded')
	`, "t-svc-cr"); err != nil {
		t.Fatal(err)
	}
	got, ok := ts.Get(ctx, domain.TournamentID("t-svc-cr"))
	if !ok {
		t.Fatal("missing tournament")
	}
	round, _ := got.Round(1)
	now := time.Now().UTC()
	for i, s := range round.Slots {
		if !s.RoomID.Valid() {
			continue
		}
		standings := make([]MatchPlayerStanding, len(s.SeededPlayers))
		for j, p := range s.SeededPlayers {
			standings[j] = MatchPlayerStanding{
				PlayerID: string(p), MatchWins: len(s.SeededPlayers) - j,
				CumulativeCardPoints: 100 - j, FinalGameCompletedAt: now,
			}
		}
		resp, err := svc.IngestMatchCompleted(ctx, MatchCompletedEvent{
			EventID: fmt.Sprintf("svc-cr-%d", i), EventType: eventTypeMatchCompleted,
			SchemaVersion: envelope.CurrentSchemaVersion,
			RoomID:        string(s.RoomID), TournamentID: "t-svc-cr",
			RoundNumber: 1, SlotID: string(s.SlotID),
			CompletionVersion: uint64(i + 1), HasIsAbandoned: true, Players: standings,
		})
		if err != nil {
			t.Fatalf("ingest %d: %v", i, err)
		}
		if resp["disposition"] != "recorded" {
			t.Fatalf("ingest %d disposition=%v", i, resp)
		}
	}

	ready, err := ts.LoadRoundProgressReadiness(ctx, "t-svc-cr", 1)
	if err != nil || !ready.Ready {
		t.Fatalf("ready=%+v err=%v", ready, err)
	}

	// Concurrent manual + worker-style deterministic command: exactly one outbox/projection bump.
	cmdID := domain.CompleteRoundCommandID("t-svc-cr", 1)
	payload, _ := json.Marshal(map[string]any{"tournamentId": "t-svc-cr", "roundNumber": 1})
	var wg sync.WaitGroup
	results := make([]envelope.Result, 2)
	errs := make([]error, 2)
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = svc.SubmitCommand(ctx, CommandRequest{
				CommandID: cmdID, Type: CmdCompleteRound, SchemaVersion: envelope.CurrentSchemaVersion,
				Payload: payload,
			}, cmdID)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
		if results[i].Status != envelope.StatusAccepted {
			t.Fatalf("result %d: %+v", i, results[i])
		}
	}

	var outbox, proj int64
	_ = pool.QueryRow(ctx, `
		SELECT count(*) FROM outbox_events
		WHERE tournament_id=$1 AND event_type='TournamentRoundCompleted'
	`, "t-svc-cr").Scan(&outbox)
	_ = pool.QueryRow(ctx, `SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1`, "t-svc-cr").Scan(&proj)
	if outbox != 1 {
		t.Fatalf("exactly one RoundCompleted outbox, got %d", outbox)
	}
	if proj < 1 {
		t.Fatalf("projection=%d", proj)
	}

	// Restart replay of same command id.
	again, err := svc.SubmitCommand(ctx, CommandRequest{
		CommandID: cmdID, Type: CmdCompleteRound, SchemaVersion: envelope.CurrentSchemaVersion,
		Payload: payload,
	}, cmdID)
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != envelope.StatusAccepted {
		t.Fatalf("replay=%+v", again)
	}
	var outbox2 int64
	_ = pool.QueryRow(ctx, `
		SELECT count(*) FROM outbox_events
		WHERE tournament_id=$1 AND event_type='TournamentRoundCompleted'
	`, "t-svc-cr").Scan(&outbox2)
	if outbox2 != 1 {
		t.Fatalf("replay must not emit second outbox, got %d", outbox2)
	}

	cand, err := ts.FindReadyRoundCandidate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cand != nil {
		t.Fatalf("expected no candidate, got %+v", cand)
	}

	// T4 handoff: completed round schedules exactly one next-round seeding job.
	var status string
	var adv int
	_ = pool.QueryRow(ctx, `SELECT status FROM tournament_rounds WHERE tournament_id=$1 AND round_number=1`, "t-svc-cr").Scan(&status)
	_ = pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(advancing_count),0) FROM round_progress_shards
		WHERE tournament_id=$1 AND round_number=1
	`, "t-svc-cr").Scan(&adv)
	if status != "completed" || adv <= 0 {
		t.Fatalf("T4 handoff: status=%s advancing_count=%d", status, adv)
	}
	var jobN int
	var jobStatus, jobSource string
	_ = pool.QueryRow(ctx, `
		SELECT count(*), COALESCE(max(status),''), COALESCE(max(source),'')
		FROM round_seeding_jobs WHERE tournament_id=$1 AND round_number=2
	`, "t-svc-cr").Scan(&jobN, &jobStatus, &jobSource)
	if jobN != 1 || jobStatus != "pending" || jobSource != "advancement" {
		t.Fatalf("T4 handoff: want one pending advancement job for round 2, got n=%d status=%s source=%s", jobN, jobStatus, jobSource)
	}
	var r2status string
	_ = pool.QueryRow(ctx, `SELECT status FROM tournament_rounds WHERE tournament_id=$1 AND round_number=2`, "t-svc-cr").Scan(&r2status)
	if r2status != "pending" {
		t.Fatalf("T4 handoff: round 2 status=%s want pending", r2status)
	}
}

func TestIntegration_ServiceDurable_CompleteRoundInvalidStandalone(t *testing.T) {
	_, _, svc := serviceDurablePostgres(t)
	ctx := context.Background()
	payload, _ := json.Marshal(map[string]any{"tournamentId": "", "roundNumber": 0})
	res, err := svc.SubmitCommand(ctx, CommandRequest{
		CommandID: "cr-bad", Type: CmdCompleteRound, SchemaVersion: envelope.CurrentSchemaVersion,
		Payload: payload,
	}, "cr-bad")
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != envelope.StatusRejected {
		t.Fatalf("want rejected, got %+v", res)
	}
}

func TestIntegration_ServiceDurable_CompleteRoundInvalidPayloadReplay(t *testing.T) {
	pool, _, svc := serviceDurablePostgres(t)
	ctx := context.Background()
	cmdID := "cr-malformed-payload"
	res, err := svc.SubmitCommand(ctx, CommandRequest{
		CommandID: cmdID, Type: CmdCompleteRound, SchemaVersion: envelope.CurrentSchemaVersion,
		Payload: json.RawMessage(`{not-json`),
	}, "corr")
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != envelope.StatusRejected || res.Reason != "invalid_payload" {
		t.Fatalf("got %+v", res)
	}
	var n int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM command_idempotency WHERE command_id=$1`, cmdID).Scan(&n)
	if n != 1 {
		t.Fatalf("expected persisted invalid_payload outcome, count=%d", n)
	}
	replay, err := svc.SubmitCommand(ctx, CommandRequest{
		CommandID: cmdID, Type: CmdCompleteRound, SchemaVersion: envelope.CurrentSchemaVersion,
		Payload: json.RawMessage(`{"tournamentId":"t1","roundNumber":1}`),
	}, "corr-replay")
	if err != nil {
		t.Fatal(err)
	}
	if replay.Status != res.Status || replay.Reason != res.Reason {
		t.Fatalf("replay mismatch first=%+v replay=%+v", res, replay)
	}
}

func TestIntegration_ServiceDurable_TryCompleteReadyRound_NoPoisonThenSucceeds(t *testing.T) {
	pool, ts, svc := serviceDurablePostgres(t)
	ctx := context.Background()

	_ = provisionViaService(t, ts, "t-try-cr", 12)
	if _, err := pool.Exec(ctx, `
		UPDATE provisioning_batches
		SET status = 'completed', completed_at = COALESCE(completed_at, now()), updated_at = now()
		WHERE tournament_id = $1 AND round_number = 1 AND status <> 'quarantined'
	`, "t-try-cr"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE tournament_rounds SET status = 'in_progress'
		WHERE tournament_id = $1 AND round_number = 1 AND status IN ('provisioning', 'seeded')
	`, "t-try-cr"); err != nil {
		t.Fatal(err)
	}
	got, ok := ts.Get(ctx, domain.TournamentID("t-try-cr"))
	if !ok {
		t.Fatal("missing tournament")
	}
	round, _ := got.Round(1)
	now := time.Now().UTC()
	for i, s := range round.Slots {
		if !s.RoomID.Valid() {
			continue
		}
		standings := make([]MatchPlayerStanding, len(s.SeededPlayers))
		for j, p := range s.SeededPlayers {
			standings[j] = MatchPlayerStanding{
				PlayerID: string(p), MatchWins: len(s.SeededPlayers) - j,
				CumulativeCardPoints: 100 - j, FinalGameCompletedAt: now,
			}
		}
		resp, err := svc.IngestMatchCompleted(ctx, MatchCompletedEvent{
			EventID: fmt.Sprintf("try-cr-%d", i), EventType: eventTypeMatchCompleted,
			SchemaVersion: envelope.CurrentSchemaVersion,
			RoomID:        string(s.RoomID), TournamentID: "t-try-cr",
			RoundNumber: 1, SlotID: string(s.SlotID),
			CompletionVersion: uint64(i + 1), HasIsAbandoned: true, Players: standings,
		})
		if err != nil {
			t.Fatalf("ingest %d: %v", i, err)
		}
		if resp["disposition"] != "recorded" {
			t.Fatalf("ingest %d disposition=%v", i, resp)
		}
	}

	// Quarantine creates a candidate-race / not-ready gate.
	if _, err := pool.Exec(ctx, `
		UPDATE provisioning_batches SET status='quarantined', quarantine_reason='test'
		WHERE tournament_id=$1 AND round_number=1
		  AND batch_id=(SELECT batch_id FROM provisioning_batches WHERE tournament_id=$1 AND round_number=1 LIMIT 1)
	`, "t-try-cr"); err != nil {
		t.Fatal(err)
	}

	cmdID := domain.CompleteRoundCommandID("t-try-cr", 1)
	_, err := svc.TryCompleteReadyRound(ctx, "t-try-cr", 1)
	if err == nil {
		t.Fatal("expected retryable not-ready")
	}
	if !errors.Is(err, ErrCompleteRoundNotReady) {
		t.Fatalf("want ErrCompleteRoundNotReady, got %v", err)
	}
	var nOutcome int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM command_idempotency WHERE command_id=$1`, cmdID).Scan(&nOutcome)
	if nOutcome != 0 {
		t.Fatalf("quarantine try must not persist command outcome, count=%d", nOutcome)
	}

	// Repair: clear quarantine, next try succeeds exactly once.
	if _, err := pool.Exec(ctx, `
		UPDATE provisioning_batches
		SET status='completed', quarantine_reason=NULL, completed_at=COALESCE(completed_at, now()), updated_at=now()
		WHERE tournament_id=$1 AND round_number=1 AND status='quarantined'
	`, "t-try-cr"); err != nil {
		t.Fatal(err)
	}
	res, err := svc.TryCompleteReadyRound(ctx, "t-try-cr", 1)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != envelope.StatusAccepted {
		t.Fatalf("want accepted after repair, got %+v", res)
	}
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM command_idempotency WHERE command_id=$1`, cmdID).Scan(&nOutcome)
	if nOutcome != 1 {
		t.Fatalf("exactly one outcome after success, got %d", nOutcome)
	}
	var outbox int64
	_ = pool.QueryRow(ctx, `
		SELECT count(*) FROM outbox_events
		WHERE tournament_id=$1 AND event_type='TournamentRoundCompleted'
	`, "t-try-cr").Scan(&outbox)
	if outbox != 1 {
		t.Fatalf("exactly one RoundCompleted outbox, got %d", outbox)
	}

	again, err := svc.TryCompleteReadyRound(ctx, "t-try-cr", 1)
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != envelope.StatusAccepted {
		t.Fatalf("replay=%+v", again)
	}
	_ = pool.QueryRow(ctx, `
		SELECT count(*) FROM outbox_events
		WHERE tournament_id=$1 AND event_type='TournamentRoundCompleted'
	`, "t-try-cr").Scan(&outbox)
	if outbox != 1 {
		t.Fatalf("replay must not emit second outbox, got %d", outbox)
	}

	// Manual CmdCompleteRound still persists rejects (stable) when not ready.
	manualID := "cr-manual-incomplete"
	payload, _ := json.Marshal(map[string]any{"tournamentId": "t-try-cr", "roundNumber": 2})
	manual, err := svc.SubmitCommand(ctx, CommandRequest{
		CommandID: manualID, Type: CmdCompleteRound, SchemaVersion: envelope.CurrentSchemaVersion,
		Payload: payload,
	}, manualID)
	if err != nil {
		t.Fatal(err)
	}
	if manual.Status != envelope.StatusRejected {
		t.Fatalf("manual must persist reject, got %+v", manual)
	}
	var nManual int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM command_idempotency WHERE command_id=$1`, manualID).Scan(&nManual)
	if nManual != 1 {
		t.Fatalf("manual reject must persist, count=%d", nManual)
	}
}
