//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/services/tournament-orchestration/store"
	"unoarena/shared/envelope"
)

type countingDurableRepo struct {
	*durableRepo
	beginExisting atomic.Int32
}

func (r *countingDurableRepo) BeginExisting(id domain.TournamentID) (TournamentUnitOfWork, error) {
	r.beginExisting.Add(1)
	return r.durableRepo.BeginExisting(id)
}

func serviceDurableLifecyclePostgres(t *testing.T) (*store.Pool, *store.TournamentStore, *Service, *countingDurableRepo) {
	t.Helper()
	pool, ts, _ := serviceDurablePostgres(t)
	counted := &countingDurableRepo{durableRepo: &durableRepo{store: ts}}
	svc := NewService(ServiceDeps{
		Repo:           counted,
		RoundMatches:   &durableRoundMatchRepo{store: ts},
		Registrations:  &durableRegistrationRepo{store: ts},
		CompleteRounds: &durableCompleteRoundRepo{store: ts},
		Lifecycle:      &durableLifecycleRepo{store: ts},
		Audit:          NoopAudit{},
		BracketPages:   ts,
		Standings:      ts,
	})
	return pool, ts, svc, counted
}

func prepareFinalViaService(t *testing.T, pool *store.Pool, ts *store.TournamentStore, svc *Service, tid string, n int) []string {
	t.Helper()
	ctx := context.Background()
	slot := provisionViaService(t, ts, tid, n)
	_ = slot
	if _, err := pool.Exec(ctx, `
		UPDATE provisioning_batches
		SET status = 'completed', completed_at = COALESCE(completed_at, now()), updated_at = now()
		WHERE tournament_id = $1 AND round_number = 1 AND status <> 'quarantined'
	`, tid); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE tournament_rounds SET status = 'in_progress'
		WHERE tournament_id = $1 AND round_number = 1 AND status IN ('provisioning', 'seeded')
	`, tid); err != nil {
		t.Fatal(err)
	}
	got, ok := ts.Get(ctx, domain.TournamentID(tid))
	if !ok {
		t.Fatal("missing")
	}
	round, _ := got.Round(1)
	if !round.IsFinal {
		t.Fatal("expected final round")
	}
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
			EventID: fmt.Sprintf("%s-m-%d", tid, i), EventType: eventTypeMatchCompleted,
			SchemaVersion: envelope.CurrentSchemaVersion,
			RoomID:        string(s.RoomID), TournamentID: tid,
			RoundNumber: 1, SlotID: string(s.SlotID),
			CompletionVersion: uint64(i + 1), HasIsAbandoned: true, Players: standings,
		})
		if err != nil {
			t.Fatalf("ingest: %v", err)
		}
		if resp["disposition"] != "recorded" {
			t.Fatalf("disp=%v", resp)
		}
	}
	crPayload, _ := json.Marshal(map[string]any{"tournamentId": tid, "roundNumber": 1})
	cr, err := svc.SubmitCommand(ctx, CommandRequest{
		CommandID: domain.CompleteRoundCommandID(domain.TournamentID(tid), 1),
		Type:      CmdCompleteRound, SchemaVersion: envelope.CurrentSchemaVersion,
		Payload: crPayload,
	}, "cr")
	if err != nil || cr.Status != envelope.StatusAccepted {
		t.Fatalf("complete round: %+v err=%v", cr, err)
	}
	var advancing []string
	if err := pool.QueryRow(ctx, `
		SELECT advancing_player_ids FROM advancement_records
		WHERE tournament_id=$1 AND round_number=1
		ORDER BY slot_id LIMIT 1
	`, tid).Scan(&advancing); err != nil {
		t.Fatalf("load final standings: %v", err)
	}
	if len(advancing) == 0 {
		t.Fatal("expected final standings from advancement_records")
	}
	return advancing
}

func TestIntegration_ServiceDurable_CompleteAndCancelNoLegacyBeginExisting(t *testing.T) {
	pool, ts, svc, counted := serviceDurableLifecyclePostgres(t)
	ctx := context.Background()
	if svc.lifecycle == nil {
		t.Fatal("expected Lifecycle differential wiring")
	}
	standings := prepareFinalViaService(t, pool, ts, svc, "t-svc-life", 4)
	before := counted.beginExisting.Load()

	payload, _ := json.Marshal(map[string]any{"tournamentId": "t-svc-life"})
	res, err := svc.SubmitCommand(ctx, CommandRequest{
		CommandID: "svc-ct", Type: CmdCompleteTournament, SchemaVersion: envelope.CurrentSchemaVersion,
		Payload: payload,
	}, "svc-ct")
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != envelope.StatusAccepted {
		t.Fatalf("complete %+v", res)
	}
	if counted.beginExisting.Load() != before {
		t.Fatalf("CompleteTournament must not call BeginExisting (before=%d after=%d)", before, counted.beginExisting.Load())
	}

	var payloadBody []byte
	if err := pool.QueryRow(ctx, `
		SELECT payload FROM outbox_events
		WHERE tournament_id=$1 AND event_type='TournamentCompleted'
	`, "t-svc-life").Scan(&payloadBody); err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	_ = json.Unmarshal(payloadBody, &body)
	if _, has := body["championId"]; has {
		t.Fatal("championId must be absent from outbox payload")
	}
	raw, _ := body["finalStandings"].([]any)
	if len(raw) != len(standings) || raw[0] != standings[0] {
		t.Fatalf("finalStandings=%v want %v", raw, standings)
	}

	// Cancel on a fresh in-progress tournament without legacy path.
	tr, _ := domain.CreateTournament(domain.CreateTournamentCommand{
		CommandID: "c-cancel", TournamentID: "t-svc-cancel", Capacity: 8,
	})
	for i := 0; i < 8; i++ {
		_ = tr.RegisterPlayer(domain.RegisterPlayerCommand{
			CommandID: domain.CommandID(fmt.Sprintf("r-cancel-%d", i)),
			PlayerID:  domain.PlayerID(fmt.Sprintf("cp%d", i)),
		})
	}
	if err := ts.Commit(ctx, store.CommitRequest{
		Tournament: tr, CommandID: "seed-cancel-svc",
		Outcome:           envelope.Accepted("seed-cancel-svc", "CreateTournament", nil, json.RawMessage(`{}`)),
		ProjectionChanged: true,
	}); err != nil {
		t.Fatal(err)
	}
	before = counted.beginExisting.Load()
	cancelPayload, _ := json.Marshal(map[string]any{"tournamentId": "t-svc-cancel"})
	cres, err := svc.SubmitCommand(ctx, CommandRequest{
		CommandID: "svc-cancel", Type: CmdCancelTournament, SchemaVersion: envelope.CurrentSchemaVersion,
		Payload: cancelPayload,
	}, "svc-cancel")
	if err != nil {
		t.Fatal(err)
	}
	if cres.Status != envelope.StatusAccepted {
		t.Fatalf("cancel %+v", cres)
	}
	if counted.beginExisting.Load() != before {
		t.Fatal("CancelTournament must not call BeginExisting when Lifecycle wired")
	}
	var phase string
	_ = pool.QueryRow(ctx, `SELECT phase FROM tournaments WHERE tournament_id=$1`, "t-svc-cancel").Scan(&phase)
	if phase != "cancelled" {
		t.Fatalf("phase=%s", phase)
	}
}

func TestIntegration_ServiceDurable_CompleteTournamentConcurrentElection(t *testing.T) {
	pool, ts, svc, counted := serviceDurableLifecyclePostgres(t)
	ctx := context.Background()
	_ = prepareFinalViaService(t, pool, ts, svc, "t-svc-elect", 4)
	before := counted.beginExisting.Load()
	payload, _ := json.Marshal(map[string]any{"tournamentId": "t-svc-elect"})

	var wg sync.WaitGroup
	results := make([]envelope.Result, 2)
	errs := make([]error, 2)
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = svc.SubmitCommand(ctx, CommandRequest{
				CommandID: "svc-elect-ct", Type: CmdCompleteTournament, SchemaVersion: envelope.CurrentSchemaVersion,
				Payload: payload,
			}, "svc-elect-ct")
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("err %d: %v", i, err)
		}
		if results[i].Status != envelope.StatusAccepted {
			t.Fatalf("result %d: %+v", i, results[i])
		}
	}
	if counted.beginExisting.Load() != before {
		t.Fatal("concurrent complete must stay on differential path")
	}
	var outbox int64
	_ = pool.QueryRow(ctx, `
		SELECT count(*) FROM outbox_events WHERE tournament_id=$1 AND event_type='TournamentCompleted'
	`, "t-svc-elect").Scan(&outbox)
	if outbox != 1 {
		t.Fatalf("outbox=%d", outbox)
	}
}
