//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/services/tournament-orchestration/store"
	"unoarena/shared/envelope"
)

func postgresURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("TOURNAMENT_POSTGRES_URL")
	if url == "" {
		url = os.Getenv("DATABASE_URL")
	}
	if url == "" {
		t.Skip("TOURNAMENT_POSTGRES_URL not set")
	}
	if err := requireSafeTournamentTestDatabase(url); err != nil {
		t.Fatalf("%v", err)
	}
	return url
}

func applyMigration(t *testing.T, ctx context.Context, pool *store.Pool) {
	t.Helper()
	sqlBytes, err := os.ReadFile(filepath.Join("..", "..", "migrations", "001_init.sql"))
	if err != nil {
		t.Fatalf("migration: %v", err)
	}
	if _, err := pool.Exec(ctx, string(sqlBytes)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS schema_bootstrap_meta`); err != nil {
		t.Fatalf("drop meta: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		CREATE TABLE schema_bootstrap_meta (
			version TEXT NOT NULL,
			checksum TEXT NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		t.Fatalf("meta table: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO schema_bootstrap_meta (version, checksum) VALUES ($1, $2)
	`, store.ExpectedBootstrapVersion, store.ExpectedSchemaChecksum); err != nil {
		t.Fatalf("meta insert: %v", err)
	}
}

func resetPublic(t *testing.T, ctx context.Context, pool *store.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		DROP TABLE IF EXISTS round_advancing_players CASCADE;
		DROP TABLE IF EXISTS tournament_round_slot_players CASCADE;
		DROP TABLE IF EXISTS advancement_records CASCADE;
		DROP TABLE IF EXISTS match_result_quarantines CASCADE;
		DROP TABLE IF EXISTS match_results CASCADE;
		DROP TABLE IF EXISTS assigned_matches CASCADE;
		DROP TABLE IF EXISTS bracket_slots CASCADE;
		DROP TABLE IF EXISTS round_seeding_batches CASCADE;
		DROP TABLE IF EXISTS round_seeding_jobs CASCADE;
		DROP TABLE IF EXISTS round_progress_shards CASCADE;
		DROP TABLE IF EXISTS provisioning_batches CASCADE;
		DROP TABLE IF EXISTS tournament_rounds CASCADE;
		DROP TABLE IF EXISTS tournament_registrations CASCADE;
		DROP TABLE IF EXISTS tournament_registration_shards CASCADE;
		DROP TABLE IF EXISTS bracket_projection_shards CASCADE;
		DROP TABLE IF EXISTS bracket_projection_versions CASCADE;
		DROP TABLE IF EXISTS outbox_events CASCADE;
		DROP TABLE IF EXISTS command_idempotency CASCADE;
		DROP TABLE IF EXISTS kafka_consumer_quarantine CASCADE;
		DROP TABLE IF EXISTS tournaments CASCADE;
		DROP TABLE IF EXISTS schema_migrations CASCADE;
		DROP TABLE IF EXISTS schema_bootstrap_meta CASCADE;
	`); err != nil {
		t.Fatalf("reset: %v", err)
	}
}

func openStore(t *testing.T) (*store.Pool, *store.TournamentStore) {
	t.Helper()
	ctx := context.Background()
	pool, err := store.NewPool(ctx, postgresURL(t))
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(func() { pool.Close() })
	resetPublic(t, ctx, pool)
	applyMigration(t, ctx, pool)
	return pool, store.NewTournamentStore(pool.Pool)
}

func provisionedTournament(t *testing.T, ts *store.TournamentStore, tid string, players []string) (*domain.Tournament, domain.BracketSlot) {
	t.Helper()
	ctx := context.Background()
	tr, _ := domain.CreateTournament(domain.CreateTournamentCommand{
		CommandID: "c", TournamentID: domain.TournamentID(tid), Capacity: len(players),
	})
	for _, p := range players {
		_ = tr.RegisterPlayer(domain.RegisterPlayerCommand{
			CommandID: domain.CommandID("r-" + p), PlayerID: domain.PlayerID(p),
		})
	}
	_ = tr.CloseRegistration(domain.CloseRegistrationCommand{CommandID: "close"})
	_ = tr.SeedRound(domain.SeedRoundCommand{CommandID: "seed", RoundNumber: 1})
	_ = tr.ProvisionRoundMatches(domain.ProvisionRoundMatchesCommand{CommandID: "prov", RoundNumber: 1})
	cmdID := "prov-" + tid
	if err := ts.Commit(ctx, store.CommitRequest{
		Tournament: tr, CommandID: cmdID,
		Outcome:           envelope.Accepted(cmdID, "ProvisionRoundMatches", nil, json.RawMessage(`{}`)),
		ProjectionChanged: true,
	}); err != nil {
		t.Fatal(err)
	}
	got, ok := ts.Get(ctx, domain.TournamentID(tid))
	if !ok {
		t.Fatal("missing tournament")
	}
	round, ok := got.Round(1)
	if !ok || len(round.Slots) == 0 {
		t.Fatal("round missing")
	}
	return got, round.Slots[0]
}

// markRoundMatchingReady transitions provisioning → in_progress (post room assignment)
// so CompleteRound O(64) readiness can evaluate. Does not seed later rounds (T4 handoff).
func markRoundMatchingReady(t *testing.T, pool *store.Pool, tid string, roundNumber int) {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		UPDATE provisioning_batches
		SET status = 'completed', completed_at = COALESCE(completed_at, now()), updated_at = now()
		WHERE tournament_id = $1 AND round_number = $2
		  AND status <> 'quarantined'
	`, tid, roundNumber); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE tournament_rounds
		SET status = 'in_progress'
		WHERE tournament_id = $1 AND round_number = $2
		  AND status IN ('provisioning', 'seeded')
	`, tid, roundNumber); err != nil {
		t.Fatal(err)
	}
}

func standingsFour(a, b, c, d string) []domain.PlayerMatchStanding {
	now := time.Now().UTC()
	return []domain.PlayerMatchStanding{
		{PlayerID: domain.PlayerID(a), MatchWins: 2, CumulativeCardPoints: 10, FinalGameCompletedAt: now},
		{PlayerID: domain.PlayerID(b), MatchWins: 1, CumulativeCardPoints: 9, FinalGameCompletedAt: now},
		{PlayerID: domain.PlayerID(c), MatchWins: 0, CumulativeCardPoints: 8, FinalGameCompletedAt: now},
		{PlayerID: domain.PlayerID(d), MatchWins: 0, CumulativeCardPoints: 7, FinalGameCompletedAt: now},
	}
}

func standingsFromSeeded(players []domain.PlayerID) []domain.PlayerMatchStanding {
	now := time.Now().UTC()
	out := make([]domain.PlayerMatchStanding, len(players))
	for i, p := range players {
		out[i] = domain.PlayerMatchStanding{
			PlayerID: p, MatchWins: len(players) - i, CumulativeCardPoints: 100 - i,
			FinalGameCompletedAt: now.Add(time.Duration(i) * time.Millisecond),
		}
	}
	return out
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func TestIntegration_VerifySchemaExactAndDrift(t *testing.T) {
	ctx := context.Background()
	pool, _ := openStore(t)
	if err := store.VerifySchema(ctx, pool.Pool, store.DefaultSchemaExpectation()); err != nil {
		t.Fatalf("exact schema should pass: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE schema_bootstrap_meta SET checksum = 'deadbeef'`); err != nil {
		t.Fatal(err)
	}
	if err := store.VerifySchema(ctx, pool.Pool, store.DefaultSchemaExpectation()); err == nil {
		t.Fatal("drift checksum must fail")
	}
}

func TestIntegration_AtomicRollbackAndExactReplay(t *testing.T) {
	ctx := context.Background()
	_, ts := openStore(t)
	tr, out := domain.CreateTournament(domain.CreateTournamentCommand{
		CommandID: "c-create", TournamentID: "t1", Capacity: 4,
	})
	if !out.Accepted() {
		t.Fatal(out)
	}
	payload, _ := json.Marshal(map[string]any{"facts": []any{}})
	res := envelope.Accepted("c-create", "CreateTournament", nil, payload)
	if err := ts.Commit(ctx, store.CommitRequest{
		Tournament: tr, CommandID: "c-create", Outcome: res,
		Events: []store.OutboxEvent{{
			EventID: "c-create:TournamentMatchAssigned:0", EventType: "TournamentMatchAssigned",
			TournamentID: "t1", Topic: "tournament.match.assigned", PartitionKey: "t1",
			SchemaVersion: 1, Payload: map[string]any{"tournamentId": "t1", "schemaVersion": 1}, CreatedAt: time.Now().UTC(),
		}},
	}); err != nil {
		t.Fatal(err)
	}
	n, _ := ts.CountOutbox(ctx)
	if n != 1 {
		t.Fatalf("outbox=%d", n)
	}

	tr2, ok := ts.Get(ctx, "t1")
	if !ok || tr2.RegisteredCount() != 0 {
		t.Fatal("hydrate create")
	}
	out2 := tr2.RegisterPlayer(domain.RegisterPlayerCommand{CommandID: "c-reg", PlayerID: "p1"})
	if !out2.Accepted() {
		t.Fatal(out2)
	}
	ts.FailNextCommits = 1
	regRes := envelope.Accepted("c-reg", "RegisterPlayer", nil, payload)
	if err := ts.Commit(ctx, store.CommitRequest{Tournament: tr2, CommandID: "c-reg", Outcome: regRes}); err == nil {
		t.Fatal("expected injected failure")
	}
	tr3, _ := ts.Get(ctx, "t1")
	if tr3.IsRegistered("p1") {
		t.Fatal("rollback must not persist registration")
	}
	if _, ok := ts.LookupOutcome(ctx, "c-reg"); ok {
		t.Fatal("outcome must not install on rollback")
	}

	// Exact replay after failure.
	tr4, _ := ts.Get(ctx, "t1")
	_ = tr4.RegisterPlayer(domain.RegisterPlayerCommand{CommandID: "c-reg", PlayerID: "p1"})
	if err := ts.Commit(ctx, store.CommitRequest{Tournament: tr4, CommandID: "c-reg", Outcome: regRes}); err != nil {
		t.Fatal(err)
	}
	if err := ts.Commit(ctx, store.CommitRequest{Tournament: tr4, CommandID: "c-reg", Outcome: regRes}); err != nil {
		t.Fatal(err)
	}
	got, _ := ts.Get(ctx, "t1")
	if !got.IsRegistered("p1") {
		t.Fatal("replay must persist once")
	}
	n2, _ := ts.CountOutbox(ctx)
	if n2 != 1 {
		t.Fatalf("register without facts must not duplicate create outbox: %d", n2)
	}
}

func TestIntegration_NoDuplicateOutboxOnRetry(t *testing.T) {
	ctx := context.Background()
	_, ts := openStore(t)
	tr, _ := domain.CreateTournament(domain.CreateTournamentCommand{
		CommandID: "c1", TournamentID: "t-out", Capacity: 2,
	})
	ev := store.OutboxEvent{
		EventID: "same-event", EventType: "TournamentMatchAssigned", TournamentID: "t-out",
		Topic: "tournament.match.assigned", PartitionKey: "t-out", SchemaVersion: 1,
		Payload: map[string]any{"tournamentId": "t-out", "schemaVersion": 1}, CreatedAt: time.Now().UTC(),
	}
	res := envelope.Accepted("c1", "CreateTournament", nil, json.RawMessage(`{"facts":[]}`))
	if err := ts.Commit(ctx, store.CommitRequest{Tournament: tr, CommandID: "c1", Outcome: res, Events: []store.OutboxEvent{ev}}); err != nil {
		t.Fatal(err)
	}
	// Different command cannot re-insert same event_id.
	tr2, _ := ts.Get(ctx, "t-out")
	res2 := envelope.Accepted("c2", "RegisterPlayer", nil, json.RawMessage(`{"facts":[]}`))
	_ = tr2.RegisterPlayer(domain.RegisterPlayerCommand{CommandID: "c2", PlayerID: "p1"})
	if err := ts.Commit(ctx, store.CommitRequest{Tournament: tr2, CommandID: "c2", Outcome: res2, Events: []store.OutboxEvent{ev}}); err != nil {
		t.Fatal(err)
	}
	n, _ := ts.CountOutbox(ctx)
	if n != 1 {
		t.Fatalf("duplicate event_id must not insert twice: %d", n)
	}
}

func TestIntegration_HydrateRestoreComplete(t *testing.T) {
	ctx := context.Background()
	_, ts := openStore(t)
	tr, _ := domain.CreateTournament(domain.CreateTournamentCommand{
		CommandID: "c", TournamentID: "t-h", Capacity: 4, RetryBudget: 5, BatchSize: 2,
	})
	for _, p := range []string{"a", "b", "c", "d"} {
		_ = tr.RegisterPlayer(domain.RegisterPlayerCommand{
			CommandID: domain.CommandID("r-" + p), PlayerID: domain.PlayerID(p),
		})
	}
	_ = tr.CloseRegistration(domain.CloseRegistrationCommand{CommandID: "close"})
	_ = tr.SeedRound(domain.SeedRoundCommand{CommandID: "seed", RoundNumber: 1})
	_ = tr.ProvisionRoundMatches(domain.ProvisionRoundMatchesCommand{CommandID: "prov", RoundNumber: 1})
	res := envelope.Accepted("prov", "ProvisionRoundMatches", nil, json.RawMessage(`{"facts":[]}`))
	if err := ts.Commit(ctx, store.CommitRequest{Tournament: tr, CommandID: "prov", Outcome: res}); err != nil {
		t.Fatal(err)
	}
	got, ok := ts.Get(ctx, "t-h")
	if !ok {
		t.Fatal("missing")
	}
	if got.Phase() != domain.PhaseInProgress {
		t.Fatalf("phase=%s want in_progress", got.Phase())
	}
	if got.RetryBudget() != 5 || got.BatchSize() != 2 {
		t.Fatalf("rules hydrate retry=%d batch=%d", got.RetryBudget(), got.BatchSize())
	}
	if got.RegisteredCount() != 4 {
		t.Fatalf("registered=%d", got.RegisteredCount())
	}
	round, ok := got.Round(1)
	if !ok || len(round.Slots) == 0 {
		t.Fatal("round/slots missing")
	}
	if !round.Slots[0].RoomID.Valid() {
		t.Fatal("assigned room missing after hydrate")
	}
}

func TestIntegration_MatchCompletedRestartExactReplay(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)
	_, slot := provisionedTournament(t, ts, "t-restart", []string{"a1", "a2", "a3", "a4"})
	standings := standingsFour("a1", "a2", "a3", "a4")

	uow, err := ts.BeginExisting(ctx, "t-restart")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = uow.Rollback() }()
	cur := uow.Loaded()
	out := cur.RecordMatchResult(domain.RecordMatchResultCommand{
		CommandID: "ingest:evt-restart", EventID: "evt-restart",
		RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
		CompletionVersion: 3, Standings: standings,
	})
	if !out.Accepted() || len(out.Facts) == 0 {
		t.Fatalf("expected recorded facts: %+v", out)
	}
	evs := []store.OutboxEvent{{
		EventID: "ingest:evt-restart:TournamentMatchResultRecorded:0", EventType: "TournamentMatchResultRecorded",
		TournamentID: "t-restart", Topic: "tournament.match.result_recorded", PartitionKey: "t-restart",
		SchemaVersion: 1, Payload: map[string]any{"roomId": string(slot.RoomID), "schemaVersion": 1}, CreatedAt: time.Now().UTC(),
	}, {
		EventID: "ingest:evt-restart:PlayersAdvanced:1", EventType: "PlayersAdvanced",
		TournamentID: "t-restart", Topic: "tournament.players.advanced", PartitionKey: "t-restart",
		SchemaVersion: 1, Payload: map[string]any{"roomId": string(slot.RoomID), "schemaVersion": 1}, CreatedAt: time.Now().UTC(),
	}}
	payload, _ := json.Marshal(map[string]any{"facts": []any{
		map[string]any{"name": "TournamentMatchResultRecorded"},
		map[string]any{"name": "PlayersAdvanced"},
	}})
	if err := uow.Commit(store.CommitRequest{
		Tournament: cur, CommandID: "ingest:evt-restart",
		Outcome: envelope.Accepted("ingest:evt-restart", "RecordMatchResult", nil, payload),
		Events:  evs,
		MatchResultSource: &store.MatchResultSource{
			EventID: "evt-restart", RoomID: string(slot.RoomID), CompletionVersion: 3,
		},
	}); err != nil {
		t.Fatal(err)
	}
	nBefore, err := ts.CountOutbox(ctx)
	if err != nil || nBefore != 2 {
		t.Fatalf("outbox before replay=%d err=%v", nBefore, err)
	}

	// Independent store instance (restart / second replica reader).
	ts2 := store.NewTournamentStore(pool.Pool)
	hydrated, ok := ts2.Get(ctx, "t-restart")
	if !ok {
		t.Fatal("hydrate after restart")
	}
	processed := hydrated.ProcessedEventsSnapshot()
	prior, ok := processed["evt-restart"]
	if !ok {
		t.Fatal("processedEvents must restore source_event_id")
	}
	keys := hydrated.ResultKeysSnapshot()
	key := string(slot.RoomID) + ":3"
	if keys[key].Disposition != domain.DispositionRecorded || keys[key].SourceEventID != "evt-restart" {
		t.Fatalf("result key=%+v", keys[key])
	}

	uow2, err := ts2.BeginExisting(ctx, "t-restart")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = uow2.Rollback() }()
	if priorOutcome, ok := uow2.LookupOutcome("ingest:evt-restart"); !ok {
		t.Fatal("command outcome must survive restart")
	} else if dispositionFromPayload(priorOutcome) != "recorded" {
		t.Fatalf("disposition=%s payload=%s", dispositionFromPayload(priorOutcome), priorOutcome.Payload)
	}
	replay := uow2.Loaded().RecordMatchResult(domain.RecordMatchResultCommand{
		CommandID: "ingest:evt-restart", EventID: "evt-restart",
		RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
		CompletionVersion: 3, Standings: standings,
	})
	if replay.Kind != domain.OutcomeDuplicate {
		t.Fatalf("expected duplicate from processedEvents, got %+v", replay)
	}
	if len(replay.Facts) == 0 {
		t.Fatalf("exact stored disposition facts required, prior=%+v replay=%+v", prior, replay)
	}
	if err := uow2.Commit(store.CommitRequest{
		Tournament: uow2.Loaded(), CommandID: "ingest:evt-restart",
		Outcome: envelope.Accepted("ingest:evt-restart", "RecordMatchResult", nil, payload),
		Events:  evs,
		MatchResultSource: &store.MatchResultSource{
			EventID: "evt-restart", RoomID: string(slot.RoomID), CompletionVersion: 3,
		},
	}); err != nil {
		t.Fatal(err)
	}
	nAfter, _ := ts2.CountOutbox(ctx)
	if nAfter != nBefore {
		t.Fatalf("duplicate replay must not emit second outbox: before=%d after=%d", nBefore, nAfter)
	}
	got, _ := ts2.Get(ctx, "t-restart")
	r, _ := got.Round(1)
	if !r.Slots[0].HasResult || len(r.Slots[0].Advancing) == 0 {
		t.Fatalf("advancement must remain: %+v", r.Slots[0])
	}
}

func dispositionFromPayload(r envelope.Result) string {
	payload := string(r.Payload)
	if strings.Contains(payload, "TournamentResultQuarantined") {
		return "quarantined"
	}
	if strings.Contains(payload, "TournamentMatchResultRecorded") {
		return "recorded"
	}
	if strings.Contains(payload, `"facts":[]`) || strings.Contains(payload, `"facts": []`) {
		return "duplicate_ignored"
	}
	return "accepted"
}

func TestIntegration_ConcurrentDuplicateAndDistinctEvents(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)
	players := make([]string, 16)
	for i := range players {
		players[i] = "p" + itoa(i)
	}
	_, _ = provisionedTournament(t, ts, "t-c", players)
	base, ok := ts.Get(ctx, "t-c")
	if !ok {
		t.Fatal("missing")
	}
	round, _ := base.Round(1)
	if len(round.Slots) < 2 {
		t.Fatalf("need two slots for distinct concurrent events, got %d", len(round.Slots))
	}
	slotA, slotB := round.Slots[0], round.Slots[1]
	standingsA := standingsFromSeeded(slotA.SeededPlayers)
	standingsB := standingsFromSeeded(slotB.SeededPlayers)
	ts2 := store.NewTournamentStore(pool.Pool)

	type attempt struct {
		store     *store.TournamentStore
		eventID   string
		commandID string
		slot      domain.BracketSlot
		standings []domain.PlayerMatchStanding
		version   uint64
	}
	attempts := []attempt{
		{ts, "evt-dup", "ingest:evt-dup", slotA, standingsA, 3},
		{ts2, "evt-dup", "ingest:evt-dup", slotA, standingsA, 3},
		{ts, "evt-other", "ingest:evt-other", slotB, standingsB, 3},
		{ts2, "evt-other", "ingest:evt-other", slotB, standingsB, 3},
		{ts, "evt-dup", "ingest:evt-dup", slotA, standingsA, 3},
		{ts2, "evt-dup", "ingest:evt-dup", slotA, standingsA, 3},
		{ts, "evt-other", "ingest:evt-other", slotB, standingsB, 3},
		{ts2, "evt-other", "ingest:evt-other", slotB, standingsB, 3},
	}

	var wg sync.WaitGroup
	errs := make(chan error, len(attempts))
	for _, a := range attempts {
		a := a
		wg.Add(1)
		go func() {
			defer wg.Done()
			uow, err := a.store.BeginExisting(ctx, "t-c")
			if err != nil {
				errs <- err
				return
			}
			defer func() { _ = uow.Rollback() }()
			if _, ok := uow.LookupOutcome(a.commandID); ok {
				return
			}
			if !uow.Exists() {
				errs <- errMissing
				return
			}
			cur := uow.Loaded()
			_ = cur.RecordMatchResult(domain.RecordMatchResultCommand{
				CommandID: domain.CommandID(a.commandID), EventID: domain.EventID(a.eventID),
				RoomID: a.slot.RoomID, RoundNumber: 1, SlotID: a.slot.SlotID,
				CompletionVersion: domain.CompletionVersion(a.version), Standings: a.standings,
			})
			res := envelope.Accepted(a.commandID, "RecordMatchResult", nil, json.RawMessage(`{"facts":[{"name":"TournamentMatchResultRecorded"}]}`))
			if err := uow.Commit(store.CommitRequest{
				Tournament: cur, CommandID: a.commandID, Outcome: res,
				Events: []store.OutboxEvent{{
					EventID: a.commandID + ":TournamentMatchResultRecorded:0", EventType: "TournamentMatchResultRecorded",
					TournamentID: "t-c", Topic: "tournament.match.result_recorded", PartitionKey: "t-c",
					SchemaVersion: 1, Payload: map[string]any{"eventId": a.eventID, "schemaVersion": 1}, CreatedAt: time.Now().UTC(),
				}},
				MatchResultSource: &store.MatchResultSource{
					EventID: a.eventID, RoomID: string(a.slot.RoomID), CompletionVersion: a.version,
				},
			}); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("goroutine commit error: %v", err)
		}
	}

	got, _ := ts.Get(ctx, "t-c")
	r, _ := got.Round(1)
	var sawA, sawB bool
	for _, s := range r.Slots {
		if s.RoomID == slotA.RoomID {
			sawA = s.HasResult && len(s.Advancing) > 0
		}
		if s.RoomID == slotB.RoomID {
			sawB = s.HasResult && len(s.Advancing) > 0
		}
	}
	if !sawA || !sawB {
		t.Fatalf("both slots must keep accepted advancement under concurrency: %+v", r.Slots)
	}
	keys := got.ResultKeysSnapshot()
	if keys[string(slotA.RoomID)+":3"].Disposition != domain.DispositionRecorded {
		t.Fatalf("evt-dup key=%+v", keys[string(slotA.RoomID)+":3"])
	}
	if keys[string(slotB.RoomID)+":3"].Disposition != domain.DispositionRecorded {
		t.Fatalf("evt-other key=%+v", keys[string(slotB.RoomID)+":3"])
	}
	processed := got.ProcessedEventsSnapshot()
	if _, ok := processed["evt-dup"]; !ok {
		t.Fatal("evt-dup must remain in processedEvents")
	}
	if _, ok := processed["evt-other"]; !ok {
		t.Fatal("evt-other must remain in processedEvents")
	}
	n, _ := ts.CountOutbox(ctx)
	if n != 2 {
		t.Fatalf("exactly one outbox row per distinct event command, got %d", n)
	}
	if err := ts.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}

var errMissing = errString("tournament missing under lock")

type errString string

func (e errString) Error() string { return string(e) }

func TestIntegration_QuarantinedNoAdvancement(t *testing.T) {
	ctx := context.Background()
	_, ts := openStore(t)
	_, slot := provisionedTournament(t, ts, "t-q", []string{"a1", "a2", "a3", "a4"})
	uow, err := ts.BeginExisting(ctx, "t-q")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = uow.Rollback() }()
	cur := uow.Loaded()
	out := cur.RecordMatchResult(domain.RecordMatchResultCommand{
		CommandID: "ingest:ab", EventID: "ab",
		RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
		CompletionVersion: 1, Standings: standingsFour("a1", "a2", "a3", "a4"),
		IsAbandoned: true,
	})
	if len(out.Facts) == 0 {
		t.Fatal("expected quarantine fact")
	}
	if err := uow.Commit(store.CommitRequest{
		Tournament: cur, CommandID: "ingest:ab",
		Outcome: envelope.Accepted("ingest:ab", "RecordMatchResult", nil, json.RawMessage(`{}`)),
		MatchResultSource: &store.MatchResultSource{
			EventID: "ab", RoomID: string(slot.RoomID), CompletionVersion: 1,
		},
	}); err != nil {
		t.Fatal(err)
	}
	got, _ := ts.Get(ctx, "t-q")
	r, _ := got.Round(1)
	if len(r.Slots[0].Advancing) != 0 || r.Slots[0].HasResult {
		t.Fatalf("quarantine must not advance: %+v", r.Slots[0])
	}
	if got.ResultKeysSnapshot()[string(slot.RoomID)+":1"].SourceEventID != "ab" {
		t.Fatal("quarantine source_event_id must persist")
	}
}

func TestIntegration_DBReconnect(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)
	if err := ts.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	// Drop and recreate pool against same DSN — proves reconnect path.
	dsn := postgresURL(t)
	pool.Close()
	pool2, err := store.NewPool(ctx, dsn)
	if err != nil {
		t.Fatalf("reconnect pool: %v", err)
	}
	defer pool2.Close()
	ts2 := store.NewTournamentStore(pool2.Pool)
	if err := ts2.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}
