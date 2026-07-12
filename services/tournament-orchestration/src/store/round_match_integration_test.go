//go:build integration

package store_test

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

func playersN(n int) []string {
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = fmt.Sprintf("p%d", i)
	}
	return out
}

func TestIntegration_RoundProgressShards_RebuildParity(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	_, slot := provisionedTournament(t, ts, "t-shard-rebuild", playersN(12))
	_ = slot

	var sumAssigned, nAssigned, nShards int
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(assigned_count),0), COUNT(*) FROM round_progress_shards WHERE tournament_id = $1
	`, "t-shard-rebuild").Scan(&sumAssigned, &nShards); err != nil {
		t.Fatalf("shards: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM assigned_matches WHERE tournament_id = $1
	`, "t-shard-rebuild").Scan(&nAssigned); err != nil {
		t.Fatalf("assigned: %v", err)
	}
	if nShards != domain.ProgressShardCount {
		t.Fatalf("shards=%d want %d", nShards, domain.ProgressShardCount)
	}
	if sumAssigned != nAssigned {
		t.Fatalf("SUM(assigned_count)=%d assigned_matches=%d", sumAssigned, nAssigned)
	}
	ready, err := ts.LoadRoundProgressReadiness(ctx, "t-shard-rebuild", 1)
	if err != nil {
		t.Fatal(err)
	}
	if ready.AssignedCount != nAssigned || ready.Ready {
		t.Fatalf("readiness=%+v", ready)
	}
}

func TestIntegration_Differential_ExactDuplicatePreservesRecorded(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	tr, slot := provisionedTournament(t, ts, "t-dup", playersN(12))
	standings := standingsFromSeeded(slot.SeededPlayers)

	cmd1 := domain.RecordMatchResultCommand{
		CommandID: "ingest:evt-1", EventID: "evt-1",
		RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
		CompletionVersion: 7, Standings: standings,
	}
	ingestDifferential(t, ts, "t-dup", cmd1, "corr-1")

	var disp string
	var outboxBefore int64
	if err := pool.QueryRow(ctx, `SELECT disposition FROM match_results WHERE room_id=$1 AND completion_version=7`, string(slot.RoomID)).Scan(&disp); err != nil {
		t.Fatal(err)
	}
	if disp != "recorded" {
		t.Fatalf("disp=%s", disp)
	}
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE tournament_id=$1`, "t-dup").Scan(&outboxBefore)
	pageBefore, err := ts.LoadBracketPage(ctx, store.BracketPageQuery{TournamentID: "t-dup", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	projBefore := pageBefore.ProjectionVersion
	var advBefore int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM advancement_records WHERE tournament_id=$1`, "t-dup").Scan(&advBefore)

	cmd2 := domain.RecordMatchResultCommand{
		CommandID: "ingest:evt-2", EventID: "evt-2",
		RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
		CompletionVersion: 7, Standings: standings,
	}
	ingestDifferential(t, ts, "t-dup", cmd2, "corr-2")

	if err := pool.QueryRow(ctx, `SELECT disposition FROM match_results WHERE room_id=$1 AND completion_version=7`, string(slot.RoomID)).Scan(&disp); err != nil {
		t.Fatal(err)
	}
	if disp != "recorded" {
		t.Fatalf("exact dup must preserve recorded, got %s", disp)
	}
	var outboxAfter int64
	var advAfter int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE tournament_id=$1`, "t-dup").Scan(&outboxAfter)
	pageAfter, err := ts.LoadBracketPage(ctx, store.BracketPageQuery{TournamentID: "t-dup", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM advancement_records WHERE tournament_id=$1`, "t-dup").Scan(&advAfter)
	if outboxAfter != outboxBefore {
		t.Fatalf("outbox changed %d -> %d", outboxBefore, outboxAfter)
	}
	if pageAfter.ProjectionVersion != projBefore {
		t.Fatalf("projection version changed %d -> %d", projBefore, pageAfter.ProjectionVersion)
	}
	if advAfter != advBefore {
		t.Fatalf("advancement duplicated %d -> %d", advBefore, advAfter)
	}
	var cmds int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM command_idempotency WHERE command_id IN ('ingest:evt-1','ingest:evt-2')`).Scan(&cmds)
	if cmds != 2 {
		t.Fatalf("want 2 command outcomes, got %d", cmds)
	}

	// Counters unchanged on exact duplicate.
	ready, _ := ts.LoadRoundProgressReadiness(ctx, "t-dup", 1)
	if ready.ResolvedCount != 1 {
		t.Fatalf("resolved=%d", ready.ResolvedCount)
	}
	_ = tr
}

func TestIntegration_Differential_QuarantineUnresolvedBlocks(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	_, slot := provisionedTournament(t, ts, "t-qblock", playersN(12))
	standings := standingsFromSeeded(slot.SeededPlayers)
	cmd := domain.RecordMatchResultCommand{
		CommandID: "ingest:ab", EventID: "ab",
		RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
		CompletionVersion: 3, Standings: standings, IsAbandoned: true,
	}
	ingestDifferential(t, ts, "t-qblock", cmd, "c")

	var status, roundStatus string
	_ = pool.QueryRow(ctx, `SELECT status FROM bracket_slots WHERE tournament_id=$1 AND slot_id=$2`, "t-qblock", string(slot.SlotID)).Scan(&status)
	_ = pool.QueryRow(ctx, `SELECT status FROM tournament_rounds WHERE tournament_id=$1 AND round_number=1`, "t-qblock").Scan(&roundStatus)
	if status != "quarantined" {
		t.Fatalf("slot=%s", status)
	}
	if roundStatus != "blocked" {
		t.Fatalf("round=%s", roundStatus)
	}
	ready, _ := ts.LoadRoundProgressReadiness(ctx, "t-qblock", 1)
	if ready.QuarantinedCount != 1 || ready.Ready {
		t.Fatalf("readiness=%+v", ready)
	}
}

func TestIntegration_Differential_ConcurrentSameRoomDuplicate(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	_, slot := provisionedTournament(t, ts, "t-conc-same", playersN(12))
	standings := standingsFromSeeded(slot.SeededPlayers)

	var recorded atomic.Int32
	var dups atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cmd := domain.RecordMatchResultCommand{
				CommandID: domain.CommandID(fmt.Sprintf("ingest:same-%d", i)),
				EventID:   domain.EventID(fmt.Sprintf("same-%d", i)),
				RoomID:    slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
				CompletionVersion: 11, Standings: standings,
			}
			kind := ingestDifferentialKind(t, ts, "t-conc-same", cmd)
			switch kind {
			case domain.RoundMatchRecord:
				recorded.Add(1)
			case domain.RoundMatchExactDuplicate:
				dups.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if recorded.Load() != 1 {
		t.Fatalf("recorded=%d want 1", recorded.Load())
	}
	if dups.Load() != 7 {
		t.Fatalf("dups=%d want 7", dups.Load())
	}
	var nResults, nAdv int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_results WHERE room_id=$1 AND completion_version=11`, string(slot.RoomID)).Scan(&nResults)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM advancement_records WHERE tournament_id=$1 AND slot_id=$2`, "t-conc-same", string(slot.SlotID)).Scan(&nAdv)
	if nResults != 1 || nAdv != 1 {
		t.Fatalf("results=%d adv=%d", nResults, nAdv)
	}
}

func TestIntegration_Differential_ConcurrentDistinctRoomsAcrossShards(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	tr, _ := provisionedTournament(t, ts, "t-conc-rooms", playersN(40))
	round, _ := tr.Round(1)
	if len(round.Slots) < 2 {
		t.Fatal("need multiple slots")
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(round.Slots))
	for i, slot := range round.Slots {
		wg.Add(1)
		go func(i int, slot domain.BracketSlot) {
			defer wg.Done()
			cmd := domain.RecordMatchResultCommand{
				CommandID: domain.CommandID(fmt.Sprintf("ingest:room-%d", i)),
				EventID:   domain.EventID(fmt.Sprintf("room-%d", i)),
				RoomID:    slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
				CompletionVersion: domain.CompletionVersion(100 + i),
				Standings:         standingsFromSeeded(slot.SeededPlayers),
			}
			if kind := ingestDifferentialKind(t, ts, "t-conc-rooms", cmd); kind != domain.RoundMatchRecord {
				errCh <- fmt.Errorf("slot %d kind=%s", i, kind)
			}
		}(i, slot)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	var nResults int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_results WHERE tournament_id=$1 AND disposition='recorded'`, "t-conc-rooms").Scan(&nResults)
	if nResults != len(round.Slots) {
		t.Fatalf("results=%d want %d", nResults, len(round.Slots))
	}
	ready, _ := ts.LoadRoundProgressReadiness(ctx, "t-conc-rooms", 1)
	if ready.ResolvedCount != len(round.Slots) {
		t.Fatalf("resolved=%d want %d", ready.ResolvedCount, len(round.Slots))
	}
}

func TestIntegration_RewriteBarrier_BothDirections(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	tr, slot := provisionedTournament(t, ts, "t-barrier", playersN(12))
	standings := standingsFromSeeded(slot.SeededPlayers)

	// Differential holds shared; legacy exclusive must wait then see committed state.
	uow, err := ts.BeginRoundMatch(ctx, "t-barrier", string(slot.RoomID), 1, string(slot.SlotID), "ingest:bar-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := uow.AttachPriorResult(string(slot.RoomID), 21); err != nil {
		t.Fatal(err)
	}
	cmd := domain.RecordMatchResultCommand{
		CommandID: "ingest:bar-1", EventID: "bar-1",
		RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
		CompletionVersion: 21, Standings: standings,
	}
	d := domain.DecideRecordMatchResult(uow.Loaded(), cmd)
	legacyDone := make(chan error, 1)
	go func() {
		legacyCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// Brief yield so this goroutine is waiting on exclusive before differential commits.
		time.Sleep(20 * time.Millisecond)
		legacy, err := ts.BeginExisting(legacyCtx, "t-barrier")
		if err != nil {
			legacyDone <- err
			return
		}
		defer func() { _ = legacy.Rollback() }()
		loaded := legacy.Loaded()
		round, _ := loaded.Round(1)
		found := false
		for _, s := range round.Slots {
			if s.SlotID == slot.SlotID && s.HasResult {
				found = true
			}
		}
		if !found {
			legacyDone <- fmt.Errorf("legacy did not observe differential result")
			return
		}
		legacyDone <- nil
	}()
	if err := uow.Commit(store.RoundMatchCommitRequest{
		TournamentID: "t-barrier", CommandID: "ingest:bar-1", CommandType: "RecordMatchResult",
		Outcome:  envelope.Accepted("ingest:bar-1", "RecordMatchResult", nil, json.RawMessage(`{"facts":[{"name":"TournamentMatchResultRecorded"}]}`)),
		Decision: d, Command: cmd, ProjectionChanged: true,
		MatchResultSource: &store.MatchResultSource{EventID: "bar-1", RoomID: string(slot.RoomID), CompletionVersion: 21},
		Events: []store.OutboxEvent{{
			EventID: "ingest:bar-1:TournamentMatchResultRecorded:0", EventType: "TournamentMatchResultRecorded",
			TournamentID: "t-barrier", Topic: "tournament.match.result.recorded", PartitionKey: "t-barrier",
			SchemaVersion: 1, Payload: map[string]any{"schemaVersion": 1}, CreatedAt: time.Now().UTC(),
		}},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-legacyDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("legacy BeginExisting blocked beyond timeout")
	}

	// Legacy exclusive then differential waits and survives rewrite.
	legacy, err := ts.BeginExisting(ctx, "t-barrier")
	if err != nil {
		t.Fatal(err)
	}
	diffDone := make(chan error, 1)
	go func() {
		diffCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		time.Sleep(20 * time.Millisecond)
		other := roundOtherSlot(t, ts, "t-barrier", slot.SlotID)
		u2, err := ts.BeginRoundMatch(diffCtx, "t-barrier", string(other.RoomID), 1, string(other.SlotID), "ingest:bar-2")
		if err != nil {
			diffDone <- err
			return
		}
		defer func() { _ = u2.Rollback() }()
		cmd2 := domain.RecordMatchResultCommand{
			CommandID: "ingest:bar-2", EventID: "bar-2",
			RoomID: other.RoomID, RoundNumber: 1, SlotID: other.SlotID,
			CompletionVersion: 22, Standings: standingsFromSeeded(other.SeededPlayers),
		}
		if err := u2.AttachPriorResult(string(other.RoomID), 22); err != nil {
			diffDone <- err
			return
		}
		d2 := domain.DecideRecordMatchResult(u2.Loaded(), cmd2)
		diffDone <- u2.Commit(store.RoundMatchCommitRequest{
			TournamentID: "t-barrier", CommandID: "ingest:bar-2", CommandType: "RecordMatchResult",
			Outcome:  envelope.Accepted("ingest:bar-2", "RecordMatchResult", nil, json.RawMessage(`{"facts":[]}`)),
			Decision: d2, Command: cmd2, ProjectionChanged: true,
			MatchResultSource: &store.MatchResultSource{EventID: "bar-2", RoomID: string(other.RoomID), CompletionVersion: 22},
		})
	}()
	cur := legacy.Loaded()
	_ = tr
	if err := legacy.Commit(store.CommitRequest{
		Tournament: cur, CommandID: "legacy-rewrite", Outcome: envelope.Accepted("legacy-rewrite", "SeedRound", nil, json.RawMessage(`{}`)),
		ProjectionChanged: false,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-diffDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("differential BeginRoundMatch blocked beyond timeout")
	}
	var n int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_results WHERE tournament_id=$1 AND disposition='recorded'`, "t-barrier").Scan(&n)
	if n < 2 {
		t.Fatalf("want differential to survive rewrite, recorded=%d", n)
	}
}

func roundOtherSlot(t *testing.T, ts *store.TournamentStore, tid string, exclude domain.SlotID) domain.BracketSlot {
	t.Helper()
	got, ok := ts.Get(context.Background(), domain.TournamentID(tid))
	if !ok {
		t.Fatal("missing")
	}
	round, _ := got.Round(1)
	for _, s := range round.Slots {
		if s.SlotID != exclude && s.RoomID.Valid() {
			return s
		}
	}
	t.Fatal("no other slot")
	return domain.BracketSlot{}
}

func ingestDifferential(t *testing.T, ts *store.TournamentStore, tid string, cmd domain.RecordMatchResultCommand, corr string) {
	t.Helper()
	kind := ingestDifferentialKind(t, ts, tid, cmd)
	if kind != domain.RoundMatchRecord && kind != domain.RoundMatchExactDuplicate &&
		kind != domain.RoundMatchQuarantineUnresolved && kind != domain.RoundMatchQuarantineConflict &&
		kind != domain.RoundMatchQuarantineHeld {
		t.Fatalf("unexpected kind %s", kind)
	}
	_ = corr
}

func ingestDifferentialKind(t *testing.T, ts *store.TournamentStore, tid string, cmd domain.RecordMatchResultCommand) domain.RoundMatchDecisionKind {
	t.Helper()
	ctx := context.Background()
	uow, err := ts.BeginRoundMatch(ctx, tid, string(cmd.RoomID), cmd.RoundNumber, string(cmd.SlotID), string(cmd.CommandID))
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = uow.Rollback() }()
	if prior, ok := uow.LookupOutcome(string(cmd.CommandID)); ok {
		_ = prior
		return domain.RoundMatchExactDuplicate
	}
	if err := uow.AttachPriorResult(string(cmd.RoomID), uint64(cmd.CompletionVersion)); err != nil {
		t.Fatalf("prior: %v", err)
	}
	loaded := uow.Loaded()
	if cmd.RoundNumber < 1 {
		cmd.RoundNumber = loaded.RoundNumber
	}
	if !cmd.SlotID.Valid() {
		cmd.SlotID = loaded.Slot.SlotID
	}
	d := domain.DecideRecordMatchResult(loaded, cmd)
	factsPayload := make([]map[string]any, 0, len(d.Outcome.Facts))
	for _, f := range d.Outcome.Facts {
		factsPayload = append(factsPayload, map[string]any{"name": string(f.Name), "data": f.Data})
	}
	payload, _ := json.Marshal(map[string]any{"facts": factsPayload})
	var events []store.OutboxEvent
	now := time.Now().UTC()
	for i, f := range d.Outcome.Facts {
		events = append(events, store.OutboxEvent{
			EventID: fmt.Sprintf("%s:%s:%d", cmd.CommandID, f.Name, i), EventType: string(f.Name),
			TournamentID: tid, Topic: "tournament.test", PartitionKey: tid, SchemaVersion: 1,
			Payload: map[string]any{"schemaVersion": 1}, CreatedAt: now,
		})
	}
	proj := len(d.Outcome.Facts) > 0
	switch d.Kind {
	case domain.RoundMatchExactDuplicate, domain.RoundMatchDuplicateEvent, domain.RoundMatchQuarantineHeld:
		proj = false
	case domain.RoundMatchQuarantineUnresolved:
		if !d.AffectsSlot {
			proj = false
		}
	}
	if err := uow.Commit(store.RoundMatchCommitRequest{
		TournamentID: tid, CommandID: string(cmd.CommandID), CommandType: "RecordMatchResult",
		Outcome: envelope.Accepted(string(cmd.CommandID), "RecordMatchResult", nil, payload),
		Events:  events, Decision: d, Command: cmd, ProjectionChanged: proj,
		MatchResultSource: &store.MatchResultSource{
			EventID: string(cmd.EventID), RoomID: string(cmd.RoomID), CompletionVersion: uint64(cmd.CompletionVersion),
		},
	}); err != nil {
		if _, ok := store.AsPriorCommandOutcome(err); ok {
			return d.Kind
		}
		t.Fatalf("commit: %v", err)
	}
	return d.Kind
}

func TestIntegration_ProjectionShards_DistinctDoNotShareRow(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	tr, _ := provisionedTournament(t, ts, "t-proj-shard", playersN(40))
	round, _ := tr.Round(1)

	var slotA, slotB domain.BracketSlot
	for _, s := range round.Slots {
		if domain.ProgressShardID(s.Index) == 0 && s.RoomID.Valid() {
			slotA = s
			break
		}
	}
	if !slotA.RoomID.Valid() {
		t.Fatal("need slot on shard 0")
	}
	for _, s := range round.Slots {
		if domain.ProgressShardID(s.Index) != 0 && s.RoomID.Valid() {
			slotB = s
			break
		}
	}
	if !slotB.RoomID.Valid() {
		t.Fatal("need slot on other shard")
	}
	shardA := domain.ProgressShardID(slotA.Index)
	shardB := domain.ProgressShardID(slotB.Index)

	// Hold projection shard A lock; prove shard B commit completes without waiting on A.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `
		INSERT INTO bracket_projection_shards (tournament_id, shard_id, version, generated_at)
		VALUES ($1, $2, 0, now()) ON CONFLICT DO NOTHING
	`, "t-proj-shard", shardA); err != nil {
		t.Fatal(err)
	}
	var locked int64
	if err := tx.QueryRow(ctx, `
		SELECT version FROM bracket_projection_shards
		WHERE tournament_id=$1 AND shard_id=$2 FOR UPDATE
	`, "t-proj-shard", shardA).Scan(&locked); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		u2, err := ts.BeginRoundMatch(ctx, "t-proj-shard", string(slotB.RoomID), 1, string(slotB.SlotID), "ingest:proj-b")
		if err != nil {
			done <- err
			return
		}
		defer func() { _ = u2.Rollback() }()
		cmd := domain.RecordMatchResultCommand{
			CommandID: "ingest:proj-b", EventID: "proj-b",
			RoomID: slotB.RoomID, RoundNumber: 1, SlotID: slotB.SlotID,
			CompletionVersion: 50, Standings: standingsFromSeeded(slotB.SeededPlayers),
		}
		if err := u2.AttachPriorResult(string(cmd.RoomID), uint64(cmd.CompletionVersion)); err != nil {
			done <- err
			return
		}
		d := domain.DecideRecordMatchResult(u2.Loaded(), cmd)
		if d.Kind != domain.RoundMatchRecord {
			done <- fmt.Errorf("kind=%s", d.Kind)
			return
		}
		payload, _ := json.Marshal(map[string]any{"facts": []any{}})
		done <- u2.Commit(store.RoundMatchCommitRequest{
			TournamentID: "t-proj-shard", CommandID: string(cmd.CommandID), CommandType: "RecordMatchResult",
			Outcome:  envelope.Accepted(string(cmd.CommandID), "RecordMatchResult", nil, payload),
			Decision: d, Command: cmd, ProjectionChanged: true,
			MatchResultSource: &store.MatchResultSource{
				EventID: string(cmd.EventID), RoomID: string(cmd.RoomID), CompletionVersion: uint64(cmd.CompletionVersion),
			},
			Events: []store.OutboxEvent{{
				EventID: "ingest:proj-b:TournamentMatchResultRecorded:0", EventType: "TournamentMatchResultRecorded",
				TournamentID: "t-proj-shard", Topic: "tournament.test", PartitionKey: "t-proj-shard",
				SchemaVersion: 1, Payload: map[string]any{"schemaVersion": 1}, CreatedAt: time.Now().UTC(),
			}},
		})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("distinct projection shard commit blocked on unrelated shard lock — shared projection row regression")
	}

	var verA, verB int64
	_ = pool.QueryRow(ctx, `SELECT COALESCE(version,0) FROM bracket_projection_shards WHERE tournament_id=$1 AND shard_id=$2`, "t-proj-shard", shardA).Scan(&verA)
	_ = pool.QueryRow(ctx, `SELECT COALESCE(version,0) FROM bracket_projection_shards WHERE tournament_id=$1 AND shard_id=$2`, "t-proj-shard", shardB).Scan(&verB)
	if verB < 1 {
		t.Fatalf("shard B version=%d", verB)
	}
	if verA != locked {
		t.Fatalf("held shard A mutated while locked: before=%d after=%d", locked, verA)
	}

	page, err := ts.LoadBracketPage(ctx, store.BracketPageQuery{TournamentID: "t-proj-shard", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	// base (from provision commit) + shard B contribution
	if page.ProjectionVersion < verB {
		t.Fatalf("aggregated projectionVersion=%d shardB=%d", page.ProjectionVersion, verB)
	}
}

func TestIntegration_Quarantine_UnknownRoomClaimedSlot(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	_, slot := provisionedTournament(t, ts, "t-q-unk", playersN(12))
	standings := standingsFromSeeded(slot.SeededPlayers)
	cmd := domain.RecordMatchResultCommand{
		CommandID: "ingest:unk", EventID: "unk",
		RoomID: "no-such-room", RoundNumber: 1, SlotID: slot.SlotID,
		CompletionVersion: 9, Standings: standings,
	}
	kind := ingestDifferentialKind(t, ts, "t-q-unk", cmd)
	if kind != domain.RoundMatchQuarantineUnresolved {
		t.Fatalf("kind=%s", kind)
	}
	var nResults, nQ int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_results WHERE tournament_id=$1`, "t-q-unk").Scan(&nResults)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_result_quarantines WHERE tournament_id=$1`, "t-q-unk").Scan(&nQ)
	if nResults != 0 {
		t.Fatalf("unknown room must not write match_results, got %d", nResults)
	}
	if nQ != 1 {
		t.Fatalf("quarantines=%d", nQ)
	}
	var status string
	var affects bool
	_ = pool.QueryRow(ctx, `SELECT status FROM bracket_slots WHERE tournament_id=$1 AND slot_id=$2`, "t-q-unk", string(slot.SlotID)).Scan(&status)
	_ = pool.QueryRow(ctx, `SELECT affects_slot FROM match_result_quarantines WHERE tournament_id=$1`, "t-q-unk").Scan(&affects)
	if status == "quarantined" {
		t.Fatal("claimed slot must not be quarantined for unknown room")
	}
	if affects {
		t.Fatal("unknown room quarantine must not affect_slot")
	}
	page, _ := ts.LoadBracketPage(ctx, store.BracketPageQuery{TournamentID: "t-q-unk", Limit: 10})
	// provision bumped base once; unknown room must not bump further
	if page.ProjectionVersion != 1 {
		t.Fatalf("projectionVersion=%d want base-only 1", page.ProjectionVersion)
	}
}

func TestIntegration_Quarantine_KnownRoomWrongSlot(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	tr, slot := provisionedTournament(t, ts, "t-q-wrong", playersN(12))
	round, _ := tr.Round(1)
	other := round.Slots[1]
	standings := standingsFromSeeded(slot.SeededPlayers)
	cmd := domain.RecordMatchResultCommand{
		CommandID: "ingest:wrong", EventID: "wrong",
		RoomID: slot.RoomID, RoundNumber: 1, SlotID: other.SlotID,
		CompletionVersion: 4, Standings: standings,
	}
	kind := ingestDifferentialKind(t, ts, "t-q-wrong", cmd)
	if kind != domain.RoundMatchQuarantineUnresolved {
		t.Fatalf("kind=%s", kind)
	}
	var status, otherStatus, roundStatus string
	_ = pool.QueryRow(ctx, `SELECT status FROM bracket_slots WHERE tournament_id=$1 AND slot_id=$2`, "t-q-wrong", string(slot.SlotID)).Scan(&status)
	_ = pool.QueryRow(ctx, `SELECT status FROM bracket_slots WHERE tournament_id=$1 AND slot_id=$2`, "t-q-wrong", string(other.SlotID)).Scan(&otherStatus)
	_ = pool.QueryRow(ctx, `SELECT status FROM tournament_rounds WHERE tournament_id=$1 AND round_number=1`, "t-q-wrong").Scan(&roundStatus)
	if status != "quarantined" {
		t.Fatalf("resolved slot=%s", status)
	}
	if otherStatus == "quarantined" {
		t.Fatal("claimed wrong slot must not be quarantined")
	}
	if roundStatus != "blocked" {
		t.Fatalf("round=%s", roundStatus)
	}
	var nResults, nQ int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_results WHERE room_id=$1 AND disposition='quarantined'`, string(slot.RoomID)).Scan(&nResults)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_result_quarantines WHERE tournament_id=$1 AND affects_slot`, "t-q-wrong").Scan(&nQ)
	if nResults != 1 || nQ != 1 {
		t.Fatalf("results=%d quarantines=%d", nResults, nQ)
	}
}

func TestIntegration_Quarantine_ValidAbandoned(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	_, slot := provisionedTournament(t, ts, "t-q-ab", playersN(12))
	cmd := domain.RecordMatchResultCommand{
		CommandID: "ingest:ab2", EventID: "ab2",
		RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
		CompletionVersion: 3, Standings: standingsFromSeeded(slot.SeededPlayers), IsAbandoned: true,
	}
	if kind := ingestDifferentialKind(t, ts, "t-q-ab", cmd); kind != domain.RoundMatchQuarantineUnresolved {
		t.Fatalf("kind=%s", kind)
	}
	var disp string
	if err := pool.QueryRow(ctx, `SELECT disposition FROM match_results WHERE room_id=$1 AND completion_version=3`, string(slot.RoomID)).Scan(&disp); err != nil {
		t.Fatal(err)
	}
	if disp != "quarantined" {
		t.Fatalf("disp=%s", disp)
	}
	var nQ int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_result_quarantines WHERE tournament_id=$1 AND affects_slot`, "t-q-ab").Scan(&nQ)
	if nQ != 1 {
		t.Fatalf("quarantines=%d", nQ)
	}
}

func TestIntegration_QuarantineHeld_LaterCompletionVersion(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	_, slot := provisionedTournament(t, ts, "t-held", playersN(12))
	standings := standingsFromSeeded(slot.SeededPlayers)
	first := domain.RecordMatchResultCommand{
		CommandID: "ingest:h1", EventID: "h1",
		RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
		CompletionVersion: 1, Standings: standings, IsAbandoned: true,
	}
	ingestDifferential(t, ts, "t-held", first, "c")
	var qBefore, resBefore, outboxBefore, projShardBefore int64
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_result_quarantines WHERE tournament_id=$1`, "t-held").Scan(&qBefore)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_results WHERE tournament_id=$1`, "t-held").Scan(&resBefore)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE tournament_id=$1`, "t-held").Scan(&outboxBefore)
	_ = pool.QueryRow(ctx, `SELECT COALESCE(SUM(version),0) FROM bracket_projection_shards WHERE tournament_id=$1`, "t-held").Scan(&projShardBefore)

	later := domain.RecordMatchResultCommand{
		CommandID: "ingest:h2", EventID: "h2",
		RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
		CompletionVersion: 2, Standings: standings,
	}
	if kind := ingestDifferentialKind(t, ts, "t-held", later); kind != domain.RoundMatchQuarantineHeld {
		t.Fatalf("kind=%s", kind)
	}
	var qAfter, resAfter, outboxAfter, projShardAfter int64
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_result_quarantines WHERE tournament_id=$1`, "t-held").Scan(&qAfter)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_results WHERE tournament_id=$1`, "t-held").Scan(&resAfter)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE tournament_id=$1`, "t-held").Scan(&outboxAfter)
	_ = pool.QueryRow(ctx, `SELECT COALESCE(SUM(version),0) FROM bracket_projection_shards WHERE tournament_id=$1`, "t-held").Scan(&projShardAfter)
	if qAfter != qBefore || resAfter != resBefore || outboxAfter != outboxBefore || projShardAfter != projShardBefore {
		t.Fatalf("held mutated state q %d->%d res %d->%d outbox %d->%d shard %d->%d",
			qBefore, qAfter, resBefore, resAfter, outboxBefore, outboxAfter, projShardBefore, projShardAfter)
	}
	var cmds int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM command_idempotency WHERE command_id IN ('ingest:h1','ingest:h2')`).Scan(&cmds)
	if cmds != 2 {
		t.Fatalf("cmds=%d", cmds)
	}
	// Same event id replay
	again := later
	again.CommandID = "ingest:h2"
	again.EventID = "h2"
	uow, err := ts.BeginRoundMatch(ctx, "t-held", string(slot.RoomID), 1, string(slot.SlotID), "ingest:h2")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := uow.LookupOutcome("ingest:h2"); !ok {
		t.Fatal("expected prior outcome")
	}
	_ = uow.Rollback()
}

func TestIntegration_RewriteBarrier_BeginCreateBlocksOnShared(t *testing.T) {
	if !store.RewriteBarrierUsesHashtextextended() {
		t.Fatal("barrier must use hashtextextended")
	}
	_, ts := openStore(t)
	ctx := context.Background()
	_, slot := provisionedTournament(t, ts, "t-bar-create", playersN(12))

	uow, err := ts.BeginRoundMatch(ctx, "t-bar-create", string(slot.RoomID), 1, string(slot.SlotID), "ingest:bar-create-hold")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = uow.Rollback() }()

	createCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err = ts.BeginCreate(createCtx, "t-bar-create")
	elapsed := time.Since(started)
	if err == nil {
		t.Fatal("BeginCreate must block/fail while differential shared barrier held")
	}
	if elapsed < 50*time.Millisecond {
		t.Fatalf("BeginCreate returned too quickly (%s); expected wait on exclusive barrier", elapsed)
	}
}

func TestIntegration_Readiness_BlockedByRoundAndBatch(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	tr, _ := provisionedTournament(t, ts, "t-ready", playersN(12))
	markRoundMatchingReady(t, pool, "t-ready", 1)
	round, _ := tr.Round(1)

	// Resolve all slots so counters look ready, then force round blocked + batch quarantine.
	for i, slot := range round.Slots {
		cmd := domain.RecordMatchResultCommand{
			CommandID: domain.CommandID(fmt.Sprintf("ingest:rdy-%d", i)),
			EventID:   domain.EventID(fmt.Sprintf("rdy-%d", i)),
			RoomID:    slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
			CompletionVersion: domain.CompletionVersion(i + 1),
			Standings:         standingsFromSeeded(slot.SeededPlayers),
		}
		if kind := ingestDifferentialKind(t, ts, "t-ready", cmd); kind != domain.RoundMatchRecord {
			t.Fatalf("slot %d kind=%s", i, kind)
		}
	}
	ready, err := ts.LoadRoundProgressReadiness(ctx, "t-ready", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !ready.Ready {
		t.Fatalf("expected ready when counters complete and round/batch clear: %+v", ready)
	}
	if ready.RoundStatus == "blocked" || ready.RoundStatus == "completed" {
		t.Fatalf("unexpected terminal/blocked round status: %+v", ready)
	}

	if _, err := pool.Exec(ctx, `UPDATE tournament_rounds SET status='blocked' WHERE tournament_id=$1 AND round_number=1`, "t-ready"); err != nil {
		t.Fatal(err)
	}
	ready, _ = ts.LoadRoundProgressReadiness(ctx, "t-ready", 1)
	if ready.Ready || ready.RoundStatus != "blocked" {
		t.Fatalf("blocked round must not be ready: %+v", ready)
	}

	if _, err := pool.Exec(ctx, `UPDATE tournament_rounds SET status='in_progress' WHERE tournament_id=$1 AND round_number=1`, "t-ready"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE provisioning_batches
		SET status='quarantined', quarantine_reason='test'
		WHERE tournament_id=$1 AND round_number=1
	`, "t-ready"); err != nil {
		t.Fatal(err)
	}
	ready, _ = ts.LoadRoundProgressReadiness(ctx, "t-ready", 1)
	if ready.Ready || ready.QuarantinedBatches < 1 {
		t.Fatalf("quarantined batch must block readiness: %+v", ready)
	}
}

func TestIntegration_RoundMatch_SameCommandIdConcurrentCanonicalPrior(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	_, slot := provisionedTournament(t, ts, "t-same-cmd", playersN(12))
	standingsA := standingsFromSeeded(slot.SeededPlayers)
	standingsB := make([]domain.PlayerMatchStanding, len(standingsA))
	copy(standingsB, standingsA)
	if len(standingsB) > 1 {
		standingsB[0].MatchWins = 1
		standingsB[1].MatchWins = len(standingsB)
	}

	const cmdID = "http:same-cmd-concurrent"
	var wg sync.WaitGroup
	results := make([]envelope.Result, 2)
	errs := make([]error, 2)
	wg.Add(2)
	for i, standings := range [][]domain.PlayerMatchStanding{standingsA, standingsB} {
		go func(i int, standings []domain.PlayerMatchStanding) {
			defer wg.Done()
			uow, err := ts.BeginRoundMatch(ctx, "t-same-cmd", string(slot.RoomID), 1, string(slot.SlotID), cmdID)
			if err != nil {
				errs[i] = err
				return
			}
			defer func() { _ = uow.Rollback() }()
			if prior, ok := uow.LookupOutcome(cmdID); ok {
				results[i] = prior
				return
			}
			if err := uow.AttachPriorResult(string(slot.RoomID), 9); err != nil {
				errs[i] = err
				return
			}
			cmd := domain.RecordMatchResultCommand{
				CommandID: domain.CommandID(cmdID), EventID: domain.EventID(fmt.Sprintf("evt-%d", i)),
				RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
				CompletionVersion: 9, Standings: standings,
			}
			d := domain.DecideRecordMatchResult(uow.Loaded(), cmd)
			payload, _ := json.Marshal(map[string]any{"facts": []any{}, "idx": i})
			res := envelope.Accepted(cmdID, "RecordMatchResult", nil, payload)
			if err := uow.Commit(store.RoundMatchCommitRequest{
				TournamentID: "t-same-cmd", CommandID: cmdID, CommandType: "RecordMatchResult",
				Outcome: res, Decision: d, Command: cmd, ProjectionChanged: d.Kind == domain.RoundMatchRecord,
			}); err != nil {
				if prior, ok := store.AsPriorCommandOutcome(err); ok {
					results[i] = prior
					return
				}
				errs[i] = err
				return
			}
			if stored, ok := ts.LookupOutcome(ctx, cmdID); ok {
				results[i] = stored
				return
			}
			results[i] = res
		}(i, standings)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
	}
	if !outcomesEqual(results[0], results[1]) {
		t.Fatalf("canonical mismatch:\n%#v\n%#v", results[0], results[1])
	}
	stored, ok := ts.LookupOutcome(ctx, cmdID)
	if !ok || !outcomesEqual(stored, results[0]) {
		t.Fatalf("stored outcome mismatch ok=%v", ok)
	}
	var nResults int
	if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM match_results WHERE tournament_id=$1`, "t-same-cmd").Scan(&nResults); err != nil {
		t.Fatal(err)
	}
	if nResults != 1 {
		t.Fatalf("want exactly one match_results row, got %d", nResults)
	}
}
