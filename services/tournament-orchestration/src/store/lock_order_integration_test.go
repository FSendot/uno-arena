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

type quarantineRaceResult struct {
	out envelope.Result
	err error
}

type roundMatchRaceResult struct {
	kind domain.RoundMatchDecisionKind
	err  error
}

// Non-fatal QuarantineTournamentResult path for concurrency tests. Callers must
// assert err on the main test goroutine only (never t.Fatal from helpers used by races).
func tryCommitQuarantineResult(ts *store.TournamentStore, tid, roomID string, ver uint64, cmdID, reason string) (envelope.Result, error) {
	ctx := context.Background()
	uow, err := ts.BeginQuarantineTournamentResult(ctx, tid, roomID, ver, cmdID)
	if err != nil {
		return envelope.Result{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = uow.Rollback() }()
	if prior, ok := uow.LookupOutcome(cmdID); ok {
		return prior, nil
	}
	cmd := domain.QuarantineTournamentResultCommand{
		CommandID: domain.CommandID(cmdID), RoomID: domain.RoomID(roomID),
		CompletionVersion: domain.CompletionVersion(ver), Reason: reason,
	}
	d := domain.DecideQuarantineTournamentResult(uow.Loaded(), cmd)
	factsPayload := make([]map[string]any, 0, len(d.Outcome.Facts))
	for _, f := range d.Outcome.Facts {
		data := map[string]any{}
		for k, v := range f.Data {
			if k == "reason" {
				data[k] = domain.ExplicitQuarantineReasonCode
				continue
			}
			data[k] = v
		}
		factsPayload = append(factsPayload, map[string]any{"name": string(f.Name), "data": data})
	}
	out := envelope.Accepted(cmdID, "QuarantineTournamentResult", nil, mustJSON(map[string]any{"facts": factsPayload}))
	if d.Outcome.Rejected() {
		out = envelope.Rejected(cmdID, "QuarantineTournamentResult", string(d.Outcome.Rejection.Code), nil)
	}
	proj := (d.Kind == domain.QuarantineResultLedgerOnly || d.Kind == domain.QuarantineResultInsertQuarantined) && len(d.Outcome.Facts) > 0
	if err := uow.Commit(store.QuarantineResultCommitRequest{
		TournamentID: tid, CommandID: cmdID, CommandType: "QuarantineTournamentResult",
		Outcome: out, Decision: d, Command: cmd, ProjectionChanged: proj,
	}); err != nil {
		if prior, ok := store.AsPriorCommandOutcome(err); ok {
			return prior, nil
		}
		return envelope.Result{}, fmt.Errorf("commit: %w", err)
	}
	return out, nil
}

// Non-fatal RoundMatch ingest path for concurrency tests. Callers must assert
// err on the main test goroutine only.
func tryIngestDifferentialKind(ts *store.TournamentStore, tid string, cmd domain.RecordMatchResultCommand) (domain.RoundMatchDecisionKind, error) {
	ctx := context.Background()
	uow, err := ts.BeginRoundMatch(ctx, tid, string(cmd.RoomID), cmd.RoundNumber, string(cmd.SlotID), string(cmd.CommandID))
	if err != nil {
		return "", fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = uow.Rollback() }()
	if _, ok := uow.LookupOutcome(string(cmd.CommandID)); ok {
		return domain.RoundMatchExactDuplicate, nil
	}
	if err := uow.AttachPriorResult(string(cmd.RoomID), uint64(cmd.CompletionVersion)); err != nil {
		return "", fmt.Errorf("prior: %w", err)
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
			return d.Kind, nil
		}
		return "", fmt.Errorf("commit: %w", err)
	}
	return d.Kind, nil
}

// Explicit QuarantineTournamentResult racing D1 RoundMatch on the same known
// (room, version) must complete without deadlock and leave one ledger row with
// consistent fact semantics (one factful winner or recorded+ledger_only).
func TestIntegration_LockOrder_QuarantineVsRoundMatch_KnownKey(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	_, slot := provisionedTournament(t, ts, "t-lo-known", playersN(12))
	room := string(slot.RoomID)
	standings := standingsFromSeeded(slot.SeededPlayers)
	const ver uint64 = 42

	qrCh := make(chan quarantineRaceResult, 1)
	rmCh := make(chan roundMatchRaceResult, 1)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		out, err := tryCommitQuarantineResult(ts, "t-lo-known", room, ver, "qr-lo-known", "ops")
		qrCh <- quarantineRaceResult{out: out, err: err}
	}()
	go func() {
		defer wg.Done()
		cmd := domain.RecordMatchResultCommand{
			CommandID: "ingest:lo-known", EventID: "lo-known",
			RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
			CompletionVersion: domain.CompletionVersion(ver), Standings: standings,
		}
		kind, err := tryIngestDifferentialKind(ts, "t-lo-known", cmd)
		rmCh <- roundMatchRaceResult{kind: kind, err: err}
	}()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("deadlock: quarantine vs RoundMatch known-key race timed out")
	}
	qr := <-qrCh
	rm := <-rmCh
	if qr.err != nil || rm.err != nil {
		t.Fatalf("qrErr=%v rmErr=%v", qr.err, rm.err)
	}
	if qr.out.Status != envelope.StatusAccepted {
		t.Fatalf("quarantine status=%v", qr.out.Status)
	}
	switch rm.kind {
	case domain.RoundMatchRecord, domain.RoundMatchQuarantineHeld,
		domain.RoundMatchQuarantineConflict, domain.RoundMatchQuarantineUnresolved,
		domain.RoundMatchExactDuplicate:
		// all consistent with race winner ordering
	default:
		t.Fatalf("unexpected RoundMatch kind %s", rm.kind)
	}
	var nQ int64
	_ = pool.QueryRow(ctx, `
		SELECT count(*) FROM match_result_quarantines
		WHERE claimed_room_id=$1 AND completion_version=$2
	`, room, int64(ver)).Scan(&nQ)
	if nQ != 1 {
		t.Fatalf("want exactly one business quarantine ledger row, got %d", nQ)
	}
	var nResults int64
	var disp string
	_ = pool.QueryRow(ctx, `
		SELECT count(*), coalesce(max(disposition),'')
		FROM match_results WHERE room_id=$1 AND completion_version=$2
	`, room, int64(ver)).Scan(&nResults, &disp)
	if nResults > 1 {
		t.Fatalf("match_results rows=%d", nResults)
	}
	if nResults == 1 && disp != "recorded" && disp != "quarantined" {
		t.Fatalf("unexpected disposition %q", disp)
	}
}

// Unknown-room same-key race: explicit quarantine + RoundMatch quarantine path.
func TestIntegration_LockOrder_QuarantineVsRoundMatch_UnknownRoom(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	_, _ = provisionedTournament(t, ts, "t-lo-unk", playersN(12))
	const room = "room-lo-unknown-77"
	const ver uint64 = 9

	qrCh := make(chan quarantineRaceResult, 1)
	rmCh := make(chan roundMatchRaceResult, 1)
	var wg sync.WaitGroup
	done := make(chan struct{})
	wg.Add(2)
	go func() {
		defer wg.Done()
		out, err := tryCommitQuarantineResult(ts, "t-lo-unk", room, ver, "qr-lo-unk", "ops")
		qrCh <- quarantineRaceResult{out: out, err: err}
	}()
	go func() {
		defer wg.Done()
		cmd := domain.RecordMatchResultCommand{
			CommandID: "ingest:lo-unk", EventID: "lo-unk",
			RoomID: domain.RoomID(room), RoundNumber: 1, SlotID: "s-fake",
			CompletionVersion: domain.CompletionVersion(ver),
			Standings:         []domain.PlayerMatchStanding{{PlayerID: "p0", MatchWins: 1}},
		}
		kind, err := tryIngestDifferentialKind(ts, "t-lo-unk", cmd)
		rmCh <- roundMatchRaceResult{kind: kind, err: err}
	}()
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("deadlock: unknown-room same-key race timed out")
	}
	qr := <-qrCh
	rm := <-rmCh
	if qr.err != nil || rm.err != nil {
		t.Fatalf("qrErr=%v rmErr=%v", qr.err, rm.err)
	}
	var nQ int64
	_ = pool.QueryRow(ctx, `
		SELECT count(*) FROM match_result_quarantines
		WHERE claimed_room_id=$1 AND completion_version=$2
	`, room, int64(ver)).Scan(&nQ)
	if nQ != 1 {
		t.Fatalf("want one ledger row, got %d", nQ)
	}
}

// Cancel exclusive barrier fences in-flight RoundMatch: serial winner ordering.
// Result may not mutate after cancel transition commits.
func TestIntegration_LockOrder_CancelFencesRoundMatch(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()

	t.Run("result_wins_then_cancel", func(t *testing.T) {
		_, slot := provisionedTournament(t, ts, "t-lo-cancel-rm", playersN(12))
		standings := standingsFromSeeded(slot.SeededPlayers)
		uow, err := ts.BeginRoundMatch(ctx, "t-lo-cancel-rm", string(slot.RoomID), 1, string(slot.SlotID), "ingest:hold-rm")
		if err != nil {
			t.Fatal(err)
		}
		if err := uow.AttachPriorResult(string(slot.RoomID), 71); err != nil {
			t.Fatal(err)
		}

		cancelCtx, cancel := context.WithTimeout(ctx, 400*time.Millisecond)
		defer cancel()
		started := time.Now()
		_, err = ts.BeginCancelTournament(cancelCtx, "t-lo-cancel-rm", "cancel-blocked")
		elapsed := time.Since(started)
		if err == nil {
			t.Fatal("Cancel must wait on exclusive barrier while RoundMatch holds shared")
		}
		if elapsed < 50*time.Millisecond {
			t.Fatalf("Cancel returned too quickly (%s)", elapsed)
		}

		cmd := domain.RecordMatchResultCommand{
			CommandID: "ingest:hold-rm", EventID: "hold-rm",
			RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
			CompletionVersion: 71, Standings: standings,
		}
		d := domain.DecideRecordMatchResult(uow.Loaded(), cmd)
		if d.Kind != domain.RoundMatchRecord {
			t.Fatalf("kind=%s", d.Kind)
		}
		factsPayload := make([]map[string]any, 0, len(d.Outcome.Facts))
		for _, f := range d.Outcome.Facts {
			factsPayload = append(factsPayload, map[string]any{"name": string(f.Name), "data": f.Data})
		}
		if err := uow.Commit(store.RoundMatchCommitRequest{
			TournamentID: "t-lo-cancel-rm", CommandID: "ingest:hold-rm", CommandType: "RecordMatchResult",
			Outcome:  envelope.Accepted("ingest:hold-rm", "RecordMatchResult", nil, mustJSON(map[string]any{"facts": factsPayload})),
			Decision: d, Command: cmd, ProjectionChanged: len(d.Outcome.Facts) > 0,
			MatchResultSource: &store.MatchResultSource{EventID: "hold-rm", RoomID: string(slot.RoomID), CompletionVersion: 71},
		}); err != nil {
			t.Fatal(err)
		}

		out := commitCancelTournament(t, ts, "t-lo-cancel-rm", "cancel-after-rm", domain.CancelTournamentDecision{})
		if out.Status != envelope.StatusAccepted {
			t.Fatalf("cancel after result: %+v", out)
		}
		var phase, disp string
		_ = pool.QueryRow(ctx, `SELECT phase FROM tournaments WHERE tournament_id=$1`, "t-lo-cancel-rm").Scan(&phase)
		_ = pool.QueryRow(ctx, `SELECT disposition FROM match_results WHERE room_id=$1 AND completion_version=71`, string(slot.RoomID)).Scan(&disp)
		if phase != "cancelled" || disp != "recorded" {
			t.Fatalf("phase=%s disp=%s", phase, disp)
		}
	})

	t.Run("cancel_wins_then_result_rejects", func(t *testing.T) {
		_, slot := provisionedTournament(t, ts, "t-lo-cancel-first", playersN(12))
		standings := standingsFromSeeded(slot.SeededPlayers)
		cuow, err := ts.BeginCancelTournament(ctx, "t-lo-cancel-first", "cancel-hold")
		if err != nil {
			t.Fatal(err)
		}

		rmCtx, cancel := context.WithTimeout(ctx, 400*time.Millisecond)
		defer cancel()
		started := time.Now()
		_, err = ts.BeginRoundMatch(rmCtx, "t-lo-cancel-first", string(slot.RoomID), 1, string(slot.SlotID), "ingest:blocked")
		elapsed := time.Since(started)
		if err == nil {
			t.Fatal("RoundMatch must wait on shared barrier while Cancel holds exclusive")
		}
		if elapsed < 50*time.Millisecond {
			t.Fatalf("RoundMatch returned too quickly (%s)", elapsed)
		}

		d := domain.DecideCancelTournament(cuow.CancelContext(), domain.CancelTournamentCommand{CommandID: "cancel-hold"})
		if d.Kind != domain.CancelTournamentSuccess {
			t.Fatalf("cancel decide=%+v", d)
		}
		factsPayload := make([]map[string]any, 0, len(d.Outcome.Facts))
		for _, f := range d.Outcome.Facts {
			factsPayload = append(factsPayload, map[string]any{"name": string(f.Name), "data": f.Data})
		}
		if err := cuow.Commit(store.LifecycleCommitRequest{
			TournamentID: "t-lo-cancel-first", CommandID: "cancel-hold", CommandType: "CancelTournament",
			Outcome: envelope.Accepted("cancel-hold", "CancelTournament", nil, mustJSON(map[string]any{"facts": factsPayload})),
			Cancel:  d,
		}); err != nil {
			t.Fatal(err)
		}

		cmd := domain.RecordMatchResultCommand{
			CommandID: "ingest:after-cancel", EventID: "after-cancel",
			RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
			CompletionVersion: 72, Standings: standings,
		}
		kind := ingestDifferentialKind(t, ts, "t-lo-cancel-first", cmd)
		if kind != domain.RoundMatchReject {
			t.Fatalf("after cancel want reject, got %s", kind)
		}
		var nRes int64
		_ = pool.QueryRow(ctx, `
			SELECT count(*) FROM match_results WHERE room_id=$1 AND completion_version=72
		`, string(slot.RoomID)).Scan(&nRes)
		if nRes != 0 {
			t.Fatalf("no result mutation after cancel; rows=%d", nRes)
		}
	})
}

// Exclusive lifecycle barrier must not serialize two ordinary RoundMatch ops (shared/shared).
func TestIntegration_LockOrder_SharedRoundMatchesCoexist(t *testing.T) {
	_, ts := openStore(t)
	ctx := context.Background()
	_, slotA := provisionedTournament(t, ts, "t-lo-shared", playersN(12))
	slotB := roundOtherSlot(t, ts, "t-lo-shared", slotA.SlotID)

	uowA, err := ts.BeginRoundMatch(ctx, "t-lo-shared", string(slotA.RoomID), 1, string(slotA.SlotID), "ingest:shared-a")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = uowA.Rollback() }()

	started := time.Now()
	uowB, err := ts.BeginRoundMatch(ctx, "t-lo-shared", string(slotB.RoomID), 1, string(slotB.SlotID), "ingest:shared-b")
	elapsed := time.Since(started)
	if err != nil {
		t.Fatalf("second shared RoundMatch must coexist: %v", err)
	}
	defer func() { _ = uowB.Rollback() }()
	if elapsed > 2*time.Second {
		t.Fatalf("shared/shared RoundMatch serialized unexpectedly (%s)", elapsed)
	}
}

// Stress: many alternating quarantine + RoundMatch races must not deadlock.
func TestIntegration_LockOrder_QuarantineRoundMatchStress(t *testing.T) {
	_, ts := openStore(t)
	_, slot := provisionedTournament(t, ts, "t-lo-stress", playersN(12))
	room := string(slot.RoomID)
	standings := standingsFromSeeded(slot.SeededPlayers)

	const n = 8
	errCh := make(chan error, n*2)
	var wg sync.WaitGroup
	wg.Add(n * 2)
	for i := 0; i < n; i++ {
		i := i
		ver := uint64(100 + i)
		go func() {
			defer wg.Done()
			_, err := tryCommitQuarantineResult(ts, "t-lo-stress", room, ver, fmt.Sprintf("qr-stress-%d", i), "ops")
			if err != nil {
				errCh <- fmt.Errorf("quarantine[%d]: %w", i, err)
			}
		}()
		go func() {
			defer wg.Done()
			cmd := domain.RecordMatchResultCommand{
				CommandID: domain.CommandID(fmt.Sprintf("ingest:stress-%d", i)),
				EventID:   domain.EventID(fmt.Sprintf("stress-%d", i)),
				RoomID:    slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
				CompletionVersion: domain.CompletionVersion(ver), Standings: standings,
			}
			_, err := tryIngestDifferentialKind(ts, "t-lo-stress", cmd)
			if err != nil {
				errCh <- fmt.Errorf("roundmatch[%d]: %w", i, err)
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("stress race timed out (likely deadlock)")
	}
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}
