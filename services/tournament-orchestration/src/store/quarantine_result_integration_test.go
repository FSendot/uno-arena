//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/services/tournament-orchestration/store"
	"unoarena/shared/envelope"
)

func commitQuarantineResult(t *testing.T, ts *store.TournamentStore, tid, roomID string, ver uint64, cmdID, reason string) (envelope.Result, domain.QuarantineTournamentResultDecision) {
	t.Helper()
	ctx := context.Background()
	uow, err := ts.BeginQuarantineTournamentResult(ctx, tid, roomID, ver, cmdID)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = uow.Rollback() }()
	if prior, ok := uow.LookupOutcome(cmdID); ok {
		return prior, domain.QuarantineTournamentResultDecision{Kind: domain.QuarantineResultAlreadyDone}
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
			return prior, d
		}
		t.Fatalf("commit: %v", err)
	}
	return out, d
}

func TestIntegration_QuarantineResult_KnownRoomNoResult(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	_, slot := provisionedTournament(t, ts, "t-qr-known", playersN(12))

	var projBefore int64
	_ = pool.QueryRow(ctx, `SELECT COALESCE((SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1),0)`, "t-qr-known").Scan(&projBefore)

	out, d := commitQuarantineResult(t, ts, "t-qr-known", string(slot.RoomID), 9, "qr-known", "operator SECRET hold")
	if out.Status != envelope.StatusAccepted || d.Kind != domain.QuarantineResultInsertQuarantined {
		t.Fatalf("out=%+v d=%+v", out, d)
	}

	var disp, reason string
	var ranked []byte
	if err := pool.QueryRow(ctx, `
		SELECT disposition, quarantine_reason, ranked_result::text FROM match_results
		WHERE room_id=$1 AND completion_version=9
	`, string(slot.RoomID)).Scan(&disp, &reason, &ranked); err != nil {
		t.Fatal(err)
	}
	if disp != "quarantined" || reason != domain.ExplicitQuarantineReasonCode {
		t.Fatalf("disp=%s reason=%q", disp, reason)
	}
	if string(ranked) != "{}" && string(ranked) != `{}` {
		// jsonb empty object
		var m map[string]any
		_ = json.Unmarshal(ranked, &m)
		if len(m) != 0 {
			t.Fatalf("ranked_result must be empty safe object: %s", ranked)
		}
	}
	if reason == "operator SECRET hold" || containsAny(reason, "SECRET", "operator") {
		t.Fatal("raw reason must not reach match_results")
	}

	var ledgerReason string
	var affects bool
	var resRound *int
	var resSlot *string
	if err := pool.QueryRow(ctx, `
		SELECT reason, affects_slot, resolved_round_number, resolved_slot_id
		FROM match_result_quarantines WHERE claimed_room_id=$1 AND completion_version=9
	`, string(slot.RoomID)).Scan(&ledgerReason, &affects, &resRound, &resSlot); err != nil {
		t.Fatal(err)
	}
	if ledgerReason != domain.ExplicitQuarantineReasonCode || !affects {
		t.Fatalf("ledger reason=%q affects=%v", ledgerReason, affects)
	}
	if resRound == nil || *resRound != 1 || resSlot == nil || *resSlot != string(slot.SlotID) {
		t.Fatalf("resolved round/slot=%v/%v", resRound, resSlot)
	}

	var status string
	_ = pool.QueryRow(ctx, `SELECT status FROM bracket_slots WHERE tournament_id=$1 AND slot_id=$2`, "t-qr-known", string(slot.SlotID)).Scan(&status)
	if status == "quarantined" {
		t.Fatal("must not mutate slot status")
	}

	var projAfter, outbox int64
	_ = pool.QueryRow(ctx, `SELECT COALESCE((SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1),0)`, "t-qr-known").Scan(&projAfter)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE tournament_id=$1 AND event_type='TournamentResultQuarantined'`, "t-qr-known").Scan(&outbox)
	if projAfter != projBefore+1 {
		t.Fatalf("proj before=%d after=%d want +1", projBefore, projAfter)
	}
	if outbox != 0 {
		t.Fatalf("must not invent outbox for TournamentResultQuarantined, got %d", outbox)
	}
}

func TestIntegration_QuarantineResult_RecordedPreserved(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	_, slot := provisionedTournament(t, ts, "t-qr-rec", playersN(12))
	standings := standingsFromSeeded(slot.SeededPlayers)
	ingestDifferential(t, ts, "t-qr-rec", domain.RecordMatchResultCommand{
		CommandID: "ingest:rec", EventID: "evt-rec",
		RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
		CompletionVersion: 3, Standings: standings,
	}, "corr")

	var advBefore int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM advancement_records WHERE tournament_id=$1 AND slot_id=$2`, "t-qr-rec", string(slot.SlotID)).Scan(&advBefore)
	if advBefore == 0 {
		t.Fatal("expected advancement")
	}

	out, d := commitQuarantineResult(t, ts, "t-qr-rec", string(slot.RoomID), 3, "qr-rec", "ops")
	if out.Status != envelope.StatusAccepted || d.Kind != domain.QuarantineResultLedgerOnly {
		t.Fatalf("out=%+v d=%+v", out, d)
	}

	var disp string
	_ = pool.QueryRow(ctx, `SELECT disposition FROM match_results WHERE room_id=$1 AND completion_version=3`, string(slot.RoomID)).Scan(&disp)
	if disp != "recorded" {
		t.Fatalf("must preserve recorded, got %s", disp)
	}
	var advAfter, nQ int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM advancement_records WHERE tournament_id=$1 AND slot_id=$2`, "t-qr-rec", string(slot.SlotID)).Scan(&advAfter)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_result_quarantines WHERE claimed_room_id=$1 AND completion_version=3`, string(slot.RoomID)).Scan(&nQ)
	if advAfter != advBefore || nQ != 1 {
		t.Fatalf("adv %d→%d quarantines=%d", advBefore, advAfter, nQ)
	}
}

func TestIntegration_QuarantineResult_ExistingQuarantinedNoop(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	_, slot := provisionedTournament(t, ts, "t-qr-held", playersN(12))
	_, _ = commitQuarantineResult(t, ts, "t-qr-held", string(slot.RoomID), 4, "qr-1", "first")

	var projMid int64
	_ = pool.QueryRow(ctx, `SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1`, "t-qr-held").Scan(&projMid)

	out, d := commitQuarantineResult(t, ts, "t-qr-held", string(slot.RoomID), 4, "qr-2", "second")
	if out.Status != envelope.StatusAccepted || d.Kind != domain.QuarantineResultAlreadyDone || len(mustFacts(out)) != 0 {
		t.Fatalf("want factless already-done: out=%+v d=%+v", out, d)
	}
	var projAfter, nQ int64
	_ = pool.QueryRow(ctx, `SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1`, "t-qr-held").Scan(&projAfter)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_result_quarantines WHERE claimed_room_id=$1 AND completion_version=4`, string(slot.RoomID)).Scan(&nQ)
	if projAfter != projMid {
		t.Fatalf("already quarantined must not bump: mid=%d after=%d", projMid, projAfter)
	}
	if nQ != 1 {
		t.Fatalf("quarantines=%d", nQ)
	}
}

func TestIntegration_QuarantineResult_UnknownRoomLedgerOnly(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	_, _ = provisionedTournament(t, ts, "t-qr-unk", playersN(12))

	out, d := commitQuarantineResult(t, ts, "t-qr-unk", "room-unknown-99", 8, "qr-unk", "ops")
	if out.Status != envelope.StatusAccepted || d.Kind != domain.QuarantineResultLedgerOnly || d.AffectsSlot {
		t.Fatalf("out=%+v d=%+v", out, d)
	}
	var nResults, nQ int
	var affects bool
	var claimedRound *int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_results WHERE room_id=$1`, "room-unknown-99").Scan(&nResults)
	_ = pool.QueryRow(ctx, `
		SELECT count(*), bool_or(affects_slot), max(claimed_round_number)
		FROM match_result_quarantines WHERE claimed_room_id=$1 AND completion_version=8
	`, "room-unknown-99").Scan(&nQ, &affects, &claimedRound)
	if nResults != 0 || nQ != 1 || affects {
		t.Fatalf("results=%d q=%d affects=%v", nResults, nQ, affects)
	}
	if claimedRound != nil {
		t.Fatalf("unknown room must not claim fake round: %v", claimedRound)
	}
}

func TestIntegration_QuarantineResult_SameCommandReplay(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	_, slot := provisionedTournament(t, ts, "t-qr-replay", playersN(12))
	first, _ := commitQuarantineResult(t, ts, "t-qr-replay", string(slot.RoomID), 5, "qr-same", "ops")
	var proj1 int64
	_ = pool.QueryRow(ctx, `SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1`, "t-qr-replay").Scan(&proj1)

	second, _ := commitQuarantineResult(t, ts, "t-qr-replay", string(slot.RoomID), 5, "qr-same", "ops")
	if second.CommandID != first.CommandID || second.Status != first.Status {
		t.Fatalf("replay mismatch first=%+v second=%+v", first, second)
	}
	var proj2, nQ int64
	_ = pool.QueryRow(ctx, `SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1`, "t-qr-replay").Scan(&proj2)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_result_quarantines WHERE claimed_room_id=$1 AND completion_version=5`, string(slot.RoomID)).Scan(&nQ)
	if proj2 != proj1 || nQ != 1 {
		t.Fatalf("replay proj %d→%d q=%d", proj1, proj2, nQ)
	}
}

func TestIntegration_QuarantineResult_ConcurrentDifferentCommandElection(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	_, slot := provisionedTournament(t, ts, "t-qr-race", playersN(12))
	room := string(slot.RoomID)

	var wg sync.WaitGroup
	results := make([]envelope.Result, 2)
	errs := make([]error, 2)
	wg.Add(2)
	for i := 0; i < 2; i++ {
		i := i
		go func() {
			defer wg.Done()
			uow, err := ts.BeginQuarantineTournamentResult(ctx, "t-qr-race", room, 11, fmt.Sprintf("qr-race-%d", i))
			if err != nil {
				errs[i] = err
				return
			}
			defer func() { _ = uow.Rollback() }()
			cmdID := fmt.Sprintf("qr-race-%d", i)
			if prior, ok := uow.LookupOutcome(cmdID); ok {
				results[i] = prior
				return
			}
			cmd := domain.QuarantineTournamentResultCommand{
				CommandID: domain.CommandID(cmdID), RoomID: domain.RoomID(room),
				CompletionVersion: 11, Reason: "ops",
			}
			d := domain.DecideQuarantineTournamentResult(uow.Loaded(), cmd)
			factsPayload := make([]map[string]any, 0, len(d.Outcome.Facts))
			for _, f := range d.Outcome.Facts {
				factsPayload = append(factsPayload, map[string]any{
					"name": string(f.Name),
					"data": map[string]any{"reason": domain.ExplicitQuarantineReasonCode},
				})
			}
			out := envelope.Accepted(cmdID, "QuarantineTournamentResult", nil, mustJSON(map[string]any{"facts": factsPayload}))
			proj := len(d.Outcome.Facts) > 0
			if err := uow.Commit(store.QuarantineResultCommitRequest{
				TournamentID: "t-qr-race", CommandID: cmdID, CommandType: "QuarantineTournamentResult",
				Outcome: out, Decision: d, Command: cmd, ProjectionChanged: proj,
			}); err != nil {
				if prior, ok := store.AsPriorCommandOutcome(err); ok {
					results[i] = prior
					return
				}
				errs[i] = err
				return
			}
			// Re-read canonical outcome (election may demote to factless).
			if stored, ok := ts.LookupQuarantineResultOutcome(ctx, cmdID); ok {
				results[i] = stored
			} else {
				results[i] = out
			}
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
	}

	var factful, factless int
	for _, r := range results {
		if r.Status != envelope.StatusAccepted {
			t.Fatalf("status=%v", r.Status)
		}
		if len(mustFacts(r)) > 0 {
			factful++
		} else {
			factless++
		}
	}
	if factful != 1 || factless != 1 {
		t.Fatalf("want 1 factful + 1 factless, got factful=%d factless=%d results=%+v", factful, factless, results)
	}
	var nQ int64
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_result_quarantines WHERE claimed_room_id=$1 AND completion_version=11`, room).Scan(&nQ)
	if nQ != 1 {
		t.Fatalf("quarantines=%d", nQ)
	}
}

func TestIntegration_QuarantineResult_CommitRollback(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	_, slot := provisionedTournament(t, ts, "t-qr-roll", playersN(12))
	ts.FailNextCommits = 1
	_, err := func() (envelope.Result, error) {
		uow, err := ts.BeginQuarantineTournamentResult(ctx, "t-qr-roll", string(slot.RoomID), 6, "qr-roll")
		if err != nil {
			return envelope.Result{}, err
		}
		defer func() { _ = uow.Rollback() }()
		cmd := domain.QuarantineTournamentResultCommand{
			CommandID: "qr-roll", RoomID: slot.RoomID, CompletionVersion: 6, Reason: "ops",
		}
		d := domain.DecideQuarantineTournamentResult(uow.Loaded(), cmd)
		out := envelope.Accepted("qr-roll", "QuarantineTournamentResult", nil, mustJSON(map[string]any{"facts": []any{}}))
		err = uow.Commit(store.QuarantineResultCommitRequest{
			TournamentID: "t-qr-roll", CommandID: "qr-roll", CommandType: "QuarantineTournamentResult",
			Outcome: out, Decision: d, Command: cmd, ProjectionChanged: true,
		})
		return out, err
	}()
	if err == nil {
		t.Fatal("expected injected commit failure")
	}
	var nQ, nOutcome int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_result_quarantines WHERE claimed_room_id=$1 AND completion_version=6`, string(slot.RoomID)).Scan(&nQ)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM command_idempotency WHERE command_id=$1`, "qr-roll").Scan(&nOutcome)
	if nQ != 0 || nOutcome != 0 {
		t.Fatalf("rollback leaked q=%d outcome=%d", nQ, nOutcome)
	}
	// Retry succeeds.
	out, _ := commitQuarantineResult(t, ts, "t-qr-roll", string(slot.RoomID), 6, "qr-roll", "ops")
	if out.Status != envelope.StatusAccepted {
		t.Fatalf("%+v", out)
	}
}

func TestIntegration_RecordThenExplicitQuarantineThenConflictingRecord_Factless(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	_, slot := provisionedTournament(t, ts, "t-qr-ledger-prec", playersN(12))
	standings := standingsFromSeeded(slot.SeededPlayers)
	const ver uint64 = 13

	ingestDifferential(t, ts, "t-qr-ledger-prec", domain.RecordMatchResultCommand{
		CommandID: "ingest:ledger-prec", EventID: "evt-ledger-prec",
		RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
		CompletionVersion: domain.CompletionVersion(ver), Standings: standings,
	}, "corr")

	var advBefore int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM advancement_records WHERE tournament_id=$1 AND slot_id=$2`,
		"t-qr-ledger-prec", string(slot.SlotID)).Scan(&advBefore)
	if advBefore == 0 {
		t.Fatal("expected advancement after record")
	}
	var dispBefore string
	_ = pool.QueryRow(ctx, `SELECT disposition FROM match_results WHERE room_id=$1 AND completion_version=$2`,
		string(slot.RoomID), ver).Scan(&dispBefore)
	if dispBefore != "recorded" {
		t.Fatalf("disp=%s", dispBefore)
	}

	outQ, dQ := commitQuarantineResult(t, ts, "t-qr-ledger-prec", string(slot.RoomID), ver, "qr-ledger-prec", "ops")
	if outQ.Status != envelope.StatusAccepted || dQ.Kind != domain.QuarantineResultLedgerOnly {
		t.Fatalf("explicit quarantine: out=%+v d=%+v", outQ, dQ)
	}
	var nQ, projMid int64
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_result_quarantines WHERE claimed_room_id=$1 AND completion_version=$2`,
		string(slot.RoomID), ver).Scan(&nQ)
	_ = pool.QueryRow(ctx, `SELECT COALESCE((SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1),0)`,
		"t-qr-ledger-prec").Scan(&projMid)
	if nQ != 1 {
		t.Fatalf("quarantines=%d", nQ)
	}

	conflict := make([]domain.PlayerMatchStanding, len(standings))
	copy(conflict, standings)
	if len(conflict) >= 2 {
		conflict[0].MatchWins, conflict[1].MatchWins = conflict[1].MatchWins, conflict[0].MatchWins
	}
	kind := ingestDifferentialKind(t, ts, "t-qr-ledger-prec", domain.RecordMatchResultCommand{
		CommandID: "ingest:ledger-prec-conflict", EventID: "evt-ledger-prec-conflict",
		RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
		CompletionVersion: domain.CompletionVersion(ver), Standings: conflict,
	})
	if kind != domain.RoundMatchExactDuplicate && kind != domain.RoundMatchQuarantineHeld {
		t.Fatalf("ledger must force factless accept, kind=%s", kind)
	}

	var nQAfter, nResults, advAfter, projAfter int64
	var dispAfter string
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_result_quarantines WHERE claimed_room_id=$1 AND completion_version=$2`,
		string(slot.RoomID), ver).Scan(&nQAfter)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_results WHERE room_id=$1 AND completion_version=$2`,
		string(slot.RoomID), ver).Scan(&nResults)
	_ = pool.QueryRow(ctx, `SELECT disposition FROM match_results WHERE room_id=$1 AND completion_version=$2`,
		string(slot.RoomID), ver).Scan(&dispAfter)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM advancement_records WHERE tournament_id=$1 AND slot_id=$2`,
		"t-qr-ledger-prec", string(slot.SlotID)).Scan(&advAfter)
	_ = pool.QueryRow(ctx, `SELECT COALESCE((SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1),0)`,
		"t-qr-ledger-prec").Scan(&projAfter)
	if nQAfter != 1 {
		t.Fatalf("second ledger row forbidden: q=%d", nQAfter)
	}
	if nResults != 1 || dispAfter != "recorded" {
		t.Fatalf("recorded row must stay: results=%d disp=%s", nResults, dispAfter)
	}
	if advAfter != int64(advBefore) {
		t.Fatalf("advancement mutated: %d→%d", advBefore, advAfter)
	}
	if projAfter != projMid {
		t.Fatalf("projection bump forbidden: mid=%d after=%d", projMid, projAfter)
	}
}

func TestIntegration_QuarantineResult_ZeroVersionAndMissingTournament(t *testing.T) {
	_, ts := openStore(t)
	ctx := context.Background()
	uow, err := ts.BeginQuarantineTournamentResult(ctx, "t-missing", "room-1", 1, "qr-miss")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = uow.Rollback() }()
	if uow.Exists() {
		t.Fatal("missing tournament")
	}
	d := domain.DecideQuarantineTournamentResult(uow.Loaded(), domain.QuarantineTournamentResultCommand{
		CommandID: "qr-miss", RoomID: "room-1", CompletionVersion: 1,
	})
	if d.Kind != domain.QuarantineResultReject {
		t.Fatalf("%+v", d)
	}

	d0 := domain.DecideQuarantineTournamentResult(domain.QuarantineTournamentResultContext{Exists: true}, domain.QuarantineTournamentResultCommand{
		CommandID: "qr-0", RoomID: "room-1", CompletionVersion: 0,
	})
	if d0.Kind != domain.QuarantineResultReject {
		t.Fatalf("%+v", d0)
	}
}

func containsAny(s string, parts ...string) bool {
	for _, p := range parts {
		if strings.Contains(s, p) {
			return true
		}
	}
	return false
}

func mustFacts(out envelope.Result) []any {
	var payload map[string]any
	b, _ := json.Marshal(out.Payload)
	_ = json.Unmarshal(b, &payload)
	if payload == nil {
		return nil
	}
	facts, _ := payload["facts"].([]any)
	return facts
}
