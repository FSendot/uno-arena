//go:build integration

package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/services/tournament-orchestration/store"
	"unoarena/shared/envelope"
)

func serviceDurableQuarantineResultPostgres(t *testing.T) (*store.Pool, *store.TournamentStore, *Service, *countingDurableRepo) {
	t.Helper()
	pool, ts, _ := serviceDurablePostgres(t)
	counted := &countingDurableRepo{durableRepo: &durableRepo{store: ts}}
	svc := NewService(ServiceDeps{
		Repo:              counted,
		RoundMatches:      &durableRoundMatchRepo{store: ts},
		Registrations:     &durableRegistrationRepo{store: ts},
		QuarantineResults: &durableQuarantineResultRepo{store: ts},
		Audit:             NoopAudit{},
		BracketPages:      ts,
	})
	return pool, ts, svc, counted
}

func TestIntegration_ServiceDurable_RecordThenExplicitQuarantineThenConflictingRecord_Factless(t *testing.T) {
	pool, ts, svc, _ := serviceDurableQuarantineResultPostgres(t)
	ctx := context.Background()

	tr, _ := domain.CreateTournament(domain.CreateTournamentCommand{
		CommandID: "c", TournamentID: "t-svc-qr-prec", Capacity: 12,
	})
	for i := 0; i < 12; i++ {
		_ = tr.RegisterPlayer(domain.RegisterPlayerCommand{
			CommandID: domain.CommandID("r" + itoa(i)), PlayerID: domain.PlayerID("p" + itoa(i)),
		})
	}
	_ = tr.CloseRegistration(domain.CloseRegistrationCommand{CommandID: "close"})
	_ = tr.SeedRound(domain.SeedRoundCommand{CommandID: "seed", RoundNumber: 1})
	_ = tr.ProvisionRoundMatches(domain.ProvisionRoundMatchesCommand{CommandID: "prov", RoundNumber: 1})
	if err := ts.Commit(ctx, store.CommitRequest{
		Tournament: tr, CommandID: "setup-qr-prec",
		Outcome:           envelope.Accepted("setup-qr-prec", "ProvisionRoundMatches", nil, json.RawMessage(`{}`)),
		ProjectionChanged: true,
	}); err != nil {
		t.Fatal(err)
	}
	got, ok := ts.Get(ctx, "t-svc-qr-prec")
	if !ok {
		t.Fatal("missing")
	}
	round, _ := got.Round(1)
	slot := round.Slots[0]
	standings := standingsFromSlotPlayers(slot)
	const ver uint64 = 17

	rec, err := svc.SubmitCommand(ctx, CommandRequest{
		CommandID: "svc-qr-prec-rec", Type: CmdRecordMatchResult, SchemaVersion: envelope.CurrentSchemaVersion,
		Payload: recordMatchHTTPPayload("t-svc-qr-prec", string(slot.RoomID), string(slot.SlotID), 1, ver, standings, map[string]any{
			"eventId": "svc-qr-prec-evt",
		}),
	}, "corr")
	if err != nil || rec.Status != envelope.StatusAccepted {
		t.Fatalf("record: err=%v out=%+v", err, rec)
	}

	var advBefore int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM advancement_records WHERE tournament_id=$1 AND slot_id=$2`,
		"t-svc-qr-prec", string(slot.SlotID)).Scan(&advBefore)
	if advBefore == 0 {
		t.Fatal("expected advancement")
	}

	qPayload, _ := json.Marshal(map[string]any{
		"tournamentId": "t-svc-qr-prec", "roomId": string(slot.RoomID), "completionVersion": ver,
		"reason": "operator hold",
	})
	qOut, err := svc.SubmitCommand(ctx, CommandRequest{
		CommandID: "svc-qr-prec-q", Type: CmdQuarantineResult, SchemaVersion: envelope.CurrentSchemaVersion,
		Payload: qPayload,
	}, "corr")
	if err != nil || qOut.Status != envelope.StatusAccepted {
		t.Fatalf("quarantine: err=%v out=%+v", err, qOut)
	}
	var nQ, projMid int64
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_result_quarantines WHERE claimed_room_id=$1 AND completion_version=$2`,
		string(slot.RoomID), ver).Scan(&nQ)
	_ = pool.QueryRow(ctx, `SELECT COALESCE((SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1),0)`,
		"t-svc-qr-prec").Scan(&projMid)
	if nQ != 1 {
		t.Fatalf("quarantines=%d", nQ)
	}

	conflictStandings := make([]map[string]any, len(standings))
	for i, row := range standings {
		cp := map[string]any{}
		for k, v := range row {
			cp[k] = v
		}
		if i == 0 {
			cp["matchWins"] = 1
		} else if i == 1 {
			cp["matchWins"] = len(standings)
		}
		conflictStandings[i] = cp
	}
	conflict, err := svc.SubmitCommand(ctx, CommandRequest{
		CommandID: "svc-qr-prec-conflict", Type: CmdRecordMatchResult, SchemaVersion: envelope.CurrentSchemaVersion,
		Payload: recordMatchHTTPPayload("t-svc-qr-prec", string(slot.RoomID), string(slot.SlotID), 1, ver, conflictStandings, map[string]any{
			"eventId": "svc-qr-prec-conflict-evt",
		}),
	}, "corr")
	if err != nil || conflict.Status != envelope.StatusAccepted {
		t.Fatalf("conflict: err=%v out=%+v", err, conflict)
	}
	var body map[string]any
	_ = json.Unmarshal(conflict.Payload, &body)
	facts, _ := body["facts"].([]any)
	if len(facts) != 0 {
		t.Fatalf("must be factless, facts=%v payload=%s", facts, conflict.Payload)
	}

	var nQAfter, nResults, advAfter, projAfter int64
	var disp string
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_result_quarantines WHERE claimed_room_id=$1 AND completion_version=$2`,
		string(slot.RoomID), ver).Scan(&nQAfter)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_results WHERE room_id=$1 AND completion_version=$2`,
		string(slot.RoomID), ver).Scan(&nResults)
	_ = pool.QueryRow(ctx, `SELECT disposition FROM match_results WHERE room_id=$1 AND completion_version=$2`,
		string(slot.RoomID), ver).Scan(&disp)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM advancement_records WHERE tournament_id=$1 AND slot_id=$2`,
		"t-svc-qr-prec", string(slot.SlotID)).Scan(&advAfter)
	_ = pool.QueryRow(ctx, `SELECT COALESCE((SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1),0)`,
		"t-svc-qr-prec").Scan(&projAfter)
	if nQAfter != 1 {
		t.Fatalf("second ledger row forbidden: %d", nQAfter)
	}
	if nResults != 1 || disp != "recorded" {
		t.Fatalf("recorded preserved: results=%d disp=%s", nResults, disp)
	}
	if advAfter != int64(advBefore) {
		t.Fatalf("advancement mutated: %d→%d", advBefore, advAfter)
	}
	if projAfter != projMid {
		t.Fatalf("projection bump forbidden: mid=%d after=%d", projMid, projAfter)
	}
}

func TestIntegration_ServiceDurable_QuarantineResultNoLegacyBeginExisting(t *testing.T) {
	pool, ts, svc, counted := serviceDurableQuarantineResultPostgres(t)
	ctx := context.Background()

	tr, _ := domain.CreateTournament(domain.CreateTournamentCommand{
		CommandID: "c", TournamentID: "t-svc-qr", Capacity: 12,
	})
	for i := 0; i < 12; i++ {
		_ = tr.RegisterPlayer(domain.RegisterPlayerCommand{
			CommandID: domain.CommandID("r" + itoa(i)), PlayerID: domain.PlayerID("p" + itoa(i)),
		})
	}
	_ = tr.CloseRegistration(domain.CloseRegistrationCommand{CommandID: "close"})
	_ = tr.SeedRound(domain.SeedRoundCommand{CommandID: "seed", RoundNumber: 1})
	_ = tr.ProvisionRoundMatches(domain.ProvisionRoundMatchesCommand{CommandID: "prov", RoundNumber: 1})
	if err := ts.Commit(ctx, store.CommitRequest{
		Tournament: tr, CommandID: "setup-qr",
		Outcome:           envelope.Accepted("setup-qr", "ProvisionRoundMatches", nil, json.RawMessage(`{}`)),
		ProjectionChanged: true,
	}); err != nil {
		t.Fatal(err)
	}
	got, ok := ts.Get(ctx, "t-svc-qr")
	if !ok {
		t.Fatal("missing")
	}
	round, _ := got.Round(1)
	slot := round.Slots[0]

	before := counted.beginExisting.Load()
	payload, _ := json.Marshal(map[string]any{
		"tournamentId": "t-svc-qr", "roomId": string(slot.RoomID), "completionVersion": 7,
		"reason": "operator hold NEVER persist",
	})
	out, err := svc.SubmitCommand(ctx, CommandRequest{
		CommandID: "svc-qr-1", Type: CmdQuarantineResult, SchemaVersion: 1, Payload: payload,
	}, "corr")
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != envelope.StatusAccepted {
		t.Fatalf("%+v", out)
	}
	if counted.beginExisting.Load() != before {
		t.Fatalf("QuarantineTournamentResult must not call BeginExisting (before=%d after=%d)", before, counted.beginExisting.Load())
	}

	var reason string
	var nQ int
	_ = pool.QueryRow(ctx, `
		SELECT quarantine_reason FROM match_results WHERE room_id=$1 AND completion_version=7
	`, string(slot.RoomID)).Scan(&reason)
	_ = pool.QueryRow(ctx, `
		SELECT count(*) FROM match_result_quarantines WHERE claimed_room_id=$1 AND completion_version=7
	`, string(slot.RoomID)).Scan(&nQ)
	if reason != domain.ExplicitQuarantineReasonCode || nQ != 1 {
		t.Fatalf("reason=%q nQ=%d", reason, nQ)
	}
	if strings.Contains(string(out.Payload), "operator") || strings.Contains(string(out.Payload), "NEVER") {
		t.Fatalf("raw reason in outcome: %s", out.Payload)
	}

	unkPayload, _ := json.Marshal(map[string]any{
		"tournamentId": "t-svc-qr", "roomId": "no-such-room", "completionVersion": 2,
	})
	before = counted.beginExisting.Load()
	unk, err := svc.SubmitCommand(ctx, CommandRequest{
		CommandID: "svc-qr-unk", Type: CmdQuarantineResult, SchemaVersion: 1, Payload: unkPayload,
	}, "corr")
	if err != nil {
		t.Fatal(err)
	}
	if unk.Status != envelope.StatusAccepted {
		t.Fatalf("%+v", unk)
	}
	if counted.beginExisting.Load() != before {
		t.Fatal("unknown-room quarantine must stay on differential path")
	}
	var nResults int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM match_results WHERE room_id=$1`, "no-such-room").Scan(&nResults)
	if nResults != 0 {
		t.Fatalf("unknown room must not insert match_results, got %d", nResults)
	}
}
