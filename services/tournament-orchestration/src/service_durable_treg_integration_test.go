//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"unoarena/shared/envelope"
)

func TestIntegration_ServiceTReg_BypassesMutexAndEnvelope(t *testing.T) {
	pool, _, svc := serviceDurablePostgres(t)
	if !svc.UsesRegistrationDifferential() {
		t.Fatal("expected registration differential wired")
	}

	tid := "svc-treg-1"
	res, err := svc.SubmitCommand(context.Background(), CommandRequest{
		CommandID: "svc-create", Type: CmdCreateTournament, SchemaVersion: 1,
		Payload: mustJSON(map[string]any{"tournamentId": tid, "capacity": 4}),
	}, "corr")
	if err != nil || res.Status != envelope.StatusAccepted {
		t.Fatalf("create: %v %#v", err, res)
	}

	var wg sync.WaitGroup
	wg.Add(20)
	for i := 0; i < 20; i++ {
		go func(i int) {
			defer wg.Done()
			_, _ = svc.SubmitCommand(context.Background(), CommandRequest{
				CommandID: fmt.Sprintf("svc-reg-%d", i), Type: CmdRegisterPlayer, SchemaVersion: 1,
				Payload: mustJSON(map[string]any{"tournamentId": tid, "playerId": "same"}),
			}, "corr")
		}(i)
	}
	wg.Wait()

	ctx := context.Background()
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM tournament_registrations WHERE tournament_id=$1`, tid).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("rows=%d", rows)
	}

	again, err := svc.SubmitCommand(context.Background(), CommandRequest{
		CommandID: "svc-create", Type: CmdCreateTournament, SchemaVersion: 1,
		Payload: mustJSON(map[string]any{"tournamentId": tid, "capacity": 4}),
	}, "corr")
	if err != nil {
		t.Fatal(err)
	}
	if again.CommandID != "svc-create" {
		t.Fatalf("replay commandId=%s", again.CommandID)
	}

	// Close preserves audit/envelope shape.
	closeRes, err := svc.SubmitCommand(context.Background(), CommandRequest{
		CommandID: "svc-close", Type: CmdCloseRegistration, SchemaVersion: 1,
		Payload: mustJSON(map[string]any{"tournamentId": tid}),
	}, "corr")
	if err != nil || closeRes.Status != envelope.StatusAccepted {
		t.Fatalf("close: %v %#v", err, closeRes)
	}
}

func TestIntegration_ServiceTReg_SameCommandIdCanonical(t *testing.T) {
	pool, _, svc := serviceDurablePostgres(t)
	tid := "svc-treg-same-cmd"
	_, err := svc.SubmitCommand(context.Background(), CommandRequest{
		CommandID: "svc-create-2", Type: CmdCreateTournament, SchemaVersion: 1,
		Payload: mustJSON(map[string]any{"tournamentId": tid, "capacity": 8}),
	}, "corr")
	if err != nil {
		t.Fatal(err)
	}

	const cmdID = "svc-shared-reg"
	var wg sync.WaitGroup
	results := make([]envelope.Result, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		res, err := svc.SubmitCommand(context.Background(), CommandRequest{
			CommandID: cmdID, Type: CmdRegisterPlayer, SchemaVersion: 1,
			Payload: mustJSON(map[string]any{"tournamentId": tid, "playerId": "alice"}),
		}, "corr")
		if err != nil {
			t.Errorf("alice: %v", err)
			return
		}
		results[0] = res
	}()
	go func() {
		defer wg.Done()
		res, err := svc.SubmitCommand(context.Background(), CommandRequest{
			CommandID: cmdID, Type: CmdRegisterPlayer, SchemaVersion: 1,
			Payload: mustJSON(map[string]any{"tournamentId": tid, "playerId": "bob"}),
		}, "corr")
		if err != nil {
			t.Errorf("bob: %v", err)
			return
		}
		results[1] = res
	}()
	wg.Wait()
	if results[0].CommandID != results[1].CommandID || results[0].Status != results[1].Status ||
		results[0].Type != results[1].Type || string(results[0].Payload) != string(results[1].Payload) {
		t.Fatalf("canonical mismatch:\n%#v\n%#v", results[0], results[1])
	}
	var rows int
	if err := pool.QueryRow(context.Background(), `SELECT count(*)::int FROM tournament_registrations WHERE tournament_id=$1`, tid).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("rows=%d", rows)
	}
}

func mustJSON(v map[string]any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}
