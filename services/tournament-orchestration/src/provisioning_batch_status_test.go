package main

import (
	"context"
	"encoding/json"
	"testing"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/shared/envelope"
)

func TestProcessProvisioningBatch_RejectsCancelledAndUnexpectedStatus(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	corr := map[string]string{"X-Correlation-Id": "corr-cancel-batch"}
	seedTiny(t, h, mux, "t-cancel-batch", corr)

	w := postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("prov-cb", "ProvisionRoundMatches", map[string]any{
		"tournamentId": "t-cancel-batch",
		"roundNumber":  1,
	}, "op", "s"), corr)
	if decodeResult(t, w).Status != envelope.StatusAccepted {
		t.Fatalf("prov: %s", w.Body.String())
	}

	bracket := getJSON(t, mux, "/v1/tournaments/t-cancel-batch/bracket")
	var body map[string]any
	_ = json.NewDecoder(bracket.Body).Decode(&body)
	_ = body
	b0 := bracketBatches(t, h, "t-cancel-batch", 1)[0]
	batchID := string(b0.BatchID)

	forceBatchStatus(t, h, "t-cancel-batch", batchID, domain.BatchCancelled, "force-cancelled")

	work := ProvisioningBatchWork{
		CommandID:    "proc-cancelled",
		TournamentID: "t-cancel-batch",
		RoundNumber:  1,
		BatchID:      batchID,
		SlotFrom:     string(b0.SlotFrom),
		SlotTo:       string(b0.SlotTo),
		SlotSize:     len(b0.SlotIndexes),
	}

	before := h.rooms.CallCount()
	res, err := h.svc.ProcessProvisioningBatch(context.Background(), work)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != envelope.StatusRejected || res.Reason != string(domain.RejectBatchCancelled) {
		t.Fatalf("want batch_cancelled reject, got %+v", res)
	}
	if h.rooms.CallCount() != before {
		t.Fatal("cancelled must not provision rooms")
	}

	forceBatchStatus(t, h, "t-cancel-batch", batchID, domain.BatchStatus("weird"), "force-weird")
	work.CommandID = "proc-weird"
	res, err = h.svc.ProcessProvisioningBatch(context.Background(), work)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != envelope.StatusRejected || res.Reason != string(domain.RejectUnexpectedBatchStatus) {
		t.Fatalf("want unexpected_batch_status reject, got %+v", res)
	}
	if h.rooms.CallCount() != before {
		t.Fatal("unexpected status must not provision rooms")
	}
}

func forceBatchStatus(t *testing.T, h testHarness, tid, batchID string, status domain.BatchStatus, commandID string) {
	t.Helper()
	uow, err := h.repo.BeginExisting(domain.TournamentID(tid))
	if err != nil {
		t.Fatal(err)
	}
	tr := uow.Loaded()
	round, ok := tr.Round(1)
	if !ok {
		_ = uow.Rollback()
		t.Fatal("round missing")
	}
	batch, ok := round.FindBatch(domain.BatchID(batchID))
	if !ok {
		_ = uow.Rollback()
		t.Fatal("batch missing")
	}
	batch.Status = status
	if err := uow.Commit(CommitRequest{
		Tournament: tr,
		CommandID:  commandID,
		Outcome:    envelope.Accepted(commandID, "ForceBatchStatus", nil, json.RawMessage(`{}`)),
	}); err != nil {
		t.Fatal(err)
	}
}
