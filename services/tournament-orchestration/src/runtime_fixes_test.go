package main

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/shared/envelope"
)

func TestAtomicCommit_PublishFailureLeavesPendingRetry(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	corr := map[string]string{"X-Correlation-Id": "corr-outbox"}

	seedTiny(t, h, mux, "t-outbox", corr)
	w := postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("prov-outbox", "ProvisionRoundMatches", map[string]any{
		"tournamentId": "t-outbox",
		"roundNumber":  1,
	}, "op", "s"), corr)
	if decodeResult(t, w).Status != envelope.StatusAccepted {
		t.Fatalf("provision: %s", w.Body.String())
	}
	if h.repo.PendingOutboxLen() == 0 {
		t.Fatal("expected pending contract outbox after provision")
	}
	for _, e := range h.repo.Events() {
		if e.Topic == "tournament.lifecycle" || e.Topic == "" {
			t.Fatalf("unexpected non-contract topic %q for %s", e.Topic, e.EventType)
		}
	}

	h.pub.FailNext = 1
	n, err := h.svc.DrainOutbox(context.Background(), 10)
	if err == nil {
		t.Fatal("expected publish failure")
	}
	if n != 0 {
		t.Fatalf("published=%d want 0", n)
	}
	if h.repo.PendingOutboxLen() == 0 {
		t.Fatal("publish failure must leave pending outbox")
	}

	n, err = h.svc.DrainOutbox(context.Background(), 10)
	if err != nil {
		t.Fatalf("retry drain: %v", err)
	}
	if n == 0 {
		t.Fatal("expected publish on retry")
	}
	if h.repo.PendingOutboxLen() != 0 {
		t.Fatalf("pending after success=%d", h.repo.PendingOutboxLen())
	}
}

func TestDuplicateAcceptedCannotLoseOutboxFacts(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	corr := map[string]string{"X-Correlation-Id": "corr-dup-facts"}

	seedTiny(t, h, mux, "t-dup", corr)
	before := h.repo.OutboxLen()
	w := postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("prov-1", "ProvisionRoundMatches", map[string]any{
		"tournamentId": "t-dup",
		"roundNumber":  1,
	}, "op", "s"), corr)
	if decodeResult(t, w).Status != envelope.StatusAccepted {
		t.Fatalf("prov: %s", w.Body.String())
	}
	after := h.repo.OutboxLen()
	if after <= before {
		t.Fatal("expected TournamentMatchAssigned outbox facts")
	}
	assigned := 0
	for _, e := range h.repo.Events() {
		if e.EventType == "TournamentMatchAssigned" {
			assigned++
		}
	}
	if assigned == 0 {
		t.Fatal("missing assignment facts")
	}

	// Replay same commandId: stable accepted, facts remain.
	w = postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("prov-1", "ProvisionRoundMatches", map[string]any{
		"tournamentId": "t-dup",
		"roundNumber":  1,
	}, "op", "s"), corr)
	if decodeResult(t, w).Status != envelope.StatusAccepted {
		t.Fatal("replay must be accepted")
	}
	if h.repo.OutboxLen() != after {
		t.Fatalf("replay must not drop or duplicate outbox rows: before=%d after=%d", after, h.repo.OutboxLen())
	}
}

func TestCommitFailureDoesNotInstallOutcome(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	corr := map[string]string{"X-Correlation-Id": "corr-commit-fail"}

	h.repo.FailNextCommits = 1
	w := postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("c-fail", "CreateTournament", map[string]any{
		"tournamentId": "t-fail",
		"capacity":     4,
	}, "op", "s"), corr)
	if w.Code == http.StatusOK {
		// Service surfaces commit errors as 400 invalid_envelope.
		if decodeResult(t, w).Status == envelope.StatusAccepted {
			t.Fatal("commit failure must not accept")
		}
	}
	if _, ok := h.repo.LookupOutcome("c-fail"); ok {
		t.Fatal("failed commit must not install outcome")
	}
	if _, ok := h.repo.Get(domain.TournamentID("t-fail")); ok {
		t.Fatal("failed commit must not persist tournament")
	}

	w = postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("c-fail", "CreateTournament", map[string]any{
		"tournamentId": "t-fail",
		"capacity":     4,
	}, "op", "s"), corr)
	if decodeResult(t, w).Status != envelope.StatusAccepted {
		t.Fatalf("retry after commit fault: %s", w.Body.String())
	}
	if _, ok := h.repo.Get(domain.TournamentID("t-fail")); !ok {
		t.Fatal("retry must persist tournament")
	}
	if _, ok := h.repo.LookupOutcome("c-fail"); !ok {
		t.Fatal("retry must install outcome")
	}
	// CreateTournament is internal-only — no durable Kafka/outbox row.
	if h.repo.PendingOutboxLen() != 0 {
		t.Fatalf("CreateTournament must not emit outbox, pending=%d", h.repo.PendingOutboxLen())
	}
}

func TestStrictMatchCompletedIngestValidation(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	corr := map[string]string{"X-Correlation-Id": "corr-strict"}
	setupFinalFour(t, h, mux, corr)

	bracket := getJSON(t, mux, "/v1/tournaments/tour-ingest/bracket")
	var body map[string]any
	_ = json.NewDecoder(bracket.Body).Decode(&body)
	slot0 := bracketSlots(t, body)[0].(map[string]any)

	base := map[string]any{
		"eventId":           "evt-strict",
		"eventType":         "MatchCompleted",
		"schemaVersion":     1,
		"roomId":            slot0["roomId"],
		"tournamentId":      "tour-ingest",
		"roundNumber":       1,
		"slotId":            slot0["slotId"],
		"completionVersion": 1,
		"isAbandoned":       false,
		"players": []map[string]any{
			{"playerId": "a1", "matchWins": 2, "cumulativeCardPoints": 1, "finalGameCompletedAt": time.Now().UTC().Format(time.RFC3339Nano)},
			{"playerId": "a2", "matchWins": 1, "cumulativeCardPoints": 1, "finalGameCompletedAt": time.Now().UTC().Format(time.RFC3339Nano)},
			{"playerId": "a3", "matchWins": 0, "cumulativeCardPoints": 1, "finalGameCompletedAt": time.Now().UTC().Format(time.RFC3339Nano)},
			{"playerId": "a4", "matchWins": 0, "cumulativeCardPoints": 1, "finalGameCompletedAt": time.Now().UTC().Format(time.RFC3339Nano)},
		},
	}

	cases := []struct {
		name string
		mut  func(map[string]any)
	}{
		{"missing_schema", func(m map[string]any) { delete(m, "schemaVersion") }},
		{"bad_event_type", func(m map[string]any) { m["eventType"] = "GameCompleted" }},
		{"empty_event_id", func(m map[string]any) { m["eventId"] = "" }},
		{"missing_abandoned", func(m map[string]any) { delete(m, "isAbandoned") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := cloneMap(base)
			tc.mut(payload)
			w := postJSON(t, mux, "/internal/v1/tournaments/tour-ingest/match-results", testCred, payload, corr)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("want 400, got %d %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestAbandonedIngestQuarantinesAndExactReplayStable(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	corr := map[string]string{"X-Correlation-Id": "corr-abandon"}
	setupFinalFour(t, h, mux, corr)

	bracket := getJSON(t, mux, "/v1/tournaments/tour-ingest/bracket")
	var body map[string]any
	_ = json.NewDecoder(bracket.Body).Decode(&body)
	slot0 := bracketSlots(t, body)[0].(map[string]any)
	base := time.Date(2026, 7, 10, 21, 0, 0, 0, time.UTC)
	ingest := map[string]any{
		"eventId":           "evt-abandon-1",
		"eventType":         "MatchCompleted",
		"schemaVersion":     1,
		"roomId":            slot0["roomId"],
		"tournamentId":      "tour-ingest",
		"roundNumber":       1,
		"slotId":            slot0["slotId"],
		"completionVersion": 4,
		"isAbandoned":       true,
		"players": []map[string]any{
			{"playerId": "a1", "matchWins": 0, "cumulativeCardPoints": 0, "finalGameCompletedAt": base.Format(time.RFC3339Nano)},
			{"playerId": "a2", "matchWins": 0, "cumulativeCardPoints": 0, "finalGameCompletedAt": base.Format(time.RFC3339Nano)},
			{"playerId": "a3", "matchWins": 0, "cumulativeCardPoints": 0, "finalGameCompletedAt": base.Format(time.RFC3339Nano)},
			{"playerId": "a4", "matchWins": 0, "cumulativeCardPoints": 0, "finalGameCompletedAt": base.Format(time.RFC3339Nano)},
		},
	}
	w := postJSON(t, mux, "/internal/v1/tournaments/tour-ingest/match-results", testCred, ingest, corr)
	if w.Code != http.StatusOK {
		t.Fatalf("abandon ingest: %d %s", w.Code, w.Body.String())
	}
	var first map[string]any
	_ = json.NewDecoder(w.Body).Decode(&first)
	if first["disposition"] != "quarantined" {
		t.Fatalf("want quarantined, got %v", first)
	}
	bracket = getJSON(t, mux, "/v1/tournaments/tour-ingest/bracket")
	_ = json.NewDecoder(bracket.Body).Decode(&body)
	slot := bracketSlots(t, body)[0].(map[string]any)
	if slot["status"] != "quarantined" {
		t.Fatalf("slot status=%v", slot["status"])
	}
	adv, _ := slot["advancingPlayerIds"].([]any)
	if len(adv) != 0 {
		t.Fatalf("abandoned must not advance: %v", adv)
	}

	w = postJSON(t, mux, "/internal/v1/tournaments/tour-ingest/match-results", testCred, ingest, corr)
	if w.Code != http.StatusOK {
		t.Fatalf("replay: %d %s", w.Code, w.Body.String())
	}
	var second map[string]any
	_ = json.NewDecoder(w.Body).Decode(&second)
	if second["disposition"] != first["disposition"] || second["commandId"] != first["commandId"] {
		t.Fatalf("exact replay unstable: first=%v second=%v", first, second)
	}
}

func TestRoomProvisionFailureKeepsRetryableWork(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	corr := map[string]string{"X-Correlation-Id": "corr-room-fail"}
	seedTiny(t, h, mux, "t-room-fail", corr)

	w := postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("prov-rf", "ProvisionRoundMatches", map[string]any{
		"tournamentId": "t-room-fail",
		"roundNumber":  1,
	}, "op", "s"), corr)
	if decodeResult(t, w).Status != envelope.StatusAccepted {
		t.Fatalf("prov: %s", w.Body.String())
	}
	assignedBefore := 0
	for _, e := range h.repo.Events() {
		if e.EventType == "TournamentMatchAssigned" {
			assignedBefore++
		}
	}
	if assignedBefore == 0 {
		t.Fatal("assignment outbox required before Room calls")
	}

	bracket := getJSON(t, mux, "/v1/tournaments/t-room-fail/bracket")
	var body map[string]any
	_ = json.NewDecoder(bracket.Body).Decode(&body)
	_ = body
	b0 := bracketBatches(t, h, "t-room-fail", 1)[0]

	h.rooms.FailOnCall = 1
	w = postJSON(t, mux, "/internal/v1/tournaments/t-room-fail/rounds/1/provisioning-batches", testCred, map[string]any{
		"commandId": "worker-fail-1",
		"batchId":   string(b0.BatchID),
		"slotFrom":  string(b0.SlotFrom),
		"slotTo":    string(b0.SlotTo),
		"slotSize":  len(b0.SlotIndexes),
	}, corr)
	res := decodeResult(t, w)
	if res.Status != envelope.StatusAccepted {
		t.Fatalf("failure path should record retry/quarantine saga, got %+v body=%s", res, w.Body.String())
	}
	assignedAfter := 0
	for _, e := range h.repo.Events() {
		if e.EventType == "TournamentMatchAssigned" {
			assignedAfter++
		}
	}
	if assignedAfter != assignedBefore {
		t.Fatalf("Room failure must not drop assignment facts (%d -> %d)", assignedBefore, assignedAfter)
	}

	tr, _ := h.repo.Get(domain.TournamentID("t-room-fail"))
	round, _ := tr.Round(1)
	batch, _ := round.FindBatch(b0.BatchID)
	if batch.Status == domain.BatchCompleted {
		t.Fatal("failed batch must not be completed")
	}
	if batch.Status != domain.BatchRetried && batch.Status != domain.BatchQuarantined {
		t.Fatalf("want retryable/quarantine status, got %s", batch.Status)
	}
}

func TestProvisioningWorkerDoesNotHoldGlobalLockDuringRoomCalls(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	corr := map[string]string{"X-Correlation-Id": "corr-nolock"}
	seedTiny(t, h, mux, "t-nolock", corr)
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("prov-nl", "ProvisionRoundMatches", map[string]any{
		"tournamentId": "t-nolock",
		"roundNumber":  1,
	}, "op", "s"), corr)

	bracket := getJSON(t, mux, "/v1/tournaments/t-nolock/bracket")
	var body map[string]any
	_ = json.NewDecoder(bracket.Body).Decode(&body)
	_ = body
	b0 := bracketBatches(t, h, "t-nolock", 1)[0]

	var concurrentOK atomic.Bool
	gate := make(chan struct{})
	h.rooms.HoldDuringCall = func() {
		// While Room call is in flight, standings must proceed (service lock released).
		go func() {
			w := getJSON(t, mux, "/v1/tournaments/t-nolock/standings")
			if w.Code == http.StatusOK {
				concurrentOK.Store(true)
			}
			close(gate)
		}()
		select {
		case <-gate:
		case <-time.After(2 * time.Second):
			t.Error("standings blocked during Room call")
		}
	}

	w := postJSON(t, mux, "/internal/v1/tournaments/t-nolock/rounds/1/provisioning-batches", testCred, map[string]any{
		"commandId": "worker-nl",
		"batchId":   string(b0.BatchID),
		"slotFrom":  string(b0.SlotFrom),
		"slotTo":    string(b0.SlotTo),
		"slotSize":  len(b0.SlotIndexes),
	}, corr)
	if decodeResult(t, w).Status != envelope.StatusAccepted {
		t.Fatalf("worker: %s", w.Body.String())
	}
	if !concurrentOK.Load() {
		t.Fatal("expected concurrent read during Room provision")
	}
}

func TestMillionScaleBatchBoundValidation(t *testing.T) {
	h := newTestHarness(t)
	work := ProvisioningBatchWork{
		CommandID:    "scale-1",
		TournamentID: "t",
		RoundNumber:  1,
		BatchID:      "batch_0",
		SlotFrom:     "slot_0",
		SlotTo:       "slot_9999",
		SlotSize:     domain.MaxProvisioningBatchSize + 1,
	}
	res, err := h.svc.ProcessProvisioningBatch(context.Background(), work)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if res.Status != envelope.StatusRejected {
		t.Fatalf("want rejected oversized batch, got %+v", res)
	}
}

func TestConcurrentIngestExactEventIdempotent(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	corr := map[string]string{"X-Correlation-Id": "corr-conc"}
	setupFinalFour(t, h, mux, corr)

	bracket := getJSON(t, mux, "/v1/tournaments/tour-ingest/bracket")
	var body map[string]any
	_ = json.NewDecoder(bracket.Body).Decode(&body)
	slot0 := bracketSlots(t, body)[0].(map[string]any)
	base := time.Date(2026, 7, 10, 22, 0, 0, 0, time.UTC)
	ingest := map[string]any{
		"eventId":           "evt-conc-1",
		"eventType":         "MatchCompleted",
		"schemaVersion":     1,
		"roomId":            slot0["roomId"],
		"tournamentId":      "tour-ingest",
		"roundNumber":       1,
		"slotId":            slot0["slotId"],
		"completionVersion": 11,
		"isAbandoned":       false,
		"players": []map[string]any{
			{"playerId": "a1", "matchWins": 2, "cumulativeCardPoints": 50, "finalGameCompletedAt": base.Format(time.RFC3339Nano)},
			{"playerId": "a2", "matchWins": 1, "cumulativeCardPoints": 40, "finalGameCompletedAt": base.Add(time.Minute).Format(time.RFC3339Nano)},
			{"playerId": "a3", "matchWins": 0, "cumulativeCardPoints": 30, "finalGameCompletedAt": base.Add(2 * time.Minute).Format(time.RFC3339Nano)},
			{"playerId": "a4", "matchWins": 0, "cumulativeCardPoints": 20, "finalGameCompletedAt": base.Add(3 * time.Minute).Format(time.RFC3339Nano)},
		},
	}

	var wg sync.WaitGroup
	errs := make(chan int, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := postJSON(t, mux, "/internal/v1/tournaments/tour-ingest/match-results", testCred, ingest, corr)
			errs <- w.Code
		}()
	}
	wg.Wait()
	close(errs)
	ok := 0
	for code := range errs {
		if code == http.StatusOK {
			ok++
		}
	}
	if ok != 16 {
		t.Fatalf("expected all 200, got %d ok", ok)
	}
	quarantine := 0
	recorded := 0
	for _, e := range h.repo.Events() {
		switch e.EventType {
		case "TournamentMatchResultRecorded":
			recorded++
		case "TournamentResultQuarantined":
			quarantine++
		}
	}
	if recorded != 1 || quarantine != 0 {
		t.Fatalf("concurrent exact replay must record once: recorded=%d quarantine=%d", recorded, quarantine)
	}
}

func seedTiny(t *testing.T, h testHarness, mux http.Handler, tournamentID string, corr map[string]string) {
	t.Helper()
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody(tournamentID+"-c", "CreateTournament", map[string]any{
		"tournamentId": tournamentID,
		"capacity":     4,
		"batchSize":    2,
		"retryBudget":  3,
	}, "op", "s"), corr)
	for _, p := range []string{"p1", "p2", "p3", "p4"} {
		postJSON(t, mux, "/internal/v1/commands", testCred, commandBody(tournamentID+"-r-"+p, "RegisterPlayer", map[string]any{
			"tournamentId": tournamentID,
			"playerId":     p,
		}, p, "s"), corr)
	}
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody(tournamentID+"-close", "CloseRegistration", map[string]any{
		"tournamentId": tournamentID,
	}, "op", "s"), corr)
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody(tournamentID+"-seed", "SeedRound", map[string]any{
		"tournamentId": tournamentID,
		"roundNumber":  1,
	}, "op", "s"), corr)
}

func cloneMap(in map[string]any) map[string]any {
	b, _ := json.Marshal(in)
	out := map[string]any{}
	_ = json.Unmarshal(b, &out)
	return out
}

func TestFinding_ReadyRequiresInternalCredential(t *testing.T) {
	svc := NewService(ServiceDeps{
		Repo:  NewMemoryTournamentRepository(),
		IDs:   &seqIDs{},
		Clock: fixedClock{now: time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)},
		Audit: NewMemoryAudit(),
	})
	srv := NewServer(svc, "")
	mux := srv.Routes()
	w := getJSON(t, mux, "/ready")
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("ready status=%d", w.Code)
	}
	w2 := postJSON(t, mux, "/internal/v1/commands", "", commandBody("c1", "CreateTournament", map[string]any{
		"tournamentId": "t1", "capacity": 4,
	}, "op", "s"), nil)
	if w2.Code != http.StatusServiceUnavailable {
		t.Fatalf("write status=%d want 503", w2.Code)
	}
}
