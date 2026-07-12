package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/services/tournament-orchestration/store"
	"unoarena/shared/envelope"
)

func TestBracketPage_SummaryUsesBatchCountNotBatchList(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	corr := map[string]string{"X-Correlation-Id": "corr-batch-count"}

	// 110 players → 11 slots with batchSize=1 → 11 batches (lexical batch_10 trap).
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("bc-create", "CreateTournament", map[string]any{
		"tournamentId": "t-batch-count",
		"capacity":     110,
		"batchSize":    1,
	}, "op", "s"), corr)
	for i := 0; i < 110; i++ {
		p := "p" + itoa(i)
		postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("bc-reg-"+p, "RegisterPlayer", map[string]any{
			"tournamentId": "t-batch-count", "playerId": p,
		}, p, "s"), corr)
	}
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("bc-close", "CloseRegistration", map[string]any{
		"tournamentId": "t-batch-count",
	}, "op", "s"), corr)
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("bc-seed", "SeedRound", map[string]any{
		"tournamentId": "t-batch-count", "roundNumber": 1,
	}, "op", "s"), corr)
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("bc-prov", "ProvisionRoundMatches", map[string]any{
		"tournamentId": "t-batch-count", "roundNumber": 1,
	}, "op", "s"), corr)

	batches := bracketBatches(t, h, "t-batch-count", 1)
	if len(batches) < 11 {
		t.Fatalf("want >=11 batches for lexical ordering trap, got %d", len(batches))
	}

	w := getJSON(t, mux, "/v1/tournaments/t-batch-count/bracket?limit=2")
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d %s", w.Code, w.Body.String())
	}
	body := decodeBracket(t, w)
	got := bracketSummaryBatchCount(t, body, 0)
	if got != len(batches) {
		t.Fatalf("batchCount=%d want %d", got, len(batches))
	}
}

func TestProjectionVersion_RejectAndNoOpStable(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	corr := map[string]string{"X-Correlation-Id": "corr-proj-ver"}

	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("pv-create", "CreateTournament", map[string]any{
		"tournamentId": "t-proj-ver",
		"capacity":     4,
		"batchSize":    1,
	}, "op", "s"), corr)
	verAfterCreate, _ := h.repo.ProjectionCheckpoint(domain.TournamentID("t-proj-ver"))
	if verAfterCreate < 1 {
		t.Fatalf("create should bump projection, got %d", verAfterCreate)
	}

	// Rejected command must not bump.
	w := postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("pv-reg-bad", "RegisterPlayer", map[string]any{
		"tournamentId": "t-proj-ver",
		"playerId":     "",
	}, "", "s"), corr)
	if decodeResult(t, w).Status != envelope.StatusRejected {
		t.Fatalf("want reject: %s", w.Body.String())
	}
	verAfterReject, _ := h.repo.ProjectionCheckpoint(domain.TournamentID("t-proj-ver"))
	if verAfterReject != verAfterCreate {
		t.Fatalf("reject bumped version %d -> %d", verAfterCreate, verAfterReject)
	}

	for _, p := range []string{"a", "b", "c", "d"} {
		postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("pv-reg-"+p, "RegisterPlayer", map[string]any{
			"tournamentId": "t-proj-ver", "playerId": p,
		}, p, "s"), corr)
	}
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("pv-close", "CloseRegistration", map[string]any{
		"tournamentId": "t-proj-ver",
	}, "op", "s"), corr)
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("pv-seed", "SeedRound", map[string]any{
		"tournamentId": "t-proj-ver", "roundNumber": 1,
	}, "op", "s"), corr)
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("pv-prov", "ProvisionRoundMatches", map[string]any{
		"tournamentId": "t-proj-ver", "roundNumber": 1,
	}, "op", "s"), corr)

	verBeforeComplete, _ := h.repo.ProjectionCheckpoint(domain.TournamentID("t-proj-ver"))
	b0 := bracketBatches(t, h, "t-proj-ver", 1)[0]
	w = postJSON(t, mux, "/internal/v1/tournaments/t-proj-ver/rounds/1/provisioning-batches", testCred, map[string]any{
		"commandId":     "pv-worker",
		"schemaVersion": 1,
		"batchId":       string(b0.BatchID),
		"slotFrom":      string(b0.SlotFrom),
		"slotTo":        string(b0.SlotTo),
		"slotSize":      len(b0.SlotIndexes),
	}, corr)
	if decodeResult(t, w).Status != envelope.StatusAccepted {
		t.Fatalf("worker complete: %s", w.Body.String())
	}
	verAfterComplete, _ := h.repo.ProjectionCheckpoint(domain.TournamentID("t-proj-ver"))
	if verAfterComplete <= verBeforeComplete {
		t.Fatalf("real complete should bump: before=%d after=%d", verBeforeComplete, verAfterComplete)
	}

	// Semantic no-op: already-completed batch via domain commit path.
	uow, err := h.repo.BeginExisting(domain.TournamentID("t-proj-ver"))
	if err != nil {
		t.Fatal(err)
	}
	tr := uow.Loaded()
	out := tr.CompleteTournamentProvisioningBatch(domain.CompleteTournamentProvisioningBatchCommand{
		CommandID: "pv-complete-dup", RoundNumber: 1, BatchID: b0.BatchID,
	})
	if out.Kind != domain.OutcomeAccepted || len(out.Facts) != 0 {
		_ = uow.Rollback()
		t.Fatalf("noop complete must be factless: %+v", out)
	}
	if err := uow.Commit(CommitRequest{
		Tournament:        tr,
		CommandID:         "pv-complete-dup",
		Outcome:           envelope.Accepted("pv-complete-dup", "CompleteTournamentProvisioningBatch", nil, json.RawMessage(`{"facts":[]}`)),
		ProjectionChanged: projectionChangedFromOutcome(out),
	}); err != nil {
		t.Fatal(err)
	}
	verAfterNoop, _ := h.repo.ProjectionCheckpoint(domain.TournamentID("t-proj-ver"))
	if verAfterNoop != verAfterComplete {
		t.Fatalf("noop complete bumped version %d -> %d", verAfterComplete, verAfterNoop)
	}
}

func TestBracketPage_StoreFailureMaps503(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	corr := map[string]string{"X-Correlation-Id": "corr-503"}
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("s3-create", "CreateTournament", map[string]any{
		"tournamentId": "t-503", "capacity": 2,
	}, "op", "s"), corr)

	h.svc.bracketPages = failingBracketPageLoader{}
	w := getJSON(t, mux, "/v1/tournaments/t-503/bracket")
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503, got %d %s", w.Code, w.Body.String())
	}
	var errBody map[string]any
	_ = json.NewDecoder(w.Body).Decode(&errBody)
	if errBody["code"] != "unavailable" {
		t.Fatalf("err=%v", errBody)
	}
}

type failingBracketPageLoader struct{}

func (failingBracketPageLoader) LoadBracketPage(_ context.Context, _ store.BracketPageQuery) (store.BracketPage, error) {
	return store.BracketPage{}, errors.Join(store.ErrUnavailable, errors.New("injected"))
}
