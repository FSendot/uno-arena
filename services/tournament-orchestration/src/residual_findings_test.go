package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/shared/envelope"
)

func TestExistingAggregateCommitFailureDoesNotLeakCommandMutation(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	corr := map[string]string{"X-Correlation-Id": "corr-commit-existing"}

	w := postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("c-create", "CreateTournament", map[string]any{
		"tournamentId": "t-leak",
		"capacity":     4,
	}, "op", "s"), corr)
	if decodeResult(t, w).Status != envelope.StatusAccepted {
		t.Fatalf("create: %s", w.Body.String())
	}

	h.repo.FailNextCommits = 1
	w = postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("c-reg", "RegisterPlayer", map[string]any{
		"tournamentId": "t-leak",
		"playerId":     "p1",
	}, "p1", "s"), corr)
	if w.Code == http.StatusOK && decodeResult(t, w).Status == envelope.StatusAccepted {
		t.Fatal("commit failure must not accept register")
	}
	if _, ok := h.repo.LookupOutcome("c-reg"); ok {
		t.Fatal("failed commit must not install register outcome")
	}

	tr, ok := h.repo.Get(domain.TournamentID("t-leak"))
	if !ok {
		t.Fatal("tournament missing")
	}
	if tr.RegisteredCount() != 0 {
		t.Fatalf("mutation leaked before commit: registered=%d", tr.RegisteredCount())
	}
	if tr.IsRegistered(domain.PlayerID("p1")) {
		t.Fatal("player must not be registered after failed commit")
	}

	w = postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("c-reg", "RegisterPlayer", map[string]any{
		"tournamentId": "t-leak",
		"playerId":     "p1",
	}, "p1", "s"), corr)
	if decodeResult(t, w).Status != envelope.StatusAccepted {
		t.Fatalf("retry register: %s", w.Body.String())
	}
	tr, _ = h.repo.Get(domain.TournamentID("t-leak"))
	if !tr.IsRegistered(domain.PlayerID("p1")) {
		t.Fatal("retry must persist registration")
	}
}

func TestExistingAggregateCommitFailureDoesNotLeakIngestMutation(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	corr := map[string]string{"X-Correlation-Id": "corr-commit-ingest"}
	setupFinalFour(t, h, mux, corr)

	bracket := getJSON(t, mux, "/v1/tournaments/tour-ingest/bracket")
	var body map[string]any
	_ = json.NewDecoder(bracket.Body).Decode(&body)
	slot0 := bracketSlots(t, body)[0].(map[string]any)
	base := time.Date(2026, 7, 10, 23, 0, 0, 0, time.UTC)
	ingest := map[string]any{
		"eventId":           "evt-commit-fail",
		"eventType":         "MatchCompleted",
		"schemaVersion":     1,
		"roomId":            slot0["roomId"],
		"tournamentId":      "tour-ingest",
		"roundNumber":       1,
		"slotId":            slot0["slotId"],
		"completionVersion": 7,
		"isAbandoned":       false,
		"players": []map[string]any{
			{"playerId": "a1", "matchWins": 2, "cumulativeCardPoints": 10, "finalGameCompletedAt": base.Format(time.RFC3339Nano)},
			{"playerId": "a2", "matchWins": 1, "cumulativeCardPoints": 9, "finalGameCompletedAt": base.Add(time.Minute).Format(time.RFC3339Nano)},
			{"playerId": "a3", "matchWins": 0, "cumulativeCardPoints": 8, "finalGameCompletedAt": base.Add(2 * time.Minute).Format(time.RFC3339Nano)},
			{"playerId": "a4", "matchWins": 0, "cumulativeCardPoints": 7, "finalGameCompletedAt": base.Add(3 * time.Minute).Format(time.RFC3339Nano)},
		},
	}

	h.repo.FailNextCommits = 1
	w := postJSON(t, mux, "/internal/v1/tournaments/tour-ingest/match-results", testCred, ingest, corr)
	if w.Code == http.StatusOK {
		var res map[string]any
		_ = json.NewDecoder(w.Body).Decode(&res)
		if res["disposition"] == "recorded" {
			t.Fatal("commit failure must not record result")
		}
	}
	if _, ok := h.repo.LookupOutcome("ingest:evt-commit-fail"); ok {
		t.Fatal("failed ingest commit must not install outcome")
	}

	bracket = getJSON(t, mux, "/v1/tournaments/tour-ingest/bracket")
	_ = json.NewDecoder(bracket.Body).Decode(&body)
	slot := bracketSlots(t, body)[0].(map[string]any)
	if slot["status"] == "result_recorded" || slot["status"] == "advanced" {
		t.Fatalf("ingest mutation leaked before commit: status=%v", slot["status"])
	}

	w = postJSON(t, mux, "/internal/v1/tournaments/tour-ingest/match-results", testCred, ingest, corr)
	if w.Code != http.StatusOK {
		t.Fatalf("retry ingest: %d %s", w.Code, w.Body.String())
	}
	var okRes map[string]any
	_ = json.NewDecoder(w.Body).Decode(&okRes)
	if okRes["disposition"] != "recorded" {
		t.Fatalf("retry want recorded, got %v", okRes)
	}
}

func TestAcceptedMatchCompletedReplayPreservesRecordedDisposition(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	corr := map[string]string{"X-Correlation-Id": "corr-recorded-replay"}
	setupFinalFour(t, h, mux, corr)

	bracket := getJSON(t, mux, "/v1/tournaments/tour-ingest/bracket")
	var body map[string]any
	_ = json.NewDecoder(bracket.Body).Decode(&body)
	slot0 := bracketSlots(t, body)[0].(map[string]any)
	base := time.Date(2026, 7, 10, 23, 30, 0, 0, time.UTC)
	ingest := map[string]any{
		"eventId":           "evt-recorded-1",
		"eventType":         "MatchCompleted",
		"schemaVersion":     1,
		"roomId":            slot0["roomId"],
		"tournamentId":      "tour-ingest",
		"roundNumber":       1,
		"slotId":            slot0["slotId"],
		"completionVersion": 3,
		"isAbandoned":       false,
		"players": []map[string]any{
			{"playerId": "a1", "matchWins": 2, "cumulativeCardPoints": 50, "finalGameCompletedAt": base.Format(time.RFC3339Nano)},
			{"playerId": "a2", "matchWins": 1, "cumulativeCardPoints": 40, "finalGameCompletedAt": base.Add(time.Minute).Format(time.RFC3339Nano)},
			{"playerId": "a3", "matchWins": 0, "cumulativeCardPoints": 30, "finalGameCompletedAt": base.Add(2 * time.Minute).Format(time.RFC3339Nano)},
			{"playerId": "a4", "matchWins": 0, "cumulativeCardPoints": 20, "finalGameCompletedAt": base.Add(3 * time.Minute).Format(time.RFC3339Nano)},
		},
	}

	w := postJSON(t, mux, "/internal/v1/tournaments/tour-ingest/match-results", testCred, ingest, corr)
	if w.Code != http.StatusOK {
		t.Fatalf("ingest: %d %s", w.Code, w.Body.String())
	}
	firstRaw := w.Body.Bytes()
	var first map[string]any
	_ = json.Unmarshal(firstRaw, &first)
	if first["disposition"] != "recorded" {
		t.Fatalf("want recorded, got %v", first)
	}

	w = postJSON(t, mux, "/internal/v1/tournaments/tour-ingest/match-results", testCred, ingest, corr)
	if w.Code != http.StatusOK {
		t.Fatalf("replay: %d %s", w.Code, w.Body.String())
	}
	secondRaw := w.Body.Bytes()
	if !bytes.Equal(normalizeJSON(t, firstRaw), normalizeJSON(t, secondRaw)) {
		t.Fatalf("exact accepted replay must be byte-stable\nfirst=%s\nsecond=%s", firstRaw, secondRaw)
	}
}

func TestMatchResultPathTournamentIdMismatchRejectedWithoutMutation(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	corr := map[string]string{"X-Correlation-Id": "corr-tid-mismatch"}
	setupFinalFour(t, h, mux, corr)

	bracket := getJSON(t, mux, "/v1/tournaments/tour-ingest/bracket")
	var body map[string]any
	_ = json.NewDecoder(bracket.Body).Decode(&body)
	slot0 := bracketSlots(t, body)[0].(map[string]any)
	beforeEvents := h.repo.OutboxLen()
	base := time.Date(2026, 7, 10, 23, 45, 0, 0, time.UTC)

	w := postJSON(t, mux, "/internal/v1/tournaments/tour-ingest/match-results", testCred, map[string]any{
		"eventId":           "evt-tid-mismatch",
		"eventType":         "MatchCompleted",
		"schemaVersion":     1,
		"roomId":            slot0["roomId"],
		"tournamentId":      "other-tour",
		"roundNumber":       1,
		"slotId":            slot0["slotId"],
		"completionVersion": 5,
		"isAbandoned":       false,
		"players": []map[string]any{
			{"playerId": "a1", "matchWins": 2, "cumulativeCardPoints": 1, "finalGameCompletedAt": base.Format(time.RFC3339Nano)},
			{"playerId": "a2", "matchWins": 1, "cumulativeCardPoints": 1, "finalGameCompletedAt": base.Format(time.RFC3339Nano)},
			{"playerId": "a3", "matchWins": 0, "cumulativeCardPoints": 1, "finalGameCompletedAt": base.Format(time.RFC3339Nano)},
			{"playerId": "a4", "matchWins": 0, "cumulativeCardPoints": 1, "finalGameCompletedAt": base.Format(time.RFC3339Nano)},
		},
	}, corr)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("want 400 mismatch, got %d %s", w.Code, w.Body.String())
	}
	if h.repo.OutboxLen() != beforeEvents {
		t.Fatal("mismatch must not mutate/outbox")
	}
	if _, ok := h.repo.LookupOutcome("ingest:evt-tid-mismatch"); ok {
		t.Fatal("mismatch must not install outcome")
	}
}

func TestProvisionFailureExactReplayPreservesNextRetryWork(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	corr := map[string]string{"X-Correlation-Id": "corr-retry-replay"}
	seedTiny(t, h, mux, "t-retry-replay", corr)

	w := postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("prov-rr", "ProvisionRoundMatches", map[string]any{
		"tournamentId": "t-retry-replay",
		"roundNumber":  1,
	}, "op", "s"), corr)
	if decodeResult(t, w).Status != envelope.StatusAccepted {
		t.Fatalf("prov: %s", w.Body.String())
	}

	bracket := getJSON(t, mux, "/v1/tournaments/t-retry-replay/bracket")
	var body map[string]any
	_ = json.NewDecoder(bracket.Body).Decode(&body)
	_ = body
	b0 := bracketBatches(t, h, "t-retry-replay", 1)[0]
	batchID := string(b0.BatchID)
	firstCmd := provisioningAttemptCommandID("t-retry-replay", 1, batchID, 0)
	batchBody := map[string]any{
		"commandId":    firstCmd,
		"batchId":      batchID,
		"slotFrom":     string(b0.SlotFrom),
		"slotTo":       string(b0.SlotTo),
		"slotSize":     len(b0.SlotIndexes),
		"retryAttempt": 0,
	}

	h.rooms.FailOnCall = 1
	w = postJSON(t, mux, "/internal/v1/tournaments/t-retry-replay/rounds/1/provisioning-batches", testCred, batchBody, corr)
	first := decodeResult(t, w)
	if first.Status != envelope.StatusAccepted {
		t.Fatalf("failure path: %+v body=%s", first, w.Body.String())
	}
	var firstPayload map[string]any
	if err := json.Unmarshal(first.Payload, &firstPayload); err != nil {
		t.Fatalf("first payload: %v", err)
	}
	if _, ok := firstPayload["nextRetryWork"].(map[string]any); !ok {
		t.Fatalf("expected nextRetryWork in first payload, got %v", firstPayload)
	}
	firstPayloadRaw := normalizeJSON(t, first.Payload)

	callsAfterFail := h.rooms.CallCount()
	w = postJSON(t, mux, "/internal/v1/tournaments/t-retry-replay/rounds/1/provisioning-batches", testCred, batchBody, corr)
	replay := decodeResult(t, w)
	if replay.Status != envelope.StatusAccepted {
		t.Fatalf("exact replay: %+v body=%s", replay, w.Body.String())
	}
	if h.rooms.CallCount() != callsAfterFail {
		t.Fatal("exact attempt replay must not re-invoke Room")
	}
	replayPayloadRaw := normalizeJSON(t, replay.Payload)
	if !bytes.Equal(firstPayloadRaw, replayPayloadRaw) {
		t.Fatalf("exact replay must be byte-stable including nextRetryWork\nfirst=%s\nreplay=%s", first.Payload, replay.Payload)
	}
	var replayPayload map[string]any
	if err := json.Unmarshal(replay.Payload, &replayPayload); err != nil {
		t.Fatalf("replay payload: %v", err)
	}
	if _, ok := replayPayload["nextRetryWork"].(map[string]any); !ok {
		t.Fatalf("expected nextRetryWork in replay payload, got %v", replayPayload)
	}
}

func TestProvisioningRetryAttemptIdentityAllowsRetryAfterRoomFailure(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	corr := map[string]string{"X-Correlation-Id": "corr-retry-id"}
	seedTiny(t, h, mux, "t-retry-id", corr)

	w := postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("prov-ri", "ProvisionRoundMatches", map[string]any{
		"tournamentId": "t-retry-id",
		"roundNumber":  1,
	}, "op", "s"), corr)
	if decodeResult(t, w).Status != envelope.StatusAccepted {
		t.Fatalf("prov: %s", w.Body.String())
	}

	bracket := getJSON(t, mux, "/v1/tournaments/t-retry-id/bracket")
	var body map[string]any
	_ = json.NewDecoder(bracket.Body).Decode(&body)
	_ = body
	b0 := bracketBatches(t, h, "t-retry-id", 1)[0]
	batchID := string(b0.BatchID)

	h.rooms.FailOnCall = 1
	firstCmd := provisioningAttemptCommandID("t-retry-id", 1, batchID, 0)
	w = postJSON(t, mux, "/internal/v1/tournaments/t-retry-id/rounds/1/provisioning-batches", testCred, map[string]any{
		"commandId":    firstCmd,
		"batchId":      batchID,
		"slotFrom":     string(b0.SlotFrom),
		"slotTo":       string(b0.SlotTo),
		"slotSize":     len(b0.SlotIndexes),
		"retryAttempt": 0,
	}, corr)
	res := decodeResult(t, w)
	if res.Status != envelope.StatusAccepted {
		t.Fatalf("failure path: %+v body=%s", res, w.Body.String())
	}
	var payload map[string]any
	_ = json.Unmarshal(res.Payload, &payload)
	nextRaw, ok := payload["nextRetryWork"].(map[string]any)
	if !ok {
		t.Fatalf("expected nextRetryWork in payload, got %v", payload)
	}
	nextCmd, _ := nextRaw["commandId"].(string)
	nextAttempt := int(nextRaw["retryAttempt"].(float64))
	if nextCmd == "" || nextCmd == firstCmd {
		t.Fatalf("next retry work must use distinct attempt identity, got %q", nextCmd)
	}
	if nextAttempt != 1 {
		t.Fatalf("next retryAttempt=%d want 1", nextAttempt)
	}

	// Exact failed-attempt duplicate is stable and must not re-call Room.
	callsAfterFail := h.rooms.CallCount()
	w = postJSON(t, mux, "/internal/v1/tournaments/t-retry-id/rounds/1/provisioning-batches", testCred, map[string]any{
		"commandId":    firstCmd,
		"batchId":      batchID,
		"slotFrom":     string(b0.SlotFrom),
		"slotTo":       string(b0.SlotTo),
		"slotSize":     len(b0.SlotIndexes),
		"retryAttempt": 0,
	}, corr)
	if decodeResult(t, w).Status != envelope.StatusAccepted {
		t.Fatal("exact attempt replay must be accepted/stable")
	}
	if h.rooms.CallCount() != callsAfterFail {
		t.Fatal("exact attempt duplicate must not re-invoke Room")
	}

	// Next attempt identity must not short-circuit on prior accepted retry outcome.
	h.rooms.FailOnCall = 0
	h.rooms.ResetCalls()
	w = postJSON(t, mux, "/internal/v1/tournaments/t-retry-id/rounds/1/provisioning-batches", testCred, map[string]any{
		"commandId":    nextCmd,
		"batchId":      batchID,
		"slotFrom":     string(b0.SlotFrom),
		"slotTo":       string(b0.SlotTo),
		"slotSize":     len(b0.SlotIndexes),
		"retryAttempt": nextAttempt,
	}, corr)
	if decodeResult(t, w).Status != envelope.StatusAccepted {
		t.Fatalf("retry attempt: %s", w.Body.String())
	}
	if h.rooms.CallCount() == 0 {
		t.Fatal("next attempt must invoke Room again")
	}

	tr, _ := h.repo.Get(domain.TournamentID("t-retry-id"))
	round, _ := tr.Round(1)
	batch, _ := round.FindBatch(domain.BatchID(batchID))
	if batch.Status != domain.BatchCompleted {
		t.Fatalf("want completed after successful retry, got %s", batch.Status)
	}

	// Exact completed attempt duplicate is stable.
	callsDone := h.rooms.CallCount()
	w = postJSON(t, mux, "/internal/v1/tournaments/t-retry-id/rounds/1/provisioning-batches", testCred, map[string]any{
		"commandId":    nextCmd,
		"batchId":      batchID,
		"slotFrom":     string(b0.SlotFrom),
		"slotTo":       string(b0.SlotTo),
		"slotSize":     len(b0.SlotIndexes),
		"retryAttempt": nextAttempt,
	}, corr)
	if decodeResult(t, w).Status != envelope.StatusAccepted {
		t.Fatal("completed attempt replay must be accepted")
	}
	if h.rooms.CallCount() != callsDone {
		t.Fatal("completed attempt duplicate must not re-invoke Room")
	}
}

func normalizeJSON(t *testing.T, raw []byte) []byte {
	t.Helper()
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("json: %v", err)
	}
	out, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return out
}
