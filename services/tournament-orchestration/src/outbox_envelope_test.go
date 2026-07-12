package main

import (
	"encoding/json"
	"testing"
	"time"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/shared/envelope"
)

func TestIntegrationOutbox_CanonicalEnvelopeAndSkipsNonContract(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	corr := map[string]string{"X-Correlation-Id": "corr-canon"}

	w := postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("c-create", "CreateTournament", map[string]any{
		"tournamentId": "t-canon",
		"capacity":     4,
	}, "op", "s"), corr)
	if decodeResult(t, w).Status != envelope.StatusAccepted {
		t.Fatalf("create: %s", w.Body.String())
	}
	if h.repo.OutboxLen() != 0 {
		t.Fatalf("CreateTournament must emit zero outbox rows, got %d", h.repo.OutboxLen())
	}
	for _, e := range h.repo.Events() {
		if e.Topic == "tournament.lifecycle" {
			t.Fatal("must never emit tournament.lifecycle")
		}
	}

	seedTiny(t, h, mux, "t-canon2", corr)
	before := h.repo.OutboxLen()
	w = postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("prov-canon", "ProvisionRoundMatches", map[string]any{
		"tournamentId": "t-canon2",
		"roundNumber":  1,
	}, "op", "s"), corr)
	if decodeResult(t, w).Status != envelope.StatusAccepted {
		t.Fatalf("provision: %s", w.Body.String())
	}
	after := h.repo.OutboxLen()
	if after <= before {
		t.Fatal("expected TournamentMatchAssigned outbox rows")
	}

	events := h.repo.Events()
	var assigned *OutboxEvent
	for i := range events {
		if events[i].EventType == "TournamentMatchAssigned" {
			assigned = &events[i]
			break
		}
	}
	if assigned == nil {
		t.Fatal("missing TournamentMatchAssigned outbox event")
	}
	if assigned.Topic != "tournament.match.assigned" {
		t.Fatalf("topic=%q", assigned.Topic)
	}

	raw, err := json.Marshal(assigned.Payload)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{
		"schemaVersion", "eventId", "eventType", "correlationId", "causationId",
		"occurredAt", "tournamentId", "roundNumber", "slotId", "roomId",
	} {
		if _, ok := body[k]; !ok {
			t.Fatalf("missing envelope key %q in %s", k, raw)
		}
	}
	if int(body["schemaVersion"].(float64)) != 1 {
		t.Fatalf("schemaVersion=%v", body["schemaVersion"])
	}
	if body["eventType"] != "TournamentMatchAssigned" {
		t.Fatalf("eventType=%v", body["eventType"])
	}
	if body["tournamentId"] != "t-canon2" {
		t.Fatalf("tournamentId=%v", body["tournamentId"])
	}
	if _, ok := body["roundNumber"].(float64); !ok {
		t.Fatalf("roundNumber want number, got %T %v", body["roundNumber"], body["roundNumber"])
	}
	if _, err := time.Parse(time.RFC3339, body["occurredAt"].(string)); err != nil {
		t.Fatalf("occurredAt not RFC3339: %v", err)
	}
}

func TestTopicForFact_ContractOnly(t *testing.T) {
	cases := []struct {
		name  domain.FactName
		topic string
	}{
		{domain.FactTournamentMatchAssigned, "tournament.match.assigned"},
		{domain.FactTournamentMatchResultRecorded, "tournament.match.result_recorded"},
		{domain.FactPlayersAdvanced, "tournament.players.advanced"},
		{domain.FactTournamentRoundCompleted, "tournament.round.completed"},
		{domain.FactTournamentCompleted, "tournament.completed"},
		{domain.FactTournamentCreated, ""},
		{domain.FactPlayerRegisteredInTournament, ""},
		{domain.FactTournamentCancelled, ""},
		{domain.FactTournamentProvisioningBatchCompleted, ""},
		{domain.FactTournamentRoundSeeded, ""},
	}
	for _, tc := range cases {
		if got := topicForFact(tc.name); got != tc.topic {
			t.Fatalf("%s topic=%q want %q", tc.name, got, tc.topic)
		}
	}
}

func TestBuildTournamentIntegrationPayload_PlayersAdvancedTyped(t *testing.T) {
	at := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	payload := buildTournamentIntegrationPayload(
		"cmd:PlayersAdvanced:1",
		"PlayersAdvanced",
		"corr",
		"cmd",
		"t1",
		map[string]string{
			"tournamentId":       "t1",
			"roundNumber":        "2",
			"sourceSlotId":       "slot_0",
			"advancingPlayerIds": "p1,p2",
			"rule":               "top_half",
		},
		at,
	)
	raw, _ := json.Marshal(payload)
	var got map[string]any
	_ = json.Unmarshal(raw, &got)
	ids, ok := got["advancingPlayerIds"].([]any)
	if !ok || len(ids) != 2 || ids[0] != "p1" || ids[1] != "p2" {
		t.Fatalf("advancingPlayerIds=%v (%s)", got["advancingPlayerIds"], raw)
	}
	if int(got["roundNumber"].(float64)) != 2 {
		t.Fatalf("roundNumber=%v", got["roundNumber"])
	}
}

func TestBuildTournamentIntegrationPayload_TournamentCompletedFinalStandings(t *testing.T) {
	at := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	payload := buildTournamentIntegrationPayload(
		"cmd:TournamentCompleted:1",
		"TournamentCompleted",
		"corr",
		"cmd",
		"t1",
		map[string]string{
			"tournamentId":   "t1",
			"finalStandings": "p1,p2,p3",
			"phase":          "completed",
		},
		at,
	)
	raw, _ := json.Marshal(payload)
	var got map[string]any
	_ = json.Unmarshal(raw, &got)
	if _, hasChamp := got["championId"]; hasChamp {
		t.Fatalf("championId must be absent: %s", raw)
	}
	ids, ok := got["finalStandings"].([]any)
	if !ok || len(ids) != 3 || ids[0] != "p1" || ids[1] != "p2" || ids[2] != "p3" {
		t.Fatalf("finalStandings=%v (%s)", got["finalStandings"], raw)
	}
}

func TestBuildTournamentIntegrationPayload_ZeroOccurredAtDeterministic(t *testing.T) {
	payload := buildTournamentIntegrationPayload(
		"e1", "TournamentCompleted", "corr", "cmd", "t1",
		map[string]string{"tournamentId": "t1"},
		time.Time{},
	)
	want := time.Time{}.UTC().Format(time.RFC3339)
	if payload["occurredAt"] != want {
		t.Fatalf("occurredAt=%v want deterministic zero %q (no wall-clock fallback)", payload["occurredAt"], want)
	}
}
