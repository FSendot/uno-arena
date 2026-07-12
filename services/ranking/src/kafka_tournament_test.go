package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"unoarena/services/ranking/domain"
	"unoarena/services/ranking/store"
)

func canonicalPlayersAdvancedJSON(mut ...func(map[string]any)) []byte {
	base := time.Date(2026, 7, 11, 16, 0, 0, 0, time.UTC)
	m := map[string]any{
		"schemaVersion":      1,
		"eventId":            "evt-pa-1",
		"eventType":          "PlayersAdvanced",
		"correlationId":      "corr-pa-1",
		"causationId":        "cause-pa-1",
		"occurredAt":         base.Format(time.RFC3339Nano),
		"tournamentId":       "tour-1",
		"roundNumber":        2,
		"sourceSlotId":       "slot-a",
		"advancingPlayerIds": []string{"p1", "p2", "p3"},
	}
	for _, fn := range mut {
		fn(m)
	}
	b, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return b
}

func canonicalTournamentCompletedJSON(mut ...func(map[string]any)) []byte {
	base := time.Date(2026, 7, 11, 17, 0, 0, 0, time.UTC)
	m := map[string]any{
		"schemaVersion":  1,
		"eventId":        "evt-tc-1",
		"eventType":      "TournamentCompleted",
		"correlationId":  "corr-tc-1",
		"occurredAt":     base.Format(time.RFC3339Nano),
		"tournamentId":   "tour-1",
		"finalStandings": []string{"p1", "p2", "p3"},
	}
	for _, fn := range mut {
		fn(m)
	}
	b, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return b
}

func TestParsePlayersAdvanced_Canonical(t *testing.T) {
	evt, err := ParsePlayersAdvancedRecord(canonicalPlayersAdvancedJSON())
	if err != nil {
		t.Fatal(err)
	}
	if evt.TournamentID != "tour-1" || evt.RoundNumber != 2 || evt.SourceSlotID != "slot-a" {
		t.Fatalf("%+v", evt)
	}
	if len(evt.AdvancingPlayerIDs) != 3 || evt.BusinessKey == "" || evt.PayloadFingerprint == "" {
		t.Fatalf("%+v", evt)
	}
}

func TestParseTournamentCompleted_Canonical(t *testing.T) {
	evt, err := ParseTournamentCompletedRecord(canonicalTournamentCompletedJSON())
	if err != nil {
		t.Fatal(err)
	}
	if evt.BusinessKey != "evt-tc-1" || len(evt.FinalStandings) != 3 {
		t.Fatalf("%+v", evt)
	}
}

func TestParseTournamentTopics_Table(t *testing.T) {
	cases := []struct {
		name  string
		topic string
		raw   []byte
	}{
		{"pa_empty_ids", "advanced", canonicalPlayersAdvancedJSON(func(m map[string]any) {
			m["advancingPlayerIds"] = []string{}
		})},
		{"pa_too_many", "advanced", canonicalPlayersAdvancedJSON(func(m map[string]any) {
			m["advancingPlayerIds"] = []string{"a", "b", "c", "d"}
		})},
		{"pa_dup", "advanced", canonicalPlayersAdvancedJSON(func(m map[string]any) {
			m["advancingPlayerIds"] = []string{"a", "a"}
		})},
		{"pa_round_zero", "advanced", canonicalPlayersAdvancedJSON(func(m map[string]any) {
			m["roundNumber"] = 0
		})},
		{"pa_bad_type", "advanced", canonicalPlayersAdvancedJSON(func(m map[string]any) {
			m["eventType"] = "GameCompleted"
		})},
		{"pa_trailing", "advanced", append(canonicalPlayersAdvancedJSON(), []byte(`{"x":1}`)...)},
		{"tc_empty", "completed", canonicalTournamentCompletedJSON(func(m map[string]any) {
			m["finalStandings"] = []string{}
		})},
		{"tc_too_many", "completed", canonicalTournamentCompletedJSON(func(m map[string]any) {
			ids := make([]string, 11)
			for i := range ids {
				ids[i] = "p" + string(rune('a'+i))
			}
			m["finalStandings"] = ids
		})},
		{"tc_dup", "completed", canonicalTournamentCompletedJSON(func(m map[string]any) {
			m["finalStandings"] = []string{"p1", "p1"}
		})},
		{"tc_champion", "completed", canonicalTournamentCompletedJSON(func(m map[string]any) {
			m["championId"] = "p1"
		})},
		{"tc_trailing", "completed", append(canonicalTournamentCompletedJSON(), []byte("0")...)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			if tc.topic == "advanced" {
				_, err = ParsePlayersAdvancedRecord(tc.raw)
			} else {
				_, err = ParseTournamentCompletedRecord(tc.raw)
			}
			if err == nil || !IsTerminalKafkaConsumeError(err) {
				t.Fatalf("want terminal error, got %v", err)
			}
		})
	}
}

func TestParseTournament_UseNumberRejectsNonIntegral(t *testing.T) {
	raw := []byte(`{"schemaVersion":1.5,"eventId":"e","eventType":"PlayersAdvanced","correlationId":"c","occurredAt":"2026-07-11T16:00:00Z","tournamentId":"t","roundNumber":2,"sourceSlotId":"s","advancingPlayerIds":["p1"]}`)
	if _, err := ParsePlayersAdvancedRecord(raw); err == nil {
		t.Fatal("want schemaVersion non-integral rejection")
	}
}

func TestTournamentConsumer_PerTopicDLQAndSkipsQuarantine(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	q := &fakeQuarantine{}
	h := &fakeHandler{}
	c := newTestConsumer(src, dlq, h)
	c.quarantine = q

	// Invalid players advanced -> DLQ, no quarantine.
	err := c.ProcessBatch(context.Background(), []ConsumerRecord{{
		Topic: DefaultPlayersAdvancedTopic, Partition: 0, Offset: 1,
		Key: []byte("tour-1"), Value: []byte(`not-json`),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if q.count() != 0 {
		t.Fatal("tournament topics must not quarantine")
	}
	pubs := dlq.publications()
	if len(pubs) != 1 || pubs[0].Meta.SourceTopic != DefaultPlayersAdvancedTopic {
		t.Fatalf("dlq pubs=%+v", pubs)
	}

	// Invalid tournament completed -> separate source topic metadata.
	err = c.ProcessBatch(context.Background(), []ConsumerRecord{{
		Topic: DefaultTournamentCompletedTopic, Partition: 1, Offset: 2,
		Key: []byte("tour-1"), Value: canonicalTournamentCompletedJSON(func(m map[string]any) {
			m["championId"] = "x"
		}),
	}})
	if err != nil {
		t.Fatal(err)
	}
	pubs = dlq.publications()
	if len(pubs) != 2 || pubs[1].Meta.SourceTopic != DefaultTournamentCompletedTopic {
		t.Fatalf("dlq pubs=%+v", pubs)
	}
	if q.count() != 0 {
		t.Fatal("tournament completed must not quarantine")
	}
}

func TestTournamentConsumer_AcceptedCommits(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	h := &fakeHandler{perfFn: func(ctx context.Context, req TournamentPerformanceRequest) (TournamentPerformanceResult, error) {
		if req.SourceTopic != DefaultPlayersAdvancedTopic || len(req.Players) != 3 {
			t.Fatalf("req=%+v", req)
		}
		return TournamentPerformanceResult{Kind: domain.OutcomeAccepted, UpstreamEventID: req.UpstreamEventID}, nil
	}}
	c := newTestConsumer(src, dlq, h)
	rec := ConsumerRecord{
		Topic: DefaultPlayersAdvancedTopic, Partition: 0, Offset: 9,
		Key: []byte("tour-1"), Value: canonicalPlayersAdvancedJSON(),
	}
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{rec}); err != nil {
		t.Fatal(err)
	}
	if got := src.committedOffsets(); len(got) != 1 || got[0] != 9 {
		t.Fatalf("commits=%v", got)
	}
	if len(dlq.publications()) != 0 {
		t.Fatal("accepted must not dlq")
	}
}

func TestTournamentConsumer_ConflictIsTerminalDLQ(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	h := &fakeHandler{perfFn: func(ctx context.Context, req TournamentPerformanceRequest) (TournamentPerformanceResult, error) {
		return TournamentPerformanceResult{}, &store.TournamentPerformanceConflictError{
			SourceTopic: req.SourceTopic, BusinessKey: req.BusinessKey,
			ExistingFingerprint: "a", IncomingFingerprint: "b",
		}
	}}
	c := newTestConsumer(src, dlq, h)
	err := c.ProcessBatch(context.Background(), []ConsumerRecord{{
		Topic: DefaultTournamentCompletedTopic, Partition: 0, Offset: 3,
		Key: []byte("tour-1"), Value: canonicalTournamentCompletedJSON(),
	}})
	if err != nil {
		t.Fatal(err)
	}
	pubs := dlq.publications()
	if len(pubs) != 1 || pubs[0].Meta.Classification != KafkaFailureBusinessKeyConflict {
		t.Fatalf("pubs=%+v", pubs)
	}
	if !strings.Contains(pubs[0].Meta.ErrorSummary, "conflict") {
		t.Fatalf("summary=%q", pubs[0].Meta.ErrorSummary)
	}
}

func TestMapTournamentCompleted_PlacementOrder(t *testing.T) {
	evt, err := ParseTournamentCompletedRecord(canonicalTournamentCompletedJSON())
	if err != nil {
		t.Fatal(err)
	}
	req := MapTournamentCompletedToRequest(evt)
	if len(req.Players) != 3 || req.Players[0].Placement != 1 || req.Players[2].Placement != 3 {
		t.Fatalf("%+v", req.Players)
	}
	if req.Players[0].Reason != domain.ReasonTournamentFinalStanding {
		t.Fatalf("reason=%s", req.Players[0].Reason)
	}
}
