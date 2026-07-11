package app_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"unoarena/services/room-gameplay/app"
)

func TestMultiDestination_TopicNeverNoOp(t *testing.T) {
	pub := app.NewMultiDestinationPublisher(app.PublisherDestinations{}, nil)
	err := pub.Publish(context.Background(), app.PublishedEvent{
		Topic: app.TopicGameCompleted, RoomID: "r1", EventID: "e1",
		EventType: "GameCompleted", SequenceNumber: 1, SchemaVersion: 1,
		Payload: json.RawMessage(`{"eventType":"GameCompleted","roomType":"ad_hoc","isAbandoned":false}`),
	})
	if err == nil {
		t.Fatal("topic event with no destinations configured must fail, not no-op")
	}
}

func TestMultiDestination_RoutesByTopicAndStream(t *testing.T) {
	var mu sync.Mutex
	hits := map[string]int{}
	bodies := map[string]map[string]any{}

	record := func(name string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			hits[name]++
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			bodies[name+"|"+r.URL.Path] = body
			if r.Header.Get("X-Service-Credential") == "" {
				t.Errorf("%s missing credential", name)
			}
			w.WriteHeader(http.StatusOK)
		}
	}

	gw := httptest.NewServer(record("gateway"))
	spec := httptest.NewServer(record("spectator"))
	rank := httptest.NewServer(record("ranking"))
	an := httptest.NewServer(record("analytics"))
	tour := httptest.NewServer(record("tournament"))
	t.Cleanup(func() {
		gw.Close()
		spec.Close()
		rank.Close()
		an.Close()
		tour.Close()
	})

	pub := app.NewMultiDestinationPublisher(app.PublisherDestinations{
		GatewayURL:     gw.URL,
		GatewayCred:    "gw",
		SpectatorURL:   spec.URL,
		SpectatorCred:  "sv",
		RankingURL:     rank.URL,
		RankingCred:    "rk",
		AnalyticsURL:   an.URL,
		AnalyticsCred:  "an",
		TournamentURL:  tour.URL,
		TournamentCred: "to",
	}, nil)

	// Spectator stream → Gateway + Spectator canonical ingest.
	specPayload, _ := json.Marshal(map[string]any{
		"visibility": "public", "status": "waiting",
	})
	if err := pub.Publish(context.Background(), app.PublishedEvent{
		Topic: app.TopicSpectatorSafe, Stream: app.StreamSpectator,
		RoomID: "room_1", EventID: "ss-1", EventType: "RoomCreated",
		SequenceNumber: 1, SchemaVersion: 1, Payload: specPayload,
	}); err != nil {
		t.Fatal(err)
	}

	// GameCompleted ad_hoc → Ranking + Analytics.
	gcPayload, _ := json.Marshal(map[string]any{
		"eventId": "gc-1", "eventType": "GameCompleted", "schemaVersion": 1,
		"roomId": "room_1", "gameId": "g1", "roomType": "ad_hoc",
		"isAbandoned": false, "authoritative": true, "completed": true,
		"commandId": "cmd-gc", "placementOrder": []string{"p1", "p2"},
		"participants": []map[string]any{
			{"playerId": "p1", "placement": 1, "cardPoints": 0},
			{"playerId": "p2", "placement": 2, "cardPoints": 12},
		},
	})
	if err := pub.Publish(context.Background(), app.PublishedEvent{
		Topic: app.TopicGameCompleted, RoomID: "room_1", EventID: "gc-1",
		EventType: "GameCompleted", SequenceNumber: 2, SchemaVersion: 1,
		Payload: gcPayload,
	}); err != nil {
		t.Fatal(err)
	}

	// MatchCompleted → Tournament.
	mcPayload, _ := json.Marshal(map[string]any{
		"eventId": "mc-1", "eventType": "MatchCompleted", "schemaVersion": 1,
		"roomId": "room_1", "tournamentId": "t1", "roundNumber": 1, "slotId": "s1",
		"completionVersion": 1, "isAbandoned": false,
		"players": []map[string]any{
			{"playerId": "p1", "matchWins": 2, "cumulativeCardPoints": 1},
		},
	})
	if err := pub.Publish(context.Background(), app.PublishedEvent{
		Topic: app.TopicMatchCompleted, RoomID: "room_1", EventID: "mc-1",
		EventType: "MatchCompleted", SequenceNumber: 3, SchemaVersion: 1,
		Payload: mcPayload,
	}); err != nil {
		t.Fatal(err)
	}

	// Metrics → Analytics.
	metricPayload, _ := json.Marshal(map[string]any{
		"eventId": "m-1", "eventType": "GameplayMetric", "schemaVersion": 1,
		"correlationId": "c", "occurredAt": "2026-07-10T12:00:00Z",
		"payload": map[string]any{
			"visibility": "anonymized_adhoc", "metricType": "turn_advanced", "roomId": "room_1",
		},
	})
	if err := pub.Publish(context.Background(), app.PublishedEvent{
		Topic: app.TopicGameplayMetrics, RoomID: "room_1", EventID: "m-1",
		EventType: "GameplayMetric", SequenceNumber: 4, SchemaVersion: 1,
		Payload: metricPayload,
	}); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if hits["gateway"] < 1 {
		t.Fatalf("gateway hits=%d", hits["gateway"])
	}
	if hits["spectator"] < 1 {
		t.Fatalf("spectator hits=%d", hits["spectator"])
	}
	if hits["ranking"] < 1 {
		t.Fatalf("ranking hits=%d", hits["ranking"])
	}
	if hits["analytics"] < 2 {
		t.Fatalf("analytics hits=%d want >=2 (game+metric)", hits["analytics"])
	}
	if hits["tournament"] < 1 {
		t.Fatalf("tournament hits=%d", hits["tournament"])
	}

	rankBody := bodies["ranking|/internal/v1/rankings/games-results"]
	if rankBody["eventId"] != "gc-1" {
		t.Fatalf("ranking body=%v", rankBody)
	}
	parts, _ := rankBody["participants"].([]any)
	if len(parts) != 2 {
		t.Fatalf("ranking participants=%v", rankBody["participants"])
	}

	tourBody := bodies["tournament|/internal/v1/tournaments/t1/match-results"]
	if tourBody["eventType"] != "MatchCompleted" {
		t.Fatalf("tournament body=%v", tourBody)
	}

	specBody := bodies["spectator|/internal/v1/spectator/rooms/room_1/events"]
	if specBody["event"] != "RoomCreated" || specBody["eventId"] != "ss-1" {
		t.Fatalf("spectator body=%v", specBody)
	}
	if _, nested := specBody["payload"]; nested {
		t.Fatalf("spectator must receive canonical flat data, got nested payload: %v", specBody)
	}
}

func TestMultiDestination_PartialFailureLeavesRetry(t *testing.T) {
	var gwOK atomic.Bool
	gwOK.Store(true)
	gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !gwOK.Load() {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	specFail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(func() { gw.Close(); specFail.Close() })

	pub := app.NewMultiDestinationPublisher(app.PublisherDestinations{
		GatewayURL: gw.URL, GatewayCred: "gw",
		SpectatorURL: specFail.URL, SpectatorCred: "sv",
	}, nil)

	err := pub.Publish(context.Background(), app.PublishedEvent{
		Topic: app.TopicSpectatorSafe, Stream: app.StreamSpectator,
		RoomID: "r1", EventID: "e1", EventType: "RoomCreated",
		SequenceNumber: 1, SchemaVersion: 1,
		Payload: json.RawMessage(`{"visibility":"public"}`),
	})
	if err == nil {
		t.Fatal("partial destination failure must error for outbox retry")
	}
	if !strings.Contains(err.Error(), "spectator") && !strings.Contains(err.Error(), "503") && !strings.Contains(err.Error(), "status") {
		t.Fatalf("error should mention destination failure: %v", err)
	}
}

func TestBuildCompletionEvents_IncludesAllParticipants(t *testing.T) {
	// Covered via sanitize helpers exercised through BuildCompletionEvents with game state —
	// see completion_events_test.go companion; this asserts publisher ranking body mapping.
	raw, _ := json.Marshal(map[string]any{
		"eventId": "e", "eventType": "GameCompleted", "schemaVersion": 1,
		"commandId": "c", "roomId": "r", "gameId": "g", "roomType": "ad_hoc",
		"isAbandoned": false, "authoritative": true, "completed": true,
		"participants": []map[string]any{
			{"playerId": "a", "placement": 1, "cardPoints": 0, "outcome": "won"},
			{"playerId": "b", "placement": 2, "cardPoints": 20, "outcome": "placed"},
		},
	})
	body, err := app.RankingGameCompletedBody(app.PublishedEvent{
		Topic: app.TopicGameCompleted, EventID: "e", EventType: "GameCompleted",
		RoomID: "r", Payload: raw,
	})
	if err != nil {
		t.Fatal(err)
	}
	if body["authoritative"] != true || body["completed"] != true {
		t.Fatalf("body=%v", body)
	}
	parts, _ := body["participants"].([]map[string]any)
	if len(parts) != 2 {
		// JSON round-trip may yield []any
		if arr, ok := body["participants"].([]any); !ok || len(arr) != 2 {
			t.Fatalf("participants=%v", body["participants"])
		}
	}
}
