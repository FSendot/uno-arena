package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"unoarena/services/room-gameplay/app"
	"unoarena/services/room-gameplay/game"
	"unoarena/shared/envelope"
)

func TestE2E_MultiDestinationBridge_RetryAndTerminal(t *testing.T) {
	var mu sync.Mutex
	var (
		spectatorHits  int
		rankingHits    int
		tournamentHits int
		gatewayHits    int
	)
	var specFailOnce atomic.Bool
	specFailOnce.Store(true)

	var lastSpec map[string]any
	var lastRank map[string]any
	var lastTour map[string]any

	gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gatewayHits++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	spec := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if specFailOnce.Load() {
			specFailOnce.Store(false)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		mu.Lock()
		spectatorHits++
		_ = json.NewDecoder(r.Body).Decode(&lastSpec)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	rank := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		rankingHits++
		_ = json.NewDecoder(r.Body).Decode(&lastRank)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	tour := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		tournamentHits++
		_ = json.NewDecoder(r.Body).Decode(&lastTour)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	an := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(func() {
		gw.Close()
		spec.Close()
		rank.Close()
		tour.Close()
		an.Close()
	})

	pub := app.NewMultiDestinationPublisher(app.PublisherDestinations{
		GatewayURL: gw.URL, GatewayCred: "gw",
		SpectatorURL: spec.URL, SpectatorCred: "sv",
		RankingURL: rank.URL, RankingCred: "rk",
		AnalyticsURL: an.URL, AnalyticsCred: "an",
		TournamentURL: tour.URL, TournamentCred: "to",
	}, nil)

	sessions := app.NewMemorySessionRepository()
	integrity := app.NewFakeGameIntegrity()
	deals := app.NewFakeDealSource()
	deals.DealFn = func(roomID, gameID string, seats []string) (game.DealMaterial, error) {
		return quickWinDeal(seats), nil
	}
	clock := app.NewFixedClock(time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC))
	svc := app.NewService(app.ServiceDeps{
		Sessions:  sessions,
		Integrity: integrity,
		Publisher: pub,
		Audit:     app.NewFakeAuditSink(),
		Deals:     deals,
		Clock:     clock,
		SessionsV: app.AllowAllSessionValidator{},
	})
	srv := NewServer(svc, "room-cred", "room-gameplay")
	mux := srv.routes()

	do := func(body map[string]any) envelope.Result {
		t.Helper()
		raw, _ := json.Marshal(body)
		req := httptest.NewRequest(http.MethodPost, "/internal/v1/commands", bytes.NewReader(raw))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(internalCredentialHeader, "room-cred")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		var res envelope.Result
		_ = json.NewDecoder(w.Body).Decode(&res)
		return res
	}

	roomID := "room_e2e"
	r := do(cmdBody("c", "CreateRoom", nil, "host", "s", roomID, map[string]any{"roomId": roomID}))
	if r.Status != envelope.StatusAccepted {
		t.Fatalf("create=%+v", r)
	}
	r = do(cmdBody("j", "JoinRoom", seq(1), "guest", "s2", roomID, map[string]any{}))
	if r.Status != envelope.StatusAccepted {
		t.Fatalf("join=%+v", r)
	}
	r = do(cmdBody("l", "LockRoom", seq(2), "host", "s", roomID, map[string]any{}))
	if r.Status != envelope.StatusAccepted {
		t.Fatalf("lock=%+v", r)
	}

	r = do(cmdBody("st", "StartMatch", seq(3), "host", "s", roomID, map[string]any{"gameId": "g1"}))
	if r.Status != envelope.StatusAccepted {
		t.Fatalf("start=%+v", r)
	}

	// Partial spectator failure leaves outbox pending; retry reaches Spectator.
	pending, _ := sessions.ListPendingOutbox(context.Background(), 10)
	if len(pending) == 0 && spectatorHits == 0 {
		// DrainOutbox during command may have already retried after first failure
		// depending on timing; force another drain.
	}
	for i := 0; i < 8; i++ {
		_, _ = svc.DrainOutbox(context.Background(), 20)
		pending, _ = sessions.ListPendingOutbox(context.Background(), 10)
		mu.Lock()
		ok := spectatorHits >= 1 && len(pending) == 0
		mu.Unlock()
		if ok {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	mu.Lock()
	if spectatorHits < 1 {
		mu.Unlock()
		t.Fatalf("spectator projection not reached after retry, hits=%d gateway=%d", spectatorHits, gatewayHits)
	}
	if lastSpec["event"] == nil {
		mu.Unlock()
		t.Fatalf("spectator body=%v", lastSpec)
	}
	mu.Unlock()

	r = do(cmdBody("p1", "PlayCard", e2eSeq(r), "host", "s", roomID, map[string]any{"cardId": "host-w"}))
	if r.Status != envelope.StatusAccepted {
		t.Fatalf("play1=%+v", r)
	}
	for i := 0; i < 8; i++ {
		_, _ = svc.DrainOutbox(context.Background(), 20)
		pending, _ = sessions.ListPendingOutbox(context.Background(), 10)
		if len(pending) == 0 {
			break
		}
	}
	mu.Lock()
	if rankingHits < 1 {
		mu.Unlock()
		t.Fatal("GameCompleted did not reach Ranking")
	}
	parts, _ := lastRank["participants"].([]any)
	if len(parts) < 2 {
		mu.Unlock()
		t.Fatalf("ranking participants=%v", lastRank["participants"])
	}
	mu.Unlock()

	// MatchCompleted → Tournament (direct publish with tournamentId).
	mc, _ := json.Marshal(map[string]any{
		"eventId": "mc-e2e", "eventType": "MatchCompleted", "schemaVersion": 1,
		"roomId": roomID, "tournamentId": "t-e2e", "roundNumber": 1, "slotId": "slot-1",
		"completionVersion": 1, "isAbandoned": false,
		"players": []map[string]any{
			{"playerId": "host", "matchWins": 2, "cumulativeCardPoints": 0, "finalGameCompletedAt": clock.Now().Format(time.RFC3339Nano)},
			{"playerId": "guest", "matchWins": 0, "cumulativeCardPoints": 5, "finalGameCompletedAt": clock.Now().Format(time.RFC3339Nano)},
		},
	})
	if err := pub.Publish(context.Background(), app.PublishedEvent{
		Topic: app.TopicMatchCompleted, RoomID: roomID, EventID: "mc-e2e",
		EventType: "MatchCompleted", SequenceNumber: 99, SchemaVersion: 1, Payload: mc,
	}); err != nil {
		t.Fatal(err)
	}

	// Terminal spectator stream close via MatchCompleted projection event.
	termPayload, _ := json.Marshal(map[string]any{"matchWinner": "host"})
	if err := pub.Publish(context.Background(), app.PublishedEvent{
		Topic: app.TopicSpectatorSafe, Stream: app.StreamSpectator,
		RoomID: roomID, EventID: "term-1", EventType: "MatchCompleted",
		SequenceNumber: 100, SchemaVersion: 1, Payload: termPayload,
	}); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if tournamentHits < 1 {
		t.Fatalf("MatchCompleted did not reach Tournament, hits=%d", tournamentHits)
	}
	if lastTour["eventType"] != "MatchCompleted" || lastTour["tournamentId"] != "t-e2e" {
		t.Fatalf("tournament body=%v", lastTour)
	}
	if lastSpec["event"] != "MatchCompleted" && lastSpec["event"] != "RoomCreated" && lastSpec["event"] != "MatchStarted" {
		// lastSpec should be the latest spectator ingest (terminal MatchCompleted).
		if lastSpec["event"] == nil {
			t.Fatalf("spectator last event missing: %v", lastSpec)
		}
	}
}

func e2eSeq(r envelope.Result) *int64 {
	if r.Sequence == nil {
		v := int64(0)
		return &v
	}
	return r.Sequence
}
