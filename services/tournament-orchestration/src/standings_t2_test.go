package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/services/tournament-orchestration/store"
	"unoarena/shared/envelope"
)

func TestStandingsDurablePath_AntiFallback(t *testing.T) {
	fn := extractFuncBody(t, "service.go", "Standings")
	// Durable branch is the first if; ensure it does not take the mutex or hydrate.
	durable := fn
	if idx := strings.Index(fn, "s.mu.Lock"); idx >= 0 {
		durable = fn[:idx]
	}
	for _, bad := range []string{
		"s.mu.Lock",
		"s.mu.Unlock",
		"s.repo.Get(",
		"BeginExisting(",
		"loadTournamentQ",
	} {
		if strings.Contains(durable, bad) {
			t.Fatalf("durable Standings path must not contain %q", bad)
		}
	}
	if !strings.Contains(durable, "LoadStandingsProjection") {
		t.Fatal("durable Standings must call LoadStandingsProjection")
	}
}

func TestStandings_MemoryShapePreFinalAndCompleted(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	corr := map[string]string{"X-Correlation-Id": "corr-standings-shape"}

	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("st-create", "CreateTournament", map[string]any{
		"tournamentId": "t-standings-shape", "capacity": 4,
	}, "op", "s"), corr)

	w := getJSON(t, mux, "/v1/tournaments/t-standings-shape/standings")
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d %s", w.Code, w.Body.String())
	}
	var body map[string]any
	_ = json.NewDecoder(w.Body).Decode(&body)
	for _, key := range []string{"tournamentId", "projectionVersion", "generatedAt", "phase", "registeredCount", "currentRound", "finalStandings"} {
		if _, ok := body[key]; !ok {
			t.Fatalf("missing %s", key)
		}
	}
	if _, has := body["championId"]; has {
		t.Fatal("championId must be absent")
	}
	if _, has := body["registeredPlayers"]; has {
		t.Fatal("registeredPlayers must be absent")
	}
	final, ok := body["finalStandings"].([]any)
	if !ok || len(final) != 0 {
		t.Fatalf("pre-final finalStandings=%v", body["finalStandings"])
	}

	w404 := getJSON(t, mux, "/v1/tournaments/missing-standings/standings")
	if w404.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %d", w404.Code)
	}
}

func TestStandings_MemoryFinalOrderMax10(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	corr := map[string]string{"X-Correlation-Id": "corr-standings-final"}

	// capacity 4 → single final round with 4 players.
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("sf-create", "CreateTournament", map[string]any{
		"tournamentId": "t-standings-final", "capacity": 4,
	}, "op", "s"), corr)
	for i := 1; i <= 4; i++ {
		p := "p" + itoa(i)
		postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("sf-reg-"+p, "RegisterPlayer", map[string]any{
			"tournamentId": "t-standings-final", "playerId": p,
		}, p, "s"), corr)
	}
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("sf-close", "CloseRegistration", map[string]any{
		"tournamentId": "t-standings-final",
	}, "op", "s"), corr)
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("sf-seed", "SeedRound", map[string]any{
		"tournamentId": "t-standings-final", "roundNumber": 1,
	}, "op", "s"), corr)
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("sf-prov", "ProvisionRoundMatches", map[string]any{
		"tournamentId": "t-standings-final", "roundNumber": 1,
	}, "op", "s"), corr)

	tr, ok := h.repo.Get(domain.TournamentID("t-standings-final"))
	if !ok {
		t.Fatal("missing tournament")
	}
	rounds := tr.RoundsSnapshot()
	if len(rounds) != 1 || !rounds[0].IsFinal || len(rounds[0].Slots) != 1 {
		t.Fatalf("want final round with 1 slot, got %+v", rounds)
	}
	slot := rounds[0].Slots[0]
	// Ascending matchWins in seed order: producer input places the champion last.
	standings := make([]map[string]any, len(slot.SeededPlayers))
	for i, pid := range slot.SeededPlayers {
		standings[i] = map[string]any{
			"playerId":             string(pid),
			"matchWins":            i,
			"cumulativeCardPoints": (len(slot.SeededPlayers) - i) * 10,
			"finalGameCompletedAt": "2026-01-01T00:00:0" + itoa(i) + "Z",
			"forfeited":            false,
		}
	}
	w := postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("sf-result", "RecordMatchResult", map[string]any{
		"tournamentId":      "t-standings-final",
		"roundNumber":       1,
		"slotId":            string(slot.SlotID),
		"roomId":            string(slot.RoomID),
		"completionVersion": 1,
		"standings":         standings,
	}, "op", "s"), corr)
	if decodeResult(t, w).Status != envelope.StatusAccepted {
		t.Fatalf("record: %s", w.Body.String())
	}

	sw := getJSON(t, mux, "/v1/tournaments/t-standings-final/standings")
	if sw.Code != http.StatusOK {
		t.Fatalf("standings status=%d %s", sw.Code, sw.Body.String())
	}
	var body map[string]any
	_ = json.NewDecoder(sw.Body).Decode(&body)
	final, ok := body["finalStandings"].([]any)
	if !ok || len(final) != 4 {
		t.Fatalf("finalStandings=%v", body["finalStandings"])
	}
	wantFirst := string(slot.SeededPlayers[len(slot.SeededPlayers)-1])
	if final[0] != wantFirst {
		t.Fatalf("first=%v want %s (ranked, not input order; inputFirst=%s)",
			final[0], wantFirst, standings[0]["playerId"])
	}
	if final[0] == standings[0]["playerId"] {
		t.Fatalf("finalStandings echoed unsorted input: %v", final)
	}
	if len(final) > domain.FinalPlayerThreshold {
		t.Fatalf("exceeded max 10: %d", len(final))
	}
}

func TestStandings_DurableLoaderUsesProjectionNotRepoGet(t *testing.T) {
	loader := &stubStandingsLoader{
		page: store.StandingsProjection{
			TournamentID:      "t-stub",
			ProjectionVersion: 7,
			Phase:             "in_progress",
			RegisteredCount:   3,
			CurrentRound:      1,
			FinalStandings:    []string{},
		},
	}
	repo := &countingGetRepo{inner: NewMemoryTournamentRepository()}
	svc := NewService(ServiceDeps{
		Repo:      repo,
		Standings: loader,
	})
	body, err := svc.Standings("t-stub")
	if err != nil {
		t.Fatal(err)
	}
	if repo.gets != 0 {
		t.Fatalf("durable Standings must not call repo.Get, got %d", repo.gets)
	}
	if body["registeredCount"] != 3 || body["projectionVersion"] != int64(7) {
		t.Fatalf("body=%v", body)
	}
	final, _ := body["finalStandings"].([]string)
	if final == nil {
		if arr, ok := body["finalStandings"].([]any); ok && len(arr) == 0 {
			return
		}
		if fs, ok := body["finalStandings"].([]string); !ok || len(fs) != 0 {
			t.Fatalf("finalStandings=%v", body["finalStandings"])
		}
	}
}

func TestStandings_DurableMalformedMapsUnavailable(t *testing.T) {
	svc := NewService(ServiceDeps{
		Repo: NewMemoryTournamentRepository(),
		Standings: &stubStandingsLoader{
			err: store.ErrMalformedStandings,
		},
	})
	_, err := svc.Standings("t-bad")
	if !errors.Is(err, store.ErrMalformedStandings) {
		t.Fatalf("err=%v", err)
	}
	h := newTestHarness(t)
	repo := NewMemoryTournamentRepository()
	tr, out := domain.CreateTournament(domain.CreateTournamentCommand{
		CommandID: "c-bad", TournamentID: "t-bad", Capacity: 4,
	})
	if !out.Accepted() {
		t.Fatal(out)
	}
	if err := repo.Commit(CommitRequest{
		Tournament: tr, CommandID: "c-bad",
		Outcome: envelope.Accepted("c-bad", "CreateTournament", nil, nil),
	}); err != nil {
		t.Fatal(err)
	}
	h.srv.svc = NewService(ServiceDeps{
		Repo: repo,
		Standings: &stubStandingsLoader{
			err: store.ErrMalformedStandings,
		},
	})
	w := getJSON(t, h.srv.Routes(), "/v1/tournaments/t-bad/standings")
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503, got %d %s", w.Code, w.Body.String())
	}
}

type stubStandingsLoader struct {
	page store.StandingsProjection
	err  error
}

func (s *stubStandingsLoader) LoadStandingsProjection(_ context.Context, _ string) (store.StandingsProjection, error) {
	return s.page, s.err
}

type countingGetRepo struct {
	inner TournamentRepository
	gets  int
}

func (r *countingGetRepo) BeginExisting(id domain.TournamentID) (TournamentUnitOfWork, error) {
	return r.inner.BeginExisting(id)
}
func (r *countingGetRepo) BeginCreate(id domain.TournamentID) (TournamentUnitOfWork, error) {
	return r.inner.BeginCreate(id)
}
func (r *countingGetRepo) Get(id domain.TournamentID) (*domain.Tournament, bool) {
	r.gets++
	return r.inner.Get(id)
}
func (r *countingGetRepo) Commit(req CommitRequest) error { return r.inner.Commit(req) }
func (r *countingGetRepo) LookupOutcome(commandID string) (envelope.Result, bool) {
	return r.inner.LookupOutcome(commandID)
}
func (r *countingGetRepo) ListPendingOutbox(limit int) ([]OutboxEvent, error) {
	return r.inner.ListPendingOutbox(limit)
}
func (r *countingGetRepo) MarkOutboxPublished(eventID string, at time.Time) error {
	return r.inner.MarkOutboxPublished(eventID, at)
}

func TestStandingsStoreFile_NoHydrateAPIs(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("store", "standings_page.go"))
	if err != nil {
		t.Fatal(err)
	}
	src := string(b)
	for _, bad := range []string{"loadTournamentQ", "BeginExisting", "persistTournamentTx"} {
		if strings.Contains(src, bad) {
			t.Fatalf("standings_page.go must not contain %q", bad)
		}
	}
	if !strings.Contains(src, "QueryRow") {
		t.Fatal("standings must use QueryRow snapshot")
	}
	if !strings.Contains(src, "tournament_registration_shards") {
		t.Fatal("registeredCount must SUM registration shards")
	}
}
