package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"unoarena/services/ranking/domain"
)

// internalCredentialHeader authenticates service-to-service callers of internal routes.
const internalCredentialHeader = "X-Service-Credential"

// Server wires HTTP handlers to the Ranking domain + in-memory store.
type Server struct {
	store              RatingStore
	mem                *MemoryRatingStore // concrete store for locked mutations; nil if unwired
	internalCredential string
}

// NewServer constructs a Ranking HTTP server. Pass a MemoryRatingStore for offline mode.
func NewServer(store *MemoryRatingStore, internalCredential string) *Server {
	var rs RatingStore
	if store != nil {
		rs = store
	}
	return &Server{
		store:              rs,
		mem:                store,
		internalCredential: internalCredential,
	}
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.healthHandler)
	mux.HandleFunc("/ready", s.readyHandler)
	mux.HandleFunc("/v1/rankings/leaderboards", s.leaderboardsHandler)
	mux.HandleFunc("/v1/players/", s.ratingHistoryHandler)
	mux.HandleFunc("/internal/v1/rankings/games-results", s.gamesResultsHandler)
	mux.HandleFunc("/internal/v1/rankings/tournament-placements", s.tournamentPlacementsHandler)
	mux.HandleFunc("/internal/v1/rankings/rebuild-status", s.rebuildStatusHandler)
	return mux
}

func (s *Server) authorizeInternal(r *http.Request) bool {
	// Fail-closed: empty configured credential rejects all internal callers.
	if s.internalCredential == "" {
		return false
	}
	got := r.Header.Get(internalCredentialHeader)
	if got == "" {
		return false
	}
	if len(got) != len(s.internalCredential) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(s.internalCredential)) == 1
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "ranking"})
	logRequest(r, "/health")
}

func (s *Server) readyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if !s.isReady() {
		reason := "store_unwired"
		if s.store != nil && s.internalCredential == "" {
			reason = "internal_credential_unconfigured"
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status":  "not_ready",
			"service": "ranking",
			"reason":  reason,
		})
		logRequest(r, "/ready")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "ready",
		"service": "ranking",
		"mode":    "offline",
	})
	logRequest(r, "/ready")
}

func (s *Server) isReady() bool {
	return s.store != nil && s.internalCredential != ""
}

func (s *Server) requireReady(w http.ResponseWriter, r *http.Request) bool {
	if s.isReady() {
		return true
	}
	writeError(w, r, http.StatusServiceUnavailable, "not_ready", "required dependencies not configured")
	return false
}

func (s *Server) leaderboardsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if s.store == nil {
		writeError(w, r, http.StatusServiceUnavailable, "not_ready", "store unwired")
		return
	}
	boardType := domain.SourceCasualElo
	switch r.URL.Query().Get("boardType") {
	case "", string(domain.SourceCasualElo):
		boardType = domain.SourceCasualElo
	case string(domain.SourceTournamentPlacement):
		boardType = domain.SourceTournamentPlacement
	default:
		writeError(w, r, http.StatusBadRequest, "bad_request", "boardType must be casual_elo or tournament_placement")
		return
	}
	entries := domain.LeaderboardFromSnapshots(s.store.AllSnapshots(), boardType)
	writeJSON(w, http.StatusOK, map[string]any{"entries": leaderboardEntriesJSON(entries)})
	logRequest(r, "/v1/rankings/leaderboards")
}

func (s *Server) ratingHistoryHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	// /v1/players/{playerId}/rating-history
	path := strings.TrimPrefix(r.URL.Path, "/v1/players/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 2 || parts[1] != "rating-history" || parts[0] == "" {
		writeError(w, r, http.StatusNotFound, "not_found", "not found")
		return
	}
	if s.store == nil {
		writeError(w, r, http.StatusServiceUnavailable, "not_ready", "store unwired")
		return
	}
	playerID := domain.PlayerID(parts[0])
	var (
		hist []domain.RatingHistoryEntry
		ok   bool
	)
	if s.mem != nil {
		hist, ok = s.mem.History(playerID)
	} else {
		rating, found := s.store.Get(playerID)
		if found {
			ok = true
			hist = rating.History()
		}
	}
	if !ok {
		writeError(w, r, http.StatusNotFound, "not_found", "player not found")
		return
	}
	writeJSON(w, http.StatusOK, historyEntriesJSON(playerID, hist))
	logRequest(r, r.URL.Path)
}

func (s *Server) gamesResultsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if !s.requireReady(w, r) {
		return
	}
	if !s.authorizeInternal(r) {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "invalid service credential")
		return
	}
	if s.mem == nil {
		writeError(w, r, http.StatusServiceUnavailable, "not_ready", "store unwired")
		return
	}
	var body struct {
		CommandID     string `json:"commandId"`
		EventID       string `json:"eventId"`
		GameID        string `json:"gameId"`
		RoomID        string `json:"roomId"`
		RoomType      string `json:"roomType"`
		IsAbandoned   bool   `json:"isAbandoned"`
		Authoritative bool   `json:"authoritative"`
		Completed     bool   `json:"completed"`
		Participants  []struct {
			PlayerID  string `json:"playerId"`
			Rating    int    `json:"rating"` // ignored — server loads current ratings
			Placement int    `json:"placement"`
		} `json:"participants"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", "invalid json body")
		return
	}
	parts := make([]domain.RatedPlacement, 0, len(body.Participants))
	for _, p := range body.Participants {
		parts = append(parts, domain.RatedPlacement{
			PlayerID:  domain.PlayerID(p.PlayerID),
			Placement: p.Placement,
			// Rating intentionally omitted / ignored
		})
	}
	result := s.mem.ApplyCasualGameCompleted(GameCompletedRequest{
		CommandID:     domain.CommandID(body.CommandID),
		EventID:       domain.EventID(body.EventID),
		GameID:        domain.GameID(body.GameID),
		RoomID:        domain.RoomID(body.RoomID),
		RoomType:      domain.RoomType(body.RoomType),
		IsAbandoned:   body.IsAbandoned,
		Authoritative: body.Authoritative,
		Completed:     body.Completed,
		Participants:  parts,
	})
	status := http.StatusOK
	if result.Kind == domain.OutcomeRejected {
		status = http.StatusConflict
	}
	writeJSON(w, status, gameCompletedJSON(result))
	logRequest(r, r.URL.Path)
}

func gameCompletedJSON(out GameCompletedResult) map[string]any {
	resp := map[string]any{
		"kind":      string(out.Kind),
		"commandId": string(out.CommandID),
		"eventId":   string(out.EventID),
		"facts":     factsJSON(out.Facts),
	}
	if out.Rejection != nil {
		resp["rejection"] = map[string]string{
			"code":    string(out.Rejection.Code),
			"message": out.Rejection.Message,
		}
	}
	players := make([]map[string]any, 0, len(out.PerPlayer))
	for _, p := range out.PerPlayer {
		players = append(players, outcomeJSON(p))
	}
	if len(players) > 0 {
		resp["participants"] = players
	}
	return resp
}

func (s *Server) tournamentPlacementsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if !s.requireReady(w, r) {
		return
	}
	if !s.authorizeInternal(r) {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "invalid service credential")
		return
	}
	if s.mem == nil {
		writeError(w, r, http.StatusServiceUnavailable, "not_ready", "store unwired")
		return
	}
	var body struct {
		CommandID        string `json:"commandId"`
		EventID          string `json:"eventId"`
		PlayerID         string `json:"playerId"`
		TournamentID     string `json:"tournamentId"`
		PlacementEventID string `json:"placementEventId"`
		Placement        int    `json:"placement"`
		Delta            int    `json:"delta"`
		Reason           string `json:"reason"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", "invalid json body")
		return
	}
	cmd := domain.ApplyTournamentPlacementUpdateCommand{
		CommandID:        domain.CommandID(body.CommandID),
		EventID:          domain.EventID(body.EventID),
		PlayerID:         domain.PlayerID(body.PlayerID),
		TournamentID:     domain.TournamentID(body.TournamentID),
		PlacementEventID: domain.PlacementEventID(body.PlacementEventID),
		Placement:        body.Placement,
		Delta:            body.Delta,
		Reason:           domain.RatingHistoryReason(body.Reason),
	}

	var out domain.CommandOutcome
	s.mem.WithLock(func() {
		rating := s.mem.getOrCreateLocked(cmd.PlayerID)
		out = rating.ApplyTournamentPlacementUpdate(cmd)
	})
	status := http.StatusOK
	if out.Kind == domain.OutcomeRejected {
		status = http.StatusConflict
	}
	writeJSON(w, status, outcomeJSON(out))
	logRequest(r, r.URL.Path)
}

func (s *Server) rebuildStatusHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if !s.authorizeInternal(r) {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "invalid service credential")
		return
	}
	if s.mem == nil {
		writeError(w, r, http.StatusServiceUnavailable, "not_ready", "store unwired")
		return
	}

	var playerCount int
	var top *map[string]any
	s.mem.WithLock(func() {
		playerCount = s.mem.playerCountLocked()
		snaps := s.mem.allSnapshotsLocked()
		entries := domain.LeaderboardFromSnapshots(snaps, domain.SourceCasualElo)
		_ = domain.PublishLeaderboardSnapshot(domain.PublishLeaderboardSnapshotCommand{
			CommandID:  domain.CommandID("rebuild-status"),
			SnapshotID: domain.SnapshotID("offline-rebuild"),
			BoardType:  domain.SourceCasualElo,
			Entries:    entries,
		})
		if len(entries) > 0 {
			e := map[string]any{
				"playerId": string(entries[0].PlayerID),
				"rating":   entries[0].Rating,
			}
			top = &e
		}
	})

	resp := map[string]any{
		"playerCount": playerCount,
		"mode":        "offline",
	}
	if top != nil {
		resp["topEntry"] = *top
	}
	writeJSON(w, http.StatusOK, resp)
	logRequest(r, r.URL.Path)
}

func leaderboardEntriesJSON(entries []domain.LeaderboardEntry) []map[string]any {
	out := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		out = append(out, map[string]any{
			"playerId": string(e.PlayerID),
			"rating":   e.Rating,
		})
	}
	return out
}

func historyEntriesJSON(playerID domain.PlayerID, hist []domain.RatingHistoryEntry) []map[string]any {
	if hist == nil {
		return []map[string]any{}
	}
	out := make([]map[string]any, 0, len(hist))
	for _, h := range hist {
		entry := map[string]any{
			"playerId":       string(playerID),
			"sourceType":     string(h.SourceType),
			"previousRating": h.PreviousRating,
			"newRating":      h.NewRating,
			"delta":          h.Delta,
			"reason":         string(h.Reason),
			"placement":      h.Placement,
		}
		if h.GameID.Valid() {
			entry["gameId"] = string(h.GameID)
		}
		if h.RoomID.Valid() {
			entry["roomId"] = string(h.RoomID)
		}
		if h.EventID.Valid() {
			entry["eventId"] = string(h.EventID)
		}
		if h.TournamentID.Valid() {
			entry["tournamentId"] = string(h.TournamentID)
		}
		if h.PlacementEventID.Valid() {
			entry["placementEventId"] = string(h.PlacementEventID)
		}
		out = append(out, entry)
	}
	return out
}

func outcomeJSON(out domain.CommandOutcome) map[string]any {
	resp := map[string]any{
		"kind":      string(out.Kind),
		"commandId": string(out.CommandID),
		"facts":     factsJSON(out.Facts),
	}
	if out.Rejection != nil {
		resp["rejection"] = map[string]string{
			"code":    string(out.Rejection.Code),
			"message": out.Rejection.Message,
		}
	}
	return resp
}

func factsJSON(facts []domain.Fact) []map[string]any {
	if facts == nil {
		return []map[string]any{}
	}
	out := make([]map[string]any, 0, len(facts))
	for _, f := range facts {
		out = append(out, map[string]any{
			"name": string(f.Name),
			"data": f.Data,
		})
	}
	return out
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	body := map[string]string{
		"code":    code,
		"message": message,
	}
	if cid := r.Header.Get("X-Correlation-Id"); cid != "" {
		body["correlationId"] = cid
	}
	writeJSON(w, status, body)
}

func logRequest(r *http.Request, path string) {
	log.Printf(`{"level":"info","service":"ranking","event":"request","path":%q,"correlationId":%q}`,
		path, r.Header.Get("X-Correlation-Id"))
}

func main() {
	// Fail-closed when unset: internal routes reject all callers.
	cred := os.Getenv("RANKING_INTERNAL_CREDENTIAL")
	store := NewMemoryRatingStore()
	srv := NewServer(store, cred)

	httpSrv := &http.Server{Addr: ":8080", Handler: srv.routes()}
	errCh := make(chan error, 1)
	go func() {
		log.Printf(`{"level":"info","service":"ranking","event":"startup","mode":"offline"}`)
		errCh <- httpSrv.ListenAndServe()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	case <-sigCh:
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(ctx)
}
