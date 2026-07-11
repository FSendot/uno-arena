package main

import (
	"crypto/subtle"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"unoarena/services/game-integrity/domain"
)

// internalCredentialHeader authenticates service-to-service callers of internal routes.
const internalCredentialHeader = "X-Service-Credential"

/*
Internal HTTP contract (offline Game Integrity).

  Room credential (GAME_INTEGRITY_INTERNAL_CREDENTIAL):
    POST /internal/v1/game-logs/{roomId}/append
    POST /internal/v1/game-logs/{roomId}/deck-operations
      initialize | reserve_deal | reserve_draw | confirm | cancel

  Operator/audit credential (GAME_INTEGRITY_AUDIT_CREDENTIAL):
    GET /internal/v1/game-logs/{roomId}/replay?from=
    GET /internal/v1/audit/exports/{gameId}?roomId=

  Fail-closed when the relevant env credential is empty.
  Caller cannot supply seed/cards on initialize — GI generates them.
*/

// Server wires HTTP handlers to the offline Game Integrity service.
type Server struct {
	svc               *Service
	repo              StreamRepository
	roomCredential    string
	auditCredential   string
	mode              string
	notReadyReason    string
	exposeSeedInAudit bool
}

// NewServer constructs a Game Integrity HTTP server.
func NewServer(repo StreamRepository, roomCredential, auditCredential, mode, notReadyReason string) *Server {
	var svc *Service
	if repo != nil {
		svc = NewService(repo)
	}
	return &Server{
		svc:               svc,
		repo:              repo,
		roomCredential:    roomCredential,
		auditCredential:   auditCredential,
		mode:              mode,
		notReadyReason:    notReadyReason,
		exposeSeedInAudit: os.Getenv("GAME_INTEGRITY_EXPOSE_SEED") == "true",
	}
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.healthHandler)
	mux.HandleFunc("/ready", s.readyHandler)
	mux.HandleFunc("/internal/v1/game-logs/{roomId}/append", s.appendHandler)
	mux.HandleFunc("/internal/v1/game-logs/{roomId}/deck-operations", s.deckOperationsHandler)
	mux.HandleFunc("/internal/v1/game-logs/{roomId}/replay", s.replayHandler)
	mux.HandleFunc("/internal/v1/audit/exports/{gameId}", s.exportHandler)
	return mux
}

func (s *Server) authorizeRoom(r *http.Request) bool {
	return constantTimeEq(r.Header.Get(internalCredentialHeader), s.roomCredential)
}

func (s *Server) authorizeAudit(r *http.Request) bool {
	return constantTimeEq(r.Header.Get(internalCredentialHeader), s.auditCredential)
}

func constantTimeEq(got, want string) bool {
	if want == "" || got == "" {
		return false
	}
	if len(got) != len(want) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "game-integrity"})
	logRequest(r, "/health")
}

func (s *Server) readyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if s.notReadyReason != "" || s.repo == nil || s.svc == nil {
		reason := s.notReadyReason
		if reason == "" {
			reason = "repository_unwired"
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status":  "not_ready",
			"service": "game-integrity",
			"reason":  reason,
		})
		logRequest(r, "/ready")
		return
	}
	body := map[string]string{
		"status":  "ready",
		"service": "game-integrity",
	}
	if s.mode != "" {
		body["mode"] = s.mode
	}
	writeJSON(w, http.StatusOK, body)
	logRequest(r, "/ready")
}

func (s *Server) appendHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if !s.authorizeRoom(r) {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "invalid service credential")
		return
	}
	if s.svc == nil {
		writeError(w, r, http.StatusServiceUnavailable, "not_ready", "repository unwired")
		return
	}
	roomID := r.PathValue("roomId")
	if roomID == "" {
		writeError(w, r, http.StatusBadRequest, "bad_request", "roomId is required")
		return
	}
	var body struct {
		GameID           string          `json:"gameId"`
		EventID          string          `json:"eventId"`
		ExpectedRevision int64           `json:"expectedRevision"`
		EventType        string          `json:"eventType"`
		Payload          json.RawMessage `json:"payload"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", "invalid json body")
		return
	}
	res, rej, err := s.svc.Append(AppendRequest{
		RoomID:           roomID,
		GameID:           body.GameID,
		EventID:          body.EventID,
		ExpectedRevision: body.ExpectedRevision,
		EventType:        body.EventType,
		Payload:          payloadBytes(body.Payload),
	})
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if rej != nil {
		writeError(w, r, rejectionHTTPStatus(rej.Code), string(rej.Code), fmtRejection(rej))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"kind":      string(res.Kind),
		"logOffset": res.LogOffset,
		"revision":  res.Revision,
	})
	logRequest(r, r.URL.Path)
}

func (s *Server) deckOperationsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if !s.authorizeRoom(r) {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "invalid service credential")
		return
	}
	if s.svc == nil {
		writeError(w, r, http.StatusServiceUnavailable, "not_ready", "repository unwired")
		return
	}
	roomID := r.PathValue("roomId")
	if roomID == "" {
		writeError(w, r, http.StatusBadRequest, "bad_request", "roomId is required")
		return
	}
	var body struct {
		Operation     string   `json:"operation"`
		GameID        string   `json:"gameId"`
		Seed          string   `json:"seed"`
		Cards         []any    `json:"cards"`
		OperationID   string   `json:"operationId"`
		ReservationID string   `json:"reservationId"`
		Seats         []string `json:"seats"`
		CardsPerHand  int      `json:"cardsPerHand"`
		Count         int      `json:"count"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", "invalid json body")
		return
	}
	switch body.Operation {
	case "initialize":
		if body.Seed != "" || len(body.Cards) > 0 {
			writeError(w, r, http.StatusBadRequest, "invalid_command", "caller must not supply seed or cards")
			return
		}
		res, rej, err := s.svc.InitializeDeck(InitializeDeckRequest{RoomID: roomID, GameID: body.GameID})
		if err != nil {
			writeError(w, r, http.StatusBadRequest, "bad_request", err.Error())
			return
		}
		if rej != nil {
			writeError(w, r, rejectionHTTPStatus(rej.Code), string(rej.Code), fmtRejection(rej))
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"kind":           string(res.Kind),
			"remaining":      res.Remaining,
			"seedCommitment": res.SeedCommitment,
		})
	case "reserve_deal":
		res, rej, err := s.svc.ReserveDeal(ReserveDealRequest{
			RoomID: roomID, GameID: body.GameID, OperationID: body.OperationID,
			Seats: body.Seats, CardsPerHand: body.CardsPerHand,
		})
		if err != nil {
			writeError(w, r, http.StatusBadRequest, "bad_request", err.Error())
			return
		}
		if rej != nil {
			writeError(w, r, rejectionHTTPStatus(rej.Code), string(rej.Code), fmtRejection(rej))
			return
		}
		out := map[string]any{
			"kind":          string(res.Kind),
			"reservationId": res.ReservationID,
		}
		if res.Deal != nil {
			out["hands"] = res.Deal.Hands
			out["discardTop"] = res.Deal.DiscardTop
			out["activeColor"] = res.Deal.ActiveColor
			out["currentSeat"] = res.Deal.CurrentSeat
			out["direction"] = res.Deal.Direction
			out["applyTopEffects"] = res.Deal.ApplyTopEffects
		}
		writeJSON(w, http.StatusOK, out)
	case "reserve_draw":
		res, rej, err := s.svc.ReserveDraw(ReserveDrawRequest{
			RoomID: roomID, GameID: body.GameID, OperationID: body.OperationID, Count: body.Count,
		})
		if err != nil {
			writeError(w, r, http.StatusBadRequest, "bad_request", err.Error())
			return
		}
		if rej != nil {
			writeError(w, r, rejectionHTTPStatus(rej.Code), string(rej.Code), fmtRejection(rej))
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"kind":          string(res.Kind),
			"reservationId": res.ReservationID,
			"cards":         res.Cards,
		})
	case "confirm":
		kind, rej, err := s.svc.ConfirmReservation(roomID, body.GameID, body.ReservationID)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, "bad_request", err.Error())
			return
		}
		if rej != nil {
			writeError(w, r, rejectionHTTPStatus(rej.Code), string(rej.Code), fmtRejection(rej))
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"kind": string(kind)})
	case "cancel":
		if rej := s.svc.CancelReservation(roomID, body.GameID, body.ReservationID); rej != nil {
			writeError(w, r, rejectionHTTPStatus(rej.Code), string(rej.Code), fmtRejection(rej))
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"kind": "accepted"})
	default:
		writeError(w, r, http.StatusBadRequest, "bad_request",
			"operation must be initialize, reserve_deal, reserve_draw, confirm, or cancel")
		return
	}
	logRequest(r, r.URL.Path)
}

func (s *Server) replayHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if !s.authorizeAudit(r) {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "invalid audit credential")
		return
	}
	if s.svc == nil {
		writeError(w, r, http.StatusServiceUnavailable, "not_ready", "repository unwired")
		return
	}
	roomID := r.PathValue("roomId")
	if roomID == "" {
		writeError(w, r, http.StatusBadRequest, "bad_request", "roomId is required")
		return
	}
	var from uint64
	if raw := r.URL.Query().Get("from"); raw != "" {
		v, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, "bad_request", "from must be a non-negative integer")
			return
		}
		from = v
	}
	res, rej, err := s.svc.Replay(roomID, from)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if rej != nil {
		status := rejectionHTTPStatus(rej.Code)
		if rej.Message == "stream not found" {
			status = http.StatusNotFound
		}
		writeError(w, r, status, string(rej.Code), fmtRejection(rej))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"roomId":   res.RoomID,
		"revision": res.Revision,
		"entries":  res.Entries,
	})
	logRequest(r, r.URL.Path)
}

func (s *Server) exportHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if !s.authorizeAudit(r) {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "invalid audit credential")
		return
	}
	if s.svc == nil {
		writeError(w, r, http.StatusServiceUnavailable, "not_ready", "repository unwired")
		return
	}
	gameID := r.PathValue("gameId")
	if gameID == "" {
		writeError(w, r, http.StatusBadRequest, "bad_request", "gameId is required")
		return
	}
	roomID := r.URL.Query().Get("roomId")
	includeSeed := s.exposeSeedInAudit
	var (
		res ExportResult
		rej *domain.Rejection
		err error
	)
	if roomID != "" {
		res, rej, err = s.svc.Export(roomID, gameID, includeSeed)
	} else {
		res, rej, err = s.svc.ExportByGameID(gameID, includeSeed)
	}
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if rej != nil {
		status := rejectionHTTPStatus(rej.Code)
		if rej.Code == domain.RejectInvalidCommand {
			status = http.StatusNotFound
		}
		writeError(w, r, status, string(rej.Code), fmtRejection(rej))
		return
	}
	writeJSON(w, http.StatusOK, res)
	logRequest(r, r.URL.Path)
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
	log.Printf(`{"level":"info","service":"game-integrity","event":"request","path":%q,"correlationId":%q}`,
		path, r.Header.Get("X-Correlation-Id"))
}

// resolveRuntime selects the offline memory repository or reports a readiness block.
// Never pretends EventStore durability when EVENTSTORE_URL is set without a real adapter.
func resolveRuntime() (repo StreamRepository, mode, notReadyReason string) {
	if strings.TrimSpace(os.Getenv("EVENTSTORE_URL")) != "" {
		return nil, "", "eventstore_adapter_blocked"
	}
	if os.Getenv("GAME_INTEGRITY_ALLOW_MEMORY") == "true" {
		return NewMemoryStreamRepository(), "offline", ""
	}
	return nil, "", "memory_not_allowed"
}

func main() {
	roomCred := os.Getenv("GAME_INTEGRITY_INTERNAL_CREDENTIAL")
	auditCred := os.Getenv("GAME_INTEGRITY_AUDIT_CREDENTIAL")
	repo, mode, reason := resolveRuntime()
	srv := NewServer(repo, roomCred, auditCred, mode, reason)

	log.Printf(`{"level":"info","service":"game-integrity","event":"startup","mode":%q,"readyReason":%q}`, mode, reason)
	log.Fatal(http.ListenAndServe(":8080", srv.routes()))
}
