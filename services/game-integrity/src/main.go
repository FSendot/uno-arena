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
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"

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
    Required human audit headers (credential alone is not an actor):
      X-Audit-Actor, X-Audit-Reason
    Every decrypt/export attempt records a durable audit event (ADR-0024).

  Fail-closed when the relevant env credential is empty.
  Caller cannot supply seed/cards on initialize — GI generates them.
*/

// Server wires HTTP handlers to the offline Game Integrity service.
type Server struct {
	svc               *Service
	repo              StreamRepository
	audit             AuditRecorder
	roomCredential    string
	auditCredential   string
	mode              string
	notReadyReason    string
	exposeSeedInAudit bool
}

// NewServer constructs a Game Integrity HTTP server.
func NewServer(repo StreamRepository, roomCredential, auditCredential, mode, notReadyReason string) *Server {
	return NewServerWithAudit(repo, nil, roomCredential, auditCredential, mode, notReadyReason)
}

// NewServerWithAudit constructs a server with an injectable decrypt/export audit sink.
func NewServerWithAudit(repo StreamRepository, audit AuditRecorder, roomCredential, auditCredential, mode, notReadyReason string) *Server {
	var svc *Service
	if repo != nil {
		svc = NewService(repo)
	}
	if audit == nil {
		audit = &MemoryAuditRecorder{}
	}
	return &Server{
		svc:               svc,
		repo:              repo,
		audit:             audit,
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
	if prober, ok := s.repo.(interface{ Ready(context.Context) error }); ok {
		ctx, cancel := context.WithTimeout(r.Context(), kurrentOpTimeout)
		defer cancel()
		if err := prober.Ready(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"status":  "not_ready",
				"service": "game-integrity",
				"reason":  "store_or_key_unavailable",
			})
			logRequest(r, "/ready")
			return
		}
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
	res, rej, err := s.svc.Append(r.Context(), AppendRequest{
		RoomID:           roomID,
		GameID:           body.GameID,
		EventID:          body.EventID,
		ExpectedRevision: body.ExpectedRevision,
		EventType:        body.EventType,
		Payload:          payloadBytes(body.Payload),
	})
	if err != nil {
		writeStoreError(w, r, err)
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
		res, rej, err := s.svc.InitializeDeck(r.Context(), InitializeDeckRequest{RoomID: roomID, GameID: body.GameID})
		if err != nil {
			writeStoreError(w, r, err)
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
		res, rej, err := s.svc.ReserveDeal(r.Context(), ReserveDealRequest{
			RoomID: roomID, GameID: body.GameID, OperationID: body.OperationID,
			Seats: body.Seats, CardsPerHand: body.CardsPerHand,
		})
		if err != nil {
			writeStoreError(w, r, err)
			return
		}
		if rej != nil {
			writeError(w, r, rejectionHTTPStatus(rej.Code), string(rej.Code), fmtRejection(rej))
			return
		}
		out := map[string]any{
			"kind":          string(res.Kind),
			"reservationId": res.ReservationID,
			"remaining":     res.Remaining,
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
		res, rej, err := s.svc.ReserveDraw(r.Context(), ReserveDrawRequest{
			RoomID: roomID, GameID: body.GameID, OperationID: body.OperationID, Count: body.Count,
		})
		if err != nil {
			writeStoreError(w, r, err)
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
			"remaining":     res.Remaining,
		})
	case "confirm":
		kind, rej, err := s.svc.ConfirmReservation(r.Context(), roomID, body.GameID, body.ReservationID)
		if err != nil {
			writeStoreError(w, r, err)
			return
		}
		if rej != nil {
			writeError(w, r, rejectionHTTPStatus(rej.Code), string(rej.Code), fmtRejection(rej))
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"kind": string(kind)})
	case "cancel":
		rej, err := s.svc.CancelReservation(r.Context(), roomID, body.GameID, body.ReservationID)
		if err != nil {
			writeStoreError(w, r, err)
			return
		}
		if rej != nil {
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

func (s *Server) requireAuditHeaders(w http.ResponseWriter, r *http.Request) (actor, reason string, ok bool) {
	actor = strings.TrimSpace(r.Header.Get(auditActorHeader))
	reason = strings.TrimSpace(r.Header.Get(auditReasonHeader))
	if actor == "" || reason == "" {
		writeError(w, r, http.StatusBadRequest, "bad_request", "X-Audit-Actor and X-Audit-Reason are required")
		return "", "", false
	}
	return actor, reason, true
}

func (s *Server) recordDecryptAudit(ctx context.Context, rec DecryptAuditRecord) error {
	if s.audit == nil {
		return errors.New("audit recorder unwired")
	}
	return s.audit.Record(ctx, rec)
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
	actor, reason, ok := s.requireAuditHeaders(w, r)
	if !ok {
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
	corr := r.Header.Get("X-Correlation-Id")
	stream := roomStreamName(domain.RoomID(roomID))
	ctx, trace := withDecryptTrace(r.Context())
	res, rej, err := s.svc.Replay(ctx, roomID, from)
	outcome := "success"
	errMsg := ""
	if err != nil {
		outcome = "failure"
		errMsg = err.Error()
	} else if rej != nil {
		outcome = "failure"
		errMsg = fmtRejection(rej)
	}
	if auditErr := s.recordDecryptAudit(ctx, newAuditRecord(
		"replay", actor, reason, roomID, "", stream, corr, trace.keyVersions(), outcome, errMsg,
	)); auditErr != nil {
		writeError(w, r, http.StatusInternalServerError, "audit_failed", auditErr.Error())
		return
	}
	if err != nil {
		writeStoreError(w, r, err)
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
	actor, reason, ok := s.requireAuditHeaders(w, r)
	if !ok {
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
	corr := r.Header.Get("X-Correlation-Id")
	var (
		res ExportResult
		rej *domain.Rejection
		err error
	)
	ctx, trace := withDecryptTrace(r.Context())
	if roomID != "" {
		res, rej, err = s.svc.Export(ctx, roomID, gameID, includeSeed)
	} else {
		res, rej, err = s.svc.ExportByGameID(ctx, gameID, includeSeed)
	}
	outcome := "success"
	errMsg := ""
	if err != nil {
		outcome = "failure"
		errMsg = err.Error()
	} else if rej != nil {
		outcome = "failure"
		errMsg = fmtRejection(rej)
	}
	if roomID == "" {
		roomID = res.RoomID
	}
	stream := ""
	if roomID != "" && gameID != "" {
		stream = deckStreamName(domain.RoomID(roomID), domain.GameID(gameID))
	} else if roomID != "" {
		stream = roomStreamName(domain.RoomID(roomID))
	}
	if auditErr := s.recordDecryptAudit(ctx, newAuditRecord(
		"export", actor, reason, roomID, gameID, stream, corr, trace.keyVersions(), outcome, errMsg,
	)); auditErr != nil {
		writeError(w, r, http.StatusInternalServerError, "audit_failed", auditErr.Error())
		return
	}
	if err != nil {
		writeStoreError(w, r, err)
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

// writeStoreError maps repository/Kurrent/decrypt/binding failures to server errors (never 400).
func writeStoreError(w http.ResponseWriter, r *http.Request, err error) {
	if err == nil {
		return
	}
	writeError(w, r, http.StatusServiceUnavailable, "store_unavailable", err.Error())
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

// resolveRuntime selects durable KurrentDB or offline memory, fail-closed on bad config.
func resolveRuntime() (repo StreamRepository, audit AuditRecorder, mode, notReadyReason string) {
	url := strings.TrimSpace(os.Getenv("KURRENTDB_URL"))
	if url != "" {
		provider, reason := resolveEnvelopeProvider()
		if reason != "" {
			return nil, nil, "", reason
		}
		client, err := openKurrentClient(url)
		if err != nil {
			return nil, nil, "", "kurrentdb_url_invalid"
		}
		return NewKurrentStreamRepository(client, provider), &KurrentAuditRecorder{client: client}, "durable", ""
	}
	if os.Getenv("GAME_INTEGRITY_ALLOW_MEMORY") == "true" {
		return NewMemoryStreamRepository(), &MemoryAuditRecorder{}, "offline", ""
	}
	return nil, nil, "", "memory_not_allowed"
}

func resolveEnvelopeProvider() (KeyProvider, string) {
	providerName := strings.TrimSpace(os.Getenv("GAME_INTEGRITY_ENVELOPE_PROVIDER"))
	if providerName == "" {
		return nil, "envelope_provider_required"
	}
	if providerName == "kms" {
		return nil, "envelope_provider_kms_unsupported"
	}
	if providerName != "dev" {
		return nil, "envelope_provider_unsupported"
	}
	if !deploymentAllowsDevProvider(resolveDeploymentEnv()) {
		return nil, "envelope_dev_provider_forbidden_outside_local"
	}
	keyVersion, err := parsePositiveInt(strings.TrimSpace(os.Getenv("GAME_INTEGRITY_ENVELOPE_KEY_VERSION")))
	if err != nil {
		return nil, "envelope_key_version_invalid"
	}
	keyringRaw := strings.TrimSpace(os.Getenv("GAME_INTEGRITY_ENVELOPE_DEV_KEYS"))
	var keys map[int]string
	if keyringRaw != "" {
		keys, err = ParseDevKeyring(keyringRaw)
		if err != nil {
			return nil, "envelope_dev_keyring_invalid"
		}
	} else {
		// Backward-compatible single-key env for transitional local wiring.
		master := strings.TrimSpace(os.Getenv("GAME_INTEGRITY_ENVELOPE_DEV_MASTER_KEY"))
		if master == "" {
			return nil, "envelope_dev_keyring_required"
		}
		keys = map[int]string{keyVersion: master}
	}
	p, err := NewDevKeyProviderFromKeyring(keys, keyVersion)
	if err != nil {
		return nil, "envelope_dev_keyring_invalid"
	}
	return p, ""
}

func openKurrentClient(rawURL string) (*kurrentdb.Client, error) {
	cfg, err := kurrentdb.ParseConnectionString(rawURL)
	if err != nil {
		return nil, err
	}
	return kurrentdb.NewClient(cfg)
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	roomCred := os.Getenv("GAME_INTEGRITY_INTERNAL_CREDENTIAL")
	auditCred := os.Getenv("GAME_INTEGRITY_AUDIT_CREDENTIAL")
	repo, audit, mode, reason := resolveRuntime()
	if closer, ok := repo.(interface{ Close() error }); ok {
		defer func() { _ = closer.Close() }()
	}
	srv := NewServerWithAudit(repo, audit, roomCred, auditCred, mode, reason)

	httpSrv := &http.Server{Addr: ":8080", Handler: srv.routes()}
	errCh := make(chan error, 1)
	go func() {
		log.Printf(`{"level":"info","service":"game-integrity","event":"startup","mode":%q,"readyReason":%q}`, mode, reason)
		errCh <- httpSrv.ListenAndServe()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-sigCh:
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(ctx)
		return nil
	}
}
