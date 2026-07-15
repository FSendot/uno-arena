package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"unoarena/services/gateway/bff"
	"unoarena/shared/correlation"
	"unoarena/shared/envelope"
)

func TestHealthViaBFF(t *testing.T) {
	srv := bff.NewServer(bff.Dependencies{
		Identity:   bff.NewFakeIdentity(),
		Room:       bff.NewFakeRoom(),
		Tournament: bff.NewFakeTournament(),
		Reads:      &bff.FakeReads{},
		Spectator:  bff.NewFakeSpectatorGate(),
		Ready:      true,
	})
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestBuildServer_RequiresURLsForReady(t *testing.T) {
	cfg := gatewayConfig{}
	srv, mode, err := buildServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if mode != "http-backends" {
		t.Fatalf("mode=%s", mode)
	}
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "rate_limiter_not_configured") {
		t.Fatalf("body=%s", w.Body.String())
	}
}

func TestBuildServer_AllowFakesDemo(t *testing.T) {
	t.Setenv("GATEWAY_ALLOW_FAKES", "1")
	defer os.Unsetenv("GATEWAY_ALLOW_FAKES")
	cfg := loadGatewayConfig()
	if !cfg.AllowFakes {
		t.Fatal("expected allow fakes")
	}
	srv, mode, err := buildServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if mode != "demo-fakes" {
		t.Fatalf("mode=%s", mode)
	}
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d", w.Code)
	}
}

func TestBuildServer_HTTPClientsWhenURLsSetStillNotReadyWithoutDistributedLimiter(t *testing.T) {
	health := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	t.Cleanup(health.Close)

	cfg := gatewayConfig{
		IdentityURL:                 health.URL,
		RoomURL:                     health.URL,
		TournamentURL:               health.URL,
		SpectatorURL:                health.URL,
		RankingURL:                  health.URL,
		AnalyticsURL:                health.URL,
		ServiceCredential:           "svc",
		IdentityProducerCredential:  "ident",
		RoomProducerCredential:      "room",
		SpectatorProducerCredential: "spec",
	}
	if cfg.StaticReady() {
		t.Fatal("without distributed limiter adapter, URLs alone must not be ready")
	}
	srv, mode, err := buildServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if mode != "http-backends" {
		t.Fatalf("mode=%s", mode)
	}
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "rate_limiter_not_configured") {
		t.Fatalf("body=%s", w.Body.String())
	}
}

func TestBuildServer_PartialRedisURLStillBlocks(t *testing.T) {
	cfg := gatewayConfig{
		IdentityURL:                 "http://identity:8080",
		RoomURL:                     "http://room:8080",
		TournamentURL:               "http://tournament:8080",
		SpectatorURL:                "http://spectator:8080",
		RankingURL:                  "http://ranking:8080",
		AnalyticsURL:                "http://analytics:8080",
		ServiceCredential:           "svc",
		IdentityProducerCredential:  "ident",
		RoomProducerCredential:      "room",
		SpectatorProducerCredential: "spec",
		RedisURL:                    "redis://localhost:6379/6",
	}
	if cfg.StaticReady() {
		t.Fatal("partial REDIS_URL must block static ready")
	}
	if !cfg.redisAdapterBlocked() {
		t.Fatal("partial redis config must keep adapter blocked")
	}
	srv, mode, err := buildServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if mode != "http-backends" {
		t.Fatalf("mode=%s", mode)
	}
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "redis_adapter_blocked") {
		t.Fatalf("body=%s", w.Body.String())
	}
}

func TestBuildServer_AbsentRedisAlsoNotReady(t *testing.T) {
	cfg := gatewayConfig{
		IdentityURL:                 "http://identity:8080",
		RoomURL:                     "http://room:8080",
		TournamentURL:               "http://tournament:8080",
		SpectatorURL:                "http://spectator:8080",
		RankingURL:                  "http://ranking:8080",
		AnalyticsURL:                "http://analytics:8080",
		ServiceCredential:           "svc",
		IdentityProducerCredential:  "ident",
		RoomProducerCredential:      "room",
		SpectatorProducerCredential: "spec",
	}
	if cfg.StaticReady() {
		t.Fatal("absent REDIS_URL without adapter must not be ready")
	}
	if !strings.Contains(cfg.notReadyReason(), "GATEWAY_ALLOW_FAKES") {
		t.Fatalf("reason=%q", cfg.notReadyReason())
	}
}

func TestBuildServer_DurableRedisModeWiresAdapters(t *testing.T) {
	readyOK := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ready" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	}))
	t.Cleanup(readyOK.Close)

	cfg := gatewayConfig{
		IdentityURL:                 readyOK.URL,
		RoomURL:                     readyOK.URL,
		TournamentURL:               readyOK.URL,
		SpectatorURL:                readyOK.URL,
		RankingURL:                  readyOK.URL,
		AnalyticsURL:                readyOK.URL,
		ServiceCredential:           "svc",
		IdentityProducerCredential:  "ident",
		RoomProducerCredential:      "room",
		SpectatorProducerCredential: "spec",
		RedisURL:                    "redis://127.0.0.1:1/6",
		PlayerFeedRedisURL:          "redis://127.0.0.1:1/2",
		SpectatorRedisURL:           "redis://127.0.0.1:1/5",
		SpectatorRedisKeyPrefix:     "spectator:",
		EdgeRateLimit:               1000,
		EdgeRateWindow:              time.Minute,
		PrincipalRateLimit:          1000,
		PrincipalRateWindow:         time.Minute,
	}
	if cfg.redisAdapterBlocked() {
		t.Fatal("full redis URLs must unblock adapter gate")
	}
	srv, mode, err := buildServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if mode != "durable-redis" {
		t.Fatalf("mode=%s", mode)
	}
	// Redis on port 1 is unreachable → /ready fails closed with redis_unavailable, not adapter_blocked.
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "redis_unavailable") {
		t.Fatalf("body=%s", w.Body.String())
	}
	if strings.Contains(w.Body.String(), "redis_adapter_blocked") {
		t.Fatal("durable mode must not report redis_adapter_blocked")
	}
}

func TestOpenAuditSink_FailClosedMissingDir(t *testing.T) {
	_, err := openAuditSink("/no/such/dir/audit.jsonl")
	if err == nil {
		t.Fatal("expected open failure")
	}
}

func TestBuildServer_CapabilityMode_DispatchHitsHTTPIdentityNotFakes(t *testing.T) {
	var loginHits int
	identity := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ready":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ready"}`))
		case "/v1/auth/login":
			loginHits++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"sessionId":"s1","playerId":"p1","token":"tok1"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(identity.Close)

	readyOK := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ready" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	}))
	t.Cleanup(readyOK.Close)

	cfg := gatewayConfig{
		CapabilityMode:              true,
		IdentityURL:                 identity.URL,
		RoomURL:                     readyOK.URL,
		TournamentURL:               readyOK.URL,
		SpectatorURL:                readyOK.URL,
		RankingURL:                  readyOK.URL,
		AnalyticsURL:                readyOK.URL,
		ServiceCredential:           "svc",
		IdentityProducerCredential:  "ident",
		RoomProducerCredential:      "room",
		SpectatorProducerCredential: "spec",
	}
	if !cfg.StaticReady() {
		t.Fatal("capability mode must be statically ready (probe-gated)")
	}
	if cfg.redisAdapterBlocked() {
		t.Fatal("capability mode must not report redis adapter blocked")
	}

	srv, mode, err := buildServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if mode != "capability" {
		t.Fatalf("mode=%s", mode)
	}

	readyReq := httptest.NewRequest(http.MethodGet, "/ready", nil)
	readyW := httptest.NewRecorder()
	srv.Handler().ServeHTTP(readyW, readyReq)
	if readyW.Code != http.StatusOK {
		t.Fatalf("ready status=%d body=%s", readyW.Code, readyW.Body.String())
	}

	loginReq := httptest.NewRequest(http.MethodPost, "/v1/auth/login", strings.NewReader(`{"username":"alice","password":"secret"}`))
	loginReq.Header.Set("Content-Type", "application/json")
	loginW := httptest.NewRecorder()
	srv.Handler().ServeHTTP(loginW, loginReq)
	if loginW.Code != http.StatusOK {
		t.Fatalf("login status=%d body=%s", loginW.Code, loginW.Body.String())
	}
	if loginHits != 1 {
		t.Fatalf("expected real HTTP Identity login hit, got %d (fakes would be 0)", loginHits)
	}
	if !strings.Contains(loginW.Body.String(), `"token":"tok1"`) {
		t.Fatalf("body=%s", loginW.Body.String())
	}
}

func TestBuildServer_CapabilityMode_UpstreamNotReadyBlocks(t *testing.T) {
	var probed []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probed = append(probed, r.URL.Path)
		switch r.URL.Path {
		case "/health":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case "/ready":
			http.Error(w, "not ready", http.StatusServiceUnavailable)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(upstream.Close)

	cfg := gatewayConfig{
		CapabilityMode:              true,
		IdentityURL:                 upstream.URL,
		RoomURL:                     upstream.URL,
		TournamentURL:               upstream.URL,
		SpectatorURL:                upstream.URL,
		RankingURL:                  upstream.URL,
		AnalyticsURL:                upstream.URL,
		ServiceCredential:           "svc",
		IdentityProducerCredential:  "ident",
		RoomProducerCredential:      "room",
		SpectatorProducerCredential: "spec",
	}
	srv, mode, err := buildServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if mode != "capability" {
		t.Fatalf("mode=%s", mode)
	}
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "upstream_unhealthy") {
		t.Fatalf("body=%s", w.Body.String())
	}
	for _, path := range probed {
		if path == "/health" {
			t.Fatalf("capability upstream probes must use /ready, never /health; probed=%v", probed)
		}
		if path != "/ready" {
			t.Fatalf("unexpected probe path %q; probed=%v", path, probed)
		}
	}
	if len(probed) == 0 {
		t.Fatal("expected at least one /ready upstream probe")
	}
}

func TestBuildServer_CapabilityMode_UnhealthyUpstreamNotReady(t *testing.T) {
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "down", http.StatusServiceUnavailable)
	}))
	t.Cleanup(down.Close)

	cfg := gatewayConfig{
		CapabilityMode:              true,
		IdentityURL:                 down.URL,
		RoomURL:                     down.URL,
		TournamentURL:               down.URL,
		SpectatorURL:                down.URL,
		RankingURL:                  down.URL,
		AnalyticsURL:                down.URL,
		ServiceCredential:           "svc",
		IdentityProducerCredential:  "ident",
		RoomProducerCredential:      "room",
		SpectatorProducerCredential: "spec",
	}
	srv, mode, err := buildServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if mode != "capability" {
		t.Fatalf("mode=%s", mode)
	}
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "upstream_unhealthy") {
		t.Fatalf("body=%s", w.Body.String())
	}
}

func TestBuildServer_CapabilityMode_DistinctFromAllowFakes(t *testing.T) {
	t.Setenv("GATEWAY_CAPABILITY_MODE", "1")
	t.Setenv("GATEWAY_ALLOW_FAKES", "")
	cfg := loadGatewayConfig()
	if !cfg.CapabilityMode {
		t.Fatal("expected capability mode from env")
	}
	if cfg.AllowFakes {
		t.Fatal("capability must not imply allow-fakes")
	}
}

func TestBuildServer_AllowFakesTakesPrecedenceOverCapability(t *testing.T) {
	cfg := gatewayConfig{AllowFakes: true, CapabilityMode: true}
	_, mode, err := buildServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if mode != "demo-fakes" {
		t.Fatalf("allow-fakes must win for isolated tests, mode=%s", mode)
	}
}

func TestBuildServer_ConfiguredRemainsBlockedWithoutCapability(t *testing.T) {
	cfg := gatewayConfig{
		IdentityURL:                 "http://identity:8080",
		RoomURL:                     "http://room:8080",
		TournamentURL:               "http://tournament:8080",
		SpectatorURL:                "http://spectator:8080",
		RankingURL:                  "http://ranking:8080",
		AnalyticsURL:                "http://analytics:8080",
		ServiceCredential:           "svc",
		IdentityProducerCredential:  "ident",
		RoomProducerCredential:      "room",
		SpectatorProducerCredential: "spec",
	}
	srv, mode, err := buildServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if mode != "http-backends" {
		t.Fatalf("mode=%s", mode)
	}
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "rate_limiter_not_configured") {
		t.Fatalf("body=%s", w.Body.String())
	}
}

func TestRateLimit_AdapterFailureReturns503(t *testing.T) {
	failing := failingRateLimiter{}
	srv := bff.NewServer(bff.Dependencies{
		Identity:    bff.NewFakeIdentity(),
		Room:        bff.NewFakeRoom(),
		Tournament:  bff.NewFakeTournament(),
		Reads:       &bff.FakeReads{},
		Spectator:   bff.NewFakeSpectatorGate(),
		Ready:       true,
		EdgeLimiter: failing,
	})
	req := httptest.NewRequest(http.MethodGet, "/v1/streams/control", nil)
	req.Header.Set("Authorization", "Bearer tok")
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "rate_limiter_unavailable") {
		t.Fatalf("body=%s", w.Body.String())
	}
}

func TestLoadGatewayConfig_ExplicitOutboundBackendCredentials(t *testing.T) {
	t.Setenv("GATEWAY_SERVICE_CREDENTIAL", "generic-fallback")
	t.Setenv("GATEWAY_IDENTITY_SERVICE_CREDENTIAL", "ident-out")
	t.Setenv("GATEWAY_ROOM_SERVICE_CREDENTIAL", "room-out")
	t.Setenv("GATEWAY_TOURNAMENT_SERVICE_CREDENTIAL", "tour-out")
	t.Setenv("GATEWAY_SPECTATOR_SERVICE_CREDENTIAL", "spec-out")
	t.Setenv("GATEWAY_IDENTITY_CREDENTIAL", "ident-in")
	t.Setenv("GATEWAY_ROOM_CREDENTIAL", "room-in")
	t.Setenv("GATEWAY_SPECTATOR_CREDENTIAL", "spec-in")

	cfg := loadGatewayConfig()
	if cfg.ServiceCredential != "generic-fallback" {
		t.Fatalf("ServiceCredential=%q", cfg.ServiceCredential)
	}
	if cfg.IdentityServiceCredential != "ident-out" {
		t.Fatalf("IdentityServiceCredential=%q", cfg.IdentityServiceCredential)
	}
	if cfg.RoomServiceCredential != "room-out" {
		t.Fatalf("RoomServiceCredential=%q", cfg.RoomServiceCredential)
	}
	if cfg.TournamentServiceCredential != "tour-out" {
		t.Fatalf("TournamentServiceCredential=%q", cfg.TournamentServiceCredential)
	}
	if cfg.SpectatorServiceCredential != "spec-out" {
		t.Fatalf("SpectatorServiceCredential=%q", cfg.SpectatorServiceCredential)
	}
	if cfg.IdentityProducerCredential != "ident-in" {
		t.Fatalf("IdentityProducerCredential=%q (inbound must stay separate)", cfg.IdentityProducerCredential)
	}
	if cfg.RoomProducerCredential != "room-in" {
		t.Fatalf("RoomProducerCredential=%q", cfg.RoomProducerCredential)
	}
	if cfg.SpectatorProducerCredential != "spec-in" {
		t.Fatalf("SpectatorProducerCredential=%q", cfg.SpectatorProducerCredential)
	}
}

func TestLoadGatewayConfig_OutboundFallsBackToServiceCredential(t *testing.T) {
	t.Setenv("GATEWAY_SERVICE_CREDENTIAL", "generic-fallback")
	t.Setenv("GATEWAY_IDENTITY_SERVICE_CREDENTIAL", "")
	t.Setenv("GATEWAY_ROOM_SERVICE_CREDENTIAL", "")
	t.Setenv("GATEWAY_TOURNAMENT_SERVICE_CREDENTIAL", "")
	t.Setenv("GATEWAY_SPECTATOR_SERVICE_CREDENTIAL", "")
	t.Setenv("GATEWAY_IDENTITY_CREDENTIAL", "ident-in")
	t.Setenv("GATEWAY_ROOM_CREDENTIAL", "room-in")
	t.Setenv("GATEWAY_SPECTATOR_CREDENTIAL", "spec-in")

	cfg := loadGatewayConfig()
	for _, got := range []struct {
		name string
		val  string
	}{
		{"IdentityServiceCredential", cfg.IdentityServiceCredential},
		{"RoomServiceCredential", cfg.RoomServiceCredential},
		{"TournamentServiceCredential", cfg.TournamentServiceCredential},
		{"SpectatorServiceCredential", cfg.SpectatorServiceCredential},
	} {
		if got.val != "generic-fallback" {
			t.Fatalf("%s=%q want generic-fallback", got.name, got.val)
		}
	}
	if cfg.IdentityProducerCredential != "ident-in" || cfg.RoomProducerCredential != "room-in" || cfg.SpectatorProducerCredential != "spec-in" {
		t.Fatalf("inbound producers must not be overwritten by outbound fallback: %+v", cfg)
	}
}

func TestLoadGatewayConfig_BackendHTTPTimeoutFromEnvironment(t *testing.T) {
	t.Setenv("GATEWAY_BACKEND_HTTP_TIMEOUT", "10s")

	cfg := loadGatewayConfig()
	if cfg.BackendHTTPTimeout != 10*time.Second {
		t.Fatalf("BackendHTTPTimeout=%s want 10s", cfg.BackendHTTPTimeout)
	}
}

func TestLoadGatewayConfig_BackendHTTPTimeoutDefaultsToFiveSeconds(t *testing.T) {
	t.Setenv("GATEWAY_BACKEND_HTTP_TIMEOUT", "")

	cfg := loadGatewayConfig()
	if cfg.BackendHTTPTimeout != 5*time.Second {
		t.Fatalf("BackendHTTPTimeout=%s want 5s", cfg.BackendHTTPTimeout)
	}
}

func TestLoadGatewayConfig_InvalidBackendHTTPTimeoutFallsBackToFiveSeconds(t *testing.T) {
	for _, raw := range []string{"not-a-duration", "0s", "-1s", "31s"} {
		t.Run(raw, func(t *testing.T) {
			t.Setenv("GATEWAY_BACKEND_HTTP_TIMEOUT", raw)

			cfg := loadGatewayConfig()
			if cfg.BackendHTTPTimeout != 5*time.Second {
				t.Fatalf("BackendHTTPTimeout=%s want 5s", cfg.BackendHTTPTimeout)
			}
		})
	}
}

func TestWireGatewayHTTPClients_UsesPerBackendOutboundCredentials(t *testing.T) {
	var (
		identityCred   string
		roomCred       string
		tournamentCred string
		spectatorCred  string
	)

	identity := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/sessions/validate" {
			http.NotFound(w, r)
			return
		}
		identityCred = r.Header.Get("X-Service-Credential")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"playerId":"p1","sessionId":"s1","username":"alice"}`))
	}))
	t.Cleanup(identity.Close)

	room := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/commands" {
			http.NotFound(w, r)
			return
		}
		roomCred = r.Header.Get("X-Service-Credential")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"Accepted","commandId":"cmd_1","type":"CreateRoom"}`))
	}))
	t.Cleanup(room.Close)

	tournament := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/commands" {
			http.NotFound(w, r)
			return
		}
		tournamentCred = r.Header.Get("X-Service-Credential")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"Accepted","commandId":"cmd_2","type":"CreateTournament"}`))
	}))
	t.Cleanup(tournament.Close)

	spectator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/spectator-admission") {
			http.NotFound(w, r)
			return
		}
		spectatorCred = r.Header.Get("X-Service-Credential")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"allowed":true}`))
	}))
	t.Cleanup(spectator.Close)

	cfg := gatewayConfig{
		IdentityURL:                 identity.URL,
		RoomURL:                     room.URL,
		TournamentURL:               tournament.URL,
		SpectatorURL:                spectator.URL,
		ServiceCredential:           "generic-must-not-win",
		IdentityServiceCredential:   "ident-out",
		RoomServiceCredential:       "room-out",
		TournamentServiceCredential: "tour-out",
		SpectatorServiceCredential:  "spec-out",
		IdentityProducerCredential:  "ident-in-must-not-send",
		RoomProducerCredential:      "room-in-must-not-send",
		SpectatorProducerCredential: "spec-in-must-not-send",
	}
	httpClient := &http.Client{Timeout: 5 * time.Second}
	idClient, roomClient, tourClient, specGate, _ := wireGatewayHTTPClients(cfg, httpClient)

	corr := correlation.Headers{CorrelationID: "c1"}
	if _, err := idClient.ValidateSession(context.Background(), "tok", corr); err != nil {
		t.Fatalf("identity: %v", err)
	}
	if _, err := roomClient.SubmitCommand(context.Background(), bff.CommandDispatch{
		Command: envelope.Command{
			CommandID:     "cmd_1",
			Type:          "CreateRoom",
			SchemaVersion: 1,
			Payload:       json.RawMessage(`{}`),
		},
		Principal:   bff.Principal{PlayerID: "p1", SessionID: "s1"},
		Correlation: corr,
	}); err != nil {
		t.Fatalf("room: %v", err)
	}
	if _, err := tourClient.SubmitCommand(context.Background(), bff.CommandDispatch{
		Command: envelope.Command{
			CommandID:     "cmd_2",
			Type:          "CreateTournament",
			SchemaVersion: 1,
			Payload:       json.RawMessage(`{}`),
		},
		Principal:   bff.Principal{PlayerID: "p1", SessionID: "s1"},
		Correlation: corr,
	}); err != nil {
		t.Fatalf("tournament: %v", err)
	}
	if ok, _, err := specGate.Admit(context.Background(), bff.SpectatorAdmitRequest{
		RoomID:      "r1",
		Correlation: corr,
	}); err != nil || !ok {
		t.Fatalf("spectator: ok=%v err=%v", ok, err)
	}

	if identityCred != "ident-out" {
		t.Fatalf("identity X-Service-Credential=%q", identityCred)
	}
	if roomCred != "room-out" {
		t.Fatalf("room X-Service-Credential=%q", roomCred)
	}
	if tournamentCred != "tour-out" {
		t.Fatalf("tournament X-Service-Credential=%q", tournamentCred)
	}
	if spectatorCred != "spec-out" {
		t.Fatalf("spectator X-Service-Credential=%q", spectatorCred)
	}
	for _, leaked := range []string{
		"generic-must-not-win",
		"ident-in-must-not-send",
		"room-in-must-not-send",
		"spec-in-must-not-send",
	} {
		for _, got := range []string{identityCred, roomCred, tournamentCred, spectatorCred} {
			if got == leaked {
				t.Fatalf("outbound client leaked %q", leaked)
			}
		}
	}
}

type failingRateLimiter struct{}

func (failingRateLimiter) Allow(context.Context, string) (bool, time.Duration, error) {
	return false, 0, bff.ErrRateLimiterUnavailable
}
