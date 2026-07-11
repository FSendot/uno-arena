package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"unoarena/services/gateway/bff"
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

func TestBuildServer_RedisURLBlocksReadiness(t *testing.T) {
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
		RedisURL:                    "redis://localhost:6379",
	}
	if cfg.StaticReady() {
		t.Fatal("REDIS_URL must block static ready")
	}
	srv, _, err := buildServer(cfg)
	if err != nil {
		t.Fatal(err)
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
