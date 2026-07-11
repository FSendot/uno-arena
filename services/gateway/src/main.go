package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"unoarena/services/gateway/bff"
)

func main() {
	svc := os.Getenv("SERVICE_NAME")
	if svc == "" {
		svc = "gateway"
	}

	cfg := loadGatewayConfig()
	server, mode, err := buildServer(cfg)
	if err != nil {
		log.Fatalf(`{"level":"error","service":"%s","event":"startup_failed","error":%q}`, svc, err.Error())
	}

	addr := ":8080"
	if v := os.Getenv("PORT"); v != "" {
		addr = ":" + v
	}
	log.Printf(`{"level":"info","service":"%s","event":"startup","addr":"%s","mode":"%s","ready":%t,"redisURL":%t}`,
		svc, addr, mode, cfg.StaticReady(), cfg.RedisURL != "")
	log.Fatal(http.ListenAndServe(addr, server.Handler()))
}

type gatewayConfig struct {
	IdentityURL                 string
	RoomURL                     string
	TournamentURL               string
	SpectatorURL                string
	RankingURL                  string
	AnalyticsURL                string
	ServiceCredential           string
	IdentityProducerCredential  string
	RoomProducerCredential      string
	SpectatorProducerCredential string
	AuditLogPath                string
	RedisURL                    string
	AllowFakes                  bool
	CapabilityMode              bool
}

func loadGatewayConfig() gatewayConfig {
	svcCred := strings.TrimSpace(firstEnv("GATEWAY_SERVICE_CREDENTIAL", "SERVICE_CREDENTIAL"))
	identCred := strings.TrimSpace(firstEnv("GATEWAY_IDENTITY_CREDENTIAL", "IDENTITY_PRODUCER_CREDENTIAL"))
	roomCred := strings.TrimSpace(firstEnv("GATEWAY_ROOM_CREDENTIAL", "ROOM_PRODUCER_CREDENTIAL"))
	specCred := strings.TrimSpace(firstEnv("GATEWAY_SPECTATOR_CREDENTIAL", "SPECTATOR_PRODUCER_CREDENTIAL"))
	if identCred == "" {
		identCred = svcCred
	}
	if roomCred == "" {
		roomCred = svcCred
	}
	if specCred == "" {
		specCred = svcCred
	}
	return gatewayConfig{
		IdentityURL:                 strings.TrimSpace(os.Getenv("IDENTITY_URL")),
		RoomURL:                     strings.TrimSpace(firstEnv("ROOM_GAMEPLAY_URL", "ROOM_URL")),
		TournamentURL:               strings.TrimSpace(firstEnv("TOURNAMENT_URL", "TOURNAMENT_ORCHESTRATION_URL")),
		SpectatorURL:                strings.TrimSpace(firstEnv("SPECTATOR_VIEW_URL", "SPECTATOR_URL")),
		RankingURL:                  strings.TrimSpace(os.Getenv("RANKING_URL")),
		AnalyticsURL:                strings.TrimSpace(os.Getenv("ANALYTICS_URL")),
		ServiceCredential:           svcCred,
		IdentityProducerCredential:  identCred,
		RoomProducerCredential:      roomCred,
		SpectatorProducerCredential: specCred,
		AuditLogPath:                strings.TrimSpace(os.Getenv("GATEWAY_AUDIT_LOG_PATH")),
		RedisURL:                    strings.TrimSpace(os.Getenv("REDIS_URL")),
		AllowFakes:                  envTruthy("GATEWAY_ALLOW_FAKES") || envTruthy("ALLOW_FAKES"),
		CapabilityMode:              envTruthy("GATEWAY_CAPABILITY_MODE"),
	}
}

// StaticReady reports whether the gateway may advertise static readiness.
// GATEWAY_ALLOW_FAKES is isolated fake-backend demo. GATEWAY_CAPABILITY_MODE
// uses real HTTP backends with bounded in-memory limiters; /ready still probes
// upstreams. Configured mode stays blocked without a distributed Redis adapter.
func (c gatewayConfig) StaticReady() bool {
	return c.AllowFakes || c.CapabilityMode
}

func (c gatewayConfig) notReadyReason() string {
	if c.AllowFakes || c.CapabilityMode {
		return ""
	}
	if c.RedisURL != "" {
		return "REDIS_URL configured; distributed Redis rate-limit adapter is not available in offline gateway"
	}
	return "distributed rate-limit adapter not configured; set GATEWAY_ALLOW_FAKES for offline in-memory limiter"
}

func (c gatewayConfig) redisAdapterBlocked() bool {
	// Capability mode explicitly uses in-memory limiters (not a Redis masquerade).
	// Configured mode without a real Redis adapter remains blocked.
	return !c.AllowFakes && !c.CapabilityMode
}

func buildServer(cfg gatewayConfig) (*bff.Server, string, error) {
	auditSink, err := openAuditSink(cfg.AuditLogPath)
	if err != nil {
		return nil, "", fmt.Errorf("audit sink: %w", err)
	}

	if cfg.AllowFakes {
		return bff.NewServer(bff.Dependencies{
			Identity:                    bff.NewFakeIdentity(),
			Room:                        bff.NewFakeRoom(),
			Tournament:                  bff.NewFakeTournament(),
			Reads:                       &bff.FakeReads{},
			Spectator:                   bff.NewFakeSpectatorGate(),
			Audit:                       auditSink,
			Hub:                         bff.NewHub(),
			ServiceCredential:           cfg.ServiceCredential,
			IdentityProducerCredential:  cfg.IdentityProducerCredential,
			RoomProducerCredential:      cfg.RoomProducerCredential,
			SpectatorProducerCredential: cfg.SpectatorProducerCredential,
			Ready:                       true,
			EdgeLimiter:                 bff.NewMemoryRateLimiter(1000, time.Minute),
			PrincipalLimiter:            bff.NewMemoryRateLimiter(1000, time.Minute),
		}), "demo-fakes", nil
	}

	httpClient := &http.Client{Timeout: 5 * time.Second}
	cred := cfg.ServiceCredential
	identity, room, tournament, spectator, reads := wireGatewayHTTPClients(cfg, httpClient, cred)
	probes := buildUpstreamProbes(cfg, httpClient)

	if cfg.CapabilityMode {
		// Real HTTP clients + hub/audit; bounded in-memory limiters; probe-gated ready.
		return bff.NewServer(bff.Dependencies{
			Identity:                    identity,
			Room:                        room,
			Tournament:                  tournament,
			Reads:                       reads,
			Spectator:                   spectator,
			Audit:                       auditSink,
			Hub:                         bff.NewHub(),
			ServiceCredential:           cred,
			IdentityProducerCredential:  cfg.IdentityProducerCredential,
			RoomProducerCredential:      cfg.RoomProducerCredential,
			SpectatorProducerCredential: cfg.SpectatorProducerCredential,
			Ready:                       true,
			UpstreamProbes:              probes,
			EdgeLimiter:                 bff.NewMemoryRateLimiter(1000, time.Minute),
			PrincipalLimiter:            bff.NewMemoryRateLimiter(1000, time.Minute),
		}), "capability", nil
	}

	// Configured mode: real HTTP clients; never wire in-memory limiter as Redis stand-in.
	return bff.NewServer(bff.Dependencies{
		Identity:                    identity,
		Room:                        room,
		Tournament:                  tournament,
		Reads:                       reads,
		Spectator:                   spectator,
		Audit:                       auditSink,
		Hub:                         bff.NewHub(),
		ServiceCredential:           cred,
		IdentityProducerCredential:  cfg.IdentityProducerCredential,
		RoomProducerCredential:      cfg.RoomProducerCredential,
		SpectatorProducerCredential: cfg.SpectatorProducerCredential,
		Ready:                       cfg.StaticReady(),
		NotReadyReason:              cfg.notReadyReason(),
		RedisAdapterBlocked:         cfg.redisAdapterBlocked(),
		UpstreamProbes:              probes,
		EdgeLimiter:                 bff.AllowAll{},
		PrincipalLimiter:            bff.AllowAll{},
	}), "http-backends", nil
}

func wireGatewayHTTPClients(cfg gatewayConfig, httpClient *http.Client, cred string) (
	bff.IdentityClient, bff.RoomClient, bff.TournamentClient, bff.SpectatorGate, bff.ReadModelClient,
) {
	var identity bff.IdentityClient = bff.ClosedIdentity{}
	if cfg.IdentityURL != "" {
		identity = bff.NewHTTPIdentityClient(bff.HTTPClientConfig{
			BaseURL:           cfg.IdentityURL,
			ServiceCredential: cred,
			HTTPClient:        httpClient,
		})
	}

	var room bff.RoomClient = bff.ClosedRoom{}
	if cfg.RoomURL != "" {
		room = bff.NewHTTPRoomClient(bff.HTTPClientConfig{
			BaseURL:           cfg.RoomURL,
			ServiceCredential: cred,
			HTTPClient:        httpClient,
		})
	}

	var tournament bff.TournamentClient = bff.ClosedTournament{}
	if cfg.TournamentURL != "" {
		tournament = bff.NewHTTPTournamentClient(bff.HTTPClientConfig{
			BaseURL:           cfg.TournamentURL,
			ServiceCredential: cred,
			HTTPClient:        httpClient,
		})
	}

	var spectator bff.SpectatorGate = bff.ClosedSpectator{}
	if cfg.SpectatorURL != "" {
		spectator = bff.NewHTTPSpectatorGate(bff.HTTPClientConfig{
			BaseURL:           cfg.SpectatorURL,
			ServiceCredential: cred,
			HTTPClient:        httpClient,
		})
	}

	var reads bff.ReadModelClient = bff.ClosedReads{}
	if cfg.RankingURL != "" || cfg.AnalyticsURL != "" {
		reads = bff.NewHTTPReadModelClient(cfg.RankingURL, cfg.AnalyticsURL, httpClient)
	}
	return identity, room, tournament, spectator, reads
}

func openAuditSink(path string) (bff.AuditSink, error) {
	if strings.TrimSpace(path) == "" {
		return bff.NewStderrJSONLAudit(), nil
	}
	return bff.OpenJSONLAudit(path)
}

func buildUpstreamProbes(cfg gatewayConfig, client *http.Client) []bff.UpstreamProbe {
	do := func(ctx context.Context, method, url string) (int, error) {
		req, err := http.NewRequestWithContext(ctx, method, url, nil)
		if err != nil {
			return 0, err
		}
		resp, err := client.Do(req)
		if err != nil {
			return 0, err
		}
		defer resp.Body.Close()
		return resp.StatusCode, nil
	}
	out := make([]bff.UpstreamProbe, 0, 6)
	add := func(name, base string) {
		base = strings.TrimRight(strings.TrimSpace(base), "/")
		if base == "" {
			return
		}
		out = append(out, bff.UpstreamProbe{
			Name: name,
			URL:  base + "/ready",
			Do:   do,
		})
	}
	add("identity", cfg.IdentityURL)
	add("room", cfg.RoomURL)
	add("tournament", cfg.TournamentURL)
	add("spectator", cfg.SpectatorURL)
	add("ranking", cfg.RankingURL)
	add("analytics", cfg.AnalyticsURL)
	return out
}

func firstEnv(keys ...string) string {
	for _, k := range keys {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v
		}
	}
	return ""
}

func envTruthy(key string) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	return v == "1" || v == "true" || v == "yes"
}
