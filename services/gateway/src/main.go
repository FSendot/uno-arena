package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"unoarena/services/gateway/bff"
	"unoarena/services/gateway/bff/store"
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
		svc, addr, mode, cfg.StaticReady() || mode == "durable-redis", cfg.RedisURL != "")
	log.Fatal(http.ListenAndServe(addr, server.Handler()))
}

type gatewayConfig struct {
	IdentityURL                 string
	RoomURL                     string
	TournamentURL               string
	SpectatorURL                string
	RankingURL                  string
	AnalyticsURL                string
	ServiceCredential           string // generic outbound fallback (GATEWAY_SERVICE_CREDENTIAL)
	IdentityServiceCredential   string // outbound Gateway→Identity
	RoomServiceCredential       string // outbound Gateway→Room
	TournamentServiceCredential string // outbound Gateway→Tournament
	SpectatorServiceCredential  string // outbound Gateway→Spectator
	IdentityProducerCredential  string // inbound: Identity session-invalidation producer
	RoomProducerCredential      string // inbound: Room player-private / spectator-safe / room-terminal
	SpectatorProducerCredential string // inbound: Spectator projection / spectator-stream only
	AuditLogPath                string
	RedisURL                    string
	PlayerFeedRedisURL          string
	SpectatorRedisURL           string
	SpectatorRedisKeyPrefix     string
	EdgeRateLimit               int
	EdgeRateWindow              time.Duration
	PrincipalRateLimit          int
	PrincipalRateWindow         time.Duration
	AllowFakes                  bool
	CapabilityMode              bool
}

func loadGatewayConfig() gatewayConfig {
	svcCred := strings.TrimSpace(firstEnv("GATEWAY_SERVICE_CREDENTIAL", "SERVICE_CREDENTIAL"))
	// Inbound producer credentials (never reused for outbound backend calls).
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
	// Explicit outbound backend credentials; fall back to GATEWAY_SERVICE_CREDENTIAL.
	identOut := strings.TrimSpace(os.Getenv("GATEWAY_IDENTITY_SERVICE_CREDENTIAL"))
	roomOut := strings.TrimSpace(os.Getenv("GATEWAY_ROOM_SERVICE_CREDENTIAL"))
	tourOut := strings.TrimSpace(os.Getenv("GATEWAY_TOURNAMENT_SERVICE_CREDENTIAL"))
	specOut := strings.TrimSpace(os.Getenv("GATEWAY_SPECTATOR_SERVICE_CREDENTIAL"))
	if identOut == "" {
		identOut = svcCred
	}
	if roomOut == "" {
		roomOut = svcCred
	}
	if tourOut == "" {
		tourOut = svcCred
	}
	if specOut == "" {
		specOut = svcCred
	}
	return gatewayConfig{
		IdentityURL:                 strings.TrimSpace(os.Getenv("IDENTITY_URL")),
		RoomURL:                     strings.TrimSpace(firstEnv("ROOM_GAMEPLAY_URL", "ROOM_URL")),
		TournamentURL:               strings.TrimSpace(firstEnv("TOURNAMENT_URL", "TOURNAMENT_ORCHESTRATION_URL")),
		SpectatorURL:                strings.TrimSpace(firstEnv("SPECTATOR_VIEW_URL", "SPECTATOR_URL")),
		RankingURL:                  strings.TrimSpace(os.Getenv("RANKING_URL")),
		AnalyticsURL:                strings.TrimSpace(os.Getenv("ANALYTICS_URL")),
		ServiceCredential:           svcCred,
		IdentityServiceCredential:   identOut,
		RoomServiceCredential:       roomOut,
		TournamentServiceCredential: tourOut,
		SpectatorServiceCredential:  specOut,
		IdentityProducerCredential:  identCred,
		RoomProducerCredential:      roomCred,
		SpectatorProducerCredential: specCred,
		AuditLogPath:                strings.TrimSpace(os.Getenv("GATEWAY_AUDIT_LOG_PATH")),
		RedisURL:                    strings.TrimSpace(os.Getenv("REDIS_URL")),
		PlayerFeedRedisURL:          strings.TrimSpace(os.Getenv("GATEWAY_PLAYER_FEED_REDIS_URL")),
		SpectatorRedisURL:           strings.TrimSpace(os.Getenv("GATEWAY_SPECTATOR_REDIS_URL")),
		SpectatorRedisKeyPrefix:     strings.TrimSpace(os.Getenv("GATEWAY_SPECTATOR_REDIS_KEY_PREFIX")),
		EdgeRateLimit:               envInt("GATEWAY_EDGE_RATE_LIMIT", 1000),
		EdgeRateWindow:              envDuration("GATEWAY_EDGE_RATE_WINDOW", time.Minute),
		PrincipalRateLimit:          envInt("GATEWAY_PRINCIPAL_RATE_LIMIT", 1000),
		PrincipalRateWindow:         envDuration("GATEWAY_PRINCIPAL_RATE_WINDOW", time.Minute),
		AllowFakes:                  envTruthy("GATEWAY_ALLOW_FAKES") || envTruthy("ALLOW_FAKES"),
		CapabilityMode:              envTruthy("GATEWAY_CAPABILITY_MODE"),
	}
}

// StaticReady reports whether the gateway may advertise static readiness without Redis.
// GATEWAY_ALLOW_FAKES is isolated fake-backend demo. GATEWAY_CAPABILITY_MODE
// uses real HTTP backends with bounded in-memory limiters; /ready still probes
// upstreams. Configured durable mode requires Redis rate-limit + LiveFeed adapters.
func (c gatewayConfig) StaticReady() bool {
	return c.AllowFakes || c.CapabilityMode
}

func (c gatewayConfig) durableRedisConfigured() bool {
	return c.RedisURL != "" && c.PlayerFeedRedisURL != "" && c.SpectatorRedisURL != ""
}

func (c gatewayConfig) notReadyReason() string {
	if c.AllowFakes || c.CapabilityMode {
		return ""
	}
	if c.durableRedisConfigured() {
		return ""
	}
	if c.RedisURL != "" {
		return "REDIS_URL configured but GATEWAY_PLAYER_FEED_REDIS_URL and GATEWAY_SPECTATOR_REDIS_URL are required for durable Redis adapters"
	}
	return "distributed rate-limit adapter not configured; set GATEWAY_ALLOW_FAKES for offline in-memory limiter"
}

func (c gatewayConfig) redisAdapterBlocked() bool {
	// Capability/fakes use in-memory limiters. Configured mode unblocks only with durable Redis.
	return !c.AllowFakes && !c.CapabilityMode && !c.durableRedisConfigured()
}

func buildServer(cfg gatewayConfig) (*bff.Server, string, error) {
	auditSink, err := openAuditSink(cfg.AuditLogPath)
	if err != nil {
		return nil, "", fmt.Errorf("audit sink: %w", err)
	}

	if cfg.AllowFakes {
		hub := bff.NewHub()
		return bff.NewServer(bff.Dependencies{
			Identity:                    bff.NewFakeIdentity(),
			Room:                        bff.NewFakeRoom(),
			Tournament:                  bff.NewFakeTournament(),
			Reads:                       &bff.FakeReads{},
			Spectator:                   bff.NewFakeSpectatorGate(),
			Audit:                       auditSink,
			Hub:                         hub,
			LiveFeed:                    bff.NewHubLiveFeed(hub),
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
	identity, room, tournament, spectator, reads := wireGatewayHTTPClients(cfg, httpClient)
	probes := buildUpstreamProbes(cfg, httpClient)

	if cfg.CapabilityMode {
		hub := bff.NewHub()
		// Real HTTP clients + hub/audit; bounded in-memory limiters; probe-gated ready.
		return bff.NewServer(bff.Dependencies{
			Identity:                    identity,
			Room:                        room,
			Tournament:                  tournament,
			Reads:                       reads,
			Spectator:                   spectator,
			Audit:                       auditSink,
			Hub:                         hub,
			LiveFeed:                    bff.NewHubLiveFeed(hub),
			ServiceCredential:           cfg.ServiceCredential,
			IdentityProducerCredential:  cfg.IdentityProducerCredential,
			RoomProducerCredential:      cfg.RoomProducerCredential,
			SpectatorProducerCredential: cfg.SpectatorProducerCredential,
			Ready:                       true,
			UpstreamProbes:              probes,
			EdgeLimiter:                 bff.NewMemoryRateLimiter(1000, time.Minute),
			PrincipalLimiter:            bff.NewMemoryRateLimiter(1000, time.Minute),
		}), "capability", nil
	}

	if cfg.durableRedisConfigured() {
		return buildDurableRedisServer(cfg, identity, room, tournament, spectator, reads, auditSink, probes)
	}

	// Configured mode without durable Redis: stay blocked (no memory limiter masquerade).
	hub := bff.NewHub()
	return bff.NewServer(bff.Dependencies{
		Identity:                    identity,
		Room:                        room,
		Tournament:                  tournament,
		Reads:                       reads,
		Spectator:                   spectator,
		Audit:                       auditSink,
		Hub:                         hub,
		LiveFeed:                    bff.NewHubLiveFeed(hub),
		ServiceCredential:           cfg.ServiceCredential,
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

func buildDurableRedisServer(
	cfg gatewayConfig,
	identity bff.IdentityClient,
	room bff.RoomClient,
	tournament bff.TournamentClient,
	spectator bff.SpectatorGate,
	reads bff.ReadModelClient,
	auditSink bff.AuditSink,
	probes []bff.UpstreamProbe,
) (*bff.Server, string, error) {
	rateRDB, err := store.NewRedisFromURL(cfg.RedisURL)
	if err != nil {
		return nil, "", fmt.Errorf("rate-limit redis: %w", err)
	}
	playerRDB, err := store.NewRedisFromURL(cfg.PlayerFeedRedisURL)
	if err != nil {
		_ = rateRDB.Close()
		return nil, "", fmt.Errorf("player-feed redis: %w", err)
	}
	spectatorRDB, err := store.NewRedisFromURL(cfg.SpectatorRedisURL)
	if err != nil {
		_ = rateRDB.Close()
		_ = playerRDB.Close()
		return nil, "", fmt.Errorf("spectator redis: %w", err)
	}

	hub := bff.NewHub()
	// Hub remains process-local control-state gate until Kafka SessionInvalidated lands.
	liveFeed := bff.NewRedisLiveFeed(playerRDB, spectatorRDB, cfg.SpectatorRedisKeyPrefix, hub)
	edge := bff.NewRedisRateLimiter(rateRDB, cfg.EdgeRateLimit, cfg.EdgeRateWindow)
	principal := bff.NewRedisRateLimiter(rateRDB, cfg.PrincipalRateLimit, cfg.PrincipalRateWindow)

	clients := []redis.UniversalClient{rateRDB, playerRDB, spectatorRDB}
	redisReady := func(ctx context.Context) error {
		for i, c := range clients {
			if err := store.PingRedis(ctx, c); err != nil {
				return fmt.Errorf("redis client %d: %w", i, err)
			}
		}
		return nil
	}

	return bff.NewServer(bff.Dependencies{
		Identity:                    identity,
		Room:                        room,
		Tournament:                  tournament,
		Reads:                       reads,
		Spectator:                   spectator,
		Audit:                       auditSink,
		Hub:                         hub,
		LiveFeed:                    liveFeed,
		ServiceCredential:           cfg.ServiceCredential,
		IdentityProducerCredential:  cfg.IdentityProducerCredential,
		RoomProducerCredential:      cfg.RoomProducerCredential,
		SpectatorProducerCredential: cfg.SpectatorProducerCredential,
		Ready:                       true,
		RedisAdapterBlocked:         false,
		RedisReady:                  redisReady,
		UpstreamProbes:              probes,
		EdgeLimiter:                 edge,
		PrincipalLimiter:            principal,
	}), "durable-redis", nil
}

func wireGatewayHTTPClients(cfg gatewayConfig, httpClient *http.Client) (
	bff.IdentityClient, bff.RoomClient, bff.TournamentClient, bff.SpectatorGate, bff.ReadModelClient,
) {
	var identity bff.IdentityClient = bff.ClosedIdentity{}
	if cfg.IdentityURL != "" {
		identity = bff.NewHTTPIdentityClient(bff.HTTPClientConfig{
			BaseURL:           cfg.IdentityURL,
			ServiceCredential: cfg.IdentityServiceCredential,
			HTTPClient:        httpClient,
		})
	}

	var room bff.RoomClient = bff.ClosedRoom{}
	if cfg.RoomURL != "" {
		room = bff.NewHTTPRoomClient(bff.HTTPClientConfig{
			BaseURL:           cfg.RoomURL,
			ServiceCredential: cfg.RoomServiceCredential,
			HTTPClient:        httpClient,
		})
	}

	var tournament bff.TournamentClient = bff.ClosedTournament{}
	if cfg.TournamentURL != "" {
		tournament = bff.NewHTTPTournamentClient(bff.HTTPClientConfig{
			BaseURL:           cfg.TournamentURL,
			ServiceCredential: cfg.TournamentServiceCredential,
			HTTPClient:        httpClient,
		})
	}

	var spectator bff.SpectatorGate = bff.ClosedSpectator{}
	if cfg.SpectatorURL != "" {
		spectator = bff.NewHTTPSpectatorGate(bff.HTTPClientConfig{
			BaseURL:           cfg.SpectatorURL,
			ServiceCredential: cfg.SpectatorServiceCredential,
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

func envInt(key string, def int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return def
	}
	return n
}

func envDuration(key string, def time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return def
	}
	return d
}
