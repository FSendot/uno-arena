package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"unoarena/services/gateway/bff"
	"unoarena/services/gateway/bff/store"
)

const (
	defaultGatewayBackendHTTPTimeout           = 5 * time.Second
	maxGatewayBackendHTTPTimeout               = 30 * time.Second
	defaultGatewayBackendConnectTimeout        = time.Second
	maxGatewayBackendConnectTimeout            = 10 * time.Second
	defaultGatewayBackendResponseHeaderTimeout = 4 * time.Second
	maxGatewayBackendResponseHeaderTimeout     = 20 * time.Second
	defaultGatewayBackendIdleConnTimeout       = 90 * time.Second
	maxGatewayBackendIdleConnTimeout           = 5 * time.Minute
	defaultGatewayCircuitFailureThreshold      = 5
	maxGatewayCircuitFailureThreshold          = 100
	defaultGatewayCircuitOpenTimeout           = 30 * time.Second
	maxGatewayCircuitOpenTimeout               = 5 * time.Minute
	defaultGatewayServerReadHeaderTimeout      = 5 * time.Second
	maxGatewayServerReadHeaderTimeout          = 30 * time.Second
	defaultGatewayServerReadTimeout            = 15 * time.Second
	maxGatewayServerReadTimeout                = time.Minute
	defaultGatewayServerIdleTimeout            = 2 * time.Minute
	maxGatewayServerIdleTimeout                = 10 * time.Minute
	defaultGatewayServerMaxHeaderBytes         = 1 << 20
	maxGatewayServerMaxHeaderBytes             = 2 << 20
)

func main() {
	if err := runGateway(); err != nil {
		slog.ErrorContext(context.Background(), "gateway stopped", "event", "service_exit_failure", "error", err)
		os.Exit(1)
	}
}

func runGateway() error {
	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()
	telemetryRuntime, err := startGatewayTelemetry(rootCtx)
	if err != nil {
		return fmt.Errorf("start telemetry: %w", err)
	}
	slog.SetDefault(telemetryRuntime.Logger)
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := telemetryRuntime.Shutdown(shutdownCtx); err != nil {
			telemetryRuntime.Logger.ErrorContext(shutdownCtx, "telemetry shutdown failed", "event", "telemetry_shutdown_failure", "error", err)
		}
	}()

	cfg := loadGatewayConfig()
	if err := validateGatewayAuditConfig(cfg.AuditLogPath); err != nil {
		telemetryRuntime.Logger.ErrorContext(rootCtx, "gateway audit configuration failed", "event", "startup_failure", "error", err)
		return err
	}
	client := gatewayHTTPClientForConfig(telemetryRuntime, cfg)
	built, err := buildGatewayRuntimeWithTelemetry(cfg, client, newGatewayClientInstrumentation(telemetryRuntime))
	if err != nil {
		telemetryRuntime.Logger.ErrorContext(rootCtx, "gateway runtime failed", "event", "startup_failure", "error", err)
		return err
	}
	defer built.close()

	addr := ":8080"
	if v := os.Getenv("PORT"); v != "" {
		addr = ":" + v
	}

	built.startWorkers(rootCtx)

	httpSrv := newGatewayHTTPServer(addr, gatewayHTTPHandler(telemetryRuntime, built.server.Handler()), cfg)
	errCh := make(chan error, 1)
	go func() {
		telemetryRuntime.Logger.InfoContext(rootCtx, "gateway started", "event", "service_started", "addr", addr,
			"mode", built.mode, "ready", cfg.StaticReady() || built.mode == "durable-redis",
			"redisConfigured", cfg.RedisURL != "", "kafkaConfigured", built.kafka != nil)
		errCh <- httpSrv.ListenAndServe()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	var serveErr error
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr = fmt.Errorf("serve HTTP: %w", err)
		}
	case <-sigCh:
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown HTTP: %w", err)
	}
	rootCancel()
	built.stopWorkers()
	if serveErr != nil {
		return serveErr
	}
	return nil
}

type gatewayConfig struct {
	IdentityURL                  string
	RoomURL                      string
	TournamentURL                string
	SpectatorURL                 string
	RankingURL                   string
	AnalyticsURL                 string
	ServiceCredential            string // generic outbound fallback (GATEWAY_SERVICE_CREDENTIAL)
	IdentityServiceCredential    string // outbound Gateway→Identity
	RoomServiceCredential        string // outbound Gateway→Room
	TournamentServiceCredential  string // outbound Gateway→Tournament
	SpectatorServiceCredential   string // outbound Gateway→Spectator
	IdentityProducerCredential   string // inbound: Identity session-invalidation producer
	RoomProducerCredential       string // inbound: Room player-private / spectator-safe / room-terminal
	SpectatorProducerCredential  string // inbound: Spectator projection / spectator-stream only
	AuditLogPath                 string
	RedisURL                     string
	PlayerFeedRedisURL           string
	SpectatorRedisURL            string
	SpectatorRedisKeyPrefix      string
	BackendHTTPTimeout           time.Duration
	BackendConnectTimeout        time.Duration
	BackendResponseHeaderTimeout time.Duration
	BackendIdleConnTimeout       time.Duration
	CircuitFailureThreshold      int
	CircuitOpenTimeout           time.Duration
	ServerReadHeaderTimeout      time.Duration
	ServerReadTimeout            time.Duration
	ServerIdleTimeout            time.Duration
	ServerMaxHeaderBytes         int
	SessionInvalidationTTL       time.Duration
	EdgeRateLimit                int
	EdgeRateWindow               time.Duration
	PrincipalRateLimit           int
	PrincipalRateWindow          time.Duration
	TrustedProxyCIDRs            []string
	AllowFakes                   bool
	CapabilityMode               bool
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
		IdentityURL:                  strings.TrimSpace(os.Getenv("IDENTITY_URL")),
		RoomURL:                      strings.TrimSpace(firstEnv("ROOM_GAMEPLAY_URL", "ROOM_URL")),
		TournamentURL:                strings.TrimSpace(firstEnv("TOURNAMENT_URL", "TOURNAMENT_ORCHESTRATION_URL")),
		SpectatorURL:                 strings.TrimSpace(firstEnv("SPECTATOR_VIEW_URL", "SPECTATOR_URL")),
		RankingURL:                   strings.TrimSpace(os.Getenv("RANKING_URL")),
		AnalyticsURL:                 strings.TrimSpace(os.Getenv("ANALYTICS_URL")),
		ServiceCredential:            svcCred,
		IdentityServiceCredential:    identOut,
		RoomServiceCredential:        roomOut,
		TournamentServiceCredential:  tourOut,
		SpectatorServiceCredential:   specOut,
		IdentityProducerCredential:   identCred,
		RoomProducerCredential:       roomCred,
		SpectatorProducerCredential:  specCred,
		AuditLogPath:                 strings.TrimSpace(os.Getenv("GATEWAY_AUDIT_LOG_PATH")),
		RedisURL:                     strings.TrimSpace(os.Getenv("REDIS_URL")),
		PlayerFeedRedisURL:           strings.TrimSpace(os.Getenv("GATEWAY_PLAYER_FEED_REDIS_URL")),
		SpectatorRedisURL:            strings.TrimSpace(os.Getenv("GATEWAY_SPECTATOR_REDIS_URL")),
		SpectatorRedisKeyPrefix:      strings.TrimSpace(os.Getenv("GATEWAY_SPECTATOR_REDIS_KEY_PREFIX")),
		BackendHTTPTimeout:           envDurationMax("GATEWAY_BACKEND_HTTP_TIMEOUT", defaultGatewayBackendHTTPTimeout, maxGatewayBackendHTTPTimeout),
		BackendConnectTimeout:        envDurationMax("GATEWAY_BACKEND_CONNECT_TIMEOUT", defaultGatewayBackendConnectTimeout, maxGatewayBackendConnectTimeout),
		BackendResponseHeaderTimeout: envDurationMax("GATEWAY_BACKEND_RESPONSE_HEADER_TIMEOUT", defaultGatewayBackendResponseHeaderTimeout, maxGatewayBackendResponseHeaderTimeout),
		BackendIdleConnTimeout:       envDurationMax("GATEWAY_BACKEND_IDLE_CONN_TIMEOUT", defaultGatewayBackendIdleConnTimeout, maxGatewayBackendIdleConnTimeout),
		CircuitFailureThreshold:      envIntMax("GATEWAY_CIRCUIT_BREAKER_FAILURE_THRESHOLD", defaultGatewayCircuitFailureThreshold, maxGatewayCircuitFailureThreshold),
		CircuitOpenTimeout:           envDurationMax("GATEWAY_CIRCUIT_BREAKER_OPEN_TIMEOUT", defaultGatewayCircuitOpenTimeout, maxGatewayCircuitOpenTimeout),
		ServerReadHeaderTimeout:      envDurationMax("GATEWAY_SERVER_READ_HEADER_TIMEOUT", defaultGatewayServerReadHeaderTimeout, maxGatewayServerReadHeaderTimeout),
		ServerReadTimeout:            envDurationMax("GATEWAY_SERVER_READ_TIMEOUT", defaultGatewayServerReadTimeout, maxGatewayServerReadTimeout),
		ServerIdleTimeout:            envDurationMax("GATEWAY_SERVER_IDLE_TIMEOUT", defaultGatewayServerIdleTimeout, maxGatewayServerIdleTimeout),
		ServerMaxHeaderBytes:         envIntMax("GATEWAY_SERVER_MAX_HEADER_BYTES", defaultGatewayServerMaxHeaderBytes, maxGatewayServerMaxHeaderBytes),
		SessionInvalidationTTL:       envDuration("GATEWAY_SESSION_INVALIDATION_TTL", store.DefaultSessionInvalidationTTL),
		EdgeRateLimit:                envInt("GATEWAY_EDGE_RATE_LIMIT", 1000),
		EdgeRateWindow:               envDuration("GATEWAY_EDGE_RATE_WINDOW", time.Minute),
		PrincipalRateLimit:           envInt("GATEWAY_PRINCIPAL_RATE_LIMIT", 1000),
		PrincipalRateWindow:          envDuration("GATEWAY_PRINCIPAL_RATE_WINDOW", time.Minute),
		TrustedProxyCIDRs:            splitCommaSeparated(os.Getenv("GATEWAY_TRUSTED_PROXY_CIDRS")),
		AllowFakes:                   envTruthy("GATEWAY_ALLOW_FAKES") || envTruthy("ALLOW_FAKES"),
		CapabilityMode:               envTruthy("GATEWAY_CAPABILITY_MODE"),
	}
}

// StaticReady reports whether the gateway may advertise static readiness without Redis.
// GATEWAY_ALLOW_FAKES is isolated fake-backend demo. GATEWAY_CAPABILITY_MODE
// uses real HTTP backends with bounded in-memory limiters. Configured durable
// mode requires Redis rate-limit + LiveFeed adapters.
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

type gatewayRuntime struct {
	server   *bff.Server
	mode     string
	kafka    *sessionInvalidatedKafkaLifecycle
	sub      *sessionInvalidationSubLifecycle
	closeFns []func()
}

func (rt *gatewayRuntime) close() {
	rt.stopWorkers()
	for i := len(rt.closeFns) - 1; i >= 0; i-- {
		rt.closeFns[i]()
	}
}

func (rt *gatewayRuntime) startWorkers(parent context.Context) {
	if rt.kafka != nil {
		rt.kafka.start(parent)
		slog.InfoContext(parent, "Kafka consumer started", "event", "kafka_consumer_started",
			"group", rt.kafka.consumer.cfg.Group, "topic", rt.kafka.consumer.cfg.Topic)
	}
	if rt.sub != nil {
		rt.sub.start(parent)
		slog.InfoContext(parent, "session invalidation subscriber started", "event", "session_invalidation_subscriber_started")
	}
}

func (rt *gatewayRuntime) stopWorkers() {
	if rt.kafka != nil {
		rt.kafka.stop()
		rt.kafka = nil
	}
	if rt.sub != nil {
		rt.sub.stop()
		rt.sub = nil
	}
}

func (rt *gatewayRuntime) workerReady(_ context.Context) error {
	// Durable configured mode must never advertise ready without a Kafka consumer.
	// Capability/fakes never set mode durable-redis and ignore Kafka entirely.
	if rt.mode == "durable-redis" && rt.kafka == nil {
		return fmt.Errorf("kafka_consumer_not_configured")
	}
	if rt.kafka != nil && !rt.kafka.Healthy() {
		return fmt.Errorf("kafka_consumer_stopped")
	}
	if rt.sub != nil && !rt.sub.Healthy() {
		return fmt.Errorf("si_subscriber_stopped")
	}
	return nil
}

// buildServer preserves the historical test seam used by main_test.go.
func buildServer(cfg gatewayConfig) (*bff.Server, string, error) {
	rt, err := buildGatewayRuntime(cfg)
	if err != nil {
		return nil, "", err
	}
	return rt.server, rt.mode, nil
}

func buildGatewayRuntime(cfg gatewayConfig) (*gatewayRuntime, error) {
	return buildGatewayRuntimeWithHTTPClient(cfg, nil)
}

func buildGatewayRuntimeWithHTTPClient(cfg gatewayConfig, httpClient *http.Client) (*gatewayRuntime, error) {
	return buildGatewayRuntimeWithTelemetry(cfg, httpClient, gatewayClientInstrumentation{})
}

func buildGatewayRuntimeWithTelemetry(cfg gatewayConfig, httpClient *http.Client, instrumentation gatewayClientInstrumentation) (*gatewayRuntime, error) {
	clientIPResolver, err := bff.NewTrustedProxyClientIPResolver(cfg.TrustedProxyCIDRs)
	if err != nil {
		return nil, fmt.Errorf("trusted proxy configuration: %w", err)
	}
	auditSink, err := openAuditSink(cfg.AuditLogPath, instrumentation.audit)
	if err != nil {
		return nil, fmt.Errorf("audit sink: %w", err)
	}

	if cfg.AllowFakes {
		hub := bff.NewHub()
		return &gatewayRuntime{
			server: bff.NewServer(bff.Dependencies{
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
				ClientIPResolver:            clientIPResolver,
			}),
			mode: "demo-fakes",
		}, nil
	}

	if httpClient == nil {
		httpClient = &http.Client{Timeout: 5 * time.Second}
	}
	identity, room, tournament, spectator, reads := wireGatewayHTTPClients(cfg, httpClient, instrumentation.breaker)

	if cfg.CapabilityMode {
		// Capability must ignore Kafka env entirely.
		hub := bff.NewHub()
		return &gatewayRuntime{
			server: bff.NewServer(bff.Dependencies{
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
				EdgeLimiter:                 bff.NewMemoryRateLimiter(1000, time.Minute),
				PrincipalLimiter:            bff.NewMemoryRateLimiter(1000, time.Minute),
				ClientIPResolver:            clientIPResolver,
			}),
			mode: "capability",
		}, nil
	}

	if cfg.durableRedisConfigured() {
		return buildDurableRedisRuntime(cfg, identity, room, tournament, spectator, reads, auditSink, clientIPResolver, instrumentation)
	}

	hub := bff.NewHub()
	return &gatewayRuntime{
		server: bff.NewServer(bff.Dependencies{
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
			EdgeLimiter:                 bff.AllowAll{},
			PrincipalLimiter:            bff.AllowAll{},
			ClientIPResolver:            clientIPResolver,
		}),
		mode: "http-backends",
	}, nil
}

func buildDurableRedisRuntime(
	cfg gatewayConfig,
	identity bff.IdentityClient,
	room bff.RoomClient,
	tournament bff.TournamentClient,
	spectator bff.SpectatorGate,
	reads bff.ReadModelClient,
	auditSink bff.AuditSink,
	clientIPResolver bff.ClientIPResolver,
	instrumentation gatewayClientInstrumentation,
) (*gatewayRuntime, error) {
	rateRDB, err := store.NewRedisFromURL(cfg.RedisURL)
	if err != nil {
		return nil, fmt.Errorf("rate-limit redis: %w", err)
	}
	playerRDB, err := store.NewRedisFromURL(cfg.PlayerFeedRedisURL)
	if err != nil {
		_ = rateRDB.Close()
		return nil, fmt.Errorf("player-feed redis: %w", err)
	}
	spectatorRDB, err := store.NewRedisFromURL(cfg.SpectatorRedisURL)
	if err != nil {
		_ = rateRDB.Close()
		_ = playerRDB.Close()
		return nil, fmt.Errorf("spectator redis: %w", err)
	}
	if instrumentation.redis != nil {
		for _, client := range []redis.UniversalClient{rateRDB, playerRDB, spectatorRDB} {
			if err := instrumentation.redis(client); err != nil {
				_ = spectatorRDB.Close()
				_ = playerRDB.Close()
				_ = rateRDB.Close()
				return nil, fmt.Errorf("instrument Redis: %w", err)
			}
		}
	}

	hub := bff.NewHub()
	siStore := store.NewSessionInvalidationStore(rateRDB, cfg.SessionInvalidationTTL)
	// Script load is best-effort; go-redis Eval falls back when SHA is missing.
	_ = siStore.LoadScripts(context.Background())

	liveFeed := bff.NewRedisLiveFeed(playerRDB, spectatorRDB, cfg.SpectatorRedisKeyPrefix, hub).
		WithSessionInvalidationStore(siStore)
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

	rt := &gatewayRuntime{
		mode: "durable-redis",
		closeFns: []func(){
			func() { _ = spectatorRDB.Close() },
			func() { _ = playerRDB.Close() },
			func() { _ = rateRDB.Close() },
		},
	}

	// Kafka is loaded only in durable mode so capability stays Kafka-offline.
	kafkaCfg, kafkaEnabled, err := LoadSessionInvalidatedKafkaConfigFromEnv()
	if err != nil {
		rt.close()
		return nil, fmt.Errorf("kafka config: %w", err)
	}
	ingester := &sessionInvalidationIngester{store: siStore, hub: hub}
	if kafkaEnabled {
		life, err := newSessionInvalidatedKafkaLifecycle(kafkaCfg, ingester, storeQuarantineAdapter{store: siStore}, instrumentation.kafkaHooks...)
		if err != nil {
			rt.close()
			return nil, err
		}
		rt.kafka = life
	}

	sub := store.NewSessionInvalidationSubscriber(rateRDB, siStore.NotifyChannel(), func(_ context.Context, n store.SessionInvalidationNotify) error {
		_, _, err := hub.ApplySessionInvalidation(n.EventID, n.SessionID)
		return err
	})
	rt.sub = newSessionInvalidationSubLifecycle(sub)

	rt.server = bff.NewServer(bff.Dependencies{
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
		WorkerReady:                 rt.workerReady,
		SessionInvalidation:         &httpSessionInvalidationBridge{store: siStore},
		EdgeLimiter:                 edge,
		PrincipalLimiter:            principal,
		ClientIPResolver:            clientIPResolver,
	})
	return rt, nil
}

type sessionInvalidationIngester struct {
	store *store.SessionInvalidationStore
	hub   *bff.Hub
}

func (i *sessionInvalidationIngester) Apply(ctx context.Context, evt ParsedSessionInvalidated) (store.SessionInvalidationApplyKind, error) {
	kind, err := i.store.Apply(ctx, store.SessionInvalidationRecord{
		EventID:       evt.EventID,
		SessionID:     evt.SessionID,
		PlayerID:      evt.PlayerID,
		Reason:        evt.Reason,
		CorrelationID: evt.CorrelationID,
		OccurredAt:    evt.OccurredAt,
	})
	if err != nil {
		return "", err
	}
	if kind == store.SessionInvalidationAccepted || kind == store.SessionInvalidationRestored || kind == store.SessionInvalidationDuplicate {
		_, _, _ = i.hub.ApplySessionInvalidation(evt.EventID, evt.SessionID)
	}
	return kind, nil
}

type httpSessionInvalidationBridge struct {
	store *store.SessionInvalidationStore
}

func (b *httpSessionInvalidationBridge) Apply(ctx context.Context, eventID, sessionID, playerID, reason, correlationID string, occurredAt time.Time) (bool, error) {
	kind, err := b.store.Apply(ctx, store.SessionInvalidationRecord{
		EventID:       eventID,
		SessionID:     sessionID,
		PlayerID:      playerID,
		Reason:        reason,
		CorrelationID: correlationID,
		OccurredAt:    occurredAt,
	})
	if err != nil {
		return false, err
	}
	switch kind {
	case store.SessionInvalidationDuplicate:
		return true, nil
	case store.SessionInvalidationConflict:
		return false, bff.ErrControlConflict
	default:
		return false, nil
	}
}

type storeQuarantineAdapter struct {
	store *store.SessionInvalidationStore
}

func (a storeQuarantineAdapter) IsQuarantined(ctx context.Context, consumerGroup, sourceTopic, aggregateKey string) (bool, error) {
	return a.store.IsKafkaAggregateQuarantined(ctx, consumerGroup, sourceTopic, aggregateKey)
}

func (a storeQuarantineAdapter) Quarantine(ctx context.Context, rec AggregateQuarantineRecord) error {
	return a.store.QuarantineKafkaAggregate(ctx, store.KafkaAggregateQuarantine{
		ConsumerGroup:   rec.ConsumerGroup,
		SourceTopic:     rec.SourceTopic,
		AggregateKey:    rec.AggregateKey,
		Classification:  rec.Classification,
		Reason:          rec.Reason,
		SourcePartition: rec.SourcePartition,
		SourceOffset:    rec.SourceOffset,
		EventID:         rec.EventID,
		CorrelationID:   rec.CorrelationID,
	})
}

type sessionInvalidatedKafkaLifecycle struct {
	consumer   *SessionInvalidatedKafkaConsumer
	client     *franzSessionInvalidatedClient
	cancel     context.CancelFunc
	done       chan struct{}
	healthy    atomic.Bool
	stoppedErr atomic.Value
}

func newSessionInvalidatedKafkaLifecycle(cfg SessionInvalidatedKafkaConfig, handler SessionInvalidatedIngester, quarantine AggregateQuarantineStore, hooks ...kgo.Hook) (*sessionInvalidatedKafkaLifecycle, error) {
	client, err := newFranzSessionInvalidatedClient(cfg, hooks...)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	consumer := &SessionInvalidatedKafkaConsumer{
		source:     client,
		dlq:        client,
		handler:    handler,
		quarantine: quarantine,
		cfg:        cfg,
		clock:      systemClock{},
	}
	return &sessionInvalidatedKafkaLifecycle{consumer: consumer, client: client}, nil
}

func (l *sessionInvalidatedKafkaLifecycle) start(parent context.Context) {
	if l == nil || l.consumer == nil {
		return
	}
	l.healthy.Store(true)
	ctx, cancel := context.WithCancel(parent)
	l.cancel = cancel
	l.done = make(chan struct{})
	go func() {
		defer close(l.done)
		err := l.consumer.Run(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			l.healthy.Store(false)
			l.stoppedErr.Store(err)
			slog.ErrorContext(ctx, "Kafka consumer stopped", "event", "kafka_consumer_stopped", "error", sanitizeDLQErrorSummary(err.Error()))
			return
		}
		l.healthy.Store(false)
		l.stoppedErr.Store(fmt.Errorf("kafka consumer exited unexpectedly"))
		slog.ErrorContext(ctx, "Kafka consumer stopped", "event", "kafka_consumer_stopped", "error", "exited unexpectedly")
	}()
}

func (l *sessionInvalidatedKafkaLifecycle) stop() {
	if l == nil {
		return
	}
	if l.cancel != nil {
		l.cancel()
		l.cancel = nil
	}
	if l.done != nil {
		<-l.done
		l.done = nil
	}
	if l.client != nil {
		_ = l.client.Close()
		l.client = nil
	}
	l.healthy.Store(false)
}

func (l *sessionInvalidatedKafkaLifecycle) Healthy() bool {
	if l == nil {
		return true
	}
	return l.healthy.Load()
}

type sessionInvalidationSubLifecycle struct {
	sub     *store.SessionInvalidationSubscriber
	cancel  context.CancelFunc
	done    chan struct{}
	healthy atomic.Bool
}

func newSessionInvalidationSubLifecycle(sub *store.SessionInvalidationSubscriber) *sessionInvalidationSubLifecycle {
	return &sessionInvalidationSubLifecycle{sub: sub}
}

func (l *sessionInvalidationSubLifecycle) start(parent context.Context) {
	if l == nil || l.sub == nil {
		return
	}
	l.healthy.Store(true)
	ctx, cancel := context.WithCancel(parent)
	l.cancel = cancel
	l.done = make(chan struct{})
	go func() {
		defer close(l.done)
		err := l.sub.Run(ctx)
		if ctx.Err() != nil {
			return
		}
		l.healthy.Store(false)
		if err != nil {
			slog.ErrorContext(ctx, "session invalidation subscriber stopped", "event", "session_invalidation_subscriber_stopped", "error", sanitizeDLQErrorSummary(err.Error()))
			return
		}
		slog.ErrorContext(ctx, "session invalidation subscriber stopped", "event", "session_invalidation_subscriber_stopped", "error", "exited unexpectedly")
	}()
}

func (l *sessionInvalidationSubLifecycle) stop() {
	if l == nil {
		return
	}
	if l.cancel != nil {
		l.cancel()
		l.cancel = nil
	}
	if l.done != nil {
		<-l.done
		l.done = nil
	}
	l.healthy.Store(false)
}

func (l *sessionInvalidationSubLifecycle) Healthy() bool {
	if l == nil {
		return true
	}
	return l.healthy.Load()
}

func wireGatewayHTTPClients(cfg gatewayConfig, httpClient *http.Client, observers ...bff.CircuitObserver) (
	bff.IdentityClient, bff.RoomClient, bff.TournamentClient, bff.SpectatorGate, bff.ReadModelClient,
) {
	var observer bff.CircuitObserver
	if len(observers) > 0 {
		observer = observers[0]
	}
	breakerConfig := bff.CircuitBreakerConfig{
		FailureThreshold: cfg.CircuitFailureThreshold,
		OpenTimeout:      cfg.CircuitOpenTimeout,
		Observer:         observer,
	}
	clientFor := func(upstream string) *http.Client {
		return bff.NewCircuitBreakingHTTPClient(httpClient, upstream, breakerConfig)
	}

	var identity bff.IdentityClient = bff.ClosedIdentity{}
	if cfg.IdentityURL != "" {
		identity = bff.NewHTTPIdentityClient(bff.HTTPClientConfig{
			BaseURL:           cfg.IdentityURL,
			ServiceCredential: cfg.IdentityServiceCredential,
			HTTPClient:        clientFor("identity"),
		})
	}

	var room bff.RoomClient = bff.ClosedRoom{}
	if cfg.RoomURL != "" {
		room = bff.NewHTTPRoomClient(bff.HTTPClientConfig{
			BaseURL:           cfg.RoomURL,
			ServiceCredential: cfg.RoomServiceCredential,
			HTTPClient:        clientFor("room"),
		})
	}

	var tournament bff.TournamentClient = bff.ClosedTournament{}
	if cfg.TournamentURL != "" {
		tournament = bff.NewHTTPTournamentClient(bff.HTTPClientConfig{
			BaseURL:           cfg.TournamentURL,
			ServiceCredential: cfg.TournamentServiceCredential,
			HTTPClient:        clientFor("tournament"),
		})
	}

	var spectator bff.SpectatorGate = bff.ClosedSpectator{}
	if cfg.SpectatorURL != "" {
		spectator = bff.NewHTTPSpectatorGate(bff.HTTPClientConfig{
			BaseURL:           cfg.SpectatorURL,
			ServiceCredential: cfg.SpectatorServiceCredential,
			HTTPClient:        clientFor("spectator"),
		})
	}

	var reads bff.ReadModelClient = bff.ClosedReads{}
	if cfg.RankingURL != "" || cfg.AnalyticsURL != "" {
		reads = bff.NewHTTPReadModelClientWithClients(
			cfg.RankingURL,
			cfg.AnalyticsURL,
			clientFor("ranking"),
			clientFor("analytics"),
		)
	}
	return identity, room, tournament, spectator, reads
}

func newGatewayBackendTransport(cfg gatewayConfig) *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{
		Timeout:   cfg.BackendConnectTimeout,
		KeepAlive: 30 * time.Second,
	}).DialContext
	transport.ResponseHeaderTimeout = cfg.BackendResponseHeaderTimeout
	transport.IdleConnTimeout = cfg.BackendIdleConnTimeout
	transport.TLSHandshakeTimeout = cfg.BackendConnectTimeout
	transport.ExpectContinueTimeout = time.Second
	transport.MaxIdleConns = 100
	transport.MaxIdleConnsPerHost = 16
	return transport
}

func newGatewayHTTPServer(addr string, handler http.Handler, cfg gatewayConfig) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: cfg.ServerReadHeaderTimeout,
		ReadTimeout:       cfg.ServerReadTimeout,
		IdleTimeout:       cfg.ServerIdleTimeout,
		MaxHeaderBytes:    cfg.ServerMaxHeaderBytes,
		// SSE responses are intentionally unbounded. A non-zero WriteTimeout would
		// terminate healthy streams; connection lifetime policy belongs at the edge.
		WriteTimeout: 0,
	}
}

func openAuditSink(path string, handler ...slog.Handler) (bff.AuditSink, error) {
	if strings.TrimSpace(path) == "" {
		if len(handler) > 0 && handler[0] != nil {
			return bff.NewSlogAudit(handler[0]), nil
		}
		return bff.NewStderrJSONLAudit(), nil
	}
	return bff.OpenJSONLAudit(path)
}

func firstEnv(keys ...string) string {
	for _, k := range keys {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v
		}
	}
	return ""
}

func splitCommaSeparated(raw string) []string {
	var values []string
	for _, value := range strings.Split(raw, ",") {
		if value = strings.TrimSpace(value); value != "" {
			values = append(values, value)
		}
	}
	return values
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

func envIntMax(key string, def, max int) int {
	n := envInt(key, def)
	if n > max {
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

func envDurationMax(key string, def, max time.Duration) time.Duration {
	d := envDuration(key, def)
	if d > max {
		return def
	}
	return d
}
