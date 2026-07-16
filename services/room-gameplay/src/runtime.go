package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"unoarena/services/room-gameplay/app"
	"unoarena/services/room-gameplay/store"
	"unoarena/shared/internalprincipal"
)

type roomRuntimeConfig struct {
	ServiceName                        string
	ServiceCredential                  string
	TimerCredential                    string
	SpectatorRecoveryCredential        string
	AnalyticsBackfillCredential        string
	GatewayURL                         string
	GatewayCred                        string
	IdentityURL                        string
	IdentityCred                       string
	InternalPrincipalCurrentKeyID      string
	InternalPrincipalCurrentHMACKey    string
	InternalPrincipalPreviousKeyID     string
	InternalPrincipalPreviousHMACKey   string
	InternalPrincipalKeyVersion        string
	InternalPrincipalIssuer            string
	InternalPrincipalAudience          string
	InternalPrincipalMaxTTLSeconds     int
	InternalPrincipalFutureSkewSeconds int
	GameIntegrityURL                   string
	GameIntegrityCred                  string
	GameIntegrityHTTPTimeout           time.Duration
	DatabaseURL                        string
	RedisURL                           string
	AuditLogPath                       string
	AllowFakes                         bool
	CapabilityMode                     bool
	DeploymentEnv                      string
	WorkerRole                         string
	RoomGameplayURL                    string
	SpectatorURL                       string
	SpectatorCred                      string
	RankingURL                         string
	RankingCred                        string
	AnalyticsURL                       string
	AnalyticsCred                      string
	TournamentURL                      string
	TournamentCred                     string
	RuntimeRoomID                      string
	RuntimeGeneration                  int64
	RuntimeRouterCredential            string
	RuntimeQueueCapacity               int
	RuntimeImage                       string
	RuntimeControllerOwner             string
	RuntimeControllerClaimBatch        int
	RuntimeControllerConcurrency       int
	RuntimeControllerCadence           time.Duration
	RuntimeReadinessTimeout            time.Duration
	RuntimeCPURequest                  string
	RuntimeMemoryRequest               string
	RuntimeCPULimit                    string
	RuntimeMemoryLimit                 string
	RuntimeProbeTimeoutSeconds         int
	RuntimeProbeFailureThreshold       int
	RuntimeNodeSelector                map[string]string
	RuntimeTopologySpreadEnabled       bool
	RuntimeTopologyKey                 string
	RuntimeTopologyMaxSkew             int
	RuntimeTopologyUnsatisfiable       string
	PlayerStreamCompactorPage          int
	PlayerStreamMaxLen                 int64
	PlayerStreamTrimLimit              int64
	PlayerStreamCompactorCadence       time.Duration
	PlayerStreamOperationTimeout       time.Duration
	IntegrityReconcilerBatch           int
	IntegrityReconcilerLease           time.Duration
	IntegrityReconcilerInterval        time.Duration
	KubernetesAPIURL                   string
	KubernetesNamespace                string
	RuntimeSecretName                  string
	RuntimeSecretEnv                   map[string]string
}

type roomRuntime struct {
	Service        *app.Service
	Deps           app.ServiceDeps
	Pool           *store.Pool
	Sessions       *store.SessionStore
	Timers         *store.TimerIndex
	SchemaExp      store.SchemaExpectation
	Ready          bool
	NotReadyReason string
	Mode           string // durable | capability | capability-fakes | misconfigured
	DurableReady   func(context.Context) error
}

type roomRoleResponsibilities struct {
	TimerIndexRebuild       bool
	IntegrityReconciliation bool
}

func responsibilitiesForRoomRole(role string) roomRoleResponsibilities {
	switch strings.TrimSpace(role) {
	case "room-timer":
		return roomRoleResponsibilities{TimerIndexRebuild: true}
	case "room-integrity-reconciler":
		return roomRoleResponsibilities{IntegrityReconciliation: true}
	default:
		return roomRoleResponsibilities{}
	}
}

func loadRoomRuntimeConfig() roomRuntimeConfig {
	cred := strings.TrimSpace(os.Getenv("SERVICE_CREDENTIAL"))
	if cred == "" {
		cred = strings.TrimSpace(os.Getenv("ROOM_SERVICE_CREDENTIAL"))
	}
	gatewayCred := strings.TrimSpace(os.Getenv("GATEWAY_SERVICE_CREDENTIAL"))
	if gatewayCred == "" {
		gatewayCred = cred
	}
	svcName := os.Getenv("SERVICE_NAME")
	if svcName == "" {
		svcName = "room-gameplay"
	}
	depEnv := strings.TrimSpace(os.Getenv("DEPLOYMENT_ENV"))
	if depEnv == "" {
		depEnv = "development"
	}
	return roomRuntimeConfig{
		ServiceName:                 svcName,
		ServiceCredential:           cred,
		TimerCredential:             strings.TrimSpace(os.Getenv("ROOM_TIMER_SERVICE_CREDENTIAL")),
		SpectatorRecoveryCredential: strings.TrimSpace(os.Getenv("ROOM_SPECTATOR_RECOVERY_SERVICE_CREDENTIAL")),
		// Scoped Analytics pair only — never fall back to Gateway/generic SERVICE_CREDENTIAL.
		AnalyticsBackfillCredential: strings.TrimSpace(os.Getenv("ROOM_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL")),
		GatewayURL:                  strings.TrimSpace(os.Getenv("GATEWAY_URL")),
		GatewayCred:                 gatewayCred,
		IdentityURL:                 strings.TrimSpace(os.Getenv("IDENTITY_URL")),
		IdentityCred: firstNonEmptyEnv(
			os.Getenv("IDENTITY_SERVICE_CREDENTIAL"),
			os.Getenv("ROOM_IDENTITY_CREDENTIAL"),
			cred,
		),
		InternalPrincipalCurrentKeyID:      strings.TrimSpace(os.Getenv("ROOM_INTERNAL_PRINCIPAL_CURRENT_KEY_ID")),
		InternalPrincipalCurrentHMACKey:    strings.TrimSpace(os.Getenv("ROOM_INTERNAL_PRINCIPAL_HMAC_KEY_CURRENT")),
		InternalPrincipalPreviousKeyID:     strings.TrimSpace(os.Getenv("ROOM_INTERNAL_PRINCIPAL_PREVIOUS_KEY_ID")),
		InternalPrincipalPreviousHMACKey:   strings.TrimSpace(os.Getenv("ROOM_INTERNAL_PRINCIPAL_HMAC_KEY_PREVIOUS")),
		InternalPrincipalKeyVersion:        strings.TrimSpace(os.Getenv("ROOM_INTERNAL_PRINCIPAL_KEY_VERSION")),
		InternalPrincipalIssuer:            firstNonEmptyEnv(os.Getenv("ROOM_INTERNAL_PRINCIPAL_ISSUER"), internalprincipal.DefaultIssuer),
		InternalPrincipalAudience:          firstNonEmptyEnv(os.Getenv("ROOM_INTERNAL_PRINCIPAL_AUDIENCE"), internalprincipal.DefaultRoomAudience),
		InternalPrincipalMaxTTLSeconds:     envInt("ROOM_INTERNAL_PRINCIPAL_MAX_TTL_SECONDS", 20),
		InternalPrincipalFutureSkewSeconds: envInt("ROOM_INTERNAL_PRINCIPAL_FUTURE_SKEW_SECONDS", int(internalprincipal.DefaultFutureSkew/time.Second)),
		GameIntegrityURL:                   strings.TrimSpace(os.Getenv("GAME_INTEGRITY_URL")),
		GameIntegrityCred: firstNonEmptyEnv(
			os.Getenv("GAME_INTEGRITY_INTERNAL_CREDENTIAL"),
			os.Getenv("ROOM_GAME_INTEGRITY_CREDENTIAL"),
			cred,
		),
		GameIntegrityHTTPTimeout: boundedIntegrityHTTPTimeout(time.Duration(envInt("GAME_INTEGRITY_HTTP_TIMEOUT_MILLIS", 3000)) * time.Millisecond),
		DatabaseURL:              firstNonEmptyEnv(os.Getenv("ROOM_PGBOUNCER_URL"), os.Getenv("DATABASE_URL")),
		RedisURL:                 strings.TrimSpace(os.Getenv("REDIS_URL")),
		AuditLogPath:             strings.TrimSpace(os.Getenv("ROOM_AUDIT_LOG_PATH")),
		AllowFakes:               envTruthy("ROOM_ALLOW_FAKES") || envTruthy("ALLOW_FAKES"),
		CapabilityMode:           envTruthy("ROOM_CAPABILITY_MODE"),
		DeploymentEnv:            depEnv,
		WorkerRole:               strings.TrimSpace(os.Getenv("WORKER_ROLE")),
		RoomGameplayURL: firstNonEmptyEnv(
			os.Getenv("ROOM_GAMEPLAY_URL"),
			"http://127.0.0.1:8080",
		),
		SpectatorURL:                 strings.TrimSpace(os.Getenv("SPECTATOR_VIEW_URL")),
		SpectatorCred:                firstEnv("SPECTATOR_VIEW_SERVICE_CREDENTIAL", gatewayCred),
		RankingURL:                   strings.TrimSpace(os.Getenv("RANKING_URL")),
		RankingCred:                  firstEnv("RANKING_SERVICE_CREDENTIAL", gatewayCred),
		AnalyticsURL:                 strings.TrimSpace(os.Getenv("ANALYTICS_URL")),
		AnalyticsCred:                firstEnv("ANALYTICS_SERVICE_CREDENTIAL", gatewayCred),
		TournamentURL:                strings.TrimSpace(os.Getenv("TOURNAMENT_URL")),
		TournamentCred:               firstEnv("TOURNAMENT_SERVICE_CREDENTIAL", gatewayCred),
		RuntimeRoomID:                strings.TrimSpace(os.Getenv("ROOM_RUNTIME_ROOM_ID")),
		RuntimeGeneration:            envInt64("ROOM_RUNTIME_GENERATION", 0),
		RuntimeRouterCredential:      strings.TrimSpace(os.Getenv("ROOM_RUNTIME_ROUTER_CREDENTIAL")),
		RuntimeQueueCapacity:         envInt("ROOM_RUNTIME_MUTATION_QUEUE_CAPACITY", 16),
		RuntimeImage:                 strings.TrimSpace(os.Getenv("ROOM_RUNTIME_IMAGE")),
		RuntimeControllerOwner:       firstNonEmptyEnv(os.Getenv("POD_NAME"), os.Getenv("HOSTNAME"), "room-runtime-controller"),
		RuntimeControllerClaimBatch:  envInt("ROOM_RUNTIME_CONTROLLER_CLAIM_BATCH", 16),
		RuntimeControllerConcurrency: envInt("ROOM_RUNTIME_CONTROLLER_CONCURRENCY", 4),
		RuntimeControllerCadence:     time.Duration(envInt("ROOM_RUNTIME_CONTROLLER_CADENCE_MILLIS", 1000)) * time.Millisecond,
		RuntimeReadinessTimeout:      time.Duration(envInt("ROOM_RUNTIME_READINESS_TIMEOUT_SECONDS", 60)) * time.Second,
		RuntimeCPURequest:            firstNonEmptyEnv(os.Getenv("ROOM_RUNTIME_CPU_REQUEST"), "10m"),
		RuntimeMemoryRequest:         firstNonEmptyEnv(os.Getenv("ROOM_RUNTIME_MEMORY_REQUEST"), "32Mi"),
		RuntimeCPULimit:              firstNonEmptyEnv(os.Getenv("ROOM_RUNTIME_CPU_LIMIT"), "250m"),
		RuntimeMemoryLimit:           firstNonEmptyEnv(os.Getenv("ROOM_RUNTIME_MEMORY_LIMIT"), "256Mi"),
		RuntimeProbeTimeoutSeconds:   envInt("ROOM_RUNTIME_PROBE_TIMEOUT_SECONDS", 1),
		RuntimeProbeFailureThreshold: envInt("ROOM_RUNTIME_PROBE_FAILURE_THRESHOLD", 3),
		RuntimeNodeSelector:          envStringMap("ROOM_RUNTIME_NODE_SELECTOR_JSON"),
		RuntimeTopologySpreadEnabled: envTruthy("ROOM_RUNTIME_TOPOLOGY_SPREAD_ENABLED"),
		RuntimeTopologyKey:           firstNonEmptyEnv(os.Getenv("ROOM_RUNTIME_TOPOLOGY_KEY"), "kubernetes.io/hostname"),
		RuntimeTopologyMaxSkew:       envInt("ROOM_RUNTIME_TOPOLOGY_MAX_SKEW", 1),
		RuntimeTopologyUnsatisfiable: firstNonEmptyEnv(os.Getenv("ROOM_RUNTIME_TOPOLOGY_WHEN_UNSATISFIABLE"), "DoNotSchedule"),
		PlayerStreamCompactorPage:    boundedPlayerStreamPageSize(envInt("ROOM_PLAYER_STREAM_COMPACTOR_PAGE_SIZE", defaultPlayerStreamCompactorPage)),
		PlayerStreamMaxLen:           boundedPlayerStreamMaxLen(envInt64("ROOM_PLAYER_STREAM_MAXLEN", defaultPlayerStreamMaxLen)),
		PlayerStreamTrimLimit:        boundedPlayerStreamTrimLimit(envInt64("ROOM_PLAYER_STREAM_TRIM_LIMIT", defaultPlayerStreamTrimLimit)),
		PlayerStreamCompactorCadence: boundedPlayerStreamCadence(time.Duration(envInt("ROOM_PLAYER_STREAM_COMPACTOR_INTERVAL_MILLIS", int(defaultPlayerStreamCompactorCadence.Milliseconds()))) * time.Millisecond),
		PlayerStreamOperationTimeout: boundedPlayerStreamOperationTimeout(time.Duration(envInt("ROOM_PLAYER_STREAM_OPERATION_TIMEOUT_MILLIS", int(defaultPlayerStreamOperationTimeout.Milliseconds()))) * time.Millisecond),
		IntegrityReconcilerBatch:     envInt("ROOM_INTEGRITY_RECONCILER_CLAIM_BATCH", 32),
		IntegrityReconcilerLease:     time.Duration(envInt("ROOM_INTEGRITY_RECONCILER_LEASE_SECONDS", 60)) * time.Second,
		IntegrityReconcilerInterval:  time.Duration(envInt("ROOM_INTEGRITY_RECONCILER_INTERVAL_MILLIS", 2000)) * time.Millisecond,
		KubernetesAPIURL:             strings.TrimSpace(os.Getenv("KUBERNETES_API_URL")),
		KubernetesNamespace:          firstNonEmptyEnv(os.Getenv("POD_NAMESPACE"), os.Getenv("KUBERNETES_NAMESPACE"), "default"),
		RuntimeSecretName:            strings.TrimSpace(os.Getenv("ROOM_RUNTIME_SECRET_NAME")),
		RuntimeSecretEnv:             envStringMap("ROOM_RUNTIME_SECRET_ENV_JSON"),
	}
}

func envStringMap(name string) map[string]string {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return nil
	}
	var values map[string]string
	if err := json.Unmarshal([]byte(raw), &values); err != nil {
		return nil
	}
	return values
}

func envInt(name string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return fallback
	}
	return n
}

func envInt64(name string, fallback int64) int64 {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return fallback
	}
	return n
}

func isNonProd(env string) bool {
	switch strings.ToLower(strings.TrimSpace(env)) {
	case "production", "staging", "prod":
		return false
	default:
		return true
	}
}

func wireRoomRuntime(cfg roomRuntimeConfig) (roomRuntime, error) {
	clock := app.SystemClock{}

	if cfg.AllowFakes {
		deps := app.ServiceDeps{
			Sessions:  app.NewMemorySessionRepository(),
			Integrity: app.NewFakeGameIntegrity(),
			Publisher: app.NewFakeEventPublisher(),
			Audit:     app.NewFakeAuditSink(),
			Deals:     app.NewFakeDealSource(),
			Clock:     clock,
			SessionsV: app.AllowAllSessionValidator{},
		}
		if cfg.IdentityURL != "" {
			deps.SessionsV = app.NewHTTPSessionValidator(cfg.IdentityURL, cfg.IdentityCred, nil)
		}
		svc := app.NewService(deps)
		svc.SetAnalyticsBackfillReader(app.NewMemoryAnalyticsBackfillStore())
		svc.SetPublicListReader(deps.Sessions.(app.PublicRoomLister))
		return roomRuntime{
			Service: svc,
			Deps:    deps,
			Ready:   true,
			Mode:    "capability-fakes",
		}, nil
	}

	// Durable mode when DATABASE_URL is set and capability is not forced.
	if cfg.DatabaseURL != "" && !cfg.CapabilityMode {
		missing := roomDurableMissing(cfg)
		if len(missing) > 0 {
			return roomRuntime{
				Service: app.NewService(app.ServiceDeps{
					Sessions:  app.BlockedSessionRepository{},
					Integrity: app.ClosedGameIntegrity{},
					Publisher: app.NewFakeEventPublisher(),
					Audit:     app.NewStderrJSONLAuditSink(),
					Deals:     app.ClosedDealSource{},
					Clock:     clock,
					SessionsV: app.ClosedSessionValidator{},
				}),
				Deps: app.ServiceDeps{
					Sessions:  app.BlockedSessionRepository{},
					Integrity: app.ClosedGameIntegrity{},
					Publisher: app.NewFakeEventPublisher(),
					Audit:     app.NewStderrJSONLAuditSink(),
					Deals:     app.ClosedDealSource{},
					Clock:     clock,
					SessionsV: app.ClosedSessionValidator{},
				},
				Ready:          false,
				NotReadyReason: "durable_dependencies_missing: " + strings.Join(missing, ","),
				Mode:           "durable",
			}, nil
		}
		return wireRoomDurableRuntime(cfg, clock)
	}

	// Memory only behind explicit capability mode + non-prod.
	if cfg.CapabilityMode && isNonProd(cfg.DeploymentEnv) {
		multiPublisher := app.NewMultiDestinationPublisher(app.PublisherDestinations{
			GatewayURL: cfg.GatewayURL, GatewayCred: cfg.GatewayCred,
			SpectatorURL: cfg.SpectatorURL, SpectatorCred: cfg.SpectatorCred,
			RankingURL: cfg.RankingURL, RankingCred: cfg.RankingCred,
			AnalyticsURL: cfg.AnalyticsURL, AnalyticsCred: cfg.AnalyticsCred,
			TournamentURL: cfg.TournamentURL, TournamentCred: cfg.TournamentCred,
		}, nil)
		return wireRoomCapabilityRuntime(cfg, clock, multiPublisher)
	}

	deps := app.ServiceDeps{
		Sessions:  app.BlockedSessionRepository{},
		Integrity: app.ClosedGameIntegrity{},
		Publisher: app.NewFakeEventPublisher(),
		Audit:     app.NewStderrJSONLAuditSink(),
		Deals:     app.ClosedDealSource{},
		Clock:     clock,
		SessionsV: app.ClosedSessionValidator{},
	}
	return roomRuntime{
		Service:        app.NewService(deps),
		Deps:           deps,
		Ready:          false,
		NotReadyReason: "database_unconfigured",
		Mode:           "misconfigured",
	}, nil
}

func wireRoomDurableRuntime(cfg roomRuntimeConfig, clock app.Clock) (roomRuntime, error) {
	missing := roomDurableMissing(cfg)
	if len(missing) > 0 {
		return roomRuntime{
			Ready:          false,
			NotReadyReason: "durable_dependencies_missing: " + strings.Join(missing, ","),
			Mode:           "durable",
		}, fmt.Errorf("durable dependencies missing: %s", strings.Join(missing, ","))
	}

	pool, err := store.NewPool(context.Background(), cfg.DatabaseURL)
	if err != nil {
		return roomRuntime{}, fmt.Errorf("database pool: %w", err)
	}
	rdb, err := store.NewRedisFromURL(cfg.RedisURL)
	if err != nil {
		pool.Close()
		return roomRuntime{}, fmt.Errorf("redis: %w", err)
	}
	timers := store.NewTimerIndex(rdb)
	if err := timers.LoadScripts(context.Background()); err != nil {
		pool.Close()
		_ = rdb.Close()
		return roomRuntime{}, fmt.Errorf("redis scripts: %w", err)
	}

	sessions := store.NewSessionStoreWithPools(pool.Main, pool.Intent).WithTimers(timers)
	var auditSink app.AuditSink = app.NewStderrJSONLAuditSink()
	if telemetryRequired() && roomProcessTelemetry != nil {
		auditSink = app.NewSlogAuditSink(roomProcessTelemetry.Handler)
	} else if cfg.AuditLogPath != "" {
		sink, err := app.OpenJSONLAuditSink(cfg.AuditLogPath)
		if err != nil {
			pool.Close()
			_ = rdb.Close()
			return roomRuntime{}, err
		}
		auditSink = sink
	}

	integrityClient := app.NewIntegrityHTTPClient(cfg.GameIntegrityHTTPTimeout)
	integrity := app.NewHTTPGameIntegrity(cfg.GameIntegrityURL, cfg.GameIntegrityCred, integrityClient)
	if auditCred := strings.TrimSpace(os.Getenv("GAME_INTEGRITY_AUDIT_CREDENTIAL")); auditCred != "" {
		integrity.AuditCredential = auditCred
	}

	deps := app.ServiceDeps{
		Sessions:  sessions,
		Commands:  sessions,
		Integrity: integrity,
		Publisher: app.NewFakeEventPublisher(), // durable: CDC owns delivery; no MultiDestinationPublisher
		Audit:     auditSink,
		Deals:     app.NewHTTPDealSource(cfg.GameIntegrityURL, cfg.GameIntegrityCred, integrityClient),
		Clock:     clock,
		// ADR-0021: signed principal verification at the HTTP boundary replaces
		// a second Identity call inside the Room mutation transaction.
		SessionsV: app.ClosedSessionValidator{},
	}
	exp := store.DefaultSchemaExpectation()
	svc := app.NewService(deps)
	svc.SetAnalyticsBackfillReader(store.NewAnalyticsBackfillStore(pool.Main))
	svc.SetPublicListReader(sessions)
	return roomRuntime{
		Service:   svc,
		Deps:      deps,
		Pool:      pool,
		Sessions:  sessions,
		Timers:    timers,
		SchemaExp: exp,
		Ready:     true,
		Mode:      "durable",
		DurableReady: func(ctx context.Context) error {
			if cfg.WorkerRole != "room-runtime" && cfg.WorkerRole != "room-timer" && cfg.WorkerRole != "room-integrity-reconciler" {
				if !app.AnalyticsBackfillCursorSecretConfigured() {
					return fmt.Errorf("%w", app.ErrAnalyticsBackfillCursorSecretRequired)
				}
				if !app.PublicListCursorSecretConfigured() {
					return fmt.Errorf("%w", app.ErrPublicListCursorSecretRequired)
				}
			}
			if err := store.VerifySchema(ctx, pool.Main, exp); err != nil {
				return err
			}
			if err := timers.Ping(ctx); err != nil {
				return fmt.Errorf("redis: %w", err)
			}
			return nil
		},
	}, nil
}

func telemetryRequired() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("TELEMETRY_MODE")), "required")
}

func roomDurableMissing(cfg roomRuntimeConfig) []string {
	var missing []string
	require := func(name, v string) {
		if strings.TrimSpace(v) == "" {
			missing = append(missing, name)
		}
	}
	require("DATABASE_URL", cfg.DatabaseURL)
	require("REDIS_URL", cfg.RedisURL)
	if roomServesPlayerCommands(cfg.WorkerRole) {
		require("ROOM_INTERNAL_PRINCIPAL_CURRENT_KEY_ID", cfg.InternalPrincipalCurrentKeyID)
		require("ROOM_INTERNAL_PRINCIPAL_HMAC_KEY_CURRENT", cfg.InternalPrincipalCurrentHMACKey)
		require("ROOM_INTERNAL_PRINCIPAL_KEY_VERSION", cfg.InternalPrincipalKeyVersion)
		if (cfg.InternalPrincipalPreviousKeyID == "") != (cfg.InternalPrincipalPreviousHMACKey == "") {
			missing = append(missing, "ROOM_INTERNAL_PRINCIPAL_PREVIOUS_KEY_ID+ROOM_INTERNAL_PRINCIPAL_HMAC_KEY_PREVIOUS")
		}
	}
	if cfg.WorkerRole != "room-timer" {
		require("GAME_INTEGRITY_URL", cfg.GameIntegrityURL)
		require("GAME_INTEGRITY_CREDENTIAL", cfg.GameIntegrityCred)
	}
	if cfg.WorkerRole != "room-runtime" && cfg.WorkerRole != "room-integrity-reconciler" {
		require("ROOM_TIMER_SERVICE_CREDENTIAL", cfg.TimerCredential)
	}
	// Only stable request-serving processes authenticate the original Gateway.
	if cfg.WorkerRole != "room-runtime" && cfg.WorkerRole != "room-timer" && cfg.WorkerRole != "room-integrity-reconciler" {
		require("SERVICE_CREDENTIAL", cfg.ServiceCredential)
	}
	if cfg.WorkerRole != "room-runtime" && cfg.WorkerRole != "room-timer" && cfg.WorkerRole != "room-integrity-reconciler" {
		require("ROOM_SPECTATOR_RECOVERY_SERVICE_CREDENTIAL", cfg.SpectatorRecoveryCredential)
	}
	// Timer worker does not serve analytics-backfill or public-list; skip least-privilege secrets.
	if cfg.WorkerRole != "room-timer" && cfg.WorkerRole != "room-runtime" && cfg.WorkerRole != "room-integrity-reconciler" {
		require("ROOM_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL", cfg.AnalyticsBackfillCredential)
		if !app.AnalyticsBackfillCursorSecretConfigured() {
			missing = append(missing, "ROOM_ANALYTICS_BACKFILL_CURSOR_SECRET")
		}
		if !app.PublicListCursorSecretConfigured() {
			missing = append(missing, "ROOM_PUBLIC_LIST_CURSOR_SECRET")
		}
	}
	return missing
}

func roomServesPlayerCommands(workerRole string) bool {
	switch strings.TrimSpace(workerRole) {
	case "room-timer", "room-integrity-reconciler", "room-player-stream-compactor", "room-runtime-controller":
		return false
	default:
		return true
	}
}

func wireRoomCapabilityRuntime(cfg roomRuntimeConfig, clock app.Clock, publisher *app.MultiDestinationPublisher) (roomRuntime, error) {
	var integrity app.GameIntegrity = app.ClosedGameIntegrity{}
	var deals app.DealSource = app.ClosedDealSource{}
	if cfg.GameIntegrityURL != "" {
		integrityClient := app.NewIntegrityHTTPClient(cfg.GameIntegrityHTTPTimeout)
		integrity = app.NewHTTPGameIntegrity(cfg.GameIntegrityURL, cfg.GameIntegrityCred, integrityClient)
		deals = app.NewHTTPDealSource(cfg.GameIntegrityURL, cfg.GameIntegrityCred, integrityClient)
	}

	var sessionsV app.SessionValidator = app.ClosedSessionValidator{}
	if cfg.IdentityURL != "" {
		sessionsV = app.NewHTTPSessionValidator(cfg.IdentityURL, cfg.IdentityCred, nil)
	}

	var auditSink app.AuditSink = app.NewStderrJSONLAuditSink()
	if cfg.AuditLogPath != "" {
		sink, err := app.OpenJSONLAuditSink(cfg.AuditLogPath)
		if err != nil {
			return roomRuntime{}, err
		}
		auditSink = sink
	}

	deps := app.ServiceDeps{
		Sessions:  app.NewMemorySessionRepository(),
		Integrity: integrity,
		Publisher: publisher,
		Audit:     auditSink,
		Deals:     deals,
		Clock:     clock,
		SessionsV: sessionsV,
	}

	ready := true
	notReadyReason := ""
	if missing := roomCapabilityMissing(cfg); len(missing) > 0 {
		ready = false
		notReadyReason = "capability_dependencies_missing: " + strings.Join(missing, ",")
	}

	svc := app.NewService(deps)
	// Capability: bounded in-memory adapter (no durable outbox); fail-closed without credential.
	svc.SetAnalyticsBackfillReader(app.NewMemoryAnalyticsBackfillStore())
	svc.SetPublicListReader(deps.Sessions.(app.PublicRoomLister))
	return roomRuntime{
		Service:        svc,
		Deps:           deps,
		Ready:          ready,
		NotReadyReason: notReadyReason,
		Mode:           "capability",
	}, nil
}

func roomCapabilityMissing(cfg roomRuntimeConfig) []string {
	var missing []string
	require := func(name, v string) {
		if strings.TrimSpace(v) == "" {
			missing = append(missing, name)
		}
	}
	require("SERVICE_CREDENTIAL", cfg.ServiceCredential)
	require("ROOM_TIMER_SERVICE_CREDENTIAL", cfg.TimerCredential)
	require("ROOM_SPECTATOR_RECOVERY_SERVICE_CREDENTIAL", cfg.SpectatorRecoveryCredential)
	require("ROOM_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL", cfg.AnalyticsBackfillCredential)
	require("IDENTITY_URL", cfg.IdentityURL)
	require("IDENTITY_CREDENTIAL", cfg.IdentityCred)
	require("GAME_INTEGRITY_URL", cfg.GameIntegrityURL)
	require("GAME_INTEGRITY_CREDENTIAL", cfg.GameIntegrityCred)
	require("GATEWAY_URL", cfg.GatewayURL)
	require("GATEWAY_CREDENTIAL", cfg.GatewayCred)
	require("SPECTATOR_VIEW_URL", cfg.SpectatorURL)
	require("SPECTATOR_CREDENTIAL", cfg.SpectatorCred)
	require("RANKING_URL", cfg.RankingURL)
	require("RANKING_CREDENTIAL", cfg.RankingCred)
	require("ANALYTICS_URL", cfg.AnalyticsURL)
	require("ANALYTICS_CREDENTIAL", cfg.AnalyticsCred)
	require("TOURNAMENT_URL", cfg.TournamentURL)
	require("TOURNAMENT_CREDENTIAL", cfg.TournamentCred)
	return missing
}

func firstEnv(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func boundedIntegrityHTTPTimeout(timeout time.Duration) time.Duration {
	if timeout <= 0 {
		return app.DefaultIntegrityHTTPTimeout
	}
	if timeout < 250*time.Millisecond {
		return 250 * time.Millisecond
	}
	// ADR-0019 intentionally holds one Room row lock across this call. Keep the
	// adapter below the five-second external backend deadline even if misconfigured.
	if timeout > 4*time.Second {
		return 4 * time.Second
	}
	return timeout
}

func firstNonEmptyEnv(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func envTruthy(key string) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}
