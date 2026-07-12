package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"unoarena/services/room-gameplay/app"
	"unoarena/services/room-gameplay/store"
)

type roomRuntimeConfig struct {
	ServiceName                 string
	ServiceCredential           string
	TimerCredential             string
	SpectatorRecoveryCredential string
	AnalyticsBackfillCredential string
	GatewayURL                  string
	GatewayCred                 string
	IdentityURL                 string
	IdentityCred                string
	GameIntegrityURL            string
	GameIntegrityCred           string
	DatabaseURL                 string
	RedisURL                    string
	AuditLogPath                string
	AllowFakes                  bool
	CapabilityMode              bool
	DeploymentEnv               string
	WorkerRole                  string
	RoomGameplayURL             string
	SpectatorURL                string
	SpectatorCred               string
	RankingURL                  string
	RankingCred                 string
	AnalyticsURL                string
	AnalyticsCred               string
	TournamentURL               string
	TournamentCred              string
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
		GameIntegrityURL: strings.TrimSpace(os.Getenv("GAME_INTEGRITY_URL")),
		GameIntegrityCred: firstNonEmptyEnv(
			os.Getenv("GAME_INTEGRITY_INTERNAL_CREDENTIAL"),
			os.Getenv("ROOM_GAME_INTEGRITY_CREDENTIAL"),
			cred,
		),
		DatabaseURL:    strings.TrimSpace(os.Getenv("DATABASE_URL")),
		RedisURL:       strings.TrimSpace(os.Getenv("REDIS_URL")),
		AuditLogPath:   strings.TrimSpace(os.Getenv("ROOM_AUDIT_LOG_PATH")),
		AllowFakes:     envTruthy("ROOM_ALLOW_FAKES") || envTruthy("ALLOW_FAKES"),
		CapabilityMode: envTruthy("ROOM_CAPABILITY_MODE"),
		DeploymentEnv:  depEnv,
		WorkerRole:     strings.TrimSpace(os.Getenv("WORKER_ROLE")),
		RoomGameplayURL: firstNonEmptyEnv(
			os.Getenv("ROOM_GAMEPLAY_URL"),
			"http://127.0.0.1:8080",
		),
		SpectatorURL:   strings.TrimSpace(os.Getenv("SPECTATOR_VIEW_URL")),
		SpectatorCred:  firstEnv("SPECTATOR_VIEW_SERVICE_CREDENTIAL", gatewayCred),
		RankingURL:     strings.TrimSpace(os.Getenv("RANKING_URL")),
		RankingCred:    firstEnv("RANKING_SERVICE_CREDENTIAL", gatewayCred),
		AnalyticsURL:   strings.TrimSpace(os.Getenv("ANALYTICS_URL")),
		AnalyticsCred:  firstEnv("ANALYTICS_SERVICE_CREDENTIAL", gatewayCred),
		TournamentURL:  strings.TrimSpace(os.Getenv("TOURNAMENT_URL")),
		TournamentCred: firstEnv("TOURNAMENT_SERVICE_CREDENTIAL", gatewayCred),
	}
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
	// Best-effort rebuild of non-authoritative Redis index from Postgres deadlines.
	_ = timers.RebuildFromPostgres(context.Background(), pool.Main)
	var auditSink app.AuditSink = app.NewStderrJSONLAuditSink()
	if cfg.AuditLogPath != "" {
		sink, err := app.OpenJSONLAuditSink(cfg.AuditLogPath)
		if err != nil {
			pool.Close()
			_ = rdb.Close()
			return roomRuntime{}, err
		}
		auditSink = sink
	}

	integrity := app.NewHTTPGameIntegrity(cfg.GameIntegrityURL, cfg.GameIntegrityCred, nil)
	if auditCred := strings.TrimSpace(os.Getenv("GAME_INTEGRITY_AUDIT_CREDENTIAL")); auditCred != "" {
		integrity.AuditCredential = auditCred
	}

	deps := app.ServiceDeps{
		Sessions:  sessions,
		Commands:  sessions,
		Integrity: integrity,
		Publisher: app.NewFakeEventPublisher(), // durable: CDC owns delivery; no MultiDestinationPublisher
		Audit:     auditSink,
		Deals:     app.NewHTTPDealSource(cfg.GameIntegrityURL, cfg.GameIntegrityCred, nil),
		Clock:     clock,
		SessionsV: app.NewHTTPSessionValidator(cfg.IdentityURL, cfg.IdentityCred, nil),
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
			if !app.AnalyticsBackfillCursorSecretConfigured() {
				return fmt.Errorf("%w", app.ErrAnalyticsBackfillCursorSecretRequired)
			}
			if !app.PublicListCursorSecretConfigured() {
				return fmt.Errorf("%w", app.ErrPublicListCursorSecretRequired)
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

func roomDurableMissing(cfg roomRuntimeConfig) []string {
	var missing []string
	require := func(name, v string) {
		if strings.TrimSpace(v) == "" {
			missing = append(missing, name)
		}
	}
	require("DATABASE_URL", cfg.DatabaseURL)
	require("REDIS_URL", cfg.RedisURL)
	require("IDENTITY_URL", cfg.IdentityURL)
	require("IDENTITY_CREDENTIAL", cfg.IdentityCred)
	require("GAME_INTEGRITY_URL", cfg.GameIntegrityURL)
	require("GAME_INTEGRITY_CREDENTIAL", cfg.GameIntegrityCred)
	require("ROOM_TIMER_SERVICE_CREDENTIAL", cfg.TimerCredential)
	require("ROOM_SPECTATOR_RECOVERY_SERVICE_CREDENTIAL", cfg.SpectatorRecoveryCredential)
	require("SERVICE_CREDENTIAL", cfg.ServiceCredential)
	// Timer worker does not serve analytics-backfill or public-list; skip least-privilege secrets.
	if cfg.WorkerRole != "room-timer" {
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

func wireRoomCapabilityRuntime(cfg roomRuntimeConfig, clock app.Clock, publisher *app.MultiDestinationPublisher) (roomRuntime, error) {
	var integrity app.GameIntegrity = app.ClosedGameIntegrity{}
	var deals app.DealSource = app.ClosedDealSource{}
	if cfg.GameIntegrityURL != "" {
		integrity = app.NewHTTPGameIntegrity(cfg.GameIntegrityURL, cfg.GameIntegrityCred, nil)
		deals = app.NewHTTPDealSource(cfg.GameIntegrityURL, cfg.GameIntegrityCred, nil)
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
