package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync/atomic"

	"unoarena/services/tournament-orchestration/store"
)

type tournamentRuntime struct {
	svc          *Service
	pool         *store.Pool
	store        *store.TournamentStore
	redis        *store.RedisBracketStore
	rdbClose     func() error
	mode         string // durable | capability | misconfigured
	ready        bool
	readyReason  string
	schemaExp    store.SchemaExpectation
	durableReady func(context.Context) error
	kafka        *matchCompletedKafkaLifecycle
	workerRole   string
}

type matchCompletedKafkaLifecycle struct {
	consumer   *MatchCompletedKafkaConsumer
	client     *franzMatchCompletedClient
	cancel     context.CancelFunc
	done       chan struct{}
	healthy    atomic.Bool
	stoppedErr atomic.Value // error
}

func (l *matchCompletedKafkaLifecycle) start(parent context.Context) {
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
		// Prefer Run exiting only on cancellation; any unexpected exit fails ready.
		if err != nil {
			l.healthy.Store(false)
			l.stoppedErr.Store(err)
			log.Printf(`{"level":"error","service":"tournament-orchestration","event":"kafka_consumer_stopped","err":%q}`, sanitizeLogErr(err))
			return
		}
		l.healthy.Store(false)
		l.stoppedErr.Store(fmt.Errorf("kafka consumer exited unexpectedly"))
		log.Printf(`{"level":"error","service":"tournament-orchestration","event":"kafka_consumer_stopped","err":"exited unexpectedly"}`)
	}()
}

func (l *matchCompletedKafkaLifecycle) stop() {
	if l == nil {
		return
	}
	if l.cancel != nil {
		l.cancel()
	}
	if l.done != nil {
		<-l.done
	}
	if l.client != nil {
		_ = l.client.Close()
	}
}

func (l *matchCompletedKafkaLifecycle) Healthy() bool {
	if l == nil {
		return true
	}
	return l.healthy.Load()
}

func sanitizeLogErr(err error) string {
	if err == nil {
		return ""
	}
	return sanitizeDLQErrorSummary(err.Error())
}

func envTruthy(name string) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(name)))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

func isNonProd(env string) bool {
	switch strings.ToLower(strings.TrimSpace(env)) {
	case "production", "staging", "prod":
		return false
	default:
		return true
	}
}

func wireTournamentRuntime() (tournamentRuntime, error) {
	cred := strings.TrimSpace(os.Getenv("TOURNAMENT_INTERNAL_CREDENTIAL"))
	if cred == "" {
		cred = strings.TrimSpace(os.Getenv("SERVICE_CREDENTIAL"))
	}
	dbURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	deploymentEnv := strings.TrimSpace(os.Getenv("DEPLOYMENT_ENV"))
	if deploymentEnv == "" {
		deploymentEnv = "development"
	}
	capability := envTruthy("TOURNAMENT_CAPABILITY_MODE")
	roomURL := strings.TrimSpace(os.Getenv("ROOM_GAMEPLAY_URL"))
	roomCred := firstNonEmpty(
		strings.TrimSpace(os.Getenv("ROOM_SERVICE_CREDENTIAL")),
		strings.TrimSpace(os.Getenv("TOURNAMENT_ROOM_CREDENTIAL")),
		cred,
	)
	workerRole := strings.TrimSpace(os.Getenv("WORKER_ROLE"))
	if workerRole == workerRoleTournamentProvisioning {
		return wireProvisioningWorkerRuntime(dbURL, roomURL, roomCred, cred)
	}
	if workerRole == workerRoleTournamentSeeding {
		return wireSeedingWorkerRuntime(dbURL)
	}
	if workerRole == workerRoleTournamentCompletion {
		return wireCompletionWorkerRuntime(dbURL)
	}
	if workerRole != "" {
		return tournamentRuntime{}, fmt.Errorf("WORKER_ROLE=%s unsupported", workerRole)
	}

	if dbURL != "" && !capability {
		missing := make([]string, 0, 7)
		if roomURL == "" {
			missing = append(missing, "ROOM_GAMEPLAY_URL")
		}
		if cred == "" {
			missing = append(missing, "TOURNAMENT_INTERNAL_CREDENTIAL")
		}
		if strings.TrimSpace(os.Getenv("TOURNAMENT_BRACKET_CURSOR_SECRET")) == "" {
			missing = append(missing, "TOURNAMENT_BRACKET_CURSOR_SECRET")
		}
		if strings.TrimSpace(os.Getenv("TOURNAMENT_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL")) == "" {
			missing = append(missing, "TOURNAMENT_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL")
		}
		if !AnalyticsBackfillCursorSecretConfigured() {
			missing = append(missing, "TOURNAMENT_ANALYTICS_BACKFILL_CURSOR_SECRET")
		}
		redisURL := strings.TrimSpace(os.Getenv("REDIS_URL"))
		if redisURL == "" {
			missing = append(missing, "REDIS_URL")
		}
		// Kafka is required in durable mode; config is parsed only here so capability stays offline.
		kafkaCfg, kafkaEnabled, err := LoadMatchCompletedKafkaConfigFromEnv()
		if err != nil {
			return tournamentRuntime{}, fmt.Errorf("kafka config: %w", err)
		}
		if !kafkaEnabled {
			missing = append(missing, "kafka")
		}
		if len(missing) > 0 {
			return tournamentRuntime{
				svc: NewService(ServiceDeps{
					Repo:      NewMemoryTournamentRepository(), // never used while not ready
					Rooms:     NoopRoomProvisioner{},
					Publisher: NoopPublisher{},
					Audit:     NewMemoryAudit(),
				}),
				mode:        "durable",
				ready:       false,
				readyReason: "durable_dependencies_missing: " + strings.Join(missing, ", "),
			}, nil
		}
		pool, err := store.NewPool(context.Background(), dbURL)
		if err != nil {
			return tournamentRuntime{}, fmt.Errorf("database pool: %w", err)
		}
		rdb, err := store.NewRedisFromURL(redisURL)
		if err != nil {
			pool.Close()
			return tournamentRuntime{}, fmt.Errorf("redis client: %w", err)
		}
		keyPrefix := strings.TrimSpace(os.Getenv("TOURNAMENT_REDIS_KEY_PREFIX"))
		br := store.NewRedisBracketStore(rdb, keyPrefix)
		if err := br.LoadScripts(context.Background()); err != nil {
			_ = rdb.Close()
			pool.Close()
			return tournamentRuntime{}, fmt.Errorf("redis scripts: %w", err)
		}
		if err := br.Ping(context.Background()); err != nil {
			_ = rdb.Close()
			pool.Close()
			return tournamentRuntime{}, fmt.Errorf("redis ping: %w", err)
		}
		ts := store.NewTournamentStore(pool.Pool)
		repo := &durableRepo{store: ts}
		refresher := newRedisBracketRefresher(br, ts)
		pages := &store.CompositeBracketPageLoader{Redis: br, PG: ts}
		svc := NewService(ServiceDeps{
			Repo:              repo,
			RoundMatches:      &durableRoundMatchRepo{store: ts},
			Registrations:     &durableRegistrationRepo{store: ts},
			Seeding:           &durableSeedingRepo{store: ts},
			Provisioning:      &durableProvisioningRepo{store: ts},
			CompleteRounds:    &durableCompleteRoundRepo{store: ts},
			Lifecycle:         &durableLifecycleRepo{store: ts},
			QuarantineResults: &durableQuarantineResultRepo{store: ts},
			Rooms:             NewHTTPRoomProvisioner(roomURL, roomCred, nil),
			Publisher:         NoopPublisher{}, // CDC publishes; never DrainOutbox in durable mode
			Audit:             NewMemoryAudit(),
			Clock:             systemClock{},
			IDs:               randomIDs{},
			BracketPages:      pages,
			BracketRefresh:    refresher,
			Standings:         ts,
			ReadAuth:          ts,
			Assignments:       ts,
		})
		svc.SetAnalyticsBackfillReader(newDurableAnalyticsBackfillStore(store.NewAnalyticsBackfillStore(pool.Pool)))
		exp := store.DefaultSchemaExpectation()
		life, err := newMatchCompletedKafkaLifecycle(kafkaCfg, svc, readinessStoreAdapter{store: ts, svc: svc}, storeQuarantineAdapter{store: ts})
		if err != nil {
			_ = rdb.Close()
			pool.Close()
			return tournamentRuntime{}, err
		}
		rt := tournamentRuntime{
			svc:       svc,
			pool:      pool,
			store:     ts,
			redis:     br,
			rdbClose:  rdb.Close,
			mode:      "durable",
			ready:     true,
			schemaExp: exp,
			kafka:     life,
			durableReady: func(ctx context.Context) error {
				if !AnalyticsBackfillCursorSecretConfigured() {
					return fmt.Errorf("%w", ErrAnalyticsBackfillCursorSecretRequired)
				}
				if err := store.VerifySchema(ctx, pool.Pool, exp); err != nil {
					return err
				}
				if err := br.Ping(ctx); err != nil {
					return err
				}
				if !life.Healthy() {
					return fmt.Errorf("kafka_consumer_stopped")
				}
				return nil
			},
		}
		return rt, nil
	}

	if !capability || !isNonProd(deploymentEnv) {
		return tournamentRuntime{
			mode:        "misconfigured",
			ready:       false,
			readyReason: "database_unconfigured",
			svc: NewService(ServiceDeps{
				Repo:      NewMemoryTournamentRepository(),
				Rooms:     NoopRoomProvisioner{},
				Publisher: NoopPublisher{},
				Audit:     NewMemoryAudit(),
			}),
		}, nil
	}

	// Capability memory path (offline / checkpoint). Kafka stays offline even if brokers are set.
	repo := NewMemoryTournamentRepository()
	var rooms RoomProvisioner = NewFakeRoomProvisioner()
	if roomURL != "" {
		rooms = NewHTTPRoomProvisioner(roomURL, roomCred, nil)
	}
	analyticsCred := strings.TrimSpace(os.Getenv("TOURNAMENT_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL"))
	svc := NewService(ServiceDeps{
		Repo:      repo,
		Rooms:     rooms,
		Publisher: NoopPublisher{},
		Audit:     NewMemoryAudit(),
		Clock:     systemClock{},
		IDs:       randomIDs{},
	})
	svc.SetAnalyticsBackfillReader(NewMemoryAnalyticsBackfillStore())
	ready := cred != "" && analyticsCred != ""
	readyReason := ""
	if cred == "" {
		readyReason = "internal_credential_unconfigured"
	} else if analyticsCred == "" {
		readyReason = "analytics_backfill_credential_unconfigured"
	}
	return tournamentRuntime{
		svc:         svc,
		mode:        "capability",
		ready:       ready,
		readyReason: readyReason,
	}, nil
}

// wireProvisioningWorkerRuntime wires durable Postgres + Room provisioner +
// Provisioning differential port + Redis post-commit bracket refresh.
// Kafka is intentionally bypassed: the provisioning worker does not consume MatchCompleted.
func wireProvisioningWorkerRuntime(dbURL, roomURL, roomCred, internalCred string) (tournamentRuntime, error) {
	missing := make([]string, 0, 4)
	if dbURL == "" {
		missing = append(missing, "DATABASE_URL")
	}
	if roomURL == "" {
		missing = append(missing, "ROOM_GAMEPLAY_URL")
	}
	if internalCred == "" && roomCred == "" {
		missing = append(missing, "TOURNAMENT_INTERNAL_CREDENTIAL")
	}
	redisURL := strings.TrimSpace(os.Getenv("REDIS_URL"))
	if redisURL == "" {
		missing = append(missing, "REDIS_URL")
	}
	if len(missing) > 0 {
		return tournamentRuntime{}, fmt.Errorf("WORKER_ROLE=%s requires %s", workerRoleTournamentProvisioning, strings.Join(missing, ", "))
	}
	if roomCred == "" {
		roomCred = internalCred
	}
	pool, err := store.NewPool(context.Background(), dbURL)
	if err != nil {
		return tournamentRuntime{}, fmt.Errorf("database pool: %w", err)
	}
	rdb, err := store.NewRedisFromURL(redisURL)
	if err != nil {
		pool.Close()
		return tournamentRuntime{}, fmt.Errorf("redis client: %w", err)
	}
	br := store.NewRedisBracketStore(rdb, strings.TrimSpace(os.Getenv("TOURNAMENT_REDIS_KEY_PREFIX")))
	if err := br.LoadScripts(context.Background()); err != nil {
		_ = rdb.Close()
		pool.Close()
		return tournamentRuntime{}, fmt.Errorf("redis scripts: %w", err)
	}
	if err := br.Ping(context.Background()); err != nil {
		_ = rdb.Close()
		pool.Close()
		return tournamentRuntime{}, fmt.Errorf("redis ping: %w", err)
	}
	ts := store.NewTournamentStore(pool.Pool)
	repo := &durableRepo{store: ts}
	refresher := newRedisBracketRefresher(br, ts)
	svc := NewService(ServiceDeps{
		Repo:           repo,
		Provisioning:   &durableProvisioningRepo{store: ts},
		Rooms:          NewHTTPRoomProvisioner(roomURL, roomCred, nil),
		Publisher:      NoopPublisher{},
		Audit:          NewMemoryAudit(),
		Clock:          systemClock{},
		IDs:            randomIDs{},
		BracketRefresh: refresher,
	})
	exp := store.DefaultSchemaExpectation()
	return tournamentRuntime{
		svc:        svc,
		pool:       pool,
		store:      ts,
		redis:      br,
		rdbClose:   rdb.Close,
		mode:       "durable",
		ready:      true,
		schemaExp:  exp,
		workerRole: workerRoleTournamentProvisioning,
		durableReady: func(ctx context.Context) error {
			if err := store.VerifySchema(ctx, pool.Pool, exp); err != nil {
				return err
			}
			return br.Ping(ctx)
		},
	}, nil
}

// wireSeedingWorkerRuntime wires durable Postgres + Redis for post-finalize bracket refresh.
func wireSeedingWorkerRuntime(dbURL string) (tournamentRuntime, error) {
	missing := make([]string, 0, 2)
	if dbURL == "" {
		missing = append(missing, "DATABASE_URL")
	}
	redisURL := strings.TrimSpace(os.Getenv("REDIS_URL"))
	if redisURL == "" {
		missing = append(missing, "REDIS_URL")
	}
	if len(missing) > 0 {
		return tournamentRuntime{}, fmt.Errorf("WORKER_ROLE=%s requires %s", workerRoleTournamentSeeding, strings.Join(missing, ", "))
	}
	pool, err := store.NewPool(context.Background(), dbURL)
	if err != nil {
		return tournamentRuntime{}, fmt.Errorf("database pool: %w", err)
	}
	rdb, err := store.NewRedisFromURL(redisURL)
	if err != nil {
		pool.Close()
		return tournamentRuntime{}, fmt.Errorf("redis client: %w", err)
	}
	br := store.NewRedisBracketStore(rdb, strings.TrimSpace(os.Getenv("TOURNAMENT_REDIS_KEY_PREFIX")))
	if err := br.LoadScripts(context.Background()); err != nil {
		_ = rdb.Close()
		pool.Close()
		return tournamentRuntime{}, fmt.Errorf("redis scripts: %w", err)
	}
	if err := br.Ping(context.Background()); err != nil {
		_ = rdb.Close()
		pool.Close()
		return tournamentRuntime{}, fmt.Errorf("redis ping: %w", err)
	}
	ts := store.NewTournamentStore(pool.Pool)
	refresher := newRedisBracketRefresher(br, ts)
	exp := store.DefaultSchemaExpectation()
	return tournamentRuntime{
		svc: NewService(ServiceDeps{
			Repo:           &durableRepo{store: ts},
			Publisher:      NoopPublisher{},
			Audit:          NewMemoryAudit(),
			Clock:          systemClock{},
			IDs:            randomIDs{},
			BracketRefresh: refresher,
		}),
		pool:       pool,
		store:      ts,
		redis:      br,
		rdbClose:   rdb.Close,
		mode:       "durable",
		ready:      true,
		schemaExp:  exp,
		workerRole: workerRoleTournamentSeeding,
		durableReady: func(ctx context.Context) error {
			if err := store.VerifySchema(ctx, pool.Pool, exp); err != nil {
				return err
			}
			return br.Ping(ctx)
		},
	}, nil
}

// wireCompletionWorkerRuntime wires durable Postgres + CompleteRound + Redis refresh.
// No HTTP service, Room URL, cursor secret, or Kafka.
func wireCompletionWorkerRuntime(dbURL string) (tournamentRuntime, error) {
	missing := make([]string, 0, 2)
	if dbURL == "" {
		missing = append(missing, "DATABASE_URL")
	}
	redisURL := strings.TrimSpace(os.Getenv("REDIS_URL"))
	if redisURL == "" {
		missing = append(missing, "REDIS_URL")
	}
	if len(missing) > 0 {
		return tournamentRuntime{}, fmt.Errorf("WORKER_ROLE=%s requires %s", workerRoleTournamentCompletion, strings.Join(missing, ", "))
	}
	pool, err := store.NewPool(context.Background(), dbURL)
	if err != nil {
		return tournamentRuntime{}, fmt.Errorf("database pool: %w", err)
	}
	rdb, err := store.NewRedisFromURL(redisURL)
	if err != nil {
		pool.Close()
		return tournamentRuntime{}, fmt.Errorf("redis client: %w", err)
	}
	br := store.NewRedisBracketStore(rdb, strings.TrimSpace(os.Getenv("TOURNAMENT_REDIS_KEY_PREFIX")))
	if err := br.LoadScripts(context.Background()); err != nil {
		_ = rdb.Close()
		pool.Close()
		return tournamentRuntime{}, fmt.Errorf("redis scripts: %w", err)
	}
	if err := br.Ping(context.Background()); err != nil {
		_ = rdb.Close()
		pool.Close()
		return tournamentRuntime{}, fmt.Errorf("redis ping: %w", err)
	}
	ts := store.NewTournamentStore(pool.Pool)
	repo := &durableRepo{store: ts}
	complete := &durableCompleteRoundRepo{store: ts}
	refresher := newRedisBracketRefresher(br, ts)
	exp := store.DefaultSchemaExpectation()
	return tournamentRuntime{
		svc: NewService(ServiceDeps{
			Repo:           repo,
			CompleteRounds: complete,
			Publisher:      NoopPublisher{},
			Audit:          NewMemoryAudit(),
			Clock:          systemClock{},
			IDs:            randomIDs{},
			BracketRefresh: refresher,
		}),
		pool:       pool,
		store:      ts,
		redis:      br,
		rdbClose:   rdb.Close,
		mode:       "durable",
		ready:      true,
		schemaExp:  exp,
		workerRole: workerRoleTournamentCompletion,
		durableReady: func(ctx context.Context) error {
			if err := store.VerifySchema(ctx, pool.Pool, exp); err != nil {
				return err
			}
			return br.Ping(ctx)
		},
	}, nil
}

func newMatchCompletedKafkaLifecycle(cfg MatchCompletedKafkaConfig, svc *Service, readiness RoomRuntimeReadyIngester, quarantine AggregateQuarantineStore) (*matchCompletedKafkaLifecycle, error) {
	client, err := newFranzMatchCompletedClient(cfg)
	if err != nil {
		if client != nil {
			_ = client.Close()
		}
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	consumer := &MatchCompletedKafkaConsumer{
		source:     client,
		dlq:        client,
		handler:    serviceMatchIngester{svc: svc},
		readiness:  readiness,
		quarantine: quarantine,
		cfg:        cfg,
		clock:      systemClock{},
	}
	return &matchCompletedKafkaLifecycle{consumer: consumer, client: client}, nil
}

type readinessStoreAdapter struct {
	store *store.TournamentStore
	svc   *Service
}

func (a readinessStoreAdapter) IngestRoomRuntimeReady(ctx context.Context, evt RoomRuntimeReadyEvent) (bool, error) {
	applied, err := a.store.ApplyRoomRuntimeReady(ctx, store.RoomRuntimeReady{
		EventID:      evt.EventID,
		RoomID:       evt.RoomID,
		TournamentID: evt.TournamentID,
		RoundNumber:  evt.RoundNumber,
		SlotID:       evt.SlotID,
		Generation:   evt.Generation,
		OccurredAt:   evt.OccurredAt,
	})
	if errors.Is(err, store.ErrRuntimeReadinessMismatch) {
		return false, newTerminalKafkaError(KafkaFailurePayloadInvalid, err)
	}
	if err == nil && applied && a.svc != nil {
		a.svc.refreshBracketBestEffort(context.Background(), a.svc.scopeForSlot(
			ctx, evt.TournamentID, evt.RoundNumber, evt.SlotID,
		))
	}
	return applied, err
}

// storeQuarantineAdapter adapts store.TournamentStore to AggregateQuarantineStore.
type storeQuarantineAdapter struct {
	store *store.TournamentStore
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
