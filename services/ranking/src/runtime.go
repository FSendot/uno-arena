package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/exaring/otelpgx"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"unoarena/services/ranking/store"
)

type rankingRuntime struct {
	app                         RatingApplication
	pool                        *store.Pool
	store                       *store.RankingStore
	redis                       *store.RedisLeaderboardStore
	rdbClose                    func() error
	mode                        string // durable | capability | misconfigured
	ready                       bool
	readyReason                 string
	schemaExp                   store.SchemaExpectation
	durableReady                func(context.Context) error
	credential                  string
	analyticsBackfillCredential string
	analyticsBackfill           AnalyticsBackfillReader
	kafka                       *gameCompletedKafkaLifecycle
}

type gameCompletedKafkaLifecycle struct {
	consumer   *GameCompletedKafkaConsumer
	client     *franzRankingClient
	cancel     context.CancelFunc
	done       chan struct{}
	healthy    atomic.Bool
	stoppedErr atomic.Value // error
	stopOnce   sync.Once
}

func (l *gameCompletedKafkaLifecycle) start(parent context.Context) {
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
			processLogger().ErrorContext(ctx, "Kafka consumer stopped", "event", "kafka_consumer_stopped", "error", sanitizeLogErr(err))
			return
		}
		l.healthy.Store(false)
		l.stoppedErr.Store(fmt.Errorf("kafka consumer exited unexpectedly"))
		processLogger().ErrorContext(ctx, "Kafka consumer stopped", "event", "kafka_consumer_stopped", "error", "exited unexpectedly")
	}()
}

func (l *gameCompletedKafkaLifecycle) stop() {
	if l == nil {
		return
	}
	l.stopOnce.Do(func() {
		if l.cancel != nil {
			l.cancel()
		}
		if l.done != nil {
			<-l.done
		}
		if l.client != nil {
			_ = l.client.Close()
		}
	})
}

func (l *gameCompletedKafkaLifecycle) Healthy() bool {
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

func wireRankingRuntime() (rankingRuntime, error) {
	cred := strings.TrimSpace(os.Getenv("RANKING_INTERNAL_CREDENTIAL"))
	if cred == "" {
		cred = strings.TrimSpace(os.Getenv("SERVICE_CREDENTIAL"))
	}
	dbURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	deploymentEnv := strings.TrimSpace(os.Getenv("DEPLOYMENT_ENV"))
	if deploymentEnv == "" {
		deploymentEnv = "development"
	}
	capability := envTruthy("RANKING_CAPABILITY_MODE")
	workerRole := strings.TrimSpace(os.Getenv("WORKER_ROLE"))
	if workerRole == workerRoleLeaderboardSnapshotter {
		return wireLeaderboardSnapshotterRuntime(dbURL)
	}
	if workerRole != "" {
		return rankingRuntime{}, fmt.Errorf("WORKER_ROLE=%s unsupported", workerRole)
	}

	analyticsCred := strings.TrimSpace(os.Getenv("RANKING_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL"))

	if dbURL != "" && !capability {
		missing := make([]string, 0, 6)
		if cred == "" {
			missing = append(missing, "RANKING_INTERNAL_CREDENTIAL")
		}
		if analyticsCred == "" {
			missing = append(missing, "RANKING_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL")
		}
		if !AnalyticsBackfillCursorSecretConfigured() {
			missing = append(missing, "RANKING_ANALYTICS_BACKFILL_CURSOR_SECRET")
		}
		kafkaCfg, kafkaEnabled, err := LoadRankingKafkaConfigFromEnv()
		if err != nil {
			return rankingRuntime{}, fmt.Errorf("kafka config: %w", err)
		}
		if !kafkaEnabled {
			missing = append(missing, "kafka")
		}
		redisURL := strings.TrimSpace(os.Getenv("REDIS_URL"))
		if redisURL == "" {
			missing = append(missing, "REDIS_URL")
		}
		if !store.LeaderboardCursorSecretConfigured() {
			// Durable/prod/staging refuse the documented non-prod fallback.
			switch strings.ToLower(deploymentEnv) {
			case "production", "staging", "prod":
				missing = append(missing, "RANKING_LEADERBOARD_CURSOR_SECRET")
			default:
				// Local/dev may use documented fallback; still prefer explicit secret in kind.
			}
		}
		if len(missing) > 0 {
			return rankingRuntime{
				app:                         NewMemoryRatingStore(),
				mode:                        "durable",
				ready:                       false,
				readyReason:                 "durable_dependencies_missing: " + strings.Join(missing, ", "),
				credential:                  cred,
				analyticsBackfillCredential: analyticsCred,
			}, nil
		}
		pool, err := store.NewPoolWithTracer(context.Background(), dbURL, otelpgx.NewTracer(
			otelpgx.WithTracerProvider(processTracerProvider()),
			otelpgx.WithMeterProvider(processMeterProvider()),
			otelpgx.WithSpanNameFunc(func(string) string { return "ranking.postgres.query" }),
			otelpgx.WithDisableQuerySpanNamePrefix(),
			otelpgx.WithDisableSQLStatementInAttributes(),
		))
		if err != nil {
			return rankingRuntime{}, fmt.Errorf("database pool: %w", err)
		}
		rdb, err := store.NewRedisFromURL(redisURL)
		if err != nil {
			pool.Close()
			return rankingRuntime{}, fmt.Errorf("redis client: %w", err)
		}
		if err := redisotel.InstrumentTracing(rdb,
			redisotel.WithTracerProvider(processTracerProvider()),
			redisotel.WithDBStatement(false),
			redisotel.WithCallerEnabled(false),
		); err != nil {
			_ = rdb.Close()
			pool.Close()
			return rankingRuntime{}, fmt.Errorf("instrument redis: %w", err)
		}
		lb := store.NewRedisLeaderboardStore(rdb, "")
		if err := lb.LoadScripts(context.Background()); err != nil {
			_ = rdb.Close()
			pool.Close()
			return rankingRuntime{}, fmt.Errorf("redis scripts: %w", err)
		}
		if err := lb.Ping(context.Background()); err != nil {
			_ = rdb.Close()
			pool.Close()
			return rankingRuntime{}, fmt.Errorf("redis ping: %w", err)
		}
		rs := store.NewRankingStoreWithTracer(pool.Pool, processTracerProvider())
		exp := store.DefaultSchemaExpectation()
		app := newDurableApp(rs).withRedis(lb)
		backfill := newDurableAnalyticsBackfillStore(store.NewAnalyticsBackfillStore(pool.Pool))
		rt := rankingRuntime{
			app:                         app,
			pool:                        pool,
			store:                       rs,
			redis:                       lb,
			rdbClose:                    rdb.Close,
			mode:                        "durable",
			ready:                       true,
			schemaExp:                   exp,
			credential:                  cred,
			analyticsBackfillCredential: analyticsCred,
			analyticsBackfill:           backfill,
			durableReady: func(ctx context.Context) error {
				if !AnalyticsBackfillCursorSecretConfigured() {
					return fmt.Errorf("%w", ErrAnalyticsBackfillCursorSecretRequired)
				}
				if err := store.VerifySchema(ctx, pool.Pool, exp); err != nil {
					return err
				}
				return lb.Ping(ctx)
			},
		}
		life, err := newGameCompletedKafkaLifecycle(kafkaCfg, app, storeQuarantineAdapter{store: rs})
		if err != nil {
			_ = rdb.Close()
			pool.Close()
			return rankingRuntime{}, err
		}
		rt.kafka = life
		baseReady := rt.durableReady
		rt.durableReady = func(ctx context.Context) error {
			if baseReady != nil {
				if err := baseReady(ctx); err != nil {
					return err
				}
			}
			if !life.Healthy() {
				return fmt.Errorf("kafka_consumer_stopped")
			}
			return nil
		}
		return rt, nil
	}

	if !capability || !isNonProd(deploymentEnv) {
		return rankingRuntime{
			app:         NewMemoryRatingStore(),
			mode:        "misconfigured",
			ready:       false,
			readyReason: "database_unconfigured",
			credential:  cred,
		}, nil
	}

	mem := NewMemoryRatingStore()
	ready := cred != "" && analyticsCred != ""
	readyReason := ""
	if cred == "" {
		readyReason = "internal_credential_unconfigured"
	} else if analyticsCred == "" {
		readyReason = "analytics_backfill_credential_unconfigured"
	}
	return rankingRuntime{
		app:                         mem,
		mode:                        "capability",
		ready:                       ready,
		readyReason:                 readyReason,
		credential:                  cred,
		analyticsBackfillCredential: analyticsCred,
		analyticsBackfill:           NewMemoryAnalyticsBackfillStore(),
	}, nil
}

// wireLeaderboardSnapshotterRuntime wires durable Postgres only — no Kafka, Redis, or HTTP.
func wireLeaderboardSnapshotterRuntime(dbURL string) (rankingRuntime, error) {
	if strings.TrimSpace(dbURL) == "" {
		return rankingRuntime{}, fmt.Errorf("WORKER_ROLE=%s requires DATABASE_URL", workerRoleLeaderboardSnapshotter)
	}
	pool, err := store.NewPoolWithTracer(context.Background(), dbURL, otelpgx.NewTracer(
		otelpgx.WithTracerProvider(processTracerProvider()),
		otelpgx.WithMeterProvider(processMeterProvider()),
		otelpgx.WithSpanNameFunc(func(string) string { return "ranking.postgres.query" }),
		otelpgx.WithDisableQuerySpanNamePrefix(),
		otelpgx.WithDisableSQLStatementInAttributes(),
	))
	if err != nil {
		return rankingRuntime{}, fmt.Errorf("database pool: %w", err)
	}
	rs := store.NewRankingStoreWithTracer(pool.Pool, processTracerProvider())
	exp := store.DefaultSchemaExpectation()
	return rankingRuntime{
		app:       newDurableApp(rs),
		pool:      pool,
		store:     rs,
		mode:      "durable",
		ready:     true,
		schemaExp: exp,
		durableReady: func(ctx context.Context) error {
			return store.VerifySchema(ctx, pool.Pool, exp)
		},
	}, nil
}

func newGameCompletedKafkaLifecycle(cfg RankingKafkaConfig, app RankingKafkaHandler, quarantine AggregateQuarantineStore) (*gameCompletedKafkaLifecycle, error) {
	client, err := newFranzRankingClient(cfg)
	if err != nil {
		if client != nil {
			_ = client.Close()
		}
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	consumer := &GameCompletedKafkaConsumer{
		source:     client,
		dlq:        client,
		handler:    app,
		quarantine: quarantine,
		cfg:        cfg,
		clock:      systemClock{},
	}
	return &gameCompletedKafkaLifecycle{consumer: consumer, client: client}, nil
}

// storeQuarantineAdapter adapts store.RankingStore to AggregateQuarantineStore.
type storeQuarantineAdapter struct {
	store *store.RankingStore
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
