package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"unoarena/services/analytics/store"
)

type analyticsRuntime struct {
	app         AnalyticsApplication
	mode        string // durable | capability | misconfigured
	ready       bool
	readyReason string
	creds       ProducerCredentials
	chStore     *store.AnalyticsStore
	readiness   *durableReadiness
	kafka       *analyticsKafkaLifecycle
}

type durableReadinessStore interface {
	Ready(context.Context) error
	Ping(context.Context) error
}

// durableReadiness performs the immutable schema/generation audit until it
// succeeds once, then keeps steady-state probes cheap with a single Ping.
// Failed bootstrap audits are not cached, so readiness continues to recover
// when ClickHouse bootstrap completes.
type durableReadiness struct {
	store          durableReadinessStore
	schemaVerified atomic.Bool
}

func (r *durableReadiness) check(ctx context.Context) error {
	if r == nil || r.store == nil {
		return fmt.Errorf("durable store unconfigured")
	}
	if !r.schemaVerified.Load() {
		if err := r.store.Ready(ctx); err != nil {
			return err
		}
		r.schemaVerified.Store(true)
		return nil
	}
	return r.store.Ping(ctx)
}

type analyticsKafkaLifecycle struct {
	consumer   *AnalyticsKafkaConsumer
	client     *franzAnalyticsClient
	cancel     context.CancelFunc
	done       chan struct{}
	healthy    atomic.Bool
	stoppedErr atomic.Value // error
}

func (l *analyticsKafkaLifecycle) start(parent context.Context) {
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

func (l *analyticsKafkaLifecycle) stop() {
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

func (l *analyticsKafkaLifecycle) Healthy() bool {
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

func wireAnalyticsRuntime() (analyticsRuntime, error) {
	creds := ProducerCredentials{
		Room:       strings.TrimSpace(os.Getenv("ANALYTICS_ROOM_CREDENTIAL")),
		Ranking:    strings.TrimSpace(os.Getenv("ANALYTICS_RANKING_CREDENTIAL")),
		Tournament: strings.TrimSpace(os.Getenv("ANALYTICS_TOURNAMENT_CREDENTIAL")),
		Ops:        strings.TrimSpace(os.Getenv("ANALYTICS_OPS_CREDENTIAL")),
	}
	chURL := strings.TrimSpace(os.Getenv("CLICKHOUSE_URL"))
	chUser := strings.TrimSpace(os.Getenv("CLICKHOUSE_USER"))
	chPass := strings.TrimSpace(os.Getenv("CLICKHOUSE_PASSWORD"))
	chDB := strings.TrimSpace(os.Getenv("CLICKHOUSE_DB"))
	if chDB == "" {
		chDB = "analytics"
	}
	deploymentEnv := strings.TrimSpace(os.Getenv("DEPLOYMENT_ENV"))
	if deploymentEnv == "" {
		deploymentEnv = "development"
	}
	capability := envTruthy("ANALYTICS_CAPABILITY_MODE")

	credReason := scopedCredentialReadyReason(creds)

	// Capability mode: ignore Kafka env entirely; memory store only when opted in on non-prod.
	if capability {
		if !isNonProd(deploymentEnv) {
			return analyticsRuntime{
				app:         NewMemoryAnalyticsStore(),
				mode:        "misconfigured",
				ready:       false,
				readyReason: "capability_mode_forbidden_in_production",
				creds:       creds,
			}, nil
		}
		mem := NewMemoryAnalyticsStore()
		ready := credReason == ""
		return analyticsRuntime{
			app:         mem,
			mode:        "capability",
			ready:       ready,
			readyReason: credReason,
			creds:       creds,
		}, nil
	}

	// Durable mode: CLICKHOUSE_URL set and not explicit capability.
	if chURL != "" {
		missing := make([]string, 0, 4)
		if credReason != "" {
			missing = append(missing, credReason)
		}
		if chUser == "" || chPass == "" {
			missing = append(missing, "CLICKHOUSE_USER/CLICKHOUSE_PASSWORD")
		}
		kafkaCfg, kafkaEnabled, err := LoadAnalyticsKafkaConfigFromEnv()
		if err != nil {
			return analyticsRuntime{}, fmt.Errorf("kafka config: %w", err)
		}
		if !kafkaEnabled {
			missing = append(missing, "kafka")
		}
		if len(missing) > 0 {
			return analyticsRuntime{
				app:         NewMemoryAnalyticsStore(),
				mode:        "durable",
				ready:       false,
				readyReason: "durable_dependencies_missing: " + strings.Join(missing, ", "),
				creds:       creds,
			}, nil
		}
		client, err := store.NewClient(store.Config{
			URL: chURL, User: chUser, Password: chPass, Database: chDB,
			HTTPClient: tracedHTTPClient(30 * time.Second),
		})
		if err != nil {
			return analyticsRuntime{}, fmt.Errorf("clickhouse client: %w", err)
		}
		chs := store.NewAnalyticsStore(client)
		life, err := newAnalyticsKafkaLifecycle(kafkaCfg, chs)
		if err != nil {
			return analyticsRuntime{}, err
		}
		rt := analyticsRuntime{
			app:       chs,
			mode:      "durable",
			ready:     true,
			creds:     creds,
			chStore:   chs,
			readiness: &durableReadiness{store: chs},
			kafka:     life,
		}
		return rt, nil
	}

	return analyticsRuntime{
		app:         NewMemoryAnalyticsStore(),
		mode:        "misconfigured",
		ready:       false,
		readyReason: "clickhouse_unconfigured",
		creds:       creds,
	}, nil
}

func newAnalyticsKafkaLifecycle(cfg AnalyticsKafkaConfig, app AnalyticsIngester) (*analyticsKafkaLifecycle, error) {
	client, err := newFranzAnalyticsClient(cfg)
	if err != nil {
		if client != nil {
			_ = client.Close()
		}
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	consumer := &AnalyticsKafkaConsumer{
		source:  client,
		dlq:     client,
		handler: app,
		cfg:     cfg,
		clock:   systemClock{},
	}
	return &analyticsKafkaLifecycle{consumer: consumer, client: client}, nil
}

func (rt analyticsRuntime) durableReady(ctx context.Context) error {
	if rt.readiness == nil {
		return fmt.Errorf("durable store unconfigured")
	}
	if err := rt.readiness.check(ctx); err != nil {
		return err
	}
	if rt.kafka != nil && !rt.kafka.Healthy() {
		return fmt.Errorf("kafka_consumer_stopped")
	}
	return nil
}
