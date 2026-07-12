package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync/atomic"

	"github.com/redis/go-redis/v9"

	"unoarena/services/spectator-view/store"
)

type spectatorRuntime struct {
	app        SpectatorApplication
	feed       SpectatorLiveFeed
	mode       string // durable | capability | misconfigured
	ready      bool
	reason     string
	cred       string
	rdb        redis.UniversalClient
	projection *store.RedisProjectionStore
	kafka      *spectatorSafeKafkaLifecycle
}

type spectatorSafeKafkaLifecycle struct {
	consumer   *SpectatorSafeKafkaConsumer
	client     *franzSpectatorSafeClient
	cancel     context.CancelFunc
	done       chan struct{}
	healthy    atomic.Bool
	stoppedErr atomic.Value // error
}

func (l *spectatorSafeKafkaLifecycle) start(parent context.Context) {
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
			log.Printf(`{"level":"error","service":"spectator-view","event":"kafka_consumer_stopped","err":%q}`, sanitizeLogErr(err))
			return
		}
		l.healthy.Store(false)
		l.stoppedErr.Store(fmt.Errorf("kafka consumer exited unexpectedly"))
		log.Printf(`{"level":"error","service":"spectator-view","event":"kafka_consumer_stopped","err":"exited unexpectedly"}`)
	}()
}

func (l *spectatorSafeKafkaLifecycle) stop() {
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

func (l *spectatorSafeKafkaLifecycle) Healthy() bool {
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

func wireSpectatorRuntime() (spectatorRuntime, error) {
	cred := strings.TrimSpace(os.Getenv("SPECTATOR_VIEW_INTERNAL_CREDENTIAL"))
	redisURL := strings.TrimSpace(os.Getenv("REDIS_URL"))
	keyPrefix := strings.TrimSpace(os.Getenv("SPECTATOR_REDIS_KEY_PREFIX"))
	deploymentEnv := strings.TrimSpace(os.Getenv("DEPLOYMENT_ENV"))
	if deploymentEnv == "" {
		deploymentEnv = "development"
	}
	capability := envTruthy("SPECTATOR_CAPABILITY_MODE")

	// Durable mode: REDIS_URL set and not explicit capability.
	if redisURL != "" && !capability {
		missing := make([]string, 0, 2)
		if cred == "" {
			missing = append(missing, "SPECTATOR_VIEW_INTERNAL_CREDENTIAL")
		}
		// Kafka is required in durable mode; config is parsed only here so capability stays offline.
		kafkaCfg, kafkaEnabled, err := LoadSpectatorSafeKafkaConfigFromEnv()
		if err != nil {
			return spectatorRuntime{}, fmt.Errorf("kafka config: %w", err)
		}
		if !kafkaEnabled {
			missing = append(missing, "kafka")
		}
		if len(missing) > 0 {
			mem := NewMemoryProjectionStore()
			hub := NewStreamHub()
			return spectatorRuntime{
				app: mem, feed: hub, mode: "durable", ready: false,
				reason: "durable_dependencies_missing: " + strings.Join(missing, ", "),
				cred:   cred,
			}, nil
		}
		rdb, err := store.NewRedisFromURL(redisURL)
		if err != nil {
			return spectatorRuntime{}, fmt.Errorf("redis client: %w", err)
		}
		rs := store.NewRedisProjectionStore(rdb, keyPrefix)
		if n, ok, err := store.ParseStreamMaxLenEnv(os.Getenv("SPECTATOR_REDIS_STREAM_MAXLEN")); err != nil {
			_ = rdb.Close()
			return spectatorRuntime{}, fmt.Errorf("SPECTATOR_REDIS_STREAM_MAXLEN: %w", err)
		} else if ok {
			rs = rs.WithStreamMaxLen(n)
		}
		rs = rs.WithKafkaIdentity(kafkaCfg.Group, kafkaCfg.Topic)

		if err := rs.LoadScripts(context.Background()); err != nil {
			_ = rdb.Close()
			return spectatorRuntime{}, fmt.Errorf("redis scripts: %w", err)
		}
		app := newDurableApp(rs)
		feed := newRedisLiveFeed(rs)
		life, err := newSpectatorSafeKafkaLifecycle(kafkaCfg, app, storeQuarantineAdapter{store: rs})
		if err != nil {
			_ = rdb.Close()
			return spectatorRuntime{}, err
		}
		return spectatorRuntime{
			app: app, feed: feed, mode: "durable", ready: true, cred: cred,
			rdb: rdb, projection: rs, kafka: life,
		}, nil
	}

	// Capability memory only when explicitly opted in on non-production.
	if !capability || !isNonProd(deploymentEnv) {
		reason := "redis_unconfigured"
		if capability && !isNonProd(deploymentEnv) {
			reason = "capability_mode_forbidden_in_production"
		}
		mem := NewMemoryProjectionStore()
		hub := NewStreamHub()
		return spectatorRuntime{
			app: mem, feed: hub, mode: "misconfigured", ready: false, reason: reason, cred: cred,
		}, nil
	}

	mem := NewMemoryProjectionStore()
	hub := NewStreamHub()
	ready := cred != ""
	reason := ""
	if !ready {
		reason = "internal_credential_unconfigured"
	}
	return spectatorRuntime{
		app: mem, feed: hub, mode: "capability", ready: ready, reason: reason, cred: cred,
	}, nil
}

func newSpectatorSafeKafkaLifecycle(cfg SpectatorSafeKafkaConfig, app SpectatorApplication, quarantine AggregateQuarantineStore) (*spectatorSafeKafkaLifecycle, error) {
	client, err := newFranzSpectatorSafeClient(cfg)
	if err != nil {
		if client != nil {
			_ = client.Close()
		}
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	consumer := &SpectatorSafeKafkaConsumer{
		source:     client,
		dlq:        client,
		handler:    app,
		quarantine: quarantine,
		cfg:        cfg,
		clock:      systemClock{},
	}
	return &spectatorSafeKafkaLifecycle{consumer: consumer, client: client}, nil
}

// storeQuarantineAdapter adapts store.RedisProjectionStore to AggregateQuarantineStore.
type storeQuarantineAdapter struct {
	store *store.RedisProjectionStore
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

func (rt spectatorRuntime) durableReady(ctx context.Context) error {
	if rt.mode != "durable" {
		return nil
	}
	if err := rt.app.Ready(ctx); err != nil {
		return err
	}
	if rt.kafka != nil && !rt.kafka.Healthy() {
		return fmt.Errorf("kafka_consumer_stopped")
	}
	return nil
}

func (rt spectatorRuntime) close() {
	if rt.kafka != nil {
		rt.kafka.stop()
	}
	if rt.rdb != nil {
		_ = rt.rdb.Close()
	}
}
