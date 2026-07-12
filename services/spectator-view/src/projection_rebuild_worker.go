package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"unoarena/services/spectator-view/store"
)

const workerRoleSpectatorProjectionRebuilder = "spectator-projection-rebuilder"

// projectionRebuildWorkerRuntime is the HTTP-free ADR-0039 rebuilder process.
type projectionRebuildWorkerRuntime struct {
	rdb      redis.UniversalClient
	client   *franzSpectatorSafeClient
	consumer *ProjectionRebuildKafkaConsumer
	mode     string
}

func workerRoleFromEnv() string {
	return strings.TrimSpace(os.Getenv("WORKER_ROLE"))
}

// wireProjectionRebuildWorker fails closed when Redis, Kafka, Room URL, or scoped
// recovery credential is missing. Does not start the live spectator-safe consumer.
func wireProjectionRebuildWorker() (projectionRebuildWorkerRuntime, error) {
	redisURL := strings.TrimSpace(os.Getenv("REDIS_URL"))
	if redisURL == "" {
		return projectionRebuildWorkerRuntime{}, fmt.Errorf("WORKER_ROLE=%s requires REDIS_URL", workerRoleSpectatorProjectionRebuilder)
	}
	roomURL := strings.TrimSpace(os.Getenv("ROOM_GAMEPLAY_URL"))
	if roomURL == "" {
		return projectionRebuildWorkerRuntime{}, fmt.Errorf("WORKER_ROLE=%s requires ROOM_GAMEPLAY_URL", workerRoleSpectatorProjectionRebuilder)
	}
	roomCred := strings.TrimSpace(os.Getenv("ROOM_SPECTATOR_RECOVERY_SERVICE_CREDENTIAL"))
	if roomCred == "" {
		return projectionRebuildWorkerRuntime{}, fmt.Errorf("WORKER_ROLE=%s requires ROOM_SPECTATOR_RECOVERY_SERVICE_CREDENTIAL", workerRoleSpectatorProjectionRebuilder)
	}

	kafkaCfg, err := LoadProjectionRebuildKafkaConfigFromEnv()
	if err != nil {
		return projectionRebuildWorkerRuntime{}, fmt.Errorf("projection rebuild kafka: %w", err)
	}

	keyPrefix := strings.TrimSpace(os.Getenv("SPECTATOR_REDIS_KEY_PREFIX"))
	rdb, err := store.NewRedisFromURL(redisURL)
	if err != nil {
		return projectionRebuildWorkerRuntime{}, fmt.Errorf("redis client: %w", err)
	}
	rs := store.NewRedisProjectionStore(rdb, keyPrefix)
	if n, ok, err := store.ParseStreamMaxLenEnv(os.Getenv("SPECTATOR_REDIS_STREAM_MAXLEN")); err != nil {
		_ = rdb.Close()
		return projectionRebuildWorkerRuntime{}, fmt.Errorf("SPECTATOR_REDIS_STREAM_MAXLEN: %w", err)
	} else if ok {
		rs = rs.WithStreamMaxLen(n)
	}
	// Quarantine identity stays on the live spectator-safe consumer group/topic.
	liveTopic := firstNonEmpty(strings.TrimSpace(os.Getenv("KAFKA_SPECTATOR_SAFE_TOPIC")), DefaultSpectatorSafeTopic)
	liveGroup := firstNonEmpty(strings.TrimSpace(os.Getenv("KAFKA_CONSUMER_GROUP")), DefaultSpectatorKafkaGroup)
	rs = rs.WithKafkaIdentity(liveGroup, liveTopic)

	if err := rs.LoadScripts(context.Background()); err != nil {
		_ = rdb.Close()
		return projectionRebuildWorkerRuntime{}, fmt.Errorf("redis scripts: %w", err)
	}

	client, err := newFranzSpectatorSafeClient(kafkaCfg.ToSpectatorSafeKafkaConfig())
	if err != nil {
		_ = rdb.Close()
		if client != nil {
			_ = client.Close()
		}
		return projectionRebuildWorkerRuntime{}, fmt.Errorf("kafka client: %w", err)
	}

	held := &KafkaHeldSpectatorDLQSource{
		Brokers:           kafkaCfg.Brokers,
		DLQTopic:          kafkaCfg.HeldDLQTopic,
		ExpectedSourceTop: liveTopic,
		ExpectedConsumer:  liveGroup,
	}
	applyHeldScanLimitsFromEnv(held)

	exec := &StoreBackedProjectionRebuildExecutor{
		Room:  NewHTTPRoomSpectatorRecoveryClient(roomURL, roomCred),
		Held:  held,
		Store: rs,
		Release: store.RecoveryRelease{
			Enabled: true,
			Note:    store.ReleaseNoteRecoveryContinuityProven,
		},
	}
	idemp := NewRedisRebuildIdempotency(rdb, keyPrefix)
	consumer := &ProjectionRebuildKafkaConsumer{
		source: client,
		dlq:    client,
		exec:   exec,
		idemp:  idemp,
		cfg:    kafkaCfg,
		clock:  systemClock{},
	}
	return projectionRebuildWorkerRuntime{
		rdb:      rdb,
		client:   client,
		consumer: consumer,
		mode:     "projection-rebuilder",
	}, nil
}

func (rt projectionRebuildWorkerRuntime) close() {
	if rt.client != nil {
		_ = rt.client.Close()
	}
	if rt.rdb != nil {
		_ = rt.rdb.Close()
	}
}

// runProjectionRebuildWorker runs the long-lived rebuild consumer without HTTP.
func runProjectionRebuildWorker(rt projectionRebuildWorkerRuntime) {
	if rt.consumer == nil {
		log.Fatal("projection rebuild worker requires consumer")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	defer rt.close()

	log.Printf(`{"level":"info","service":"spectator-view","event":"projection_rebuilder_startup","mode":%q,"group":%q,"topic":%q}`,
		rt.mode, rt.consumer.cfg.Group, rt.consumer.cfg.Topic)

	errCh := make(chan error, 1)
	go func() {
		errCh <- rt.consumer.Run(ctx)
	}()

	select {
	case err := <-errCh:
		if err != nil && ctx.Err() == nil {
			log.Fatalf(`{"level":"error","service":"spectator-view","event":"projection_rebuilder_stopped","error":%q}`, err.Error())
		}
	case <-ctx.Done():
		select {
		case <-errCh:
		case <-time.After(10 * time.Second):
		}
	}
}

func applyHeldScanLimitsFromEnv(held *KafkaHeldSpectatorDLQSource) {
	if held == nil {
		return
	}
	if v := strings.TrimSpace(os.Getenv("KAFKA_HELD_DLQ_MAX_SCAN_RECORDS")); v != "" {
		if n, err := parsePositiveInt(v); err == nil {
			held.MaxScanRecords = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("KAFKA_HELD_DLQ_MAX_SCAN_BYTES")); v != "" {
		if n, err := parsePositiveInt(v); err == nil {
			held.MaxScanBytes = int64(n)
		}
	}
	if v := strings.TrimSpace(os.Getenv("KAFKA_HELD_DLQ_MAX_POLL_CYCLES")); v != "" {
		if n, err := parsePositiveInt(v); err == nil {
			held.MaxPollCycles = n
		}
	}
}
