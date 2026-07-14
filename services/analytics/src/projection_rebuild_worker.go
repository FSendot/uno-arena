package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"unoarena/services/analytics/store"
)

const workerRoleAnalyticsProjectionRebuilder = "analytics-projection-rebuilder"

// projectionRebuildWorkerRuntime is the HTTP-free ADR-0039 Analytics rebuilder.
type projectionRebuildWorkerRuntime struct {
	chStore  *store.AnalyticsStore
	client   *franzAnalyticsRebuildClient
	consumer *AnalyticsProjectionRebuildKafkaConsumer
	mode     string
}

func workerRoleFromEnv() string {
	return strings.TrimSpace(os.Getenv("WORKER_ROLE"))
}

// wireProjectionRebuildWorker fails closed when ClickHouse, Kafka, producer URLs,
// or scoped pair credentials are missing. Does not start live ingest or HTTP.
func wireProjectionRebuildWorker() (projectionRebuildWorkerRuntime, error) {
	chURL := strings.TrimSpace(os.Getenv("CLICKHOUSE_URL"))
	if chURL == "" {
		return projectionRebuildWorkerRuntime{}, fmt.Errorf("WORKER_ROLE=%s requires CLICKHOUSE_URL", workerRoleAnalyticsProjectionRebuilder)
	}
	chUser := strings.TrimSpace(os.Getenv("CLICKHOUSE_USER"))
	chPass := strings.TrimSpace(os.Getenv("CLICKHOUSE_PASSWORD"))
	if chUser == "" || chPass == "" {
		return projectionRebuildWorkerRuntime{}, fmt.Errorf("WORKER_ROLE=%s requires CLICKHOUSE_USER/CLICKHOUSE_PASSWORD", workerRoleAnalyticsProjectionRebuilder)
	}
	chDB := strings.TrimSpace(os.Getenv("CLICKHOUSE_DB"))
	if chDB == "" {
		chDB = "analytics"
	}

	roomURL := strings.TrimSpace(os.Getenv("ROOM_GAMEPLAY_URL"))
	tournURL := strings.TrimSpace(os.Getenv("TOURNAMENT_ORCHESTRATION_URL"))
	rankURL := strings.TrimSpace(os.Getenv("RANKING_URL"))
	if roomURL == "" || tournURL == "" || rankURL == "" {
		return projectionRebuildWorkerRuntime{}, fmt.Errorf("WORKER_ROLE=%s requires ROOM_GAMEPLAY_URL, TOURNAMENT_ORCHESTRATION_URL, RANKING_URL", workerRoleAnalyticsProjectionRebuilder)
	}
	roomCred := strings.TrimSpace(os.Getenv("ANALYTICS_ROOM_CREDENTIAL"))
	tournCred := strings.TrimSpace(os.Getenv("ANALYTICS_TOURNAMENT_CREDENTIAL"))
	rankCred := strings.TrimSpace(os.Getenv("ANALYTICS_RANKING_CREDENTIAL"))
	if roomCred == "" || tournCred == "" || rankCred == "" {
		return projectionRebuildWorkerRuntime{}, fmt.Errorf("WORKER_ROLE=%s requires ANALYTICS_ROOM/TOURNAMENT/RANKING_CREDENTIAL", workerRoleAnalyticsProjectionRebuilder)
	}
	// Least privilege: reject ops/API credential injection on this role.
	if strings.TrimSpace(os.Getenv("ANALYTICS_OPS_CREDENTIAL")) != "" {
		return projectionRebuildWorkerRuntime{}, fmt.Errorf("WORKER_ROLE=%s must not receive ANALYTICS_OPS_CREDENTIAL", workerRoleAnalyticsProjectionRebuilder)
	}

	kafkaCfg, err := LoadProjectionRebuildKafkaConfigFromEnv()
	if err != nil {
		return projectionRebuildWorkerRuntime{}, fmt.Errorf("projection rebuild kafka: %w", err)
	}

	clientCH, err := store.NewClient(store.Config{
		URL: chURL, User: chUser, Password: chPass, Database: chDB,
		HTTPClient: tracedHTTPClient(30 * time.Second),
	})
	if err != nil {
		return projectionRebuildWorkerRuntime{}, fmt.Errorf("clickhouse client: %w", err)
	}
	chs := store.NewAnalyticsStore(clientCH)
	if err := chs.Ready(context.Background()); err != nil {
		return projectionRebuildWorkerRuntime{}, fmt.Errorf("clickhouse ready: %w", err)
	}

	owner, err := store.NewOwnerToken()
	if err != nil {
		return projectionRebuildWorkerRuntime{}, err
	}

	kClient, err := newFranzAnalyticsRebuildClient(kafkaCfg)
	if err != nil {
		return projectionRebuildWorkerRuntime{}, fmt.Errorf("kafka client: %w", err)
	}

	exec := &StoreBackedAnalyticsProjectionRebuildExecutor{
		Store: chs,
		Backfill: &HTTPAnalyticsBackfillClients{
			HTTP:    tracedHTTPClient(30 * time.Second),
			RoomURL: roomURL, RoomCred: roomCred,
			TournURL: tournURL, TournCred: tournCred,
			RankURL: rankURL, RankCred: rankCred,
		},
		OwnerToken: owner,
		LeaseTTL:   store.DefaultRecoveryLeaseTTL,
		Clock:      systemClock{},
	}
	consumer := &AnalyticsProjectionRebuildKafkaConsumer{
		source: kClient,
		dlq:    kClient,
		follow: kClient,
		exec:   exec,
		cfg:    kafkaCfg,
		clock:  systemClock{},
	}
	return projectionRebuildWorkerRuntime{
		chStore:  chs,
		client:   kClient,
		consumer: consumer,
		mode:     "projection-rebuilder",
	}, nil
}

func (rt projectionRebuildWorkerRuntime) close() {
	if rt.client != nil {
		_ = rt.client.Close()
	}
}

// runProjectionRebuildWorker runs the long-lived rebuild consumer without HTTP.
func runProjectionRebuildWorker(rt projectionRebuildWorkerRuntime) error {
	if rt.consumer == nil {
		return fmt.Errorf("projection rebuild worker requires consumer")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	defer rt.close()

	processLogger().InfoContext(ctx, "projection rebuilder started", "event", "projection_rebuilder_started",
		"mode", rt.mode, "group", rt.consumer.cfg.Group, "topic", rt.consumer.cfg.Topic)

	errCh := make(chan error, 1)
	go func() {
		errCh <- rt.consumer.Run(ctx)
	}()

	select {
	case err := <-errCh:
		if err != nil && ctx.Err() == nil {
			return fmt.Errorf("projection rebuilder stopped: %w", err)
		}
	case <-ctx.Done():
		select {
		case <-errCh:
		case <-time.After(10 * time.Second):
		}
	}
	return nil
}
