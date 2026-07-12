package main

import (
	"strings"
	"testing"
)

func TestWireTournamentRuntime_RejectsUnknownWorkerRole(t *testing.T) {
	t.Setenv("WORKER_ROLE", "not-a-role")
	t.Setenv("DATABASE_URL", "postgres://x")
	_, err := wireTournamentRuntime()
	if err == nil {
		t.Fatal("expected rejection")
	}
	if !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("err=%v", err)
	}
}

func TestWireTournamentRuntime_SeedingWorkerRequiresDatabaseAndRedis(t *testing.T) {
	t.Setenv("WORKER_ROLE", workerRoleTournamentSeeding)
	t.Setenv("DATABASE_URL", "")
	t.Setenv("REDIS_URL", "")
	t.Setenv("ROOM_GAMEPLAY_URL", "")
	t.Setenv("TOURNAMENT_BRACKET_CURSOR_SECRET", "")
	t.Setenv("KAFKA_BROKERS", "")
	_, err := wireTournamentRuntime()
	if err == nil {
		t.Fatal("expected missing DATABASE_URL / REDIS_URL")
	}
	if !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Fatalf("err=%v", err)
	}
	if !strings.Contains(err.Error(), "REDIS_URL") {
		t.Fatalf("seeding worker must require REDIS_URL: %v", err)
	}
	if strings.Contains(err.Error(), "ROOM") || strings.Contains(err.Error(), "kafka") || strings.Contains(err.Error(), "CURSOR") {
		t.Fatalf("seeding worker must not require Room/kafka/cursor: %v", err)
	}
}

func TestWireTournamentRuntime_CompletionWorkerRequiresDatabaseAndRedis(t *testing.T) {
	t.Setenv("WORKER_ROLE", workerRoleTournamentCompletion)
	t.Setenv("DATABASE_URL", "")
	t.Setenv("REDIS_URL", "")
	t.Setenv("ROOM_GAMEPLAY_URL", "")
	t.Setenv("TOURNAMENT_BRACKET_CURSOR_SECRET", "")
	t.Setenv("KAFKA_BROKERS", "")
	_, err := wireTournamentRuntime()
	if err == nil {
		t.Fatal("expected missing DATABASE_URL / REDIS_URL")
	}
	if !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Fatalf("err=%v", err)
	}
	if !strings.Contains(err.Error(), "REDIS_URL") {
		t.Fatalf("completion worker must require REDIS_URL: %v", err)
	}
	if strings.Contains(err.Error(), "ROOM") || strings.Contains(err.Error(), "kafka") || strings.Contains(err.Error(), "CURSOR") {
		t.Fatalf("completion worker must not require Room/kafka/cursor: %v", err)
	}
}

func TestWireTournamentRuntime_ProvisioningWorkerBypassesKafkaRequirement(t *testing.T) {
	t.Setenv("WORKER_ROLE", workerRoleTournamentProvisioning)
	t.Setenv("DATABASE_URL", "postgres://tournament@127.0.0.1:1/unoarena_tournament_test_wire")
	t.Setenv("ROOM_GAMEPLAY_URL", "http://room-gameplay:8080")
	t.Setenv("TOURNAMENT_INTERNAL_CREDENTIAL", "cred")
	t.Setenv("REDIS_URL", "redis://127.0.0.1:1/7")
	t.Setenv("KAFKA_BROKERS", "")
	t.Setenv("TOURNAMENT_CAPABILITY_MODE", "")
	_, err := wireTournamentRuntime()
	// Pool connect will fail against closed port; must not fail for missing kafka / unsupported role.
	if err == nil {
		t.Fatal("expected pool connect failure without a live postgres")
	}
	if strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("provisioning worker role must be supported: %v", err)
	}
	if strings.Contains(strings.ToLower(err.Error()), "kafka") {
		t.Fatalf("worker must bypass kafka: %v", err)
	}
	if !strings.Contains(err.Error(), "database pool") {
		t.Fatalf("want database pool error, got %v", err)
	}
}

func TestWireTournamentRuntime_APIDurableStillRequiresKafka(t *testing.T) {
	t.Setenv("WORKER_ROLE", "")
	t.Setenv("DATABASE_URL", "postgres://tournament@localhost/tournament")
	t.Setenv("TOURNAMENT_CAPABILITY_MODE", "")
	t.Setenv("DEPLOYMENT_ENV", "local")
	t.Setenv("TOURNAMENT_INTERNAL_CREDENTIAL", "cred")
	t.Setenv("ROOM_GAMEPLAY_URL", "http://room-gameplay")
	t.Setenv("REDIS_URL", "redis://127.0.0.1:6379/7")
	t.Setenv("KAFKA_BROKERS", "")
	t.Setenv("TOURNAMENT_BRACKET_CURSOR_SECRET", "test-cursor-secret")
	t.Setenv("TOURNAMENT_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL", "analytics-cred")
	t.Setenv("TOURNAMENT_ANALYTICS_BACKFILL_CURSOR_SECRET", "analytics-cursor")
	rt, err := wireTournamentRuntime()
	if err != nil {
		t.Fatal(err)
	}
	if rt.mode != "durable" || rt.ready {
		t.Fatalf("want durable not ready without kafka: mode=%s ready=%v reason=%s", rt.mode, rt.ready, rt.readyReason)
	}
	if !strings.Contains(rt.readyReason, "kafka") {
		t.Fatalf("reason=%q", rt.readyReason)
	}
}
