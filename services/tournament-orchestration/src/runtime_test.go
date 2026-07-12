package main

import (
	"os"
	"strings"
	"testing"
)

func TestWireTournamentRuntime_FailClosedWithoutDatabase(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("TOURNAMENT_CAPABILITY_MODE", "")
	t.Setenv("DEPLOYMENT_ENV", "production")
	t.Setenv("WORKER_ROLE", "")
	t.Setenv("TOURNAMENT_BRACKET_CURSOR_SECRET", "")
	rt, err := wireTournamentRuntime()
	if err != nil {
		t.Fatal(err)
	}
	if rt.mode != "misconfigured" || rt.ready {
		t.Fatalf("want misconfigured not-ready, got mode=%s ready=%v", rt.mode, rt.ready)
	}
}

func TestWireTournamentRuntime_CapabilityNonProd(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("TOURNAMENT_CAPABILITY_MODE", "true")
	t.Setenv("DEPLOYMENT_ENV", "local")
	t.Setenv("WORKER_ROLE", "")
	t.Setenv("TOURNAMENT_INTERNAL_CREDENTIAL", "cred")
	t.Setenv("TOURNAMENT_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL", "analytics-cred")
	t.Setenv("TOURNAMENT_BRACKET_CURSOR_SECRET", "")
	rt, err := wireTournamentRuntime()
	if err != nil {
		t.Fatal(err)
	}
	if rt.mode != "capability" || !rt.ready {
		t.Fatalf("mode=%s ready=%v reason=%s", rt.mode, rt.ready, rt.readyReason)
	}
	_ = os.Getenv
}

func TestWireTournamentRuntime_CapabilityRequiresAnalyticsCred(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("TOURNAMENT_CAPABILITY_MODE", "true")
	t.Setenv("DEPLOYMENT_ENV", "local")
	t.Setenv("WORKER_ROLE", "")
	t.Setenv("TOURNAMENT_INTERNAL_CREDENTIAL", "cred")
	t.Setenv("TOURNAMENT_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL", "")
	rt, err := wireTournamentRuntime()
	if err != nil {
		t.Fatal(err)
	}
	if rt.mode != "capability" || rt.ready {
		t.Fatalf("mode=%s ready=%v reason=%s", rt.mode, rt.ready, rt.readyReason)
	}
	if !strings.Contains(rt.readyReason, "analytics_backfill_credential") {
		t.Fatalf("readyReason=%q", rt.readyReason)
	}
}

func TestWireTournamentRuntime_DurableRequiresCursorSecret(t *testing.T) {
	t.Setenv("WORKER_ROLE", "")
	t.Setenv("DATABASE_URL", "postgres://tournament@localhost/tournament")
	t.Setenv("TOURNAMENT_CAPABILITY_MODE", "")
	t.Setenv("DEPLOYMENT_ENV", "local")
	t.Setenv("TOURNAMENT_INTERNAL_CREDENTIAL", "cred")
	t.Setenv("ROOM_GAMEPLAY_URL", "http://room-gameplay")
	t.Setenv("REDIS_URL", "redis://127.0.0.1:6379/7")
	t.Setenv("KAFKA_BROKERS", "kafka.uno-arena.svc.cluster.local:9092")
	t.Setenv("KAFKA_CONSUMER_GROUP", "tournament-orchestration")
	t.Setenv("KAFKA_MATCH_COMPLETED_TOPIC", "room.match.completed")
	t.Setenv("KAFKA_MATCH_COMPLETED_DLQ_TOPIC", "room.match.completed.dlq.tournament-orchestration")
	t.Setenv("TOURNAMENT_BRACKET_CURSOR_SECRET", "")
	t.Setenv("TOURNAMENT_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL", "analytics-cred")
	t.Setenv("TOURNAMENT_ANALYTICS_BACKFILL_CURSOR_SECRET", "analytics-cursor")
	rt, err := wireTournamentRuntime()
	if err != nil {
		t.Fatal(err)
	}
	if rt.ready {
		t.Fatal("durable API must not be ready without TOURNAMENT_BRACKET_CURSOR_SECRET")
	}
	if !strings.Contains(rt.readyReason, "TOURNAMENT_BRACKET_CURSOR_SECRET") {
		t.Fatalf("readyReason=%q", rt.readyReason)
	}
}

func TestWireTournamentRuntime_DurableRequiresAnalyticsBackfillSecrets(t *testing.T) {
	t.Setenv("WORKER_ROLE", "")
	t.Setenv("DATABASE_URL", "postgres://tournament@localhost/tournament")
	t.Setenv("TOURNAMENT_CAPABILITY_MODE", "")
	t.Setenv("DEPLOYMENT_ENV", "local")
	t.Setenv("TOURNAMENT_INTERNAL_CREDENTIAL", "cred")
	t.Setenv("ROOM_GAMEPLAY_URL", "http://room-gameplay")
	t.Setenv("REDIS_URL", "redis://127.0.0.1:6379/7")
	t.Setenv("KAFKA_BROKERS", "kafka.uno-arena.svc.cluster.local:9092")
	t.Setenv("KAFKA_CONSUMER_GROUP", "tournament-orchestration")
	t.Setenv("KAFKA_MATCH_COMPLETED_TOPIC", "room.match.completed")
	t.Setenv("KAFKA_MATCH_COMPLETED_DLQ_TOPIC", "room.match.completed.dlq.tournament-orchestration")
	t.Setenv("TOURNAMENT_BRACKET_CURSOR_SECRET", "test-cursor-secret")
	t.Setenv("TOURNAMENT_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL", "")
	t.Setenv("TOURNAMENT_ANALYTICS_BACKFILL_CURSOR_SECRET", "")
	restore := SetAnalyticsBackfillCursorMACKeyForTest("")
	t.Cleanup(restore)
	rt, err := wireTournamentRuntime()
	if err != nil {
		t.Fatal(err)
	}
	if rt.ready {
		t.Fatal("durable API must not be ready without analytics backfill secrets")
	}
	if !strings.Contains(rt.readyReason, "TOURNAMENT_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL") {
		t.Fatalf("readyReason=%q", rt.readyReason)
	}
	if !strings.Contains(rt.readyReason, "TOURNAMENT_ANALYTICS_BACKFILL_CURSOR_SECRET") {
		t.Fatalf("readyReason=%q", rt.readyReason)
	}
}

func TestWireTournamentRuntime_DurableRequiresRedisURL(t *testing.T) {
	t.Setenv("WORKER_ROLE", "")
	t.Setenv("DATABASE_URL", "postgres://tournament@localhost/tournament")
	t.Setenv("TOURNAMENT_CAPABILITY_MODE", "")
	t.Setenv("DEPLOYMENT_ENV", "local")
	t.Setenv("TOURNAMENT_INTERNAL_CREDENTIAL", "cred")
	t.Setenv("ROOM_GAMEPLAY_URL", "http://room-gameplay")
	t.Setenv("REDIS_URL", "")
	t.Setenv("KAFKA_BROKERS", "kafka.uno-arena.svc.cluster.local:9092")
	t.Setenv("KAFKA_CONSUMER_GROUP", "tournament-orchestration")
	t.Setenv("KAFKA_MATCH_COMPLETED_TOPIC", "room.match.completed")
	t.Setenv("KAFKA_MATCH_COMPLETED_DLQ_TOPIC", "room.match.completed.dlq.tournament-orchestration")
	t.Setenv("TOURNAMENT_BRACKET_CURSOR_SECRET", "test-cursor-secret")
	t.Setenv("TOURNAMENT_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL", "analytics-cred")
	t.Setenv("TOURNAMENT_ANALYTICS_BACKFILL_CURSOR_SECRET", "analytics-cursor")
	rt, err := wireTournamentRuntime()
	if err != nil {
		t.Fatal(err)
	}
	if rt.ready {
		t.Fatal("durable API must not be ready without REDIS_URL")
	}
	if !strings.Contains(rt.readyReason, "REDIS_URL") {
		t.Fatalf("readyReason=%q", rt.readyReason)
	}
}

func TestWireProvisioningWorker_ExemptFromCursorSecret(t *testing.T) {
	t.Setenv("WORKER_ROLE", workerRoleTournamentProvisioning)
	t.Setenv("DATABASE_URL", "postgres://tournament@127.0.0.1:1/unoarena_tournament_test_wire")
	t.Setenv("ROOM_GAMEPLAY_URL", "http://room-gameplay:8080")
	t.Setenv("TOURNAMENT_INTERNAL_CREDENTIAL", "cred")
	t.Setenv("REDIS_URL", "redis://127.0.0.1:1/7")
	t.Setenv("KAFKA_BROKERS", "")
	t.Setenv("TOURNAMENT_CAPABILITY_MODE", "")
	t.Setenv("TOURNAMENT_BRACKET_CURSOR_SECRET", "")
	_, err := wireTournamentRuntime()
	if err == nil {
		t.Fatal("expected pool connect failure")
	}
	if strings.Contains(err.Error(), "TOURNAMENT_BRACKET_CURSOR_SECRET") {
		t.Fatalf("provisioning worker must not require cursor secret: %v", err)
	}
	if !strings.Contains(err.Error(), "database pool") {
		t.Fatalf("want database pool error, got %v", err)
	}
}
