package main

import (
	"strings"
	"testing"
)

func TestWireRankingRuntime_CapabilityMemory(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("RANKING_CAPABILITY_MODE", "true")
	t.Setenv("DEPLOYMENT_ENV", "local")
	t.Setenv("RANKING_INTERNAL_CREDENTIAL", "cred")
	t.Setenv("RANKING_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL", "analytics-cred")
	rt, err := wireRankingRuntime()
	if err != nil {
		t.Fatal(err)
	}
	if rt.mode != "capability" || !rt.ready {
		t.Fatalf("rt=%+v", rt)
	}
}

func TestWireRankingRuntime_CapabilityRequiresAnalyticsCred(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("RANKING_CAPABILITY_MODE", "true")
	t.Setenv("DEPLOYMENT_ENV", "local")
	t.Setenv("RANKING_INTERNAL_CREDENTIAL", "cred")
	t.Setenv("RANKING_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL", "")
	rt, err := wireRankingRuntime()
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

func TestWireRankingRuntime_FailClosedWithoutCapability(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("RANKING_CAPABILITY_MODE", "")
	t.Setenv("DEPLOYMENT_ENV", "local")
	t.Setenv("RANKING_INTERNAL_CREDENTIAL", "cred")
	rt, err := wireRankingRuntime()
	if err != nil {
		t.Fatal(err)
	}
	if rt.mode != "misconfigured" || rt.ready {
		t.Fatalf("want misconfigured, got mode=%s ready=%v", rt.mode, rt.ready)
	}
}

func TestWireRankingRuntime_CapabilityBlockedInProd(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("RANKING_CAPABILITY_MODE", "true")
	t.Setenv("DEPLOYMENT_ENV", "production")
	t.Setenv("RANKING_INTERNAL_CREDENTIAL", "cred")
	rt, err := wireRankingRuntime()
	if err != nil {
		t.Fatal(err)
	}
	if rt.mode != "misconfigured" {
		t.Fatalf("capability must fail closed in prod, got %s", rt.mode)
	}
}

func TestWireRankingRuntime_DurableRequiresAnalyticsBackfillSecrets(t *testing.T) {
	t.Setenv("WORKER_ROLE", "")
	t.Setenv("DATABASE_URL", "postgres://ranking@localhost/ranking")
	t.Setenv("RANKING_CAPABILITY_MODE", "")
	t.Setenv("DEPLOYMENT_ENV", "local")
	t.Setenv("RANKING_INTERNAL_CREDENTIAL", "cred")
	t.Setenv("REDIS_URL", "redis://127.0.0.1:6379/4")
	t.Setenv("KAFKA_BROKERS", "kafka.uno-arena.svc.cluster.local:9092")
	t.Setenv("KAFKA_CONSUMER_GROUP", "ranking")
	t.Setenv("KAFKA_GAME_COMPLETED_TOPIC", "room.game.completed")
	t.Setenv("KAFKA_GAME_COMPLETED_DLQ_TOPIC", "room.game.completed.ranking.dlq")
	t.Setenv("RANKING_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL", "")
	t.Setenv("RANKING_ANALYTICS_BACKFILL_CURSOR_SECRET", "")
	restore := SetAnalyticsBackfillCursorMACKeyForTest("")
	t.Cleanup(restore)
	rt, err := wireRankingRuntime()
	if err != nil {
		t.Fatal(err)
	}
	if rt.ready {
		t.Fatal("durable API must not be ready without analytics backfill secrets")
	}
	if !strings.Contains(rt.readyReason, "RANKING_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL") {
		t.Fatalf("readyReason=%q", rt.readyReason)
	}
	if !strings.Contains(rt.readyReason, "RANKING_ANALYTICS_BACKFILL_CURSOR_SECRET") {
		t.Fatalf("readyReason=%q", rt.readyReason)
	}
}
