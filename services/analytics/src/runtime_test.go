package main

import (
	"strings"
	"testing"
)

func TestWireRuntime_CapabilityMemory(t *testing.T) {
	t.Setenv("ANALYTICS_CAPABILITY_MODE", "1")
	t.Setenv("DEPLOYMENT_ENV", "local")
	t.Setenv("CLICKHOUSE_URL", "")
	t.Setenv("ANALYTICS_ROOM_CREDENTIAL", "r")
	t.Setenv("ANALYTICS_RANKING_CREDENTIAL", "k")
	t.Setenv("ANALYTICS_TOURNAMENT_CREDENTIAL", "t")
	t.Setenv("ANALYTICS_OPS_CREDENTIAL", "o")
	rt, err := wireAnalyticsRuntime()
	if err != nil {
		t.Fatal(err)
	}
	if rt.mode != "capability" || !rt.ready {
		t.Fatalf("rt=%+v", rt)
	}
}

func TestWireRuntime_FailClosedWithoutClickHouseOutsideCapability(t *testing.T) {
	t.Setenv("ANALYTICS_CAPABILITY_MODE", "")
	t.Setenv("DEPLOYMENT_ENV", "local")
	t.Setenv("CLICKHOUSE_URL", "")
	rt, err := wireAnalyticsRuntime()
	if err != nil {
		t.Fatal(err)
	}
	if rt.mode != "misconfigured" || rt.ready {
		t.Fatalf("want misconfigured not ready, got mode=%s ready=%v reason=%s", rt.mode, rt.ready, rt.readyReason)
	}
}

func TestWireRuntime_CapabilityForbiddenInProduction(t *testing.T) {
	t.Setenv("ANALYTICS_CAPABILITY_MODE", "1")
	t.Setenv("DEPLOYMENT_ENV", "production")
	t.Setenv("CLICKHOUSE_URL", "")
	rt, err := wireAnalyticsRuntime()
	if err != nil {
		t.Fatal(err)
	}
	if rt.mode != "misconfigured" || rt.readyReason != "capability_mode_forbidden_in_production" {
		t.Fatalf("rt=%+v", rt)
	}
}

func TestWireRuntime_DurableRequiresScopedCredsAndPassword(t *testing.T) {
	t.Setenv("ANALYTICS_CAPABILITY_MODE", "")
	t.Setenv("DEPLOYMENT_ENV", "local")
	t.Setenv("CLICKHOUSE_URL", "http://127.0.0.1:8123")
	t.Setenv("CLICKHOUSE_USER", "")
	t.Setenv("CLICKHOUSE_PASSWORD", "")
	t.Setenv("KAFKA_BROKERS", "")
	t.Setenv("ANALYTICS_ROOM_CREDENTIAL", "r")
	t.Setenv("ANALYTICS_RANKING_CREDENTIAL", "k")
	t.Setenv("ANALYTICS_TOURNAMENT_CREDENTIAL", "t")
	t.Setenv("ANALYTICS_OPS_CREDENTIAL", "o")
	rt, err := wireAnalyticsRuntime()
	if err != nil {
		t.Fatal(err)
	}
	if rt.mode != "durable" || rt.ready {
		t.Fatalf("want durable not ready without CH user/pass: %+v", rt)
	}
}

func TestWireRuntime_DurableRequiresKafka(t *testing.T) {
	t.Setenv("ANALYTICS_CAPABILITY_MODE", "")
	t.Setenv("DEPLOYMENT_ENV", "local")
	t.Setenv("CLICKHOUSE_URL", "http://127.0.0.1:8123")
	t.Setenv("CLICKHOUSE_USER", "u")
	t.Setenv("CLICKHOUSE_PASSWORD", "p")
	t.Setenv("KAFKA_BROKERS", "")
	t.Setenv("ANALYTICS_ROOM_CREDENTIAL", "r")
	t.Setenv("ANALYTICS_RANKING_CREDENTIAL", "k")
	t.Setenv("ANALYTICS_TOURNAMENT_CREDENTIAL", "t")
	t.Setenv("ANALYTICS_OPS_CREDENTIAL", "o")
	rt, err := wireAnalyticsRuntime()
	if err != nil {
		t.Fatal(err)
	}
	if rt.mode != "durable" || rt.ready {
		t.Fatalf("want durable not ready without kafka: %+v", rt)
	}
	if !strings.Contains(rt.readyReason, "kafka") {
		t.Fatalf("reason=%q", rt.readyReason)
	}
}

func TestWireRuntime_CapabilityIgnoresKafkaEnv(t *testing.T) {
	t.Setenv("ANALYTICS_CAPABILITY_MODE", "1")
	t.Setenv("DEPLOYMENT_ENV", "local")
	t.Setenv("CLICKHOUSE_URL", "")
	t.Setenv("KAFKA_BROKERS", "kafka:9092")
	t.Setenv("ANALYTICS_ROOM_CREDENTIAL", "r")
	t.Setenv("ANALYTICS_RANKING_CREDENTIAL", "k")
	t.Setenv("ANALYTICS_TOURNAMENT_CREDENTIAL", "t")
	t.Setenv("ANALYTICS_OPS_CREDENTIAL", "o")
	rt, err := wireAnalyticsRuntime()
	if err != nil {
		t.Fatal(err)
	}
	if rt.mode != "capability" || !rt.ready || rt.kafka != nil {
		t.Fatalf("capability must ignore kafka: %+v", rt)
	}
}
