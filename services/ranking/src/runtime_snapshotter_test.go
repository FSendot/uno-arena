package main

import (
	"strings"
	"testing"
)

func TestWireRankingRuntime_RejectsUnknownWorkerRole(t *testing.T) {
	t.Setenv("WORKER_ROLE", "not-a-role")
	t.Setenv("DATABASE_URL", "postgres://x")
	_, err := wireRankingRuntime()
	if err == nil {
		t.Fatal("expected rejection")
	}
	if !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("err=%v", err)
	}
}

func TestWireRankingRuntime_SnapshotterRequiresDatabaseURL(t *testing.T) {
	t.Setenv("WORKER_ROLE", workerRoleLeaderboardSnapshotter)
	t.Setenv("DATABASE_URL", "")
	t.Setenv("KAFKA_BROKERS", "")
	t.Setenv("RANKING_INTERNAL_CREDENTIAL", "")
	_, err := wireRankingRuntime()
	if err == nil {
		t.Fatal("expected missing DATABASE_URL")
	}
	if !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Fatalf("err=%v", err)
	}
	if strings.Contains(strings.ToLower(err.Error()), "kafka") {
		t.Fatalf("snapshotter must not require kafka: %v", err)
	}
}

func TestWireRankingRuntime_SnapshotterBypassesKafkaRequirement(t *testing.T) {
	t.Setenv("WORKER_ROLE", workerRoleLeaderboardSnapshotter)
	t.Setenv("DATABASE_URL", "postgres://ranking@127.0.0.1:1/unoarena_ranking_test_wire")
	t.Setenv("KAFKA_BROKERS", "")
	t.Setenv("RANKING_CAPABILITY_MODE", "")
	t.Setenv("RANKING_INTERNAL_CREDENTIAL", "")
	_, err := wireRankingRuntime()
	if err == nil {
		t.Fatal("expected pool connect failure without a live postgres")
	}
	if strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("snapshotter role must be supported: %v", err)
	}
	if strings.Contains(strings.ToLower(err.Error()), "kafka") {
		t.Fatalf("snapshotter must bypass kafka: %v", err)
	}
	if !strings.Contains(err.Error(), "database pool") {
		t.Fatalf("want database pool error, got %v", err)
	}
}

func TestWireRankingRuntime_APIDurableStillRequiresKafka(t *testing.T) {
	t.Setenv("WORKER_ROLE", "")
	t.Setenv("DATABASE_URL", "postgres://ranking@localhost/ranking")
	t.Setenv("RANKING_CAPABILITY_MODE", "")
	t.Setenv("DEPLOYMENT_ENV", "local")
	t.Setenv("RANKING_INTERNAL_CREDENTIAL", "cred")
	t.Setenv("KAFKA_BROKERS", "")
	rt, err := wireRankingRuntime()
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
