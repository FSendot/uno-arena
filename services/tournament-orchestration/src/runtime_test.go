package main

import (
	"os"
	"testing"
)

func TestWireTournamentRuntime_FailClosedWithoutDatabase(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("TOURNAMENT_CAPABILITY_MODE", "")
	t.Setenv("DEPLOYMENT_ENV", "production")
	t.Setenv("WORKER_ROLE", "")
	rt, err := wireTournamentRuntime()
	if err != nil {
		t.Fatal(err)
	}
	if rt.mode != "misconfigured" || rt.ready {
		t.Fatalf("want misconfigured not-ready, got mode=%s ready=%v", rt.mode, rt.ready)
	}
}

func TestWireTournamentRuntime_RejectsWorkerRole(t *testing.T) {
	t.Setenv("WORKER_ROLE", "tournament-provisioning")
	t.Setenv("DATABASE_URL", "postgres://x")
	_, err := wireTournamentRuntime()
	if err == nil {
		t.Fatal("expected WORKER_ROLE rejection")
	}
}

func TestWireTournamentRuntime_CapabilityNonProd(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("TOURNAMENT_CAPABILITY_MODE", "true")
	t.Setenv("DEPLOYMENT_ENV", "local")
	t.Setenv("WORKER_ROLE", "")
	t.Setenv("TOURNAMENT_INTERNAL_CREDENTIAL", "cred")
	rt, err := wireTournamentRuntime()
	if err != nil {
		t.Fatal(err)
	}
	if rt.mode != "capability" || !rt.ready {
		t.Fatalf("mode=%s ready=%v reason=%s", rt.mode, rt.ready, rt.readyReason)
	}
	_ = os.Getenv
}
