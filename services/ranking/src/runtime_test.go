package main

import "testing"

func TestWireRankingRuntime_CapabilityMemory(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("RANKING_CAPABILITY_MODE", "true")
	t.Setenv("DEPLOYMENT_ENV", "local")
	t.Setenv("RANKING_INTERNAL_CREDENTIAL", "cred")
	rt, err := wireRankingRuntime()
	if err != nil {
		t.Fatal(err)
	}
	if rt.mode != "capability" || !rt.ready {
		t.Fatalf("rt=%+v", rt)
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
