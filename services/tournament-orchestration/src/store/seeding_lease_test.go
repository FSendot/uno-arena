package store

import (
	"os"
	"strings"
	"testing"
)

func TestSanitizeSeedingReason_AllowlistOnly(t *testing.T) {
	for _, code := range []string{
		"phase_drift",
		"immutable_plan_drift",
		"source_count_shortfall",
		"unknown",
	} {
		if got := sanitizeSeedingReason(code); got != code {
			t.Fatalf("%q: got %q", code, got)
		}
	}
	for _, raw := range []string{
		"",
		"  ",
		"pq: duplicate key value violates unique constraint",
		"SELECT * FROM secrets WHERE key='x'",
		"immutable_plan_drift; DROP TABLE students",
		"phase_drift_extra",
		strings.Repeat("a", 250),
	} {
		if got := sanitizeSeedingReason(raw); got != "unknown" {
			t.Fatalf("raw %q must map to unknown, got %q", raw, got)
		}
	}
}

func TestClaimNextSeedingSource_LeaseVersionFence(t *testing.T) {
	b, err := os.ReadFile("seeding.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(b)
	claim := extractStoreFuncBody(t, src, "ClaimNextSeedingJob")
	if !strings.Contains(claim, "lease_version = j.lease_version + 1") {
		t.Fatal("claim must atomically bump lease_version")
	}
	if !strings.Contains(claim, "RETURNING") || !strings.Contains(claim, "j.lease_version") {
		t.Fatal("claim must RETURNING lease_version")
	}
	if !strings.Contains(src, "LeaseVersion") {
		t.Fatal("ClaimedSeedingJob must carry LeaseVersion")
	}

	reap := extractStoreFuncBody(t, src, "ReapExpiredSeedingLeasesBounded")
	updIdx := strings.Index(reap, "UPDATE round_seeding_jobs")
	if updIdx < 0 {
		t.Fatal("reap must UPDATE round_seeding_jobs")
	}
	upd := reap[updIdx:]
	if strings.Contains(upd, "lease_version") {
		t.Fatal("reap must not mutate lease_version")
	}

	chunk := extractStoreFuncBody(t, src, "ProcessSeedingChunk")
	if !strings.Contains(chunk, "lease_version") {
		t.Fatal("ProcessSeedingChunk must fence on lease_version")
	}
	if !strings.Contains(chunk, "AND lease_version = $") && !strings.Contains(chunk, "lease_version = $") {
		// checkpoint UPDATE uses lease_version = $8 style
		if !strings.Contains(chunk, "AND lease_version =") {
			t.Fatal("chunk checkpoint mutation must predicate on lease_version")
		}
	}

	fin := extractStoreFuncBody(t, src, "finalizeSeedingInTx")
	if !strings.Contains(fin, "AND lease_version =") {
		t.Fatal("finalize must predicate on lease_version")
	}

	q := extractStoreFuncBody(t, src, "quarantineSeedingTx")
	if !strings.Contains(q, "AND lease_version =") || !strings.Contains(q, "AND lease_owner =") {
		t.Fatal("quarantine from claimed path must be lease-fenced")
	}
	if strings.Contains(q, "status IN ('pending', 'in_progress')") {
		t.Fatal("claimed quarantine must not widen to pending")
	}

	c := extractStoreFuncBody(t, src, "cancelSeedingTx")
	if !strings.Contains(c, "AND lease_version =") || !strings.Contains(c, "AND lease_owner =") {
		t.Fatal("cancel from claimed path must be lease-fenced")
	}

	san := extractStoreFuncBody(t, src, "sanitizeSeedingReason")
	if strings.Contains(san, "reason[:") || strings.Contains(san, "[:200]") {
		t.Fatal("sanitize must not truncate arbitrary strings")
	}
	if !strings.Contains(src, "seedingQuarantineReasons") {
		t.Fatal("sanitize must use closed allowlist")
	}
}

func TestInsertNextRoundSeedingSource_FullIdentity(t *testing.T) {
	b, err := os.ReadFile("complete_round.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(b)
	start := strings.Index(src, "func (u *CompleteRoundUnitOfWork) insertNextRoundSeeding")
	if start < 0 {
		t.Fatal("insertNextRoundSeeding not found")
	}
	body := src[start:]
	if end := strings.Index(body[1:], "\nfunc "); end > 0 {
		body = body[:end+1]
	}
	if !strings.Contains(body, "SeedingJobPending") || !strings.Contains(body, "JobCommandID") {
		t.Fatal("existing next job must require pending status and command_id")
	}
	if strings.Contains(body, "!identityOK && !planOK") {
		t.Fatal("plan-only match must not accept wrong status/command_id")
	}
	if !strings.Contains(body, "if !identityOK") {
		t.Fatal("any identity mismatch must fail closed")
	}
}
