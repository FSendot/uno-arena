package store

import (
	"os"
	"strings"
	"testing"
	"time"

	"unoarena/services/tournament-orchestration/domain"
)

func TestProvisioningLeaseForRewrite_PreservesInProgressClearsTerminal(t *testing.T) {
	prev := provisioningLeaseSnapshot{
		Owner:   "worker-b",
		Expires: time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC),
		Version: 7,
	}
	owner, exp, ver := provisioningLeaseForRewrite(domain.ProvisioningBatch{Status: domain.BatchInProgress}, prev)
	if owner != "worker-b" || exp != prev.Expires || ver != int64(7) {
		t.Fatalf("in_progress preserve: owner=%v exp=%v ver=%v", owner, exp, ver)
	}
	for _, st := range []domain.BatchStatus{
		domain.BatchCompleted, domain.BatchRetried, domain.BatchQuarantined,
		domain.BatchCancelled, domain.BatchPending,
	} {
		owner, exp, ver := provisioningLeaseForRewrite(domain.ProvisioningBatch{Status: st}, prev)
		if owner != nil || exp != nil || ver != int64(0) {
			t.Fatalf("status %s must clear lease, got owner=%v exp=%v ver=%v", st, owner, exp, ver)
		}
	}
	owner, exp, ver = provisioningLeaseForRewrite(domain.ProvisioningBatch{Status: domain.BatchInProgress}, provisioningLeaseSnapshot{})
	if owner != nil || exp != nil || ver != int64(0) {
		t.Fatalf("missing prior lease must not invent one: %v %v %v", owner, exp, ver)
	}
}

func TestClaimNextSource_TournamentFirstLockOrdering(t *testing.T) {
	b, err := os.ReadFile("provisioning_claim.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(b)
	tourIdx := strings.Index(src, "FROM tournaments t")
	batchIdx := strings.Index(src, "FROM provisioning_batches b")
	if tourIdx < 0 || batchIdx < 0 {
		t.Fatal("expected tournament then batch SELECT sources")
	}
	if tourIdx > batchIdx {
		t.Fatal("ClaimNext must lock tournament before selecting batch")
	}
	if !strings.Contains(src, "FOR UPDATE OF t SKIP LOCKED") {
		t.Fatal("missing tournament FOR UPDATE SKIP LOCKED")
	}
	if !strings.Contains(src, "r.status = 'provisioning'") {
		t.Fatal("claims must require provisioning round")
	}
	if !strings.Contains(src, "t.phase NOT IN ('completed', 'cancelled')") {
		t.Fatal("claims must require nonterminal tournament")
	}
	if !strings.Contains(src, "DefaultProvisioningReapLimit") {
		t.Fatal("reaper must be bounded")
	}
	if !strings.Contains(src, "lease_version = b.lease_version + 1") {
		t.Fatal("claim must atomically bump lease_version")
	}
	if !strings.Contains(src, "RETURNING") || !strings.Contains(src, "b.lease_version") {
		t.Fatal("claim must RETURNING lease_version")
	}
}

func TestPersistSource_PreservesActiveLeases(t *testing.T) {
	b, err := os.ReadFile("persist.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(b)
	if !strings.Contains(src, "loadActiveProvisioningLeases") {
		t.Fatal("persist must snapshot leases before delete")
	}
	if !strings.Contains(src, "lease_owner, lease_expires_at, lease_version") {
		t.Fatal("persist insert must include lease_version")
	}
	if !strings.Contains(src, "provisioningLeaseForRewrite") {
		t.Fatal("persist must route lease preserve/clear policy")
	}
}

func TestPrepareSource_BulkAndFenceGuards(t *testing.T) {
	b, err := os.ReadFile("provisioning_process.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(b)
	if !strings.Contains(src, "jsonb_to_recordset") {
		t.Fatal("prepare must use set-based jsonb_to_recordset ops")
	}
	if strings.Contains(src, "jsonEqualIgnoreOccurredAt") {
		t.Fatal("must not loosely ignore occurredAt with reconstructed payload")
	}
	if !strings.Contains(src, "outboxImmutableFieldsMatch") {
		t.Fatal("must verify immutable outbox business fields")
	}
	if !strings.Contains(src, "SAVEPOINT prepare_bulk") {
		t.Fatal("bulk prepare mutations must run under a transaction savepoint")
	}
	if !strings.Contains(src, "ROLLBACK TO SAVEPOINT prepare_bulk") {
		t.Fatal("conflict path must ROLLBACK TO SAVEPOINT before quarantine")
	}
	if !strings.Contains(src, "tournament.match.assigned") || !strings.Contains(src, "SchemaVersion") {
		t.Fatal("outbox immutability must check canonical row topic/schema_version")
	}
	if !strings.Contains(src, "ErrProvisioningFence") {
		t.Fatal("fence mismatches must return ErrProvisioningFence")
	}
	if strings.Contains(src, "room_provision_failed:") {
		t.Fatal("sanitize must never append raw text")
	}
	if !strings.Contains(src, "lease_version = $") {
		t.Fatal("finalize/heartbeat predicates must include lease_version")
	}
	if !strings.Contains(src, "AND retry_attempt = $") {
		t.Fatal("finalize/heartbeat predicates must include retry_attempt")
	}
	hbIdx := strings.Index(src, "func (s *TournamentStore) HeartbeatProvisioningLease")
	if hbIdx < 0 {
		t.Fatal("missing HeartbeatProvisioningLease")
	}
	hbChunk := src[hbIdx:]
	if end := strings.Index(hbChunk, "\nfunc "); end > 0 {
		hbChunk = hbChunk[:end]
	}
	if !strings.Contains(hbChunk, "retry_attempt") {
		t.Fatal("heartbeat SQL must fence on retry_attempt")
	}
}

func TestVerifyBatchPlanSource_NumericOrder(t *testing.T) {
	b, err := os.ReadFile("provisioning.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(b)
	if !strings.Contains(src, "regexp_replace(slot_id_from") {
		t.Fatal("verifyBatchPlan must ORDER BY numeric slot range, not batch_id ASC")
	}
	if strings.Contains(src, "ORDER BY batch_id ASC") {
		t.Fatal("lexicographic batch_id order is forbidden")
	}
}
