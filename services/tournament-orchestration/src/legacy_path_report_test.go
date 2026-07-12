package main

import (
	"strings"
	"testing"
)

func TestLegacyWholeRewriteCommandsStillDocumented(t *testing.T) {
	// After T-Reg + T3 + T7 + T9 + T10 + T-Q + T11, production durable runtimes never
	// reach whole-aggregate mutation: durableRepo BeginExisting/BeginCreate/Commit
	// fail closed with ErrDurableWholeAggregateMutationDisabled.
	// Legacy aggregate helpers below remain for memory/capability-mode only.
	legacyCapabilityMemoryOnly := []string{
		CmdRecordMatchResult,             // memory/capability HTTP only — durable uses RoundMatch differential
		"ingestMatchCompletedLegacy",     // memory/capability Kafka/offline ingest
		"processProvisioningBatchLegacy", // memory/capability provisioning worker
	}
	removedFromDurableProduction := []string{
		CmdCreateTournament, CmdRegisterPlayer, CmdCloseRegistration, CmdSeedRound,
		CmdProvisionRoundMatches, CmdRetryProvisioning, CmdQuarantineBatch,
		CmdCompleteRound,
		CmdCompleteTournament, CmdCancelTournament,
		CmdQuarantineResult,
		"ProcessProvisioningBatch",
	}
	joined := strings.Join(legacyCapabilityMemoryOnly, ",")
	for _, gone := range removedFromDurableProduction {
		if strings.Contains(joined, gone) {
			t.Fatalf("%s must not remain on durable production whole-rewrite list", gone)
		}
	}
	if !strings.Contains(joined, "ingestMatchCompletedLegacy") {
		t.Fatal("memory/capability Kafka ingest must remain named as capability-memory-only legacy")
	}
	if !strings.Contains(joined, "processProvisioningBatchLegacy") {
		t.Fatal("memory/capability provisioning worker must remain named as capability-memory-only legacy")
	}
	if strings.Contains(joined, CmdSeedRound) {
		t.Fatal("SeedRound must not remain listed as durable production legacy whole rewrite")
	}

	// Production durableRepo must not bridge to store whole-aggregate UoWs.
	dr := extractTypeMethods(t, "durable_repo.go", "durableRepo")
	for _, bad := range []string{"r.store.BeginExisting", "r.store.BeginCreate", "toStoreCommit", "durableUoW"} {
		if strings.Contains(dr, bad) {
			t.Fatalf("production durableRepo must not reach store whole-aggregate via %s", bad)
		}
	}
	commit := methodBodyForRecv(t, "durable_repo.go", "*durableRepo", "Commit")
	if strings.Contains(commit, "r.store.Commit") {
		t.Fatal("production durableRepo.Commit must not call store.Commit")
	}
	if !strings.Contains(commit, "ErrDurableWholeAggregateMutationDisabled") {
		t.Fatal("production durableRepo.Commit must fail closed with sentinel")
	}
}
