package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestProvisioningDifferentialAntiFallback(t *testing.T) {
	srcRoot := "."
	files := []string{
		"service.go",
		"service_provisioning.go",
		"durable_repo.go",
		filepath.Join("store", "provisioning_process.go"),
	}
	forbidden := []string{"BeginExisting", "loadTournamentQ", "persistTournamentTx"}
	for _, rel := range files {
		path := filepath.Join(srcRoot, rel)
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		src := string(b)
		switch {
		case strings.HasSuffix(rel, "service_provisioning.go"):
			for _, fname := range []string{
				"submitCommandProvisioningDifferential",
				"processProvisioningBatchDifferential",
			} {
				fn := extractFuncBody(t, path, fname)
				for _, bad := range forbidden {
					if strings.Contains(fn, bad) {
						t.Fatalf("%s must not reference %s", fname, bad)
					}
				}
				if strings.Contains(fn, "s.mu.") {
					t.Fatalf("%s must not use Service.mu", fname)
				}
			}
			if !strings.Contains(src, "ManualRetryProvisioningBatch") {
				t.Fatal("differential provisioning must expose ManualRetryProvisioningBatch")
			}
			if !strings.Contains(src, "ManualQuarantineProvisioningBatch") {
				t.Fatal("differential provisioning must expose ManualQuarantineProvisioningBatch")
			}
		case strings.HasSuffix(rel, "service.go"):
			legacy := extractFuncBody(t, path, "processProvisioningBatchLegacy")
			if !strings.Contains(legacy, "s.mu.Lock()") {
				t.Fatal("processProvisioningBatchLegacy must retain Service.mu")
			}
			if !strings.Contains(legacy, "BeginExisting") {
				t.Fatal("processProvisioningBatchLegacy may use BeginExisting as memory fallback")
			}
		case strings.Contains(rel, "provisioning_process.go"):
			for _, bad := range []string{"persistTournamentTx", "loadTournamentQ"} {
				if strings.Contains(src, bad) {
					t.Fatalf("store provisioning_process must not contain %q", bad)
				}
			}
			retry := extractFuncBody(t, path, "ManualRetryProvisioningBatch")
			if !strings.Contains(retry, "AcquireCommandLock") {
				t.Fatal("ManualRetry must take command lock")
			}
			if !strings.Contains(retry, "retry_budget_exhausted") {
				t.Fatal("ManualRetry must use sanitized retry_budget_exhausted")
			}
			// Exact no-op for status=retried + explicit == current.
			if !strings.Contains(retry, "BatchRetried") || !strings.Contains(retry, "retryAttempt == curAttempt") {
				t.Fatal("ManualRetry must accept exact no-op when status=retried and attempt==current")
			}
			if !strings.Contains(retry, "status IN ('provisioning', 'in_progress', 'seeded')") {
				t.Fatal("ManualRetry budget quarantine must block seeded/provisioning/in_progress rounds")
			}
			if strings.Contains(retry, "status = 'provisioning'") &&
				!strings.Contains(retry, "status IN ('provisioning', 'in_progress', 'seeded')") {
				t.Fatal("ManualRetry must not only transition provisioning rounds")
			}
			quarantine := extractFuncBody(t, path, "ManualQuarantineProvisioningBatch")
			if !strings.Contains(quarantine, "roundTag.RowsAffected() == 1") {
				t.Fatal("ManualQuarantine must bump projection only on round→blocked transition")
			}
			if strings.Contains(quarantine, "roundTag.RowsAffected() == 1 || tag.RowsAffected() == 1") {
				t.Fatal("ManualQuarantine must not bump on batch-only change")
			}
		case strings.HasSuffix(rel, "durable_repo.go"):
			fn := extractTypeMethods(t, path, "durableProvisioningRepo")
			for _, bad := range forbidden {
				if strings.Contains(fn, bad) {
					t.Fatalf("durableProvisioningRepo must not reference %s", bad)
				}
			}
		}
	}
}
