package store_test

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"unoarena/services/spectator-view/store"
)

func TestValidateQuarantineReleaseNote_Allowlist(t *testing.T) {
	t.Parallel()
	for _, note := range []string{
		store.ReleaseNoteRecoveryContinuityProven,
		store.ReleaseNoteRebuildJobComplete,
	} {
		if err := store.ValidateQuarantineReleaseNote(note); err != nil {
			t.Fatalf("%s: %v", note, err)
		}
	}
	for _, bad := range []string{
		"",
		"operator said it is fine",
		"recovery_continuity_proven; DROP TABLE",
		"RECOVERY_CONTINUITY_PROVEN",
	} {
		err := store.ValidateQuarantineReleaseNote(bad)
		if !errors.Is(err, store.ErrInvalidReleaseNote) {
			t.Fatalf("%q: got %v", bad, err)
		}
	}
}

func TestRecoveryRebuildLua_DeclaresCASFenceAndRelease(t *testing.T) {
	t.Parallel()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("caller")
	}
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(file), "lua.go"))
	if err != nil {
		t.Fatal(err)
	}
	src := string(raw)
	for _, needle := range []string{
		"recoveryRebuildSwapScript",
		"kafkaQuarantineReleaseScript",
		"return 'stale'",
		"return 'conflict'",
		"return 'already_done'",
		"expectedGeneration",
		"expectedSequence",
		"releaseQuarantine",
		"release_note",
		"KEYS[6]=kafka_quarantine",
		"KEYS[7]=rebuild_done",
	} {
		if !strings.Contains(src, needle) {
			t.Fatalf("lua.go missing %q", needle)
		}
	}
	// Legacy HTTP rebuild remains unconditional (separate script).
	if !strings.Contains(src, "rebuildSwapScript") {
		t.Fatal("legacy rebuildSwapScript must remain")
	}
}

func TestRecoveryRebuildArgs_FenceBeforeMutationFields(t *testing.T) {
	t.Parallel()
	// Documents ARGV layout expected by recoveryRebuildSwapScript for callers/tests.
	// ARGV[1]=expectedGen ARGV[2]=expectedSeq ARGV[3]=newGen … release trailing.
	order := []string{
		"fenceGen", "fenceSeq", "newGen", "revision", "sequence", "status",
		"streamClosed", "eventCount", "stateJSON", "outcomeCount",
		/* pairs… */ "appendStream…", "releaseFlag", "releaseNote", "releasedAt",
		"markDone", "ttlSeconds",
	}
	if len(order) < 10 {
		t.Fatal("layout regression")
	}
	if order[0] != "fenceGen" || order[1] != "fenceSeq" {
		t.Fatal("CAS fence must be first argv pair")
	}
	if order[len(order)-5] != "releaseFlag" {
		t.Fatal("quarantine release must trail stream fields")
	}
	if order[len(order)-2] != "markDone" || order[len(order)-1] != "ttlSeconds" {
		t.Fatal("atomic idempotency marker args must trail release")
	}
}
