package store_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestSessionInvalidationApplyLua_RestoresMissingSessionHash(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("caller")
	}
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(file), "si_lua.go"))
	if err != nil {
		t.Fatal(err)
	}
	src := string(raw)
	for _, needle := range []string{
		"return 'restored'",
		"return 'accepted'",
		"return 'duplicate'",
		"return 'conflict'",
		"boundPlayer",
		"boundReason",
		"PUBLISH",
		"EXPIRE",
		// Must not treat missing session hash as a silent duplicate.
		"re-establish admission protection",
	} {
		if !strings.Contains(src, needle) {
			t.Fatalf("si_lua.go missing %q", needle)
		}
	}
	if strings.Contains(src, "session hash missing is treated as duplicate") {
		t.Fatal("stale duplicate-on-missing-session path must be removed")
	}
}
