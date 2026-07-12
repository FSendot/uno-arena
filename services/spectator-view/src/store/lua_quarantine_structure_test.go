package store_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestApplyCommitLua_IncludesQuarantineSlot(t *testing.T) {
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
		"KEYS[5]=kafka_quarantine",
		"local quarantineKey = KEYS[5]",
		"markQuarantine",
		"released_at",
		"kafkaQuarantineScript",
	} {
		if !strings.Contains(src, needle) {
			t.Fatalf("lua.go missing %q", needle)
		}
	}
}
