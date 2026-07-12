package store_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"unoarena/services/tournament-orchestration/store"
)

func TestRewriteBarrierUsesHashtextextended(t *testing.T) {
	if !store.RewriteBarrierUsesHashtextextended() {
		t.Fatal("rewrite barrier must use hashtextextended 64-bit keys")
	}
	b, err := os.ReadFile(filepath.Join("barrier.go"))
	if err != nil {
		t.Fatal(err)
	}
	src := string(b)
	if !strings.Contains(src, "hashtextextended") {
		t.Fatal("barrier.go must reference hashtextextended")
	}
	if strings.Contains(src, "pg_advisory_xact_lock(hashtext(") ||
		strings.Contains(src, "pg_advisory_xact_lock_shared(hashtext(") {
		t.Fatal("barrier.go must not use 32-bit hashtext advisory locks")
	}
	if !strings.Contains(src, "tournament:rewrite:") {
		t.Fatal("barrier namespace must remain tournament:rewrite:")
	}
}
