package store_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"unoarena/services/ranking/store"
)

func TestChecksumBytesMatchesEmbeddedConstant(t *testing.T) {
	path := filepath.Join("..", "..", "migrations", "001_init.sql")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	got := store.ChecksumBytes(b)
	if got != store.ExpectedSchemaChecksum {
		t.Fatalf("checksum mismatch: got %s want %s (update ExpectedSchemaChecksum after migration edits)", got, store.ExpectedSchemaChecksum)
	}
}

func TestDefaultSchemaExpectationUsesEmbedded(t *testing.T) {
	t.Setenv("RANKING_SCHEMA_CHECKSUM", "")
	t.Setenv("RANKING_SCHEMA_VERSION", "")
	exp := store.DefaultSchemaExpectation()
	if exp.MigrationVersion != store.ExpectedMigrationVersion {
		t.Fatalf("version=%q", exp.MigrationVersion)
	}
	if exp.Checksum != store.ExpectedSchemaChecksum {
		t.Fatalf("checksum=%q", exp.Checksum)
	}
}

func TestDefaultSchemaExpectationEnvOverride(t *testing.T) {
	t.Setenv("RANKING_SCHEMA_CHECKSUM", "abc123")
	t.Setenv("RANKING_SCHEMA_VERSION", "001_custom")
	exp := store.DefaultSchemaExpectation()
	if exp.Checksum != "abc123" || exp.MigrationVersion != "001_custom" {
		t.Fatalf("exp=%+v", exp)
	}
}

func TestOutboxPollingCapabilityOnly(t *testing.T) {
	s := store.NewRankingStore(nil)
	if _, err := s.ListPendingOutbox(t.Context(), 10); err != store.ErrCapabilityOnly {
		t.Fatalf("ListPendingOutbox: want ErrCapabilityOnly, got %v", err)
	}
	if err := s.MarkOutboxPublished(t.Context(), "e", time.Now()); err != store.ErrCapabilityOnly {
		t.Fatalf("MarkOutboxPublished: want ErrCapabilityOnly, got %v", err)
	}
}
