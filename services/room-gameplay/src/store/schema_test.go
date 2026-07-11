package store_test

import (
	"os"
	"path/filepath"
	"testing"

	"unoarena/services/room-gameplay/store"
)

func TestExpectedSchemaChecksum_MatchesMigrationFile(t *testing.T) {
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

func TestDefaultSchemaExpectation_UsesEmbeddedConstants(t *testing.T) {
	exp := store.DefaultSchemaExpectation()
	if exp.Checksum != store.ExpectedSchemaChecksum {
		t.Fatalf("checksum: got %s want %s", exp.Checksum, store.ExpectedSchemaChecksum)
	}
	if exp.MigrationVersion != store.ExpectedMigrationVersion {
		t.Fatalf("version: got %s want %s", exp.MigrationVersion, store.ExpectedMigrationVersion)
	}
}
