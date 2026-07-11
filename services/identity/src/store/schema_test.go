package store_test

import (
	"os"
	"path/filepath"
	"testing"

	"unoarena/services/identity/store"
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
	t.Setenv("IDENTITY_SCHEMA_CHECKSUM", "")
	t.Setenv("IDENTITY_SCHEMA_VERSION", "")
	exp := store.DefaultSchemaExpectation()
	if exp.MigrationVersion != store.ExpectedMigrationVersion {
		t.Fatalf("version=%q", exp.MigrationVersion)
	}
	if exp.Checksum != store.ExpectedSchemaChecksum {
		t.Fatalf("checksum=%q", exp.Checksum)
	}
}

func TestDefaultSchemaExpectationEnvOverride(t *testing.T) {
	t.Setenv("IDENTITY_SCHEMA_CHECKSUM", "abc123")
	t.Setenv("IDENTITY_SCHEMA_VERSION", "001_custom")
	exp := store.DefaultSchemaExpectation()
	if exp.Checksum != "abc123" || exp.MigrationVersion != "001_custom" {
		t.Fatalf("exp=%+v", exp)
	}
}
