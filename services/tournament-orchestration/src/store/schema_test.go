package store_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"unoarena/services/tournament-orchestration/store"
)

func TestChecksumBytesMatchesEmbeddedConstant(t *testing.T) {
	path := filepath.Join("..", "..", "migrations", "001_init.sql")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	got := store.ChecksumBytes(b)
	if got != store.ExpectedSchemaChecksum {
		t.Fatalf("checksum mismatch: got %s want %s", got, store.ExpectedSchemaChecksum)
	}
}

func TestDefaultSchemaExpectationUsesEmbedded(t *testing.T) {
	t.Setenv("TOURNAMENT_SCHEMA_CHECKSUM", "")
	t.Setenv("TOURNAMENT_SCHEMA_VERSION", "")
	exp := store.DefaultSchemaExpectation()
	if exp.MigrationVersion != store.ExpectedMigrationVersion {
		t.Fatalf("version=%q", exp.MigrationVersion)
	}
	if exp.Checksum != store.ExpectedSchemaChecksum {
		t.Fatalf("checksum=%q", exp.Checksum)
	}
}

func TestOutboxPollingIsCapabilityOnly(t *testing.T) {
	s := store.NewTournamentStore(nil)
	if _, err := s.ListPendingOutbox(context.Background(), 10); !errors.Is(err, store.ErrCapabilityOnly) {
		t.Fatalf("ListPendingOutbox: %v", err)
	}
	if err := s.MarkOutboxPublished(context.Background(), "e", time.Now()); !errors.Is(err, store.ErrCapabilityOnly) {
		t.Fatalf("MarkOutboxPublished: %v", err)
	}
}
