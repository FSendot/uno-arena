package store_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

const safeRoomTestDBPrefix = "unoarena_room_gameplay_test_"

var forbiddenRoomTestDatabases = map[string]struct{}{
	"room_gameplay": {},
	"postgres":      {},
	"template0":     {},
	"template1":     {},
}

func requireSafeRoomTestDatabase(dsn string) error {
	name, err := databaseNameFromPostgresDSN(dsn)
	if err != nil {
		return fmt.Errorf("room integration harness: refuse unsafe DSN: %w", err)
	}
	folded := strings.ToLower(strings.TrimSpace(name))
	if folded == "" {
		return fmt.Errorf("room integration harness: refuse empty database name; require prefix %s", safeRoomTestDBPrefix)
	}
	if _, banned := forbiddenRoomTestDatabases[folded]; banned {
		return fmt.Errorf("room integration harness: refuse database %q; require prefix %s", name, safeRoomTestDBPrefix)
	}
	if !strings.HasPrefix(folded, safeRoomTestDBPrefix) {
		return fmt.Errorf("room integration harness: refuse database %q; require prefix %s", name, safeRoomTestDBPrefix)
	}
	suffix := folded[len(safeRoomTestDBPrefix):]
	if suffix == "" {
		return fmt.Errorf("room integration harness: refuse database %q; require non-empty suffix after %s", name, safeRoomTestDBPrefix)
	}
	return nil
}

func databaseNameFromPostgresDSN(dsn string) (string, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return "", err
	}
	return cfg.ConnConfig.Database, nil
}

func TestRequireSafeRoomTestDatabase_RejectsForbiddenNames(t *testing.T) {
	for _, name := range []string{"room_gameplay", "postgres", "template0", "template1", "", "ROOM_GAMEPLAY"} {
		url := "postgres://room_runtime:x@127.0.0.1:5432/" + name + "?sslmode=disable"
		if name == "" {
			url = "postgres://room_runtime:x@127.0.0.1:5432/?sslmode=disable"
		}
		if err := requireSafeRoomTestDatabase(url); err == nil {
			t.Fatalf("expected reject for database %q", name)
		}
	}
}

func TestRequireSafeRoomTestDatabase_RejectsMissingPrefix(t *testing.T) {
	for _, name := range []string{"room_test_abc", "unoarena_room_gameplay_test", "other_unoarena_room_gameplay_test_x"} {
		url := "postgres://room_runtime:x@127.0.0.1:5432/" + name + "?sslmode=disable"
		if err := requireSafeRoomTestDatabase(url); err == nil {
			t.Fatalf("expected reject for database %q", name)
		}
	}
}

func TestRequireSafeRoomTestDatabase_AcceptsPrefixed(t *testing.T) {
	url := "postgres://room_runtime:x@127.0.0.1:5432/unoarena_room_gameplay_test_abc123?sslmode=disable"
	if err := requireSafeRoomTestDatabase(url); err != nil {
		t.Fatalf("expected accept: %v", err)
	}
}
