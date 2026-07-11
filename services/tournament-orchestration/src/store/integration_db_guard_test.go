package store_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

const safeTournamentTestDBPrefix = "unoarena_tournament_test_"

var forbiddenTournamentTestDatabases = map[string]struct{}{
	"tournament": {},
	"postgres":   {},
	"template0":  {},
	"template1":  {},
}

func requireSafeTournamentTestDatabase(dsn string) error {
	name, err := databaseNameFromPostgresDSN(dsn)
	if err != nil {
		return fmt.Errorf("tournament integration harness: refuse unsafe DSN: %w", err)
	}
	folded := strings.ToLower(strings.TrimSpace(name))
	if folded == "" {
		return fmt.Errorf("tournament integration harness: refuse empty database name; require prefix %s", safeTournamentTestDBPrefix)
	}
	if _, banned := forbiddenTournamentTestDatabases[folded]; banned {
		return fmt.Errorf("tournament integration harness: refuse database %q; require prefix %s", name, safeTournamentTestDBPrefix)
	}
	if !strings.HasPrefix(folded, safeTournamentTestDBPrefix) {
		return fmt.Errorf("tournament integration harness: refuse database %q; require prefix %s", name, safeTournamentTestDBPrefix)
	}
	suffix := folded[len(safeTournamentTestDBPrefix):]
	if suffix == "" {
		return fmt.Errorf("tournament integration harness: refuse database %q; require non-empty suffix after %s", name, safeTournamentTestDBPrefix)
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

func TestRequireSafeTournamentTestDatabase_RejectsForbiddenNames(t *testing.T) {
	for _, name := range []string{"tournament", "postgres", "template0", "template1", "", "TOURNAMENT"} {
		url := "postgres://tournament_runtime:x@127.0.0.1:5432/" + name + "?sslmode=disable"
		if name == "" {
			url = "postgres://tournament_runtime:x@127.0.0.1:5432/?sslmode=disable"
		}
		if err := requireSafeTournamentTestDatabase(url); err == nil {
			t.Fatalf("expected reject for database %q", name)
		}
	}
}

func TestRequireSafeTournamentTestDatabase_AcceptsPrefixed(t *testing.T) {
	url := "postgres://tournament_runtime:x@127.0.0.1:5432/unoarena_tournament_test_abc123?sslmode=disable"
	if err := requireSafeTournamentTestDatabase(url); err != nil {
		t.Fatalf("expected accept: %v", err)
	}
}
