package store_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

const safeRankingTestDBPrefix = "unoarena_ranking_test_"

var forbiddenRankingTestDatabases = map[string]struct{}{
	"ranking":   {},
	"postgres":  {},
	"template0": {},
	"template1": {},
}

func requireSafeRankingTestDatabase(dsn string) error {
	name, err := databaseNameFromPostgresDSN(dsn)
	if err != nil {
		return fmt.Errorf("ranking integration harness: refuse unsafe DSN: %w", err)
	}
	folded := strings.ToLower(strings.TrimSpace(name))
	if folded == "" {
		return fmt.Errorf("ranking integration harness: refuse empty database name; require prefix %s", safeRankingTestDBPrefix)
	}
	if _, banned := forbiddenRankingTestDatabases[folded]; banned {
		return fmt.Errorf("ranking integration harness: refuse database %q; require prefix %s", name, safeRankingTestDBPrefix)
	}
	if !strings.HasPrefix(folded, safeRankingTestDBPrefix) {
		return fmt.Errorf("ranking integration harness: refuse database %q; require prefix %s", name, safeRankingTestDBPrefix)
	}
	suffix := folded[len(safeRankingTestDBPrefix):]
	if suffix == "" {
		return fmt.Errorf("ranking integration harness: refuse database %q; require non-empty suffix after %s", name, safeRankingTestDBPrefix)
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

func TestRequireSafeRankingTestDatabase_RejectsForbiddenNames(t *testing.T) {
	for _, name := range []string{"ranking", "postgres", "template0", "template1", "", "RANKING"} {
		url := "postgres://ranking_runtime:x@127.0.0.1:5432/" + name + "?sslmode=disable"
		if name == "" {
			url = "postgres://ranking_runtime:x@127.0.0.1:5432/?sslmode=disable"
		}
		if err := requireSafeRankingTestDatabase(url); err == nil {
			t.Fatalf("expected reject for database %q", name)
		}
	}
}

func TestRequireSafeRankingTestDatabase_RejectsMissingPrefix(t *testing.T) {
	for _, name := range []string{"ranking_test_abc", "unoarena_ranking_prod", "unoarena_ranking_test"} {
		url := "postgres://ranking_runtime:x@127.0.0.1:5432/" + name + "?sslmode=disable"
		if err := requireSafeRankingTestDatabase(url); err == nil {
			t.Fatalf("expected reject for database %q", name)
		}
	}
}

func TestRequireSafeRankingTestDatabase_AcceptsPrefixed(t *testing.T) {
	url := "postgres://ranking_runtime:x@127.0.0.1:5432/unoarena_ranking_test_abc123?sslmode=disable"
	if err := requireSafeRankingTestDatabase(url); err != nil {
		t.Fatalf("expected accept: %v", err)
	}
}

func TestRequireSafeRankingTestDatabase_KeyValueDSN(t *testing.T) {
	ok := "host=127.0.0.1 port=5432 user=ranking_runtime password=x dbname=unoarena_ranking_test_kv sslmode=disable"
	if err := requireSafeRankingTestDatabase(ok); err != nil {
		t.Fatalf("expected accept: %v", err)
	}
	bad := "host=127.0.0.1 port=5432 user=ranking_runtime password=x dbname=ranking sslmode=disable"
	if err := requireSafeRankingTestDatabase(bad); err == nil {
		t.Fatal("expected reject authoritative ranking dbname")
	}
}
