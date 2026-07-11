package store_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Required database-name prefix for Identity Postgres integration harnesses.
// Authoritative / system databases must never be reset by these tests.
const safeIdentityTestDBPrefix = "unoarena_identity_test_"

var forbiddenIdentityTestDatabases = map[string]struct{}{
	"identity":  {},
	"postgres":  {},
	"template0": {},
	"template1": {},
}

// requireSafeIdentityTestDatabase fails closed unless the DSN database name is
// explicitly prefixed with unoarena_identity_test_. Call before any mutation.
func requireSafeIdentityTestDatabase(dsn string) error {
	name, err := databaseNameFromPostgresDSN(dsn)
	if err != nil {
		return fmt.Errorf("identity integration harness: refuse unsafe DSN: %w", err)
	}
	folded := strings.ToLower(strings.TrimSpace(name))
	if folded == "" {
		return fmt.Errorf("identity integration harness: refuse empty database name; require prefix %s", safeIdentityTestDBPrefix)
	}
	if _, banned := forbiddenIdentityTestDatabases[folded]; banned {
		return fmt.Errorf("identity integration harness: refuse database %q; require prefix %s", name, safeIdentityTestDBPrefix)
	}
	if !strings.HasPrefix(folded, safeIdentityTestDBPrefix) {
		return fmt.Errorf("identity integration harness: refuse database %q; require prefix %s", name, safeIdentityTestDBPrefix)
	}
	suffix := folded[len(safeIdentityTestDBPrefix):]
	if suffix == "" {
		return fmt.Errorf("identity integration harness: refuse database %q; require non-empty suffix after %s", name, safeIdentityTestDBPrefix)
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

func TestRequireSafeIdentityTestDatabase_RejectsForbiddenNames(t *testing.T) {
	forbidden := []string{
		"identity",
		"postgres",
		"template0",
		"template1",
		"",
		"IDENTITY",
		"Postgres",
	}
	for _, name := range forbidden {
		url := "postgres://identity_runtime:x@127.0.0.1:5432/" + name + "?sslmode=disable"
		if name == "" {
			url = "postgres://identity_runtime:x@127.0.0.1:5432/?sslmode=disable"
		}
		err := requireSafeIdentityTestDatabase(url)
		if err == nil {
			t.Fatalf("expected reject for database %q", name)
		}
	}
}

func TestRequireSafeIdentityTestDatabase_RejectsMissingPrefix(t *testing.T) {
	names := []string{
		"identity_test_abc",
		"unoarena_identity_prod",
		"unoarena_identity_test", // prefix requires trailing underscore + suffix
		"other_unoarena_identity_test_x",
	}
	for _, name := range names {
		url := "postgres://identity_runtime:x@127.0.0.1:5432/" + name + "?sslmode=disable"
		if err := requireSafeIdentityTestDatabase(url); err == nil {
			t.Fatalf("expected reject for database %q", name)
		}
	}
}

func TestRequireSafeIdentityTestDatabase_AcceptsPrefixed(t *testing.T) {
	url := "postgres://identity_runtime:x@127.0.0.1:5432/unoarena_identity_test_abc123?sslmode=disable"
	if err := requireSafeIdentityTestDatabase(url); err != nil {
		t.Fatalf("expected accept: %v", err)
	}
}

func TestRequireSafeIdentityTestDatabase_KeyValueDSN(t *testing.T) {
	ok := "host=127.0.0.1 port=5432 user=identity_runtime password=x dbname=unoarena_identity_test_kv sslmode=disable"
	if err := requireSafeIdentityTestDatabase(ok); err != nil {
		t.Fatalf("expected accept key/value DSN: %v", err)
	}
	bad := "host=127.0.0.1 port=5432 user=identity_runtime password=x dbname=identity sslmode=disable"
	if err := requireSafeIdentityTestDatabase(bad); err == nil {
		t.Fatal("expected reject authoritative identity dbname")
	}
}

func TestRequireSafeIdentityTestDatabase_ErrorMentionsSafety(t *testing.T) {
	err := requireSafeIdentityTestDatabase("postgres://u:p@127.0.0.1:5432/identity?sslmode=disable")
	if err == nil {
		t.Fatal("expected error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "unoarena_identity_test_") {
		t.Fatalf("error should mention required prefix, got %q", msg)
	}
}
