package store_test

import (
	"strings"
	"testing"

	"unoarena/services/analytics/store"
)

func TestValidateSafeTestDatabase_RejectsForbidden(t *testing.T) {
	for _, name := range []string{"analytics", "default", "system", "", "ANALYTICS", "unoarena_analytics_test_", "unoarena_analytics_test_ABC", "unoarena_analytics_test_ab-cd", "evil;drop"} {
		if err := store.ValidateSafeTestDatabase(name); err == nil {
			t.Fatalf("expected reject for %q", name)
		}
	}
}

func TestValidateSafeTestDatabase_AcceptsPrefixedHex(t *testing.T) {
	if err := store.ValidateSafeTestDatabase("unoarena_analytics_test_deadbeefcafebabe"); err != nil {
		t.Fatal(err)
	}
}

func TestTransformMigrationForDatabase_SafeOnly(t *testing.T) {
	src := "CREATE DATABASE IF NOT EXISTS analytics;\nCREATE TABLE analytics.gameplay_metrics (x String) ENGINE = Log;"
	out, err := store.TransformMigrationForDatabase(src, "unoarena_analytics_test_abc123")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out, "analytics.") || strings.Contains(out, "EXISTS analytics") {
		t.Fatalf("residual analytics identifier: %s", out)
	}
	if !strings.Contains(out, "unoarena_analytics_test_abc123.gameplay_metrics") {
		t.Fatalf("missing rewritten table: %s", out)
	}
	if _, err := store.TransformMigrationForDatabase(src, "analytics"); err == nil {
		t.Fatal("must refuse analytics")
	}
}

func TestTransformMigrationForDatabase_Placeholder(t *testing.T) {
	src := "CREATE DATABASE IF NOT EXISTS __UNOARENA_ANALYTICS_DB__;\nCREATE TABLE __UNOARENA_ANALYTICS_DB__.t (x String) ENGINE = Log;"
	out, err := store.TransformMigrationForDatabase(src, "unoarena_analytics_test_ff00")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out, "__UNOARENA_ANALYTICS_DB__") || strings.Contains(out, "analytics.") {
		t.Fatalf("bad transform: %s", out)
	}
}
