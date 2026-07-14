package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRankingOutboxSQLPersistsNullableTraceContext(t *testing.T) {
	assertRankingOutboxTraceColumns(t, "outbox.go", "INSERT INTO outbox_events")
	assertRankingMigrationTraceColumns(t, filepath.Join("..", "..", "migrations", "001_init.sql"))
}

func assertRankingOutboxTraceColumns(t *testing.T, file, marker string) {
	t.Helper()
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	idx := strings.Index(source, marker)
	if idx < 0 {
		t.Fatalf("no %q insert found in %s", marker, file)
	}
	end := strings.Index(source[idx:], ")")
	columns := source[idx : idx+end]
	if !strings.Contains(columns, "traceparent") || !strings.Contains(columns, "tracestate") {
		t.Fatalf("outbox insert in %s omits trace context columns", file)
	}
}

func assertRankingMigrationTraceColumns(t *testing.T, file string) {
	t.Helper()
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	if strings.Count(source, "traceparent TEXT,") != 1 || strings.Count(source, "tracestate TEXT,") != 1 {
		t.Fatal("ranking outbox migration must define one nullable traceparent/tracestate pair")
	}
	if strings.Contains(source, "traceparent TEXT NOT NULL") || strings.Contains(source, "tracestate TEXT NOT NULL") {
		t.Fatal("outbox trace context must remain nullable")
	}
}
