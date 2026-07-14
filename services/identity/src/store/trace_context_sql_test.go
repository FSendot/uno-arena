package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestIdentityOutboxSQLPersistsNullableTraceContext(t *testing.T) {
	assertOutboxTraceColumns(t, "postgres_session.go", "INSERT INTO outbox_events")
	assertMigrationHasNullableTraceColumns(t, filepath.Join("..", "..", "migrations", "001_init.sql"), 1)
}

func assertOutboxTraceColumns(t *testing.T, file, marker string) {
	t.Helper()
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	count := 0
	for offset := 0; ; {
		idx := strings.Index(source[offset:], marker)
		if idx < 0 {
			break
		}
		idx += offset
		end := strings.Index(source[idx:], ")")
		if end < 0 {
			t.Fatalf("unterminated outbox insert in %s", file)
		}
		columns := source[idx : idx+end]
		if !strings.Contains(columns, "traceparent") || !strings.Contains(columns, "tracestate") {
			t.Fatalf("outbox insert %d in %s omits trace context columns", count+1, file)
		}
		count++
		offset = idx + end + 1
	}
	if count == 0 {
		t.Fatalf("no %q inserts found in %s", marker, file)
	}
}

func assertMigrationHasNullableTraceColumns(t *testing.T, file string, tables int) {
	t.Helper()
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	if got := strings.Count(source, "traceparent TEXT,"); got != tables {
		t.Fatalf("traceparent nullable column count=%d want=%d", got, tables)
	}
	if got := strings.Count(source, "tracestate TEXT,"); got != tables {
		t.Fatalf("tracestate nullable column count=%d want=%d", got, tables)
	}
	if strings.Contains(source, "traceparent TEXT NOT NULL") || strings.Contains(source, "tracestate TEXT NOT NULL") {
		t.Fatal("outbox trace context must remain nullable")
	}
}
