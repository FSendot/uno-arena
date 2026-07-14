package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEveryTournamentOutboxWriterPersistsTraceContext(t *testing.T) {
	for _, file := range []string{"postgres.go", "provisioning_process.go"} {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		source := string(raw)
		marker := "INSERT INTO outbox_events"
		idx := strings.Index(source, marker)
		if idx < 0 {
			t.Fatalf("no outbox insert in %s", file)
		}
		end := strings.Index(source[idx:], ")")
		columns := source[idx : idx+end]
		if !strings.Contains(columns, "traceparent") || !strings.Contains(columns, "tracestate") {
			t.Fatalf("outbox insert in %s omits trace context", file)
		}
	}

	raw, err := os.ReadFile(filepath.Join("..", "..", "migrations", "001_init.sql"))
	if err != nil {
		t.Fatal(err)
	}
	schema := string(raw)
	if strings.Count(schema, "traceparent TEXT,") != 1 || strings.Count(schema, "tracestate TEXT,") != 1 {
		t.Fatal("tournament outbox migration must define one nullable traceparent/tracestate pair")
	}
	if strings.Contains(schema, "traceparent TEXT NOT NULL") || strings.Contains(schema, "tracestate TEXT NOT NULL") {
		t.Fatal("outbox trace context must remain nullable")
	}
}
