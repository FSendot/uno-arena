package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEveryRoomOutboxWriterPersistsTraceContext(t *testing.T) {
	cases := []struct {
		file   string
		marker string
		want   int
	}{
		{"postgres_persist.go", "INSERT INTO realtime_outbox_events", 1},
		{"postgres_persist.go", "INSERT INTO integration_outbox_events", 1},
		{"reconcile.go", "INSERT INTO integration_outbox_events", 1},
		{"runtime_assignment.go", "INSERT INTO integration_outbox_events", 1},
	}
	for _, tc := range cases {
		t.Run(tc.file+tc.marker, func(t *testing.T) {
			assertRoomTraceColumns(t, tc.file, tc.marker, tc.want)
		})
	}

	raw, err := os.ReadFile(filepath.Join("..", "..", "migrations", "001_init.sql"))
	if err != nil {
		t.Fatal(err)
	}
	schema := string(raw)
	if strings.Count(schema, "traceparent TEXT,") != 2 || strings.Count(schema, "tracestate TEXT,") != 2 {
		t.Fatal("both Room outboxes must define nullable traceparent/tracestate columns")
	}
	if strings.Contains(schema, "traceparent TEXT NOT NULL") || strings.Contains(schema, "tracestate TEXT NOT NULL") {
		t.Fatal("outbox trace context must remain nullable")
	}
}

func assertRoomTraceColumns(t *testing.T, file, marker string, want int) {
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
			t.Fatalf("%s insert %d in %s omits trace context", marker, count+1, file)
		}
		count++
		offset = idx + end + 1
	}
	if count != want {
		t.Fatalf("%s count=%d want=%d in %s", marker, count, want, file)
	}
}
