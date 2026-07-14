package bff_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"unoarena/services/gateway/bff"
	"unoarena/shared/audit"
)

type rejectingHandler struct{}

func (rejectingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (rejectingHandler) Handle(context.Context, slog.Record) error {
	return errors.New("stdout unavailable")
}
func (h rejectingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h rejectingHandler) WithGroup(string) slog.Handler      { return h }

func TestSlogAuditWritesStructuredRejectionAndFailsClosed(t *testing.T) {
	var output bytes.Buffer
	sink := bff.NewSlogAudit(slog.NewJSONHandler(&output, nil))
	record := audit.NewRejection("command-1", "correlation-1", "session-1", "player-1", "forbidden",
		time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC))
	if err := sink.RecordRejection(context.Background(), record); err != nil {
		t.Fatalf("record rejection: %v", err)
	}
	line := output.String()
	for _, want := range []string{`"event":"command_rejected"`, `"commandId":"command-1"`, `"correlationId":"correlation-1"`} {
		if !strings.Contains(line, want) {
			t.Fatalf("audit line missing %s: %s", want, line)
		}
	}

	if err := bff.NewSlogAudit(rejectingHandler{}).RecordRejection(context.Background(), record); err == nil {
		t.Fatal("handler write failure must fail the rejected command closed")
	}
}
