package app

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"unoarena/shared/audit"
)

type auditTestHandler struct{ err error }

func (h auditTestHandler) Enabled(context.Context, slog.Level) bool  { return true }
func (h auditTestHandler) Handle(context.Context, slog.Record) error { return h.err }
func (h auditTestHandler) WithAttrs([]slog.Attr) slog.Handler        { return h }
func (h auditTestHandler) WithGroup(string) slog.Handler             { return h }

func TestSlogAuditSinkPropagatesHandlerFailure(t *testing.T) {
	want := errors.New("stdout unavailable")
	sink := NewSlogAuditSink(auditTestHandler{err: want})
	err := sink.Record(context.Background(), audit.NewRejection("cmd-1", "corr-1", "", "", "invalid", time.Now()))
	if !errors.Is(err, want) {
		t.Fatalf("Record error = %v, want %v", err, want)
	}
}
