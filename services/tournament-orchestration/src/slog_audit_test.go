package main

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"unoarena/shared/audit"
)

type tournamentAuditTestHandler struct{ err error }

func (h tournamentAuditTestHandler) Enabled(context.Context, slog.Level) bool  { return true }
func (h tournamentAuditTestHandler) Handle(context.Context, slog.Record) error { return h.err }
func (h tournamentAuditTestHandler) WithAttrs([]slog.Attr) slog.Handler        { return h }
func (h tournamentAuditTestHandler) WithGroup(string) slog.Handler             { return h }

func TestSlogAuditSinkPropagatesHandlerFailure(t *testing.T) {
	want := errors.New("stdout unavailable")
	sink := newSlogAuditSink(tournamentAuditTestHandler{err: want})
	err := sink.RecordRejection(context.Background(), audit.NewRejection("cmd-1", "corr-1", "", "", "invalid", time.Now()))
	if !errors.Is(err, want) {
		t.Fatalf("RecordRejection error = %v, want %v", err, want)
	}
}

func TestSlogAuditSinkTreatsLaterTimestampAsSameRejection(t *testing.T) {
	sink := newSlogAuditSink(tournamentAuditTestHandler{})
	first := audit.NewRejection("cmd-1", "corr-1", "", "", "invalid", time.Now())
	if err := sink.RecordRejection(context.Background(), first); err != nil {
		t.Fatalf("first RecordRejection: %v", err)
	}
	second := audit.NewRejection("cmd-1", "corr-1", "", "", "invalid", first.Timestamp.Add(time.Minute))
	if err := sink.RecordRejection(context.Background(), second); err != nil {
		t.Fatalf("retry RecordRejection: %v", err)
	}
}

func TestSlogAuditSinkRejectsSemanticConflict(t *testing.T) {
	sink := newSlogAuditSink(tournamentAuditTestHandler{})
	when := time.Now()
	if err := sink.RecordRejection(context.Background(), audit.NewRejection("cmd-1", "corr-1", "", "", "invalid", when)); err != nil {
		t.Fatalf("first RecordRejection: %v", err)
	}
	err := sink.RecordRejection(context.Background(), audit.NewRejection("cmd-1", "corr-1", "", "", "different", when))
	if err == nil {
		t.Fatal("conflicting RecordRejection succeeded")
	}
}
