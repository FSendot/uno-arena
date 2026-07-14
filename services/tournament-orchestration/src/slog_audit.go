package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"unoarena/shared/audit"
)

type slogAuditSink struct {
	mu      sync.Mutex
	handler slog.Handler
	seen    map[string]string
}

func newSlogAuditSink(handler slog.Handler) *slogAuditSink {
	return &slogAuditSink{handler: handler, seen: make(map[string]string)}
}

func (a *slogAuditSink) RecordRejection(ctx context.Context, rec audit.RejectionRecord) error {
	if err := rec.Validate(); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	// Timestamp is emission metadata, not part of a command rejection's semantic
	// identity. A retry after audit success and database commit failure must be
	// accepted even when the clock has advanced.
	stable := rec
	stable.Timestamp = time.Time{}
	canonical, err := json.Marshal(stable)
	if err != nil {
		return err
	}
	if prior, ok := a.seen[rec.CommandID]; ok {
		if prior == string(canonical) {
			return nil
		}
		return fmt.Errorf("audit record conflict for commandId %q", rec.CommandID)
	}
	if a.handler == nil {
		return fmt.Errorf("audit telemetry handler not configured")
	}
	record := slog.NewRecord(rec.Timestamp.UTC(), slog.LevelWarn, "command rejected", 0)
	record.AddAttrs(
		slog.String("event", "command_rejected"),
		slog.String("commandId", rec.CommandID),
		slog.String("correlationId", rec.CorrelationID),
		slog.String("reason", rec.Reason),
	)
	if rec.SessionID != "" {
		record.AddAttrs(slog.String("sessionId", rec.SessionID))
	}
	if rec.PlayerID != "" {
		record.AddAttrs(slog.String("playerId", rec.PlayerID))
	}
	if rec.TournamentID != "" {
		record.AddAttrs(slog.String("tournamentId", rec.TournamentID))
	}
	if rec.SubmittedSequence != nil {
		record.AddAttrs(slog.Int64("submittedSequenceNumber", *rec.SubmittedSequence))
	}
	if rec.CurrentSequence != nil {
		record.AddAttrs(slog.Int64("currentSequenceNumber", *rec.CurrentSequence))
	}
	if err := a.handler.Handle(ctx, record); err != nil {
		return fmt.Errorf("audit telemetry write failed: %w", err)
	}
	a.seen[rec.CommandID] = string(canonical)
	return nil
}

func productionTournamentAudit() AuditSink {
	if tournamentProcessTelemetry != nil {
		return newSlogAuditSink(tournamentProcessTelemetry.Handler)
	}
	return NewMemoryAudit()
}
