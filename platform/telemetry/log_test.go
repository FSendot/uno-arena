package telemetry

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"
)

func TestJSONHandlerExactContractAndTraceCorrelation(t *testing.T) {
	var output bytes.Buffer
	handler := newJSONHandler(Config{
		ServiceName: "room-gameplay", Component: "room-runtime",
		Environment: "kind", ServiceVersion: "abc123", InstanceID: "pod-uid",
		Writer: &output,
	})
	traceID, _ := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	spanID, _ := trace.SpanIDFromHex("0102030405060708")
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled,
	}))
	record := slog.NewRecord(time.Date(2026, 7, 13, 16, 17, 18, 123456789, time.FixedZone("other", -3*60*60)), slog.LevelWarn, "command rejected", 0)
	record.AddAttrs(
		slog.String("event", "command_rejected"),
		slog.String("correlationId", "correlation-1"),
		slog.Any("err", errors.New("denied")),
		slog.String("service", "attacker-controlled"),
		slog.String("trace_id", "bad"),
		slog.Group("component", slog.String("name", "bad")),
	)
	if err := handler.Handle(ctx, record); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	want := "{\"timestamp\":\"2026-07-13T19:17:18.123456789Z\",\"level\":\"warn\",\"service\":\"room-gameplay\",\"component\":\"room-runtime\",\"environment\":\"kind\",\"version\":\"abc123\",\"instance_id\":\"pod-uid\",\"event\":\"command_rejected\",\"message\":\"command rejected\",\"trace_id\":\"0102030405060708090a0b0c0d0e0f10\",\"span_id\":\"0102030405060708\",\"correlationId\":\"correlation-1\",\"error\":\"denied\"}\n"
	if got := output.String(); got != want {
		t.Fatalf("log line mismatch\n got: %s want: %s", got, want)
	}
}

func TestJSONHandlerRejectsInvalidEventAndPropagatesWriteFailure(t *testing.T) {
	handler := newJSONHandler(Config{
		ServiceName: "gateway", Component: "api", Environment: "kind",
		ServiceVersion: "abc", InstanceID: "pod", Writer: &failingWriter{},
	})
	record := slog.NewRecord(time.Now(), slog.LevelInfo, "", 0)
	record.AddAttrs(slog.String("event", "Invalid Event"))
	if err := handler.Handle(context.Background(), record); err == nil {
		t.Fatal("invalid event must fail Handle")
	}
	record = slog.NewRecord(time.Now(), slog.LevelInfo, "", 0)
	record.AddAttrs(slog.String("event", "audit_rejected"))
	if err := handler.Handle(context.Background(), record); err == nil || !stringsContains(err.Error(), "write structured log") {
		t.Fatalf("write error = %v", err)
	}
}

type failingWriter struct{}

func (*failingWriter) Write([]byte) (int, error) { return 0, errors.New("disk full") }

func stringsContains(value, substring string) bool {
	for index := 0; index+len(substring) <= len(value); index++ {
		if value[index:index+len(substring)] == substring {
			return true
		}
	}
	return false
}
