package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/trace"
)

func TestTraceContextHeaders(t *testing.T) {
	if traceparent, tracestate := TraceContextHeaders(context.Background()); traceparent != "" || tracestate != "" {
		t.Fatalf("invalid context produced headers: %q %q", traceparent, tracestate)
	}
	traceID, _ := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	spanID, _ := trace.SpanIDFromHex("0102030405060708")
	state, err := trace.ParseTraceState("vendor=value")
	if err != nil {
		t.Fatal(err)
	}
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled, TraceState: state,
	}))
	member, _ := baggage.NewMember("private", "must-not-propagate")
	bag, _ := baggage.New(member)
	ctx = baggage.ContextWithBaggage(ctx, bag)
	traceparent, tracestate := TraceContextHeaders(ctx)
	if want := "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01"; traceparent != want {
		t.Fatalf("traceparent = %q, want %q", traceparent, want)
	}
	if tracestate != "vendor=value" {
		t.Fatalf("tracestate = %q", tracestate)
	}
	if stringsContains(traceparent+tracestate, "private") {
		t.Fatal("baggage leaked into persisted trace context")
	}
}
