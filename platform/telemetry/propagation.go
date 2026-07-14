package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// TraceContextHeaders returns the W3C trace headers to persist in a CDC
// outbox. It deliberately excludes baggage and returns empty strings when the
// context has no valid span context.
func TraceContextHeaders(ctx context.Context) (traceparent, tracestate string) {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return "", ""
	}
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	return carrier.Get("traceparent"), carrier.Get("tracestate")
}
