package app

import (
	"context"

	"go.opentelemetry.io/otel/trace"
)

func startContextSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	provider := trace.SpanFromContext(ctx).TracerProvider()
	return provider.Tracer("unoarena/services/room-gameplay").Start(ctx, name)
}
