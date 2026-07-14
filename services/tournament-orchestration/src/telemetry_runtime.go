package main

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/trace"
	"unoarena/platform/telemetry"
)

var tournamentProcessTelemetry *telemetry.Runtime

func startTournamentTelemetry(ctx context.Context) (*telemetry.Runtime, error) {
	runtime, err := telemetry.Start(ctx, telemetry.ConfigFromEnv())
	if err != nil {
		return nil, err
	}
	tournamentProcessTelemetry = runtime
	slog.SetDefault(runtime.Logger)
	initializeBusinessMetrics(ctx)
	http.DefaultTransport = otelhttp.NewTransport(http.DefaultTransport,
		otelhttp.WithTracerProvider(runtime.TracerProvider),
		otelhttp.WithPropagators(runtime.Propagator),
		otelhttp.WithSpanNameFormatter(func(string, *http.Request) string { return "tournament-orchestration.http.client" }),
	)
	return runtime, nil
}

func tournamentHTTPHandler(runtime *telemetry.Runtime, next http.Handler) http.Handler {
	return otelhttp.NewHandler(next, "tournament-orchestration.http.server",
		otelhttp.WithTracerProvider(runtime.TracerProvider),
		otelhttp.WithPropagators(runtime.Propagator),
		otelhttp.WithFilter(func(request *http.Request) bool {
			switch request.URL.Path {
			case "/health", "/healthz", "/ready", "/readyz", "/metrics":
				return false
			default:
				return true
			}
		}),
	)
}

func shutdownTournamentTelemetry(runtime *telemetry.Runtime) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runtime.Shutdown(ctx); err != nil {
		runtime.Logger.ErrorContext(ctx, "telemetry shutdown failed", "event", "telemetry_shutdown_failed", "error", err.Error())
	}
}

func contextWithTournamentSpan(ctx, carrier context.Context) context.Context {
	spanContext := trace.SpanContextFromContext(carrier)
	if !spanContext.IsValid() {
		return ctx
	}
	return trace.ContextWithSpanContext(ctx, spanContext)
}

func startTournamentSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	if tournamentProcessTelemetry == nil {
		return trace.NewNoopTracerProvider().Tracer("unoarena/services/tournament-orchestration").Start(ctx, name)
	}
	return tournamentProcessTelemetry.TracerProvider.Tracer("unoarena/services/tournament-orchestration").Start(ctx, name)
}
