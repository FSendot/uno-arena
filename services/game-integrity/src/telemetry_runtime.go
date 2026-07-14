package main

import (
	"context"
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/trace"

	"unoarena/platform/telemetry"
)

func startGameIntegrityTelemetry(ctx context.Context) (*telemetry.Runtime, error) {
	return telemetry.Start(ctx, telemetry.ConfigFromEnv())
}

func gameIntegrityTracer(runtime *telemetry.Runtime) trace.Tracer {
	return runtime.TracerProvider.Tracer("unoarena/services/game-integrity")
}

func gameIntegrityHTTPHandler(runtime *telemetry.Runtime, next http.Handler) http.Handler {
	return otelhttp.NewHandler(next, "game-integrity.http.server",
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
