package main

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"unoarena/platform/telemetry"
	"unoarena/services/room-gameplay/app"
)

var roomProcessTelemetry *telemetry.Runtime
var roomBaseHTTPTransport = http.DefaultTransport.(*http.Transport).Clone()

func startRoomTelemetry(ctx context.Context) (*telemetry.Runtime, error) {
	runtime, err := telemetry.Start(ctx, telemetry.ConfigFromEnv())
	if err != nil {
		return nil, err
	}
	roomProcessTelemetry = runtime
	slog.SetDefault(runtime.Logger)
	app.InitializeBusinessMetrics(ctx)
	http.DefaultTransport = otelhttp.NewTransport(roomBaseHTTPTransport.Clone(),
		otelhttp.WithTracerProvider(runtime.TracerProvider),
		otelhttp.WithPropagators(runtime.Propagator),
		otelhttp.WithSpanNameFormatter(func(string, *http.Request) string { return "room-gameplay.http.client" }),
	)
	return runtime, nil
}

func roomHTTPHandler(runtime *telemetry.Runtime, next http.Handler) http.Handler {
	return otelhttp.NewHandler(next, "room-gameplay.http.server",
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

func shutdownRoomTelemetry(runtime *telemetry.Runtime) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runtime.Shutdown(ctx); err != nil {
		runtime.Logger.ErrorContext(ctx, "telemetry shutdown failed", "event", "telemetry_shutdown_failed", "error", err.Error())
	}
}
