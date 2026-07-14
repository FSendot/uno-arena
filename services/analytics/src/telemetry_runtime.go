package main

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	"unoarena/platform/telemetry"
)

var serviceTelemetry *telemetry.Runtime

func startProcessTelemetry(ctx context.Context) (*telemetry.Runtime, error) {
	runtime, err := telemetry.Start(ctx, telemetry.ConfigFromEnv())
	if err != nil {
		return nil, err
	}
	serviceTelemetry = runtime
	slog.SetDefault(runtime.Logger)
	return runtime, nil
}

func shutdownProcessTelemetry(runtime *telemetry.Runtime) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runtime.Shutdown(ctx); err != nil {
		runtime.Logger.ErrorContext(ctx, "telemetry shutdown failed", "event", "telemetry_shutdown_failed", "error", err.Error())
	}
}

func processLogger() *slog.Logger {
	if serviceTelemetry != nil {
		return serviceTelemetry.Logger
	}
	return slog.Default()
}

func processTracerProvider() trace.TracerProvider {
	if serviceTelemetry != nil {
		return serviceTelemetry.TracerProvider
	}
	return tracenoop.NewTracerProvider()
}

func processPropagator() propagation.TextMapPropagator {
	if serviceTelemetry != nil {
		return serviceTelemetry.Propagator
	}
	return propagation.TraceContext{}
}

func tracedHTTPHandler(next http.Handler) http.Handler {
	options := []otelhttp.Option{
		otelhttp.WithPropagators(processPropagator()),
		otelhttp.WithFilter(func(r *http.Request) bool {
			return r.URL.Path != "/health" && r.URL.Path != "/ready" && r.URL.Path != "/metrics"
		}),
	}
	options = append(options, otelhttp.WithTracerProvider(processTracerProvider()))
	return otelhttp.NewHandler(next, "analytics.http.request", options...)
}

func tracedHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: otelhttp.NewTransport(http.DefaultTransport,
			otelhttp.WithTracerProvider(processTracerProvider()),
			otelhttp.WithPropagators(processPropagator()),
		),
	}
}
