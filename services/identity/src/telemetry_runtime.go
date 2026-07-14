package main

import (
	"context"
	"net/http"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	otelmetric "go.opentelemetry.io/otel/metric"

	"unoarena/platform/telemetry"
)

const playerCreatedDescription = "Number of authoritative player creations committed by Identity."

type playerCreationMetrics struct {
	created otelmetric.Int64Counter
}

func identityPGXTracer(runtime *telemetry.Runtime) pgx.QueryTracer {
	return otelpgx.NewTracer(
		otelpgx.WithTracerProvider(runtime.TracerProvider),
		otelpgx.WithMeterProvider(runtime.MeterProvider),
		otelpgx.WithSpanNameFunc(func(string) string { return "identity.postgres.query" }),
		otelpgx.WithDisableQuerySpanNamePrefix(),
		otelpgx.WithDisableSQLStatementInAttributes(),
		otelpgx.WithDisableConnectionDetailsInAttributes(),
	)
}

func startIdentityTelemetry(ctx context.Context) (*telemetry.Runtime, *playerCreationMetrics, error) {
	cfg := telemetry.ConfigFromEnv()
	runtime, err := telemetry.Start(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}
	counter, err := runtime.MeterProvider.Meter("unoarena/services/identity").Int64Counter(
		"unoarena.players.created",
		otelmetric.WithUnit("1"),
		otelmetric.WithDescription(playerCreatedDescription),
	)
	if err != nil {
		_ = runtime.Shutdown(ctx)
		return nil, nil, err
	}
	// Materialize the descriptor before the first authoritative creation so
	// deployment acceptance can establish a real zero baseline.
	counter.Add(ctx, 0)
	return runtime, &playerCreationMetrics{created: counter}, nil
}

func (m *playerCreationMetrics) RecordPlayerCreated(ctx context.Context) {
	if m != nil {
		m.created.Add(ctx, 1)
	}
}

func identityHTTPHandler(runtime *telemetry.Runtime, next http.Handler) http.Handler {
	return otelhttp.NewHandler(next, "identity.http.server",
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

func identityHTTPClient(runtime *telemetry.Runtime) *http.Client {
	return &http.Client{
		Timeout: 10 * time.Second,
		Transport: otelhttp.NewTransport(http.DefaultTransport,
			otelhttp.WithTracerProvider(runtime.TracerProvider),
			otelhttp.WithPropagators(runtime.Propagator),
			otelhttp.WithSpanNameFormatter(func(string, *http.Request) string {
				return "identity.http.client"
			}),
		),
	}
}
