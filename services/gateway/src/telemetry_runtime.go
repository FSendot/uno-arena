package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"unoarena/platform/telemetry"
)

type gatewayClientInstrumentation struct {
	audit      slog.Handler
	redis      func(redis.UniversalClient) error
	kafkaHooks []kgo.Hook
}

func newGatewayClientInstrumentation(runtime *telemetry.Runtime) gatewayClientInstrumentation {
	tracer := kotel.NewTracer(
		kotel.TracerProvider(runtime.TracerProvider),
		kotel.TracerPropagator(runtime.Propagator),
		kotel.ClientID("gateway"),
		kotel.ConsumerGroup("gateway"),
	)
	return gatewayClientInstrumentation{
		audit: runtime.Handler,
		redis: func(client redis.UniversalClient) error {
			return errors.Join(
				redisotel.InstrumentTracing(client,
					redisotel.WithTracerProvider(runtime.TracerProvider),
					redisotel.WithDBStatement(false),
					redisotel.WithCallerEnabled(false),
				),
				redisotel.InstrumentMetrics(client,
					redisotel.WithMeterProvider(runtime.MeterProvider),
				),
			)
		},
		kafkaHooks: kotel.NewKotel(kotel.WithTracer(tracer)).Hooks(),
	}
}

func startGatewayTelemetry(ctx context.Context) (*telemetry.Runtime, error) {
	return telemetry.Start(ctx, telemetry.ConfigFromEnv())
}

func validateGatewayAuditConfig(path string) error {
	if telemetry.ConfigFromEnv().Mode == telemetry.ModeRequired && strings.TrimSpace(path) != "" {
		return errors.New("GATEWAY_AUDIT_LOG_PATH is forbidden when TELEMETRY_MODE=required")
	}
	return nil
}

func gatewayHTTPHandler(runtime *telemetry.Runtime, next http.Handler) http.Handler {
	return otelhttp.NewHandler(next, "gateway.http.server",
		otelhttp.WithTracerProvider(runtime.TracerProvider),
		otelhttp.WithPropagators(runtime.Propagator),
		otelhttp.WithPublicEndpointFn(func(request *http.Request) bool {
			return !strings.HasPrefix(request.URL.Path, "/internal/")
		}),
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

func gatewayHTTPClient(runtime *telemetry.Runtime, timeoutClient *http.Client) *http.Client {
	base := http.DefaultTransport
	if timeoutClient.Transport != nil {
		base = timeoutClient.Transport
	}
	clone := *timeoutClient
	clone.Transport = otelhttp.NewTransport(base,
		otelhttp.WithTracerProvider(runtime.TracerProvider),
		otelhttp.WithPropagators(runtime.Propagator),
		otelhttp.WithSpanNameFormatter(func(string, *http.Request) string {
			return "gateway.http.client"
		}),
	)
	return &clone
}
