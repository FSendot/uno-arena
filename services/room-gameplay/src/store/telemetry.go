package store

import (
	"context"

	"github.com/exaring/otelpgx"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

var storeTracerProvider trace.TracerProvider
var storeMeterProvider metric.MeterProvider

func ConfigureTelemetry(tracerProvider trace.TracerProvider, meterProvider metric.MeterProvider) {
	storeTracerProvider = tracerProvider
	storeMeterProvider = meterProvider
}

func startStoreSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	if storeTracerProvider == nil {
		return trace.NewNoopTracerProvider().Tracer("unoarena/services/room-gameplay/store").Start(ctx, name)
	}
	return storeTracerProvider.Tracer("unoarena/services/room-gameplay/store").Start(ctx, name)
}

func pgxTracer() *otelpgx.Tracer {
	if storeTracerProvider == nil || storeMeterProvider == nil {
		return nil
	}
	return otelpgx.NewTracer(
		otelpgx.WithTracerProvider(storeTracerProvider),
		otelpgx.WithMeterProvider(storeMeterProvider),
		otelpgx.WithSpanNameFunc(func(string) string { return "room-gameplay.postgres.query" }),
		otelpgx.WithDisableQuerySpanNamePrefix(),
		otelpgx.WithDisableSQLStatementInAttributes(),
	)
}

func instrumentRedis(client *redis.Client) error {
	if storeTracerProvider == nil {
		return nil
	}
	return redisotel.InstrumentTracing(client,
		redisotel.WithTracerProvider(storeTracerProvider),
		redisotel.WithDBStatement(false),
		redisotel.WithCallerEnabled(false),
	)
}
