package store

import (
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

func pgxTracer() *otelpgx.Tracer {
	if storeTracerProvider == nil || storeMeterProvider == nil {
		return nil
	}
	return otelpgx.NewTracer(
		otelpgx.WithTracerProvider(storeTracerProvider),
		otelpgx.WithMeterProvider(storeMeterProvider),
		otelpgx.WithSpanNameFunc(func(string) string { return "tournament-orchestration.postgres.query" }),
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
