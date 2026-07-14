// Package telemetry provides the infrastructure-only observability bootstrap
// shared by Uno Arena processes. Domain instruments, spans, and log events stay
// in their owning bounded contexts.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/otlptranslator"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	promexporter "go.opentelemetry.io/otel/exporters/prometheus"
	otelmetric "go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

const (
	shutdownLimit = 5 * time.Second
	exportLimit   = 5 * time.Second
)

var processStarted atomic.Bool

// Runtime exposes only the capabilities application bootstrap and adapters
// need. Exporters, queues, registries, and HTTP serving stay internal.
type Runtime struct {
	Logger         *slog.Logger
	Handler        slog.Handler
	TracerProvider oteltrace.TracerProvider
	MeterProvider  otelmetric.MeterProvider
	Propagator     propagation.TextMapPropagator

	traceSDK  *sdktrace.TracerProvider
	metricSDK *metric.MeterProvider
	server    *http.Server
	listener  net.Listener

	shutdownOnce sync.Once
	shutdownErr  error
}

// Start validates the complete configuration before publishing a Runtime.
// Required mode binds /metrics synchronously and installs only the process
// MeterProvider and structured OTel error handler globally. It never installs
// a global TracerProvider or propagator.
func Start(ctx context.Context, config Config) (*Runtime, error) {
	config.normalize()
	if err := config.validate(); err != nil {
		return nil, fmt.Errorf("telemetry configuration: %w", err)
	}
	if !processStarted.CompareAndSwap(false, true) {
		return nil, errors.New("telemetry has already been started in this process")
	}
	succeeded := false
	defer func() {
		if !succeeded {
			processStarted.Store(false)
		}
	}()

	handler := newJSONHandler(config)
	logger := slog.New(handler)
	propagator := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{})
	if config.Mode == ModeDisabled {
		succeeded = true
		return &Runtime{
			Logger: logger, Handler: handler,
			TracerProvider: tracenoop.NewTracerProvider(),
			MeterProvider:  metricnoop.NewMeterProvider(),
			Propagator:     propagator,
		}, nil
	}

	listener, err := net.Listen("tcp", config.MetricsAddr)
	if err != nil {
		return nil, fmt.Errorf("bind metrics listener %q: %w", config.MetricsAddr, err)
	}
	closeListener := true
	defer func() {
		if closeListener {
			_ = listener.Close()
		}
	}()

	registry := prometheus.NewRegistry()
	if err := registry.Register(prometheus.NewGoCollector()); err != nil {
		return nil, fmt.Errorf("register Go collector: %w", err)
	}
	if err := registry.Register(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{})); err != nil {
		return nil, fmt.Errorf("register process collector: %w", err)
	}
	promReader, err := promexporter.New(
		promexporter.WithRegisterer(registry),
		promexporter.WithTranslationStrategy(otlptranslator.UnderscoreEscapingWithSuffixes),
		promexporter.WithoutScopeInfo(),
	)
	if err != nil {
		return nil, fmt.Errorf("create Prometheus exporter: %w", err)
	}
	res, err := resource.New(ctx, resource.WithAttributes(
		attribute.String("service.name", config.ServiceName),
		attribute.String("service.namespace", "uno-arena"),
		attribute.String("service.version", config.ServiceVersion),
		attribute.String("deployment.environment.name", config.Environment),
		attribute.String("service.instance.id", config.InstanceID),
		attribute.String("unoarena.component", config.Component),
	))
	if err != nil {
		return nil, fmt.Errorf("create telemetry resource: %w", err)
	}
	meterProvider := metric.NewMeterProvider(
		metric.WithResource(res),
		metric.WithReader(promReader),
	)

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpointURL(config.OTLPEndpoint),
		otlptracegrpc.WithRetry(otlptracegrpc.RetryConfig{
			Enabled:         true,
			InitialInterval: 250 * time.Millisecond,
			MaxInterval:     time.Second,
			MaxElapsedTime:  exportLimit,
		}),
	)
	if err != nil {
		_ = meterProvider.Shutdown(context.Background())
		return nil, fmt.Errorf("create OTLP trace exporter: %w", err)
	}
	failureCounter, err := meterProvider.Meter("unoarena/platform/telemetry").Int64Counter(
		"unoarena.telemetry.trace_export.failures",
		otelmetric.WithUnit("1"),
		otelmetric.WithDescription("Number of failed OTLP trace batch exports."),
	)
	if err != nil {
		_ = exporter.Shutdown(context.Background())
		_ = meterProvider.Shutdown(context.Background())
		return nil, fmt.Errorf("create trace export failure counter: %w", err)
	}
	selectedSampler, _ := sampler(config.TracesSampler, config.TracesSamplerArg)
	wrappedExporter := &observedExporter{
		delegate: exporter, counter: failureCounter, logger: logger,
	}

	// The pinned SDK constructs its self-observation instruments from the
	// global MeterProvider. Publish it immediately before the BSP is created;
	// all fallible initialization has completed at this point.
	otel.SetMeterProvider(meterProvider)
	processor := sdktrace.NewBatchSpanProcessor(wrappedExporter,
		sdktrace.WithMaxQueueSize(2048),
		sdktrace.WithMaxExportBatchSize(512),
		sdktrace.WithBatchTimeout(5*time.Second),
		sdktrace.WithExportTimeout(exportLimit),
	)
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSampler(selectedSampler),
		sdktrace.WithSpanProcessor(processor),
	)
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		logger.ErrorContext(context.Background(), "OpenTelemetry SDK failure",
			"event", "telemetry_sdk_failure",
			"error", err.Error(),
		)
	}))

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	runtime := &Runtime{
		Logger: logger, Handler: handler,
		TracerProvider: tracerProvider,
		MeterProvider:  meterProvider,
		Propagator:     propagator,
		traceSDK:       tracerProvider,
		metricSDK:      meterProvider,
		server:         server,
		listener:       listener,
	}
	go runtime.serveMetrics()
	closeListener = false
	succeeded = true
	return runtime, nil
}

func (r *Runtime) serveMetrics() {
	if err := r.server.Serve(r.listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		r.Logger.ErrorContext(context.Background(), "metrics server stopped",
			"event", "metrics_server_failure",
			"error", err.Error(),
		)
	}
}

// MetricsAddr returns the bound address, or an empty string in disabled mode.
func (r *Runtime) MetricsAddr() string {
	if r.listener == nil {
		return ""
	}
	return r.listener.Addr().String()
}

// Shutdown performs one bounded flush. Business work must be drained before
// this method is called. Repeated calls return the first result.
func (r *Runtime) Shutdown(ctx context.Context) error {
	r.shutdownOnce.Do(func() {
		bounded, cancel := boundedContext(ctx, shutdownLimit)
		defer cancel()
		var errs []error
		if r.server != nil {
			if err := r.server.Shutdown(bounded); err != nil {
				errs = append(errs, fmt.Errorf("stop metrics server: %w", err))
				_ = r.server.Close()
			}
		}
		if r.traceSDK != nil {
			if err := r.traceSDK.Shutdown(bounded); err != nil {
				errs = append(errs, fmt.Errorf("shutdown tracer provider: %w", err))
			}
		}
		if r.metricSDK != nil {
			if err := r.metricSDK.Shutdown(bounded); err != nil {
				errs = append(errs, fmt.Errorf("shutdown meter provider: %w", err))
			}
		}
		r.shutdownErr = errors.Join(errs...)
	})
	return r.shutdownErr
}

func boundedContext(parent context.Context, limit time.Duration) (context.Context, context.CancelFunc) {
	if deadline, ok := parent.Deadline(); ok && time.Until(deadline) <= limit {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, limit)
}

type observedExporter struct {
	delegate sdktrace.SpanExporter
	counter  otelmetric.Int64Counter
	logger   *slog.Logger
}

func (e *observedExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	err := e.delegate.ExportSpans(ctx, spans)
	if err != nil {
		e.counter.Add(context.Background(), 1)
		e.logger.ErrorContext(context.Background(), "OTLP trace export failed",
			"event", "trace_export_failure",
			"error", err.Error(),
		)
	}
	return err
}

func (e *observedExporter) Shutdown(ctx context.Context) error {
	return e.delegate.Shutdown(ctx)
}

var _ sdktrace.SpanExporter = (*observedExporter)(nil)
