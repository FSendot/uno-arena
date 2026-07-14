package telemetry

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestRuntimeModesInIsolatedProcesses(t *testing.T) {
	for _, mode := range []string{"disabled", "required", "bind-failure"} {
		t.Run(mode, func(t *testing.T) {
			command := exec.Command(os.Args[0], "-test.run=^TestRuntimeHelper$")
			command.Env = append(os.Environ(), "UNOARENA_TELEMETRY_TEST_MODE="+mode)
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("helper failed: %v\n%s", err, output)
			}
		})
	}
}

func TestRuntimeHelper(t *testing.T) {
	mode := os.Getenv("UNOARENA_TELEMETRY_TEST_MODE")
	if mode == "" {
		t.Skip("subprocess helper")
	}
	var logs bytes.Buffer
	if mode == "bind-failure" {
		t.Setenv("OTEL_GO_X_OBSERVABILITY", "true")
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		config := requiredTestConfig(&logs)
		config.MetricsAddr = listener.Addr().String()
		if _, err := Start(context.Background(), config); err == nil || !strings.Contains(err.Error(), "bind metrics listener") {
			t.Fatalf("occupied listener Start error = %v", err)
		}
		_ = listener.Close()
		config.MetricsAddr = "127.0.0.1:0"
		runtime, err := Start(context.Background(), config)
		if err != nil {
			t.Fatalf("Start after corrected bind: %v", err)
		}
		if err := runtime.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		return
	}
	config := Config{
		Mode: Mode(mode), ServiceName: "identity", Environment: "kind",
		ServiceVersion: "abc123", Component: "api", InstanceID: "pod-uid",
		Writer: &logs,
	}
	if mode == "required" {
		t.Setenv("OTEL_GO_X_OBSERVABILITY", "true")
		config = requiredTestConfig(&logs)
	}
	globalTracer := otel.GetTracerProvider()
	globalPropagator := otel.GetTextMapPropagator()
	globalMeter := otel.GetMeterProvider()
	runtime, err := Start(context.Background(), config)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if runtime.Logger == nil || runtime.Handler == nil || runtime.TracerProvider == nil || runtime.MeterProvider == nil || runtime.Propagator == nil {
		t.Fatal("Runtime did not expose all stable capabilities")
	}
	if otel.GetTracerProvider() != globalTracer {
		t.Fatal("Start mutated the global TracerProvider")
	}
	if otel.GetTextMapPropagator() != globalPropagator {
		t.Fatal("Start mutated the global propagator")
	}
	if _, err := Start(context.Background(), config); err == nil || !strings.Contains(err.Error(), "already been started") {
		t.Fatalf("second Start error = %v", err)
	}
	if mode == "disabled" {
		if otel.GetMeterProvider() != globalMeter {
			t.Fatal("disabled mode mutated the global MeterProvider")
		}
		if runtime.MetricsAddr() != "" {
			t.Fatalf("disabled metrics address = %q", runtime.MetricsAddr())
		}
	} else {
		if otel.GetMeterProvider() == globalMeter {
			t.Fatal("required mode did not install the runtime MeterProvider globally")
		}
		counter, err := runtime.MeterProvider.Meter("identity").Int64Counter(
			"unoarena.players.created",
			metric.WithUnit("1"),
			metric.WithDescription("Number of authoritative player creations committed by Identity."),
		)
		if err != nil {
			t.Fatal(err)
		}
		counter.Add(context.Background(), 1)
		body := scrape(t, runtime.MetricsAddr())
		for _, fragment := range []string{
			"# HELP unoarena_players_created_total Number of authoritative player creations committed by Identity.",
			"# TYPE unoarena_players_created_total counter",
			"unoarena_players_created_total 1",
			"go_goroutines",
			"process_cpu_seconds_total",
			"otel_sdk_processor_span_queue_capacity",
			"otel_sdk_processor_span_queue_size",
		} {
			if !strings.Contains(body, fragment) {
				t.Fatalf("metrics exposition missing %q\n%s", fragment, body)
			}
		}
		if strings.Contains(body, "unoarena_players_created_total{") {
			t.Fatalf("business counter unexpectedly has labels\n%s", body)
		}
		otel.Handle(errors.New("collector unavailable"))
		if !strings.Contains(logs.String(), `"event":"telemetry_sdk_failure"`) || !strings.Contains(logs.String(), `"error":"collector unavailable"`) {
			t.Fatalf("SDK error was not logged with its actual error: %s", logs.String())
		}
	}
	if err := runtime.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := runtime.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
}

func TestObservedExporterLogsActualError(t *testing.T) {
	var logs bytes.Buffer
	config := Config{
		ServiceName: "gateway", Component: "api", Environment: "kind",
		ServiceVersion: "abc", InstanceID: "pod", Writer: &logs,
	}
	provider := metricnoop.NewMeterProvider()
	counter, err := provider.Meter("test").Int64Counter("test.failures")
	if err != nil {
		t.Fatal(err)
	}
	exporter := &observedExporter{
		delegate: failingSpanExporter{err: errors.New("tempo unavailable")},
		counter:  counter,
		logger:   slog.New(newJSONHandler(config)),
	}
	if err := exporter.ExportSpans(context.Background(), nil); err == nil || err.Error() != "tempo unavailable" {
		t.Fatalf("ExportSpans error = %v", err)
	}
	if !strings.Contains(logs.String(), `"event":"trace_export_failure"`) || !strings.Contains(logs.String(), `"error":"tempo unavailable"`) {
		t.Fatalf("export failure was not logged with its actual error: %s", logs.String())
	}
}

type failingSpanExporter struct{ err error }

func (e failingSpanExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error {
	return e.err
}
func (failingSpanExporter) Shutdown(context.Context) error { return nil }

func requiredTestConfig(logs *bytes.Buffer) Config {
	return Config{
		Mode: ModeRequired, ServiceName: "identity", Environment: "kind",
		ServiceVersion: "abc123", Component: "api", InstanceID: "pod-uid",
		OTLPEndpoint: "http://127.0.0.1:1", OTLPProtocol: "grpc",
		TracesSampler: "always_off", MetricsAddr: "127.0.0.1:0", Writer: logs,
	}
}

func scrape(t *testing.T, address string) string {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	url := fmt.Sprintf("http://%s/metrics", address)
	var lastErr error
	for range 20 {
		response, err := client.Get(url)
		if err == nil {
			defer response.Body.Close()
			body, readErr := io.ReadAll(response.Body)
			if readErr != nil {
				t.Fatal(readErr)
			}
			return string(body)
		}
		lastErr = err
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("scrape %s: %v", url, lastErr)
	return ""
}
