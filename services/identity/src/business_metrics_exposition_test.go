package main

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestPlayerCreatedMetricExpositionContract(t *testing.T) {
	configureMetricContractEnv(t, "identity", "api")
	runtime, _, err := startIdentityTelemetry(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	assertMetricExposition(t, runtime.MetricsAddr(), "unoarena_players_created_total", playerCreatedDescription)
}

func configureMetricContractEnv(t *testing.T, service, component string) {
	for key, value := range map[string]string{
		"TELEMETRY_MODE": "required", "SERVICE_NAME": service, "DEPLOYMENT_ENV": "test",
		"SERVICE_VERSION": "test", "UNOARENA_COMPONENT": component, "POD_UID": "metric-contract",
		"OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:4317", "OTEL_EXPORTER_OTLP_PROTOCOL": "grpc",
		"OTEL_TRACES_SAMPLER": "always_off", "METRICS_ADDR": "127.0.0.1:0", "OTEL_GO_X_OBSERVABILITY": "true",
	} {
		t.Setenv(key, value)
	}
}

func assertMetricExposition(t *testing.T, address, metric, help string) {
	t.Helper()
	response, err := http.Get("http://" + address + "/metrics") // #nosec G107 -- loopback test listener
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, line := range []string{"# HELP " + metric + " " + help, "# TYPE " + metric + " counter", metric + " 0"} {
		if !strings.Contains(text, line+"\n") {
			t.Fatalf("missing exposition line %q\n%s", line, text)
		}
	}
	if strings.Contains(text, metric+"{") {
		t.Fatalf("business metric has instrument labels: %s", metric)
	}
}
