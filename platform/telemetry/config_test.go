package telemetry

import (
	"context"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace"
	apiTrace "go.opentelemetry.io/otel/trace"
)

func TestConfigValidation(t *testing.T) {
	t.Setenv("OTEL_GO_X_OBSERVABILITY", "true")
	valid := Config{
		Mode: ModeRequired, ServiceName: "room-gameplay",
		Environment: "kind", ServiceVersion: "abc123",
		Component: "room-runtime", InstanceID: "pod-uid",
		OTLPEndpoint: "http://alloy-otlp.observability:4317",
		OTLPProtocol: "grpc", TracesSampler: "parentbased_always_on",
		MetricsAddr: "127.0.0.1:0",
	}
	if err := valid.validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"mode", func(c *Config) { c.Mode = "optional" }, "TELEMETRY_MODE"},
		{"service", func(c *Config) { c.ServiceName = "Room Gameplay" }, "SERVICE_NAME"},
		{"environment", func(c *Config) { c.Environment = "Kind Cluster" }, "DEPLOYMENT_ENV"},
		{"version", func(c *Config) { c.ServiceVersion = "bad version" }, "SERVICE_VERSION"},
		{"instance", func(c *Config) { c.InstanceID = "bad instance" }, "service.instance.id"},
		{"component", func(c *Config) { c.Component = "room/1" }, "UNOARENA_COMPONENT"},
		{"endpoint", func(c *Config) { c.OTLPEndpoint = "alloy:4317" }, "OTEL_EXPORTER_OTLP_ENDPOINT"},
		{"protocol", func(c *Config) { c.OTLPProtocol = "http/protobuf" }, "OTEL_EXPORTER_OTLP_PROTOCOL"},
		{"sampler", func(c *Config) { c.TracesSampler = "unknown" }, "OTEL_TRACES_SAMPLER"},
		{"ratio", func(c *Config) { c.TracesSampler = "traceidratio"; c.TracesSamplerArg = "1.1" }, "OTEL_TRACES_SAMPLER_ARG"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := valid
			test.mutate(&candidate)
			if err := candidate.validate(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validate() error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestDisabledConfigDoesNotRequireExporters(t *testing.T) {
	t.Setenv("OTEL_GO_X_OBSERVABILITY", "false")
	config := Config{
		Mode: ModeDisabled, ServiceName: "gateway", Environment: "offline",
		ServiceVersion: "dev", Component: "api", InstanceID: "host",
	}
	if err := config.validate(); err != nil {
		t.Fatalf("disabled config rejected: %v", err)
	}
}

func TestRequiredModeRequiresSDKObservability(t *testing.T) {
	t.Setenv("OTEL_GO_X_OBSERVABILITY", "false")
	config := Config{
		Mode: ModeRequired, ServiceName: "gateway", Environment: "kind",
		ServiceVersion: "abc", Component: "api", InstanceID: "pod",
		OTLPEndpoint: "http://alloy:4317", OTLPProtocol: "grpc",
		TracesSampler: "always_on", MetricsAddr: "127.0.0.1:0",
	}
	if err := config.validate(); err == nil || !strings.Contains(err.Error(), "OTEL_GO_X_OBSERVABILITY") {
		t.Fatalf("validate() error = %v", err)
	}
}

func TestConfigFromEnv(t *testing.T) {
	t.Setenv("TELEMETRY_MODE", "required")
	t.Setenv("SERVICE_NAME", " gateway ")
	t.Setenv("DEPLOYMENT_ENV", "kind")
	t.Setenv("SERVICE_VERSION", "abc123")
	t.Setenv("UNOARENA_COMPONENT", "api")
	t.Setenv("POD_UID", "pod-uid")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://alloy:4317")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
	t.Setenv("OTEL_TRACES_SAMPLER", "parentbased_always_on")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "")
	t.Setenv("METRICS_ADDR", ":9090")
	config := ConfigFromEnv()
	if config.Mode != ModeRequired || config.ServiceName != "gateway" || config.InstanceID != "pod-uid" || config.MetricsAddr != ":9090" {
		t.Fatalf("ConfigFromEnv() = %+v", config)
	}
}

func TestMutationRootSamplerSamplesMutationsAndRatiosReads(t *testing.T) {
	selected, err := sampler("x_parentbased_mutations", "0")
	if err != nil {
		t.Fatalf("sampler() error = %v", err)
	}

	tests := []struct {
		name       string
		parent     apiTrace.SpanContext
		method     string
		wantSample bool
	}{
		{name: "root mutation", method: "POST", wantSample: true},
		{name: "root read", method: "GET", wantSample: false},
		{name: "sampled parent wins for read", parent: testSpanContext(true), method: "GET", wantSample: true},
		{name: "unsampled parent wins for mutation", parent: testSpanContext(false), method: "DELETE", wantSample: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			if test.parent.IsValid() {
				ctx = apiTrace.ContextWithRemoteSpanContext(ctx, test.parent)
			}
			result := selected.ShouldSample(trace.SamplingParameters{
				ParentContext: ctx,
				TraceID:       apiTrace.TraceID{1},
				Name:          "HTTP " + test.method,
				Kind:          apiTrace.SpanKindServer,
				Attributes:    []attribute.KeyValue{attribute.String("http.request.method", test.method)},
			})
			gotSample := result.Decision == trace.RecordAndSample
			if gotSample != test.wantSample {
				t.Fatalf("decision = %v, want sampled = %t", result.Decision, test.wantSample)
			}
		})
	}
}

func TestMutationRootSamplerRejectsInvalidRatio(t *testing.T) {
	if _, err := sampler("x_parentbased_mutations", "1.1"); err == nil || !strings.Contains(err.Error(), "OTEL_TRACES_SAMPLER_ARG") {
		t.Fatalf("sampler() error = %v", err)
	}
}

func testSpanContext(sampled bool) apiTrace.SpanContext {
	flags := apiTrace.TraceFlags(0)
	if sampled {
		flags = apiTrace.FlagsSampled
	}
	return apiTrace.NewSpanContext(apiTrace.SpanContextConfig{
		TraceID:    apiTrace.TraceID{1},
		SpanID:     apiTrace.SpanID{1},
		TraceFlags: flags,
		Remote:     true,
	})
}
