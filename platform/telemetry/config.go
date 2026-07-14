package telemetry

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Mode controls whether telemetry export is mandatory or explicitly disabled.
type Mode string

const (
	ModeRequired Mode = "required"
	ModeDisabled Mode = "disabled"
)

var (
	identityPattern  = regexp.MustCompile(`^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$`)
	componentPattern = identityPattern
	versionPattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:+-]*$`)
	instancePattern  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]*$`)
)

// Config is the complete process telemetry configuration. Writer is intended
// for tests and offline programs; nil means os.Stdout.
type Config struct {
	Mode             Mode
	ServiceName      string
	Environment      string
	ServiceVersion   string
	Component        string
	InstanceID       string
	OTLPEndpoint     string
	OTLPProtocol     string
	TracesSampler    string
	TracesSamplerArg string
	MetricsAddr      string
	Writer           io.Writer
}

// ConfigFromEnv reads the repository telemetry environment contract. Instance
// identity falls back to the host name for non-Kubernetes execution.
func ConfigFromEnv() Config {
	instanceID := strings.TrimSpace(os.Getenv("POD_UID"))
	if instanceID == "" {
		instanceID, _ = os.Hostname()
	}
	return Config{
		Mode:             Mode(strings.TrimSpace(os.Getenv("TELEMETRY_MODE"))),
		ServiceName:      strings.TrimSpace(os.Getenv("SERVICE_NAME")),
		Environment:      strings.TrimSpace(os.Getenv("DEPLOYMENT_ENV")),
		ServiceVersion:   strings.TrimSpace(os.Getenv("SERVICE_VERSION")),
		Component:        strings.TrimSpace(os.Getenv("UNOARENA_COMPONENT")),
		InstanceID:       strings.TrimSpace(instanceID),
		OTLPEndpoint:     strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")),
		OTLPProtocol:     strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL")),
		TracesSampler:    strings.TrimSpace(os.Getenv("OTEL_TRACES_SAMPLER")),
		TracesSamplerArg: strings.TrimSpace(os.Getenv("OTEL_TRACES_SAMPLER_ARG")),
		MetricsAddr:      strings.TrimSpace(os.Getenv("METRICS_ADDR")),
		Writer:           os.Stdout,
	}
}

func (c *Config) normalize() {
	c.ServiceName = strings.TrimSpace(c.ServiceName)
	c.Environment = strings.TrimSpace(c.Environment)
	c.ServiceVersion = strings.TrimSpace(c.ServiceVersion)
	c.Component = strings.TrimSpace(c.Component)
	c.InstanceID = strings.TrimSpace(c.InstanceID)
	c.OTLPEndpoint = strings.TrimSpace(c.OTLPEndpoint)
	c.OTLPProtocol = strings.TrimSpace(c.OTLPProtocol)
	c.TracesSampler = strings.TrimSpace(c.TracesSampler)
	c.TracesSamplerArg = strings.TrimSpace(c.TracesSamplerArg)
	c.MetricsAddr = strings.TrimSpace(c.MetricsAddr)
	if c.Writer == nil {
		c.Writer = os.Stdout
	}
	if c.InstanceID == "" {
		c.InstanceID, _ = os.Hostname()
	}
}

func (c Config) validate() error {
	var errs []error
	if c.Mode != ModeRequired && c.Mode != ModeDisabled {
		errs = append(errs, fmt.Errorf("TELEMETRY_MODE must be %q or %q", ModeRequired, ModeDisabled))
	}
	for name, value := range map[string]string{
		"SERVICE_NAME":        c.ServiceName,
		"DEPLOYMENT_ENV":      c.Environment,
		"SERVICE_VERSION":     c.ServiceVersion,
		"UNOARENA_COMPONENT":  c.Component,
		"service.instance.id": c.InstanceID,
	} {
		if value == "" {
			errs = append(errs, fmt.Errorf("%s is required", name))
		}
	}
	if c.ServiceName != "" && !identityPattern.MatchString(c.ServiceName) {
		errs = append(errs, errors.New("SERVICE_NAME must be a bounded lower-kebab-case identity"))
	}
	if len(c.ServiceName) > 63 {
		errs = append(errs, errors.New("SERVICE_NAME must not exceed 63 characters"))
	}
	if c.Environment != "" && (!identityPattern.MatchString(c.Environment) || len(c.Environment) > 63) {
		errs = append(errs, errors.New("DEPLOYMENT_ENV must be a lower-kebab-case identity of at most 63 characters"))
	}
	if c.Component != "" && !componentPattern.MatchString(c.Component) {
		errs = append(errs, errors.New("UNOARENA_COMPONENT must be a bounded lower-kebab-case identity"))
	}
	if len(c.Component) > 63 {
		errs = append(errs, errors.New("UNOARENA_COMPONENT must not exceed 63 characters"))
	}
	if c.ServiceVersion != "" && (!versionPattern.MatchString(c.ServiceVersion) || len(c.ServiceVersion) > 128) {
		errs = append(errs, errors.New("SERVICE_VERSION contains invalid characters or exceeds 128 characters"))
	}
	if c.InstanceID != "" && (!instancePattern.MatchString(c.InstanceID) || len(c.InstanceID) > 253) {
		errs = append(errs, errors.New("service.instance.id contains invalid characters or exceeds 253 characters"))
	}
	if c.Mode != ModeRequired {
		return errors.Join(errs...)
	}
	if c.MetricsAddr == "" {
		errs = append(errs, errors.New("METRICS_ADDR is required"))
	}
	if c.OTLPProtocol != "grpc" {
		errs = append(errs, errors.New("OTEL_EXPORTER_OTLP_PROTOCOL must be grpc"))
	}
	if c.OTLPEndpoint == "" {
		errs = append(errs, errors.New("OTEL_EXPORTER_OTLP_ENDPOINT is required"))
	} else if parsed, err := url.Parse(c.OTLPEndpoint); err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		errs = append(errs, errors.New("OTEL_EXPORTER_OTLP_ENDPOINT must be an http or https URL with a host"))
	}
	if os.Getenv("OTEL_GO_X_OBSERVABILITY") != "true" {
		errs = append(errs, errors.New("OTEL_GO_X_OBSERVABILITY must be true in required mode"))
	}
	if _, err := sampler(c.TracesSampler, c.TracesSamplerArg); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func sampler(name, argument string) (sdktrace.Sampler, error) {
	if name == "" {
		return nil, errors.New("OTEL_TRACES_SAMPLER is required")
	}
	var base sdktrace.Sampler
	switch name {
	case "always_on":
		base = sdktrace.AlwaysSample()
	case "always_off":
		base = sdktrace.NeverSample()
	case "traceidratio", "parentbased_traceidratio":
		ratio, err := samplerRatio(argument)
		if err != nil {
			return nil, err
		}
		base = sdktrace.TraceIDRatioBased(ratio)
	case "x_parentbased_mutations":
		ratio, err := samplerRatio(argument)
		if err != nil {
			return nil, err
		}
		return sdktrace.ParentBased(mutationRootSampler{fallback: sdktrace.TraceIDRatioBased(ratio)}), nil
	case "parentbased_always_on":
		return sdktrace.ParentBased(sdktrace.AlwaysSample()), nil
	case "parentbased_always_off":
		return sdktrace.ParentBased(sdktrace.NeverSample()), nil
	default:
		return nil, fmt.Errorf("unsupported OTEL_TRACES_SAMPLER %q", name)
	}
	if strings.HasPrefix(name, "parentbased_") {
		return sdktrace.ParentBased(base), nil
	}
	return base, nil
}

func samplerRatio(argument string) (float64, error) {
	ratio, err := strconv.ParseFloat(argument, 64)
	if err != nil || ratio < 0 || ratio > 1 {
		return 0, errors.New("OTEL_TRACES_SAMPLER_ARG must be a number between 0 and 1")
	}
	return ratio, nil
}

// mutationRootSampler keeps command traces deterministic without tracing every
// public read. ParentBased wraps it so downstream spans always follow the
// decision made at the trusted Gateway root.
type mutationRootSampler struct {
	fallback sdktrace.Sampler
}

func (s mutationRootSampler) ShouldSample(parameters sdktrace.SamplingParameters) sdktrace.SamplingResult {
	for _, candidate := range parameters.Attributes {
		if candidate.Key != "http.request.method" && candidate.Key != "http.method" {
			continue
		}
		switch strings.ToUpper(candidate.Value.AsString()) {
		case "POST", "PUT", "PATCH", "DELETE":
			return sdktrace.AlwaysSample().ShouldSample(parameters)
		default:
			return s.fallback.ShouldSample(parameters)
		}
	}
	return s.fallback.ShouldSample(parameters)
}

func (s mutationRootSampler) Description() string {
	return "MutationRootSampler{" + s.fallback.Description() + "}"
}
