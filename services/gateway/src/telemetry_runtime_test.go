package main

import (
	"context"
	"testing"
	"time"
)

func TestRequiredTelemetryRejectsFileAuditSink(t *testing.T) {
	t.Setenv("TELEMETRY_MODE", "required")
	if err := validateGatewayAuditConfig("/tmp/gateway-audit.jsonl"); err == nil {
		t.Fatal("required telemetry must reject file audit sink")
	}
	if err := validateGatewayAuditConfig(""); err != nil {
		t.Fatalf("stdout telemetry audit must remain allowed: %v", err)
	}

	t.Setenv("TELEMETRY_MODE", "disabled")
	if err := validateGatewayAuditConfig("/tmp/gateway-audit.jsonl"); err != nil {
		t.Fatalf("offline file audit must remain allowed: %v", err)
	}
}

func TestGatewayHTTPClientForConfigUsesBackendTimeout(t *testing.T) {
	for key, value := range map[string]string{
		"TELEMETRY_MODE": "disabled", "SERVICE_NAME": "gateway", "DEPLOYMENT_ENV": "test",
		"SERVICE_VERSION": "test", "UNOARENA_COMPONENT": "api",
	} {
		t.Setenv(key, value)
	}
	runtime, err := startGatewayTelemetry(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Shutdown(ctx); err != nil {
			t.Errorf("shutdown telemetry: %v", err)
		}
	})

	client := gatewayHTTPClientForConfig(runtime, gatewayConfig{BackendHTTPTimeout: 10 * time.Second})
	if client.Timeout != 10*time.Second {
		t.Fatalf("client timeout=%s want 10s", client.Timeout)
	}
}
