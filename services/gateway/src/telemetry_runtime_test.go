package main

import "testing"

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
