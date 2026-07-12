package store

import (
	"strings"
	"testing"
)

func TestSanitizeProvisioningError_NeverAppendsRaw(t *testing.T) {
	secret := `password=hunter2 raw body {"token":"abc"}`
	got := sanitizeProvisioningError(secret)
	if got != "room_provision_failed" {
		t.Fatalf("got %q", got)
	}
	if strings.Contains(got, "hunter2") || strings.Contains(got, "token") || strings.Contains(got, "{") {
		t.Fatalf("leaked: %q", got)
	}
	if sanitizeProvisioningError("room provision HTTP 5xx: "+secret) != "room_provision_http_5xx" {
		t.Fatal("5xx class")
	}
	if sanitizeProvisioningError("room provision HTTP 4xx") != "room_provision_http_4xx" {
		t.Fatal("4xx class")
	}
	if sanitizeProvisioningError("room_assignment_conflict: requested x") != "room_assignment_conflict" {
		t.Fatal("mismatch class")
	}
	if sanitizeProvisioningError("provisioning_fence") != "lease_owner_lost" {
		t.Fatal("fence class")
	}
}
