package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"unoarena/services/identity/domain"
	"unoarena/services/identity/oidc"
)

func TestWireIdentityRuntimeMisconfiguredWithoutCapabilityOrDB(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("IDENTITY_CAPABILITY_MODE", "")
	t.Setenv("DEPLOYMENT_ENV", "development")
	t.Setenv("OIDC_ISSUER_URL", "")
	t.Setenv("OIDC_AUDIENCE", "")
	t.Setenv("IDENTITY_ALLOW_PASSWORD_LOGIN", "")
	t.Setenv("IDENTITY_OIDC_DIRECT_GRANT", "")
	t.Setenv("IDENTITY_ALLOW_TEST_PROVISIONING", "")

	rt, err := wireIdentityRuntime()
	if err != nil {
		t.Fatalf("wire: %v", err)
	}
	if rt.mode != "misconfigured" {
		t.Fatalf("mode=%q want misconfigured", rt.mode)
	}
	if rt.svc != nil {
		t.Fatal("misconfigured must not allocate a memory service")
	}
	if rt.ready {
		t.Fatal("misconfigured must not be ready")
	}
}

func TestWireIdentityRuntimeMemoryOnlyWithExplicitCapabilityNonProd(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("IDENTITY_CAPABILITY_MODE", "true")
	t.Setenv("DEPLOYMENT_ENV", "local")
	t.Setenv("OIDC_ISSUER_URL", "")
	t.Setenv("OIDC_AUDIENCE", "")
	t.Setenv("IDENTITY_INTERNAL_CREDENTIAL", "cred")
	t.Setenv("IDENTITY_INVALIDATION_URL", "http://gateway.local")

	rt, err := wireIdentityRuntime()
	if err != nil {
		t.Fatalf("wire: %v", err)
	}
	if rt.mode != "capability" || rt.svc == nil {
		t.Fatalf("capability memory expected, mode=%q svc=%v", rt.mode, rt.svc != nil)
	}
}

func TestWireIdentityRuntimeRejectsCapabilityMemoryInProduction(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("IDENTITY_CAPABILITY_MODE", "true")
	t.Setenv("DEPLOYMENT_ENV", "production")
	t.Setenv("OIDC_ISSUER_URL", "https://idp.example/realms/unoarena")
	t.Setenv("OIDC_AUDIENCE", "unoarena-cli")
	t.Setenv("IDENTITY_ALLOW_PASSWORD_LOGIN", "")
	t.Setenv("IDENTITY_OIDC_DIRECT_GRANT", "")
	t.Setenv("IDENTITY_ALLOW_TEST_PROVISIONING", "")

	rt, err := wireIdentityRuntime()
	if err != nil {
		t.Fatalf("wire: %v", err)
	}
	if rt.mode != "misconfigured" || rt.svc != nil {
		t.Fatalf("prod capability must not use memory: mode=%q svc=%v", rt.mode, rt.svc != nil)
	}
}

func TestMisconfiguredServerHealthOKReady503NoPanic(t *testing.T) {
	srv := serverFromRuntime(identityRuntime{
		mode:        "misconfigured",
		ready:       false,
		readyReason: "database_unconfigured",
	}, "cred")
	mux := srv.routes()

	hw := httptest.NewRecorder()
	mux.ServeHTTP(hw, httptest.NewRequest(http.MethodGet, "/health", nil))
	if hw.Code != http.StatusOK {
		t.Fatalf("health: %d", hw.Code)
	}

	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if rw.Code != http.StatusServiceUnavailable {
		t.Fatalf("ready: %d body=%s", rw.Code, rw.Body.String())
	}
	var ready map[string]string
	_ = json.NewDecoder(rw.Body).Decode(&ready)
	if ready["reason"] != "database_unconfigured" {
		t.Fatalf("ready reason=%q", ready["reason"])
	}

	lw := httptest.NewRecorder()
	mux.ServeHTTP(lw, httptest.NewRequest(http.MethodPost, "/v1/auth/login", nil))
	if lw.Code != http.StatusServiceUnavailable {
		t.Fatalf("login without svc must 503 not panic, got %d", lw.Code)
	}
}

func TestMapDomainHTTPDurableAndOIDCUnavailableAre503(t *testing.T) {
	status, code, _ := mapDomainHTTP(domain.WrapUnavailable(errors.New("db down")))
	if status != http.StatusServiceUnavailable || code != "unavailable" {
		t.Fatalf("db: status=%d code=%s", status, code)
	}
	status, code, _ = mapDomainHTTP(oidc.ErrUnavailable)
	if status != http.StatusServiceUnavailable || code != "unavailable" {
		t.Fatalf("oidc: status=%d code=%s", status, code)
	}
	status, code, _ = mapDomainHTTP(domain.ErrInvalidCredentials)
	if status != http.StatusUnauthorized {
		t.Fatalf("creds: status=%d", status)
	}
	status, code, _ = mapDomainHTTP(oidc.ErrInvalidToken)
	if status != http.StatusUnauthorized {
		t.Fatalf("token: status=%d", status)
	}
}
