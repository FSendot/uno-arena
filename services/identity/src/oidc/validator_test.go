package oidc_test

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"unoarena/services/identity/oidc"
)

func TestValidateIDTokenRS256HappyPath(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	kid := "test-kid"
	issuer := "http://keycloak.local/realms/unoarena"
	aud := "unoarena-cli"

	mux := http.NewServeMux()
	mux.HandleFunc("/realms/unoarena/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"issuer":         issuer,
			"jwks_uri":       issuer + "/protocol/openid-connect/certs",
			"token_endpoint": issuer + "/protocol/openid-connect/token",
		})
	})
	mux.HandleFunc("/realms/unoarena/protocol/openid-connect/certs", func(w http.ResponseWriter, r *http.Request) {
		n := base64.RawURLEncoding.EncodeToString(key.N.Bytes())
		e := base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes())
		_ = json.NewEncoder(w).Encode(map[string]any{
			"keys": []map[string]string{{
				"kty": "RSA", "kid": kid, "n": n, "e": e, "alg": "RS256", "use": "sig",
			}},
		})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	issuer = srv.URL + "/realms/unoarena"

	v, err := oidc.NewValidator(oidc.Config{
		IssuerURL: issuer,
		Audiences: []string{aud},
		AllowHTTP: true,
		HTTPClient: srv.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	tok := mustSign(t, key, kid, map[string]any{
		"iss":                issuer,
		"sub":                "sub-1",
		"aud":                aud,
		"exp":                now.Add(time.Hour).Unix(),
		"iat":                now.Unix(),
		"nbf":                now.Unix(),
		"preferred_username": "test-player",
		"realm_access":       map[string]any{"roles": []any{"player"}},
	})
	claims, err := v.ValidateIDToken(context.Background(), tok, "")
	if err != nil {
		t.Fatal(err)
	}
	if claims.Subject != "sub-1" || claims.Username != "test-player" {
		t.Fatalf("claims=%+v", claims)
	}
	if len(claims.Roles) != 1 || claims.Roles[0] != "player" {
		t.Fatalf("roles=%v", claims.Roles)
	}
}

func TestValidateRejectsWrongAudienceAndExpired(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	kid := "k1"
	mux := http.NewServeMux()
	var issuer string
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"issuer": issuer, "jwks_uri": issuer + "/certs", "token_endpoint": issuer + "/token",
		})
	})
	mux.HandleFunc("/certs", func(w http.ResponseWriter, r *http.Request) {
		n := base64.RawURLEncoding.EncodeToString(key.N.Bytes())
		e := base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes())
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{
			"kty": "RSA", "kid": kid, "n": n, "e": e, "alg": "RS256",
		}}})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	issuer = srv.URL
	v, err := oidc.NewValidator(oidc.Config{IssuerURL: issuer, Audiences: []string{"good"}, AllowHTTP: true, HTTPClient: srv.Client()})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	badAud := mustSign(t, key, kid, map[string]any{
		"iss": issuer, "sub": "s", "aud": "bad", "exp": now.Add(time.Hour).Unix(), "iat": now.Unix(),
		"preferred_username": "u",
	})
	if _, err := v.ValidateIDToken(context.Background(), badAud, ""); err == nil {
		t.Fatal("expected audience rejection")
	}
	expired := mustSign(t, key, kid, map[string]any{
		"iss": issuer, "sub": "s", "aud": "good", "exp": now.Add(-time.Hour).Unix(), "iat": now.Add(-2 * time.Hour).Unix(),
		"preferred_username": "u",
	})
	if _, err := v.ValidateIDToken(context.Background(), expired, ""); err == nil {
		t.Fatal("expected expiry rejection")
	}
}

func TestProductionReadinessRejectsDirectGrant(t *testing.T) {
	cfg := oidc.Config{IssuerURL: "https://idp.example/realms/unoarena", Audiences: []string{"cli"}}
	if err := oidc.ProductionReadinessCheck("production", cfg, true, false, false); err == nil {
		t.Fatal("expected reject")
	}
	if err := oidc.ProductionReadinessCheck("production", cfg, false, false, false); err != nil {
		t.Fatal(err)
	}
	if err := oidc.ProductionReadinessCheck("local", cfg, true, true, true); err != nil {
		t.Fatal(err)
	}
}

func TestValidateMultiAudienceRequiresAZP(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	kid := "k-azp"
	mux := http.NewServeMux()
	var issuer string
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"issuer": issuer, "jwks_uri": issuer + "/certs", "token_endpoint": issuer + "/token",
		})
	})
	mux.HandleFunc("/certs", func(w http.ResponseWriter, r *http.Request) {
		n := base64.RawURLEncoding.EncodeToString(key.N.Bytes())
		e := base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes())
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{
			"kty": "RSA", "kid": kid, "n": n, "e": e, "alg": "RS256", "use": "sig",
		}}})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	issuer = srv.URL
	clientID := "unoarena-cli"
	v, err := oidc.NewValidator(oidc.Config{
		IssuerURL:  issuer,
		Audiences:  []string{clientID, "unoarena-api"},
		ClientID:   clientID,
		AllowHTTP:  true,
		HTTPClient: srv.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	base := map[string]any{
		"iss": issuer, "sub": "s", "aud": []any{clientID, "unoarena-api"},
		"exp": now.Add(time.Hour).Unix(), "iat": now.Unix(), "preferred_username": "u",
	}
	noAzp := mustSign(t, key, kid, base)
	if _, err := v.ValidateIDToken(context.Background(), noAzp, ""); err == nil {
		t.Fatal("multi-aud without azp must fail")
	}
	bad := cloneMap(base)
	bad["azp"] = "other-client"
	if _, err := v.ValidateIDToken(context.Background(), mustSign(t, key, kid, bad), ""); err == nil {
		t.Fatal("azp outside client/audiences must fail")
	}
	ok := cloneMap(base)
	ok["azp"] = clientID
	if _, err := v.ValidateIDToken(context.Background(), mustSign(t, key, kid, ok), ""); err != nil {
		t.Fatalf("azp=client_id must pass: %v", err)
	}
	okAud := cloneMap(base)
	okAud["azp"] = "unoarena-api"
	if _, err := v.ValidateIDToken(context.Background(), mustSign(t, key, kid, okAud), ""); err != nil {
		t.Fatalf("azp=accepted audience must pass: %v", err)
	}
}

func TestJWKSSkipsIncompatibleAlgOrUse(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	goodKid, badAlgKid, badUseKid := "good", "bad-alg", "bad-use"
	mux := http.NewServeMux()
	var issuer string
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"issuer": issuer, "jwks_uri": issuer + "/certs", "token_endpoint": issuer + "/token",
		})
	})
	mux.HandleFunc("/certs", func(w http.ResponseWriter, r *http.Request) {
		n := base64.RawURLEncoding.EncodeToString(key.N.Bytes())
		e := base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes())
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{
			{"kty": "RSA", "kid": badAlgKid, "n": n, "e": e, "alg": "RS384", "use": "sig"},
			{"kty": "RSA", "kid": badUseKid, "n": n, "e": e, "alg": "RS256", "use": "enc"},
			{"kty": "RSA", "kid": goodKid, "n": n, "e": e, "alg": "RS256", "use": "sig"},
		}})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	issuer = srv.URL
	v, err := oidc.NewValidator(oidc.Config{
		IssuerURL: issuer, Audiences: []string{"aud"}, AllowHTTP: true, HTTPClient: srv.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	claims := map[string]any{
		"iss": issuer, "sub": "s", "aud": "aud",
		"exp": now.Add(time.Hour).Unix(), "iat": now.Unix(), "preferred_username": "u",
	}
	if _, err := v.ValidateIDToken(context.Background(), mustSign(t, key, badAlgKid, claims), ""); err == nil {
		t.Fatal("RS384 kid must be rejected")
	}
	if _, err := v.ValidateIDToken(context.Background(), mustSign(t, key, badUseKid, claims), ""); err == nil {
		t.Fatal("use=enc kid must be rejected")
	}
	if _, err := v.ValidateIDToken(context.Background(), mustSign(t, key, goodKid, claims), ""); err != nil {
		t.Fatalf("compatible key: %v", err)
	}
}

func cloneMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func mustSign(t *testing.T, key *rsa.PrivateKey, kid string, claims map[string]any) string {
	t.Helper()
	hb, _ := json.Marshal(map[string]string{"alg": "RS256", "kid": kid, "typ": "JWT"})
	pb, _ := json.Marshal(claims)
	h := base64.RawURLEncoding.EncodeToString(hb)
	p := base64.RawURLEncoding.EncodeToString(pb)
	sum := sha256.Sum256([]byte(h + "." + p))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sum[:])
	if err != nil {
		t.Fatal(err)
	}
	return h + "." + p + "." + base64.RawURLEncoding.EncodeToString(sig)
}
