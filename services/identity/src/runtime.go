package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"unoarena/services/identity/domain"
	"unoarena/services/identity/oidc"
	"unoarena/services/identity/store"
)

type identityRuntime struct {
	svc                *domain.Service
	sessions           *domain.MemorySessionRepository // capability only
	pool               *store.Pool
	oidc               *oidc.Validator
	mode               string // durable | capability | misconfigured
	ready              bool
	readyReason        string
	allowPasswordLogin bool
	allowRegister      bool
	deploymentEnv      string
	schemaExp          store.SchemaExpectation
}

func envTruthy(name string) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(name)))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

func isNonProd(env string) bool {
	switch strings.ToLower(strings.TrimSpace(env)) {
	case "production", "staging", "prod":
		return false
	default:
		return true
	}
}

func wireIdentityRuntime() (identityRuntime, error) {
	cred := strings.TrimSpace(os.Getenv("IDENTITY_INTERNAL_CREDENTIAL"))
	dbURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	deploymentEnv := strings.TrimSpace(os.Getenv("DEPLOYMENT_ENV"))
	if deploymentEnv == "" {
		deploymentEnv = "development"
	}
	capability := envTruthy("IDENTITY_CAPABILITY_MODE")
	allowPassword := envTruthy("IDENTITY_ALLOW_PASSWORD_LOGIN") || envTruthy("IDENTITY_OIDC_DIRECT_GRANT")
	allowRegister := envTruthy("IDENTITY_ALLOW_TEST_PROVISIONING") || capability
	allowHTTP := envTruthy("OIDC_ALLOW_HTTP")

	issuer := strings.TrimSpace(os.Getenv("OIDC_ISSUER_URL"))
	tokenEP := strings.TrimSpace(os.Getenv("OIDC_TOKEN_ENDPOINT"))
	audience := strings.TrimSpace(os.Getenv("OIDC_AUDIENCE"))
	clientID := strings.TrimSpace(os.Getenv("OIDC_CLIENT_ID"))
	audiences := splitCSV(audience)

	oidcCfg := oidc.Config{
		IssuerURL:     issuer,
		TokenEndpoint: tokenEP,
		Audiences:     audiences,
		ClientID:      clientID,
		AllowHTTP:     allowHTTP && isNonProd(deploymentEnv),
	}
	if err := oidc.ProductionReadinessCheck(
		deploymentEnv,
		oidcCfg,
		allowPassword,
		envTruthy("IDENTITY_OIDC_DIRECT_GRANT"),
		envTruthy("IDENTITY_ALLOW_TEST_PROVISIONING"),
	); err != nil {
		return identityRuntime{}, err
	}

	// Durable mode when DATABASE_URL is set (production path). Capability flag forces memory.
	if dbURL != "" && !capability {
		pool, err := store.NewPool(context.Background(), dbURL)
		if err != nil {
			return identityRuntime{}, fmt.Errorf("database pool: %w", err)
		}
		players := store.NewPlayerStore(pool.Pool)
		sessions := store.NewSessionStore(pool.Pool)
		ids := domain.RandomIDGenerator{}
		svc := domain.NewService(domain.ServiceDeps{
			Players:      players,
			Sessions:     sessions,
			Hasher:       domain.NewCheckpointPasswordHasher(ids),
			IDs:          ids,
			Tokens:       ids,
			Clock:        domain.SystemClock{},
			SessionTTL:   24 * time.Hour,
			Transport:    nil, // Debezium CDC — never drain/poll
			AllowedRoles: splitCSV(os.Getenv("IDENTITY_OIDC_ALLOWED_ROLES")),
		})
		var validator *oidc.Validator
		if issuer != "" && len(audiences) > 0 {
			validator, err = oidc.NewValidator(oidcCfg)
			if err != nil {
				pool.Close()
				return identityRuntime{}, err
			}
		}
		return identityRuntime{
			svc:                svc,
			pool:               pool,
			oidc:               validator,
			mode:               "durable",
			ready:              true,
			allowPasswordLogin: allowPassword && isNonProd(deploymentEnv),
			allowRegister:      allowRegister && isNonProd(deploymentEnv),
			deploymentEnv:      deploymentEnv,
			schemaExp:          store.DefaultSchemaExpectation(),
		}, nil
	}

	// Memory only behind explicit capability mode + non-prod.
	if !capability || !isNonProd(deploymentEnv) {
		return identityRuntime{
			mode:        "misconfigured",
			ready:       false,
			readyReason: "database_unconfigured",
		}, nil
	}

	invalidationURL := strings.TrimSpace(os.Getenv("IDENTITY_INVALIDATION_URL"))
	svc, sessions, invReady := newCapabilityService(invalidationURL, cred)
	var validator *oidc.Validator
	if issuer != "" && len(audiences) > 0 {
		var err error
		validator, err = oidc.NewValidator(oidcCfg)
		if err != nil {
			return identityRuntime{}, err
		}
	}
	return identityRuntime{
		svc:                svc,
		sessions:           sessions,
		oidc:               validator,
		mode:               "capability",
		ready:              invReady,
		readyReason:        capabilityReadyReason(invReady),
		allowPasswordLogin: true,
		allowRegister:      true,
		deploymentEnv:      deploymentEnv,
	}, nil
}

func capabilityReadyReason(ready bool) string {
	if ready {
		return ""
	}
	return "invalidation_transport_unconfigured"
}

func newCapabilityService(invalidationURL, credential string) (*domain.Service, *domain.MemorySessionRepository, bool) {
	ids := domain.RandomIDGenerator{}
	sessions := domain.NewMemorySessionRepository()
	var transport domain.InvalidationTransport
	ready := false
	if invalidationURL != "" && credential != "" {
		transport = NewHTTPInvalidationTransport(invalidationURL, credential, nil)
		ready = true
	} else {
		transport = domain.NewMemoryInvalidationTransport()
	}
	svc := domain.NewService(domain.ServiceDeps{
		Players:    domain.NewMemoryPlayerRepository(),
		Sessions:   sessions,
		Hasher:     domain.NewCheckpointPasswordHasher(ids),
		IDs:        ids,
		Tokens:     ids,
		Clock:      domain.SystemClock{},
		SessionTTL: 24 * time.Hour,
		Transport:  transport,
	})
	return svc, sessions, ready
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func mapDomainHTTP(err error) (status int, code, message string) {
	switch {
	case err == nil:
		return 200, "", ""
	case errors.Is(err, domain.ErrUnavailable), errors.Is(err, oidc.ErrUnavailable):
		return 503, "unavailable", "identity unavailable"
	case errors.Is(err, domain.ErrInvalidCredentials),
		errors.Is(err, domain.ErrSessionInvalid),
		errors.Is(err, domain.ErrSessionNotFound),
		errors.Is(err, domain.ErrSessionExpired),
		errors.Is(err, oidc.ErrInvalidToken),
		errors.Is(err, oidc.ErrAudienceMismatch),
		errors.Is(err, oidc.ErrIssuerMismatch):
		return 401, "unauthorized", "unauthorized"
	case errors.Is(err, domain.ErrNotAllowed), errors.Is(err, oidc.ErrNotAllowed):
		return 403, "forbidden", "operation not allowed"
	case errors.Is(err, domain.ErrInvalidInput), errors.Is(err, domain.ErrUsernameTaken):
		return 400, "bad_request", err.Error()
	default:
		return 500, "internal_error", "internal error"
	}
}
