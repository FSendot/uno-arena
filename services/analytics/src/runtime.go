package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"unoarena/services/analytics/store"
)

type analyticsRuntime struct {
	app         AnalyticsApplication
	mode        string // durable | capability | misconfigured
	ready       bool
	readyReason string
	creds       ProducerCredentials
	chStore     *store.AnalyticsStore
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

func wireAnalyticsRuntime() (analyticsRuntime, error) {
	creds := ProducerCredentials{
		Room:       strings.TrimSpace(os.Getenv("ANALYTICS_ROOM_CREDENTIAL")),
		Ranking:    strings.TrimSpace(os.Getenv("ANALYTICS_RANKING_CREDENTIAL")),
		Tournament: strings.TrimSpace(os.Getenv("ANALYTICS_TOURNAMENT_CREDENTIAL")),
		Ops:        strings.TrimSpace(os.Getenv("ANALYTICS_OPS_CREDENTIAL")),
	}
	chURL := strings.TrimSpace(os.Getenv("CLICKHOUSE_URL"))
	chUser := strings.TrimSpace(os.Getenv("CLICKHOUSE_USER"))
	chPass := strings.TrimSpace(os.Getenv("CLICKHOUSE_PASSWORD"))
	chDB := strings.TrimSpace(os.Getenv("CLICKHOUSE_DB"))
	if chDB == "" {
		chDB = "analytics"
	}
	deploymentEnv := strings.TrimSpace(os.Getenv("DEPLOYMENT_ENV"))
	if deploymentEnv == "" {
		deploymentEnv = "development"
	}
	capability := envTruthy("ANALYTICS_CAPABILITY_MODE")

	credReason := scopedCredentialReadyReason(creds)

	// Durable mode: CLICKHOUSE_URL set and not explicit capability.
	if chURL != "" && !capability {
		if credReason != "" {
			return analyticsRuntime{
				app:         NewMemoryAnalyticsStore(),
				mode:        "durable",
				ready:       false,
				readyReason: "durable_dependencies_missing: " + credReason,
				creds:       creds,
			}, nil
		}
		if chUser == "" || chPass == "" {
			return analyticsRuntime{
				app:         NewMemoryAnalyticsStore(),
				mode:        "durable",
				ready:       false,
				readyReason: "durable_dependencies_missing: CLICKHOUSE_USER/CLICKHOUSE_PASSWORD",
				creds:       creds,
			}, nil
		}
		client, err := store.NewClient(store.Config{
			URL: chURL, User: chUser, Password: chPass, Database: chDB,
		})
		if err != nil {
			return analyticsRuntime{}, fmt.Errorf("clickhouse client: %w", err)
		}
		chs := store.NewAnalyticsStore(client)
		return analyticsRuntime{
			app:     chs,
			mode:    "durable",
			ready:   true,
			creds:   creds,
			chStore: chs,
		}, nil
	}

	// Capability memory only when explicitly opted in on non-production.
	if !capability || !isNonProd(deploymentEnv) {
		reason := "clickhouse_unconfigured"
		if capability && !isNonProd(deploymentEnv) {
			reason = "capability_mode_forbidden_in_production"
		}
		return analyticsRuntime{
			app:         NewMemoryAnalyticsStore(),
			mode:        "misconfigured",
			ready:       false,
			readyReason: reason,
			creds:       creds,
		}, nil
	}

	mem := NewMemoryAnalyticsStore()
	ready := credReason == ""
	return analyticsRuntime{
		app:         mem,
		mode:        "capability",
		ready:       ready,
		readyReason: credReason,
		creds:       creds,
	}, nil
}

func (rt analyticsRuntime) durableReady(ctx context.Context) error {
	if rt.chStore == nil {
		return fmt.Errorf("durable store unconfigured")
	}
	return rt.chStore.Ready(ctx)
}
