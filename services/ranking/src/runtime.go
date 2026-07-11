package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"unoarena/services/ranking/store"
)

type rankingRuntime struct {
	app          RatingApplication
	pool         *store.Pool
	store        *store.RankingStore
	mode         string // durable | capability | misconfigured
	ready        bool
	readyReason  string
	schemaExp    store.SchemaExpectation
	durableReady func(context.Context) error
	credential   string
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

func wireRankingRuntime() (rankingRuntime, error) {
	cred := strings.TrimSpace(os.Getenv("RANKING_INTERNAL_CREDENTIAL"))
	if cred == "" {
		cred = strings.TrimSpace(os.Getenv("SERVICE_CREDENTIAL"))
	}
	dbURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	deploymentEnv := strings.TrimSpace(os.Getenv("DEPLOYMENT_ENV"))
	if deploymentEnv == "" {
		deploymentEnv = "development"
	}
	capability := envTruthy("RANKING_CAPABILITY_MODE")

	if dbURL != "" && !capability {
		if cred == "" {
			return rankingRuntime{
				app:         NewMemoryRatingStore(),
				mode:        "durable",
				ready:       false,
				readyReason: "durable_dependencies_missing: RANKING_INTERNAL_CREDENTIAL",
				credential:  cred,
			}, nil
		}
		pool, err := store.NewPool(context.Background(), dbURL)
		if err != nil {
			return rankingRuntime{}, fmt.Errorf("database pool: %w", err)
		}
		rs := store.NewRankingStore(pool.Pool)
		exp := store.DefaultSchemaExpectation()
		return rankingRuntime{
			app:        newDurableApp(rs),
			pool:       pool,
			store:      rs,
			mode:       "durable",
			ready:      true,
			schemaExp:  exp,
			credential: cred,
			durableReady: func(ctx context.Context) error {
				return store.VerifySchema(ctx, pool.Pool, exp)
			},
		}, nil
	}

	if !capability || !isNonProd(deploymentEnv) {
		return rankingRuntime{
			app:         NewMemoryRatingStore(),
			mode:        "misconfigured",
			ready:       false,
			readyReason: "database_unconfigured",
			credential:  cred,
		}, nil
	}

	mem := NewMemoryRatingStore()
	return rankingRuntime{
		app:   mem,
		mode:  "capability",
		ready: cred != "",
		readyReason: func() string {
			if cred == "" {
				return "internal_credential_unconfigured"
			}
			return ""
		}(),
		credential: cred,
	}, nil
}
