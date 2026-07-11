package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"unoarena/services/tournament-orchestration/store"
)

type tournamentRuntime struct {
	svc         *Service
	pool        *store.Pool
	store       *store.TournamentStore
	mode        string // durable | capability | misconfigured
	ready       bool
	readyReason string
	schemaExp   store.SchemaExpectation
	durableReady func(context.Context) error
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

func wireTournamentRuntime() (tournamentRuntime, error) {
	cred := strings.TrimSpace(os.Getenv("TOURNAMENT_INTERNAL_CREDENTIAL"))
	if cred == "" {
		cred = strings.TrimSpace(os.Getenv("SERVICE_CREDENTIAL"))
	}
	dbURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	deploymentEnv := strings.TrimSpace(os.Getenv("DEPLOYMENT_ENV"))
	if deploymentEnv == "" {
		deploymentEnv = "development"
	}
	capability := envTruthy("TOURNAMENT_CAPABILITY_MODE")
	roomURL := strings.TrimSpace(os.Getenv("ROOM_GAMEPLAY_URL"))
	roomCred := firstNonEmpty(
		strings.TrimSpace(os.Getenv("ROOM_SERVICE_CREDENTIAL")),
		strings.TrimSpace(os.Getenv("TOURNAMENT_ROOM_CREDENTIAL")),
		cred,
	)
	workerRole := strings.TrimSpace(os.Getenv("WORKER_ROLE"))
	if workerRole != "" {
		// No safe WORKER_ROLE loop is implemented; refuse to pretend provisioning worker is live.
		return tournamentRuntime{}, fmt.Errorf("WORKER_ROLE=%s unsupported; provisioning worker disabled (use synchronous ProcessProvisioningBatch)", workerRole)
	}

	if dbURL != "" && !capability {
		missing := []string{}
		if roomURL == "" {
			missing = append(missing, "ROOM_GAMEPLAY_URL")
		}
		if cred == "" {
			missing = append(missing, "TOURNAMENT_INTERNAL_CREDENTIAL")
		}
		if len(missing) > 0 {
			return tournamentRuntime{
				svc: NewService(ServiceDeps{
					Repo:      NewMemoryTournamentRepository(), // never used while not ready
					Rooms:     NoopRoomProvisioner{},
					Publisher: NoopPublisher{},
					Audit:     NewMemoryAudit(),
				}),
				mode:        "durable",
				ready:       false,
				readyReason: "durable_dependencies_missing: " + strings.Join(missing, ","),
			}, nil
		}
		pool, err := store.NewPool(context.Background(), dbURL)
		if err != nil {
			return tournamentRuntime{}, fmt.Errorf("database pool: %w", err)
		}
		ts := store.NewTournamentStore(pool.Pool)
		repo := &durableRepo{store: ts}
		svc := NewService(ServiceDeps{
			Repo:      repo,
			Rooms:     NewHTTPRoomProvisioner(roomURL, roomCred, nil),
			Publisher: NoopPublisher{}, // CDC publishes; never DrainOutbox in durable mode
			Audit:     NewMemoryAudit(),
			Clock:     systemClock{},
			IDs:       randomIDs{},
		})
		exp := store.DefaultSchemaExpectation()
		return tournamentRuntime{
			svc:   svc,
			pool:  pool,
			store: ts,
			mode:  "durable",
			ready: true,
			schemaExp: exp,
			durableReady: func(ctx context.Context) error {
				return store.VerifySchema(ctx, pool.Pool, exp)
			},
		}, nil
	}

	if !capability || !isNonProd(deploymentEnv) {
		return tournamentRuntime{
			mode:        "misconfigured",
			ready:       false,
			readyReason: "database_unconfigured",
			svc: NewService(ServiceDeps{
				Repo:      NewMemoryTournamentRepository(),
				Rooms:     NoopRoomProvisioner{},
				Publisher: NoopPublisher{},
				Audit:     NewMemoryAudit(),
			}),
		}, nil
	}

	// Capability memory path (offline / checkpoint).
	repo := NewMemoryTournamentRepository()
	var rooms RoomProvisioner = NewFakeRoomProvisioner()
	if roomURL != "" {
		rooms = NewHTTPRoomProvisioner(roomURL, roomCred, nil)
	}
	return tournamentRuntime{
		svc: NewService(ServiceDeps{
			Repo:      repo,
			Rooms:     rooms,
			Publisher: NoopPublisher{},
			Audit:     NewMemoryAudit(),
			Clock:     systemClock{},
			IDs:       randomIDs{},
		}),
		mode:  "capability",
		ready: cred != "",
		readyReason: func() string {
			if cred == "" {
				return "internal_credential_unconfigured"
			}
			return ""
		}(),
	}, nil
}
