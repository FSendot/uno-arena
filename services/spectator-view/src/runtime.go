package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"unoarena/services/spectator-view/store"
)

type spectatorRuntime struct {
	app    SpectatorApplication
	feed   SpectatorLiveFeed
	mode   string // durable | capability | misconfigured
	ready  bool
	reason string
	cred   string
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

func wireSpectatorRuntime() (spectatorRuntime, error) {
	cred := strings.TrimSpace(os.Getenv("SPECTATOR_VIEW_INTERNAL_CREDENTIAL"))
	redisURL := strings.TrimSpace(os.Getenv("REDIS_URL"))
	keyPrefix := strings.TrimSpace(os.Getenv("SPECTATOR_REDIS_KEY_PREFIX"))
	deploymentEnv := strings.TrimSpace(os.Getenv("DEPLOYMENT_ENV"))
	if deploymentEnv == "" {
		deploymentEnv = "development"
	}
	capability := envTruthy("SPECTATOR_CAPABILITY_MODE")

	// Durable mode: REDIS_URL set and not explicit capability.
	if redisURL != "" && !capability {
		if cred == "" {
			mem := NewMemoryProjectionStore()
			hub := NewStreamHub()
			return spectatorRuntime{
				app: mem, feed: hub, mode: "durable", ready: false,
				reason: "durable_dependencies_missing: SPECTATOR_VIEW_INTERNAL_CREDENTIAL",
				cred:   cred,
			}, nil
		}
		rdb, err := store.NewRedisFromURL(redisURL)
		if err != nil {
			return spectatorRuntime{}, fmt.Errorf("redis client: %w", err)
		}
		rs := store.NewRedisProjectionStore(rdb, keyPrefix)
		if n, ok, err := store.ParseStreamMaxLenEnv(os.Getenv("SPECTATOR_REDIS_STREAM_MAXLEN")); err != nil {
			return spectatorRuntime{}, fmt.Errorf("SPECTATOR_REDIS_STREAM_MAXLEN: %w", err)
		} else if ok {
			rs = rs.WithStreamMaxLen(n)
		}
		if err := rs.LoadScripts(context.Background()); err != nil {
			return spectatorRuntime{}, fmt.Errorf("redis scripts: %w", err)
		}
		app := newDurableApp(rs)
		feed := newRedisLiveFeed(rs)
		return spectatorRuntime{
			app: app, feed: feed, mode: "durable", ready: true, cred: cred,
		}, nil
	}

	// Capability memory only when explicitly opted in on non-production.
	if !capability || !isNonProd(deploymentEnv) {
		reason := "redis_unconfigured"
		if capability && !isNonProd(deploymentEnv) {
			reason = "capability_mode_forbidden_in_production"
		}
		mem := NewMemoryProjectionStore()
		hub := NewStreamHub()
		return spectatorRuntime{
			app: mem, feed: hub, mode: "misconfigured", ready: false, reason: reason, cred: cred,
		}, nil
	}

	mem := NewMemoryProjectionStore()
	hub := NewStreamHub()
	ready := cred != ""
	reason := ""
	if !ready {
		reason = "internal_credential_unconfigured"
	}
	return spectatorRuntime{
		app: mem, feed: hub, mode: "capability", ready: ready, reason: reason, cred: cred,
	}, nil
}

func (rt spectatorRuntime) durableReady(ctx context.Context) error {
	if rt.mode != "durable" {
		return nil
	}
	return rt.app.Ready(ctx)
}
