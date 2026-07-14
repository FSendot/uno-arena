package store

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultIntentPoolMaxConns is a small dedicated capacity for reconciliation-intent
// transactions so they remain available when the main command pool is saturated.
const DefaultIntentPoolMaxConns = int32(2)

// Pool owns the main command pool and a dedicated reconciliation-intent pool.
type Pool struct {
	Main   *pgxpool.Pool
	Intent *pgxpool.Pool
}

// Close closes both pools.
func (p *Pool) Close() {
	if p == nil {
		return
	}
	if p.Main != nil {
		p.Main.Close()
	}
	if p.Intent != nil {
		if p.Intent != p.Main {
			p.Intent.Close()
		}
	}
}

// NewPoolFromEnv opens pools from DATABASE_URL.
func NewPoolFromEnv(ctx context.Context) (*Pool, error) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}
	return NewPool(ctx, dsn)
}

// NewPool opens a writer-only main command pool plus a small dedicated intent pool.
func NewPool(ctx context.Context, dsn string) (*Pool, error) {
	mainCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	mainCfg = runtimePoolConfig(mainCfg)
	if tracer := pgxTracer(); tracer != nil {
		mainCfg.ConnConfig.Tracer = tracer
	}
	main, err := pgxpool.NewWithConfig(ctx, mainCfg)
	if err != nil {
		return nil, err
	}
	if err := main.Ping(ctx); err != nil {
		main.Close()
		return nil, err
	}
	intentCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		main.Close()
		return nil, err
	}
	intentCfg = intentPoolConfig(intentCfg)
	if tracer := pgxTracer(); tracer != nil {
		intentCfg.ConnConfig.Tracer = tracer
	}
	intent, err := pgxpool.NewWithConfig(ctx, intentCfg)
	if err != nil {
		main.Close()
		return nil, err
	}
	if err := intent.Ping(ctx); err != nil {
		main.Close()
		intent.Close()
		return nil, err
	}
	return &Pool{Main: main, Intent: intent}, nil
}

// runtimePoolConfig prevents a pod-per-room topology from multiplying normal
// service connection pools. The pinned state-machine process is always lazy
// (min=0), one aggregate connection maximum, and quickly releases idle connections.
// Router/controller pools remain separately bounded through the same explicit
// environment knob without changing database ownership.
func runtimePoolConfig(base *pgxpool.Config) *pgxpool.Config {
	cfg := base.Copy()
	if strings.TrimSpace(os.Getenv("ROOM_RUNTIME_ROOM_ID")) != "" {
		cfg.MinConns = 0
		cfg.MaxConns = 1
		cfg.MaxConnIdleTime = 30 * time.Second
		return cfg
	}
	if raw := strings.TrimSpace(os.Getenv("ROOM_DB_POOL_MAX_CONNS")); raw != "" {
		if n, err := strconv.ParseInt(raw, 10, 32); err == nil && n > 0 {
			cfg.MaxConns = int32(n)
		}
	}
	return cfg
}

// intentPoolConfig returns a small independent pool config guaranteeing at least
// one intent connection even when the main command pool is fully acquired.
func intentPoolConfig(base *pgxpool.Config) *pgxpool.Config {
	cfg := base.Copy()
	if strings.TrimSpace(os.Getenv("ROOM_RUNTIME_ROOM_ID")) != "" {
		// The crash-recovery intent must commit autonomously while the aggregate
		// transaction remains locked, so it needs one separately bounded client
		// connection even though mutations are serialized per Room.
		cfg.MaxConns = 1
		cfg.MinConns = 0
		cfg.MaxConnIdleTime = 30 * time.Second
		return cfg
	}
	cfg.MaxConns = DefaultIntentPoolMaxConns
	if cfg.MaxConns < 1 {
		cfg.MaxConns = 1
	}
	cfg.MinConns = 1
	return cfg
}
