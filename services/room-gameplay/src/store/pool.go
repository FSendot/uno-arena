package store

import (
	"context"
	"fmt"
	"os"

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
		p.Intent.Close()
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

// intentPoolConfig returns a small independent pool config guaranteeing at least
// one intent connection even when the main command pool is fully acquired.
func intentPoolConfig(base *pgxpool.Config) *pgxpool.Config {
	cfg := base.Copy()
	cfg.MaxConns = DefaultIntentPoolMaxConns
	if cfg.MaxConns < 1 {
		cfg.MaxConns = 1
	}
	cfg.MinConns = 1
	return cfg
}
