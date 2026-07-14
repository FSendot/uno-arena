package store

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Pool is a writer-only pgx connection pool (DATABASE_URL).
type Pool struct {
	*pgxpool.Pool
}

// NewPoolFromEnv opens a pool from DATABASE_URL.
func NewPoolFromEnv(ctx context.Context) (*Pool, error) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}
	return NewPool(ctx, dsn)
}

// NewPool opens a writer-only pgxpool from dsn.
func NewPool(ctx context.Context, dsn string) (*Pool, error) {
	return NewPoolWithTracer(ctx, dsn, nil)
}

// NewPoolWithTracer opens a pool with an explicit process-owned query tracer.
func NewPoolWithTracer(ctx context.Context, dsn string, tracer pgx.QueryTracer) (*Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.ConnConfig.Tracer = tracer
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := p.Ping(ctx); err != nil {
		p.Close()
		return nil, err
	}
	return &Pool{Pool: p}, nil
}
