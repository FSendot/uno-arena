package store

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRuntimePoolIsLazyAndSingleConnection(t *testing.T) {
	t.Setenv("ROOM_RUNTIME_ROOM_ID", "r-1")
	base, err := pgxpool.ParseConfig("postgres://user:pass@localhost:5432/db")
	if err != nil {
		t.Fatal(err)
	}
	cfg := runtimePoolConfig(base)
	if cfg.MinConns != 0 || cfg.MaxConns != 1 || cfg.MaxConnIdleTime <= 0 {
		t.Fatalf("runtime pool = min=%d max=%d idle=%s", cfg.MinConns, cfg.MaxConns, cfg.MaxConnIdleTime)
	}
}

func TestRuntimeIntentPoolIsLazyAndSeparatelyBounded(t *testing.T) {
	t.Setenv("ROOM_RUNTIME_ROOM_ID", "r-1")
	base, err := pgxpool.ParseConfig("postgres://user:pass@localhost:5432/db")
	if err != nil {
		t.Fatal(err)
	}
	cfg := intentPoolConfig(base)
	if cfg.MinConns != 0 || cfg.MaxConns != 1 || cfg.MaxConnIdleTime <= 0 {
		t.Fatalf("runtime intent pool = min=%d max=%d idle=%s", cfg.MinConns, cfg.MaxConns, cfg.MaxConnIdleTime)
	}
}
