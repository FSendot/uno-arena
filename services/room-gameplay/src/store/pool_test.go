package store

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestIntentPoolConfig_GuaranteesIndependentCapacity(t *testing.T) {
	base, err := pgxpool.ParseConfig("postgres://localhost/db")
	if err != nil {
		t.Fatal(err)
	}
	base.MaxConns = 4

	intent := intentPoolConfig(base)
	if intent.MaxConns < 1 {
		t.Fatalf("intent MaxConns=%d must be >= 1", intent.MaxConns)
	}
	if intent.MinConns < 1 {
		t.Fatalf("intent MinConns=%d must guarantee at least one connection", intent.MinConns)
	}
	if intent.MaxConns >= base.MaxConns && base.MaxConns > 2 {
		// Intent pool must stay small relative to a saturated main command pool.
		if intent.MaxConns > 4 {
			t.Fatalf("intent MaxConns=%d should stay small", intent.MaxConns)
		}
	}
	if intent == base {
		t.Fatal("intent config must be a separate copy")
	}
}

func TestNewSessionStore_LegacySharesPools(t *testing.T) {
	// Nil pools are fine for constructor wiring assertions.
	s := NewSessionStore(nil)
	if s.pool != s.intentPool {
		t.Fatal("legacy constructor must share main and intent for nonconcurrent unit tests")
	}
}

func TestNewSessionStoreWithPools_KeepsPoolsSeparate(t *testing.T) {
	var mainSentinel, intentSentinel pgxpool.Pool
	s := NewSessionStoreWithPools(&mainSentinel, &intentSentinel)
	if s.pool != &mainSentinel || s.intentPool != &intentSentinel {
		t.Fatalf("expected distinct main/intent pools, got main=%p intent=%p", s.pool, s.intentPool)
	}
}
