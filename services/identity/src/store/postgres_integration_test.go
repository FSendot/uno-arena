//go:build integration

package store_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"unoarena/services/identity/domain"
	"unoarena/services/identity/store"
)

func postgresURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("IDENTITY_POSTGRES_URL")
	if url == "" {
		url = os.Getenv("DATABASE_URL")
	}
	if url == "" {
		t.Skip("IDENTITY_POSTGRES_URL not set")
	}
	// Fail closed before any connection/mutation against authoritative DBs.
	if err := requireSafeIdentityTestDatabase(url); err != nil {
		t.Fatalf("%v", err)
	}
	return url
}

func applyMigration(t *testing.T, ctx context.Context, pool *store.Pool) {
	t.Helper()
	sqlBytes, err := os.ReadFile(filepath.Join("..", "..", "migrations", "001_init.sql"))
	if err != nil {
		t.Fatalf("migration: %v", err)
	}
	if _, err := pool.Exec(ctx, string(sqlBytes)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS schema_bootstrap_meta`); err != nil {
		t.Fatalf("drop schema_bootstrap_meta: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		CREATE TABLE schema_bootstrap_meta (
			version TEXT NOT NULL,
			checksum TEXT NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		t.Fatalf("meta table: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO schema_bootstrap_meta (version, checksum) VALUES ($1, $2)
	`, store.ExpectedBootstrapVersion, store.ExpectedSchemaChecksum); err != nil {
		t.Fatalf("meta insert: %v", err)
	}
}

func resetPublic(t *testing.T, ctx context.Context, pool *store.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		DROP TABLE IF EXISTS outbox_events CASCADE;
		DROP TABLE IF EXISTS command_idempotency CASCADE;
		DROP TABLE IF EXISTS sessions CASCADE;
		DROP TABLE IF EXISTS external_identities CASCADE;
		DROP TABLE IF EXISTS player_acls CASCADE;
		DROP TABLE IF EXISTS players CASCADE;
		DROP TABLE IF EXISTS schema_migrations CASCADE;
		DROP TABLE IF EXISTS schema_bootstrap_meta CASCADE;
	`); err != nil {
		t.Fatalf("reset public schema: %v", err)
	}
}

func TestIntegration_VerifySchemaExactAndDrift(t *testing.T) {
	ctx := context.Background()
	pool, err := store.NewPool(ctx, postgresURL(t))
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()
	resetPublic(t, ctx, pool)
	applyMigration(t, ctx, pool)

	if err := store.VerifySchema(ctx, pool.Pool, store.DefaultSchemaExpectation()); err != nil {
		t.Fatalf("exact schema should pass: %v", err)
	}

	if _, err := pool.Exec(ctx, `UPDATE schema_bootstrap_meta SET checksum = 'deadbeef'`); err != nil {
		t.Fatal(err)
	}
	if err := store.VerifySchema(ctx, pool.Pool, store.DefaultSchemaExpectation()); err == nil {
		t.Fatal("drift checksum must fail")
	}
}

func TestIntegration_RegisterRollbackAndRoles(t *testing.T) {
	ctx := context.Background()
	pool, err := store.NewPool(ctx, postgresURL(t))
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()
	resetPublic(t, ctx, pool)
	applyMigration(t, ctx, pool)

	players := store.NewPlayerStore(pool.Pool)
	now := time.Now().UTC()
	p := domain.Player{ID: "player-1", Username: "alice", PasswordHash: "hash", CreatedAt: now}
	if err := players.RegisterWithDefaultACL(ctx, p, "player"); err != nil {
		t.Fatal(err)
	}
	roles, err := players.ListRoles(ctx, p.ID)
	if err != nil || len(roles) != 1 || roles[0] != "player" {
		t.Fatalf("roles=%v err=%v", roles, err)
	}
	if err := players.RegisterWithDefaultACL(ctx, p, "player"); err == nil {
		t.Fatal("duplicate register must fail")
	}
}

func TestIntegration_ConcurrentLoginSingleActive(t *testing.T) {
	ctx := context.Background()
	pool, err := store.NewPool(ctx, postgresURL(t))
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()
	resetPublic(t, ctx, pool)
	applyMigration(t, ctx, pool)

	players := store.NewPlayerStore(pool.Pool)
	sessions := store.NewSessionStore(pool.Pool)
	ids := domain.RandomIDGenerator{}
	svc := domain.NewService(domain.ServiceDeps{
		Players:    players,
		Sessions:   sessions,
		Hasher:     domain.NewCheckpointPasswordHasher(ids),
		IDs:        ids,
		Tokens:     ids,
		Clock:      domain.SystemClock{},
		SessionTTL: time.Hour,
		Transport:  nil, // durable: never drain
	})
	if _, err := svc.Register(ctx, "alice", "secret"); err != nil {
		t.Fatal(err)
	}

	const n = 16
	var wg sync.WaitGroup
	wg.Add(n)
	var okCount atomic.Int64
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			if _, err := svc.Login(ctx, "alice", "secret"); err == nil {
				okCount.Add(1)
			}
		}()
	}
	wg.Wait()
	if okCount.Load() == 0 {
		t.Fatal("expected logins")
	}
	var active int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM sessions WHERE status='active'`).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 1 {
		t.Fatalf("active=%d", active)
	}
	var outbox int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&outbox); err != nil {
		t.Fatal(err)
	}
	if outbox < 1 {
		t.Fatalf("expected outbox rows from takeovers, got %d", outbox)
	}
}

func TestIntegration_RevokeVsValidateOneOutbox(t *testing.T) {
	ctx := context.Background()
	pool, err := store.NewPool(ctx, postgresURL(t))
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()
	resetPublic(t, ctx, pool)
	applyMigration(t, ctx, pool)

	players := store.NewPlayerStore(pool.Pool)
	sessions := store.NewSessionStore(pool.Pool)
	ids := domain.RandomIDGenerator{}
	clock := &fixedClock{now: time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)}
	svc := domain.NewService(domain.ServiceDeps{
		Players: players, Sessions: sessions,
		Hasher: domain.NewCheckpointPasswordHasher(ids), IDs: ids, Tokens: ids,
		Clock: clock, SessionTTL: time.Hour,
	})
	if _, err := svc.Register(ctx, "bob", "secret"); err != nil {
		t.Fatal(err)
	}
	login, err := svc.Login(ctx, "bob", "secret")
	if err != nil {
		t.Fatal(err)
	}
	clock.now = clock.now.Add(2 * time.Hour)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = svc.RevokeSession(ctx, login.SessionID, domain.ReasonAdmin) }()
	go func() { defer wg.Done(); _, _ = svc.ValidateToken(ctx, login.Token) }()
	wg.Wait()

	var outbox int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&outbox); err != nil {
		t.Fatal(err)
	}
	if outbox != 1 {
		t.Fatalf("expected 1 outbox, got %d", outbox)
	}
}

func TestIntegration_ExternalMappingUniqueness(t *testing.T) {
	ctx := context.Background()
	pool, err := store.NewPool(ctx, postgresURL(t))
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()
	resetPublic(t, ctx, pool)
	applyMigration(t, ctx, pool)

	players := store.NewPlayerStore(pool.Pool)
	now := time.Now().UTC()
	p1 := domain.Player{ID: "p1", Username: "u1", PasswordHash: "", CreatedAt: now}
	got, _, err := players.LinkOrResolveExternal(ctx, "https://idp", "sub", "u1", p1, []string{"player"})
	if err != nil {
		t.Fatal(err)
	}
	p2 := domain.Player{ID: "p2", Username: "u2", PasswordHash: "", CreatedAt: now}
	again, _, err := players.LinkOrResolveExternal(ctx, "https://idp", "sub", "u2", p2, []string{"player"})
	if err != nil {
		t.Fatal(err)
	}
	if again.ID != got.ID {
		t.Fatalf("want same player %s got %s", got.ID, again.ID)
	}
}

func TestIntegration_DBOutageMapsUnavailable(t *testing.T) {
	ctx := context.Background()
	pool, err := store.NewPool(ctx, postgresURL(t))
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	players := store.NewPlayerStore(pool.Pool)
	pool.Close()
	_, _, err = players.FindByUsername(ctx, "x")
	if err == nil || !errors.Is(err, domain.ErrUnavailable) {
		t.Fatalf("want ErrUnavailable, got %v", err)
	}
}

func TestIntegration_LinkOrResolveExternalConcurrentSameSubject(t *testing.T) {
	ctx := context.Background()
	pool, err := store.NewPool(ctx, postgresURL(t))
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()
	resetPublic(t, ctx, pool)
	applyMigration(t, ctx, pool)

	players := store.NewPlayerStore(pool.Pool)
	now := time.Now().UTC()
	const n = 8
	var wg sync.WaitGroup
	ids := make([]domain.PlayerID, n)
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			p := domain.Player{
				ID:           domain.PlayerID(fmt.Sprintf("race-p-%d", i)),
				Username:     fmt.Sprintf("race-u-%d", i),
				PasswordHash: "",
				CreatedAt:    now,
			}
			got, _, err := players.LinkOrResolveExternal(ctx, "https://idp", "race-sub", p.Username, p, []string{"player", "admin"})
			errs[i] = err
			if err == nil {
				ids[i] = got.ID
			}
		}()
	}
	wg.Wait()
	var winner domain.PlayerID
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("goroutine %d: %v", i, errs[i])
		}
		if winner == "" {
			winner = ids[i]
		} else if ids[i] != winner {
			t.Fatalf("multiple winners: %s vs %s", winner, ids[i])
		}
	}
	var playerCount, mappingCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM players`).Scan(&playerCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM external_identities WHERE issuer=$1 AND subject=$2`, "https://idp", "race-sub").Scan(&mappingCount); err != nil {
		t.Fatal(err)
	}
	if playerCount != 1 || mappingCount != 1 {
		t.Fatalf("orphan/conflict: players=%d mappings=%d", playerCount, mappingCount)
	}
}

func TestIntegration_ReplaceActiveSessionSerializesNoPriorSession(t *testing.T) {
	ctx := context.Background()
	pool, err := store.NewPool(ctx, postgresURL(t))
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()
	resetPublic(t, ctx, pool)
	applyMigration(t, ctx, pool)

	players := store.NewPlayerStore(pool.Pool)
	sessions := store.NewSessionStore(pool.Pool)
	now := time.Now().UTC()
	p := domain.Player{ID: "p-lock", Username: "lock-user", PasswordHash: "x", CreatedAt: now}
	if err := players.RegisterWithDefaultACL(ctx, p, "player"); err != nil {
		t.Fatal(err)
	}

	const n = 6
	var wg sync.WaitGroup
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			next := domain.Session{
				ID:        domain.SessionID(fmt.Sprintf("sess-%d", i)),
				PlayerID:  p.ID,
				TokenHash: fmt.Sprintf("th-%d", i),
				Status:    domain.SessionStatusActive,
				CreatedAt: now,
				ExpiresAt: now.Add(time.Hour),
			}
			errs[i] = sessions.ReplaceActiveSession(ctx, nil, next, func(inv domain.Session) domain.SessionInvalidatedEvent {
				return domain.SessionInvalidatedEvent{
					EventID:   fmt.Sprintf("evt-%s", inv.ID),
					PlayerID:  inv.PlayerID,
					SessionID: inv.ID,
					Reason:    domain.ReasonLoginTakeover,
				}.Normalize()
			})
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("replace %d: %v", i, err)
		}
	}
	var active int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM sessions WHERE player_id=$1 AND status='active'`, p.ID.String()).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 1 {
		t.Fatalf("want 1 active session, got %d", active)
	}
}

type fixedClock struct{ now time.Time }

func (c *fixedClock) Now() time.Time { return c.now }
