package domain_test

import (
	"testing"
	"time"

	"unoarena/services/identity/domain"
)

func TestNormalizeAcceptedRolesAlwaysPlayerAllowlistDedupe(t *testing.T) {
	got := domain.NormalizeAcceptedRoles(
		[]string{" Admin ", "player", "admin", "", "hacker", "spectator"},
		[]string{"player", "admin"},
	)
	if len(got) != 2 || got[0] != "admin" || got[1] != "player" {
		t.Fatalf("got=%v want [admin player] sorted", got)
	}
	only := domain.NormalizeAcceptedRoles(nil, nil)
	if len(only) != 1 || only[0] != "player" {
		t.Fatalf("default=%v", only)
	}
}

func TestEstablishOIDCSessionPersistsAllowedRoles(t *testing.T) {
	players := domain.NewMemoryPlayerRepository()
	sessions := domain.NewMemorySessionRepository()
	ids := &seqIDs{}
	svc := domain.NewService(domain.ServiceDeps{
		Players:      players,
		Sessions:     sessions,
		Hasher:       domain.NewCheckpointPasswordHasher(ids),
		IDs:          ids,
		Tokens:       ids,
		Clock:        &fixedClock{now: time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)},
		SessionTTL:   time.Hour,
		Transport:    domain.NewFakeInvalidationTransport(),
		AllowedRoles: []string{"player", "admin"},
	})
	login, err := svc.EstablishOIDCSession(t.Context(), "https://idp", "sub-roles", "alice", []string{"admin", "hacker", "admin"})
	if err != nil {
		t.Fatal(err)
	}
	roles, err := players.ListRoles(t.Context(), login.PlayerID)
	if err != nil {
		t.Fatal(err)
	}
	if len(roles) != 2 || roles[0] != "admin" || roles[1] != "player" {
		t.Fatalf("roles=%v", roles)
	}
	principal, err := svc.ValidateToken(t.Context(), login.Token)
	if err != nil {
		t.Fatal(err)
	}
	if len(principal.Roles) != 2 {
		t.Fatalf("principal=%v", principal.Roles)
	}

	// Reconcile on existing mapping: drop admin when provider no longer asserts it.
	_, err = svc.EstablishOIDCSession(t.Context(), "https://idp", "sub-roles", "alice", []string{"player"})
	if err != nil {
		t.Fatal(err)
	}
	roles2, err := players.ListRoles(t.Context(), login.PlayerID)
	if err != nil {
		t.Fatal(err)
	}
	if len(roles2) != 1 || roles2[0] != "player" {
		t.Fatalf("reconciled=%v", roles2)
	}
}

func TestLinkOrResolveExternalAcceptsRolesArgument(t *testing.T) {
	players := domain.NewMemoryPlayerRepository()
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	p := domain.Player{ID: "player-a", Username: "alice", CreatedAt: now}
	got, roles, err := players.LinkOrResolveExternal(t.Context(), "https://idp", "sub-1", "alice", p, []string{"player", "admin"})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != p.ID {
		t.Fatalf("id=%s", got.ID)
	}
	if len(roles) != 2 {
		t.Fatalf("roles=%v", roles)
	}
}
