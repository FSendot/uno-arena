package domain_test

import (
	"testing"
	"time"

	"unoarena/services/identity/domain"
)

func TestSessionInvalidatedEventOutboxPayload(t *testing.T) {
	evt := domain.NewSessionInvalidatedEvent(
		"evt-1",
		domain.PlayerID("player-1"),
		domain.SessionID("session-1"),
		domain.ReasonLogout,
		time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC),
	)
	p := evt.OutboxPayload()
	if p["eventId"] != "evt-1" || p["eventType"] != "SessionInvalidated" {
		t.Fatalf("payload=%+v", p)
	}
	if p["schemaVersion"] != 1 || p["correlationId"] != "evt-1" {
		t.Fatalf("meta=%+v", p)
	}
	if p["playerId"] != "player-1" || p["sessionId"] != "session-1" {
		t.Fatalf("ids=%+v", p)
	}
	if p["reason"] != domain.ReasonLogout || p["occurredAt"] != "2026-07-10T12:00:00Z" {
		t.Fatalf("body=%+v", p)
	}
}

func TestRegisterGrantsDefaultPlayerRole(t *testing.T) {
	players := domain.NewMemoryPlayerRepository()
	sessions := domain.NewMemorySessionRepository()
	ids := &seqIDs{}
	svc := domain.NewService(domain.ServiceDeps{
		Players:    players,
		Sessions:   sessions,
		Hasher:     domain.NewCheckpointPasswordHasher(ids),
		IDs:        ids,
		Tokens:     ids,
		Clock:      &fixedClock{now: time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)},
		SessionTTL: time.Hour,
		Transport:  domain.NewFakeInvalidationTransport(),
	})
	p, err := svc.Register(t.Context(), "alice", "secret")
	if err != nil {
		t.Fatal(err)
	}
	roles, err := players.ListRoles(t.Context(), p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(roles) != 1 || roles[0] != "player" {
		t.Fatalf("roles=%v", roles)
	}
	login, err := svc.Login(t.Context(), "alice", "secret")
	if err != nil {
		t.Fatal(err)
	}
	principal, err := svc.ValidateToken(t.Context(), login.Token)
	if err != nil {
		t.Fatal(err)
	}
	if len(principal.Roles) != 1 || principal.Roles[0] != "player" {
		t.Fatalf("principal roles=%v", principal.Roles)
	}
}

func TestLinkOrResolveExternalIdempotent(t *testing.T) {
	players := domain.NewMemoryPlayerRepository()
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	p1 := domain.Player{ID: "player-a", Username: "alice", CreatedAt: now}
	got, roles, err := players.LinkOrResolveExternal(t.Context(), "https://idp", "sub-1", "alice", p1, []string{"player"})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != p1.ID || len(roles) != 1 {
		t.Fatalf("got=%+v roles=%v", got, roles)
	}
	p2 := domain.Player{ID: "player-b", Username: "bob", CreatedAt: now}
	again, roles2, err := players.LinkOrResolveExternal(t.Context(), "https://idp", "sub-1", "bob", p2, []string{"player"})
	if err != nil {
		t.Fatal(err)
	}
	if again.ID != p1.ID {
		t.Fatalf("must resolve existing mapping, got %s", again.ID)
	}
	if len(roles2) != 1 || roles2[0] != "player" {
		t.Fatalf("roles2=%v", roles2)
	}
}
