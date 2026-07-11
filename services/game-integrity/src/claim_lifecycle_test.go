package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"

	"unoarena/services/game-integrity/domain"
)

func TestUnit_FirstPersistCallbackErrorDoesNotClaim(t *testing.T) {
	var claims atomic.Int32
	repo := &KurrentStreamRepository{
		dekCache: map[string]cachedStreamDEK{},
		unitLoadDeckState: func(context.Context, domain.RoomID, domain.GameID, string) (*DeckState, []*kurrentdb.ResolvedEvent, error) {
			return emptyDeckState(), nil, nil
		},
		unitClaimGameBinding: func(context.Context, domain.GameID, domain.RoomID) (string, error) {
			claims.Add(1)
			return "unit-token", nil
		},
	}
	err := repo.withDeckAttempt(context.Background(), "room-cb-err", "game-cb-err", true, func(*DeckState) error {
		return errors.New("callback boom")
	}, "", true)
	if err == nil || err.Error() != "callback boom" {
		t.Fatalf("want callback boom, got %v", err)
	}
	if claims.Load() != 0 {
		t.Fatalf("callback error must not acquire binding claim, got %d claims", claims.Load())
	}
}

func TestUnit_FirstPersistNoOpDoesNotClaim(t *testing.T) {
	var claims atomic.Int32
	repo := &KurrentStreamRepository{
		dekCache: map[string]cachedStreamDEK{},
		unitLoadDeckState: func(context.Context, domain.RoomID, domain.GameID, string) (*DeckState, []*kurrentdb.ResolvedEvent, error) {
			return emptyDeckState(), nil, nil
		},
		unitClaimGameBinding: func(context.Context, domain.GameID, domain.RoomID) (string, error) {
			claims.Add(1)
			return "unit-token", nil
		},
	}
	err := repo.withDeckAttempt(context.Background(), "room-noop", "game-noop", true, func(*DeckState) error {
		return nil // no mutation
	}, "", true)
	if err != nil {
		t.Fatalf("no-op create: %v", err)
	}
	if claims.Load() != 0 {
		t.Fatalf("no-op create must not acquire binding claim, got %d claims", claims.Load())
	}
}
