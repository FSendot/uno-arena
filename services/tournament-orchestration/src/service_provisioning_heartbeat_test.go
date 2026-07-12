package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"unoarena/services/tournament-orchestration/store"
	"unoarena/shared/envelope"
)

// stubProvisioningRepo is a minimal ProvisioningRepository for heartbeat/room-call tests.
type stubProvisioningRepo struct {
	prepare   *store.PrepareProvisioningBatchResult
	heartbeat func(ctx context.Context, tid string, roundNumber int, batchID, owner string, leaseVersion int64, retryAttempt int, now time.Time, ttl time.Duration) (bool, error)
	hbCalls   atomic.Int32
	lastTTL   atomic.Int64
}

func (s *stubProvisioningRepo) BeginProvisionRound(context.Context, string, int, string) (ProvisioningUnitOfWork, error) {
	return nil, errors.New("not implemented")
}
func (s *stubProvisioningRepo) BeginStandaloneCommand(context.Context, string) (ProvisioningUnitOfWork, error) {
	return nil, errors.New("not implemented")
}
func (s *stubProvisioningRepo) LookupOutcome(string) (envelope.Result, bool) {
	return envelope.Result{}, false
}
func (s *stubProvisioningRepo) PrepareProvisioningBatch(context.Context, store.PrepareProvisioningBatchInput) (*store.PrepareProvisioningBatchResult, error) {
	return s.prepare, nil
}
func (s *stubProvisioningRepo) FinalizeProvisioningBatchSuccess(context.Context, store.FinalizeProvisioningBatchInput) (envelope.Result, error) {
	return envelope.Result{}, errors.New("finalize must not run on initial heartbeat loss")
}
func (s *stubProvisioningRepo) FinalizeProvisioningBatchFailure(context.Context, store.FinalizeProvisioningBatchInput) (envelope.Result, error) {
	return envelope.Result{}, errors.New("finalize must not run on initial heartbeat loss")
}
func (s *stubProvisioningRepo) HeartbeatProvisioningLease(ctx context.Context, tid string, roundNumber int, batchID, owner string, leaseVersion int64, retryAttempt int, now time.Time, ttl time.Duration) (bool, error) {
	s.hbCalls.Add(1)
	s.lastTTL.Store(int64(ttl))
	if s.heartbeat != nil {
		return s.heartbeat(ctx, tid, roundNumber, batchID, owner, leaseVersion, retryAttempt, now, ttl)
	}
	return true, nil
}
func (s *stubProvisioningRepo) ManualRetryProvisioningBatch(context.Context, string, string, int, string, int, string) (envelope.Result, error) {
	return envelope.Result{}, errors.New("not implemented")
}
func (s *stubProvisioningRepo) ManualQuarantineProvisioningBatch(context.Context, string, string, int, string, string, string) (envelope.Result, error) {
	return envelope.Result{}, errors.New("not implemented")
}
func (s *stubProvisioningRepo) LoadRetryBudget(context.Context, string) (int, error) { return 3, nil }

func TestProvisionRoomsConcurrent_LostInitialHeartbeatMakesZeroRoomCalls(t *testing.T) {
	rooms := NewFakeRoomProvisioner()
	prov := &stubProvisioningRepo{
		prepare: &store.PrepareProvisioningBatchResult{
			Slots: []store.PreparedSlotWork{{
				SlotID: "slot_0", SlotIndex: 0, RoomID: "room_t_r1_slot_0", BatchID: "batch_0",
				PlayerIDs: []string{"p1", "p2"},
			}},
		},
		heartbeat: func(context.Context, string, int, string, string, int64, int, time.Time, time.Duration) (bool, error) {
			return false, nil
		},
	}
	svc := NewService(ServiceDeps{
		Repo:         NewMemoryTournamentRepository(),
		Provisioning: prov,
		Rooms:        rooms,
		Clock:        fixedClock{now: time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)},
		IDs:          &seqIDs{},
		Audit:        NewMemoryAudit(),
	})

	leaseExp := time.Date(2026, 7, 11, 12, 0, 30, 0, time.UTC) // 30s remaining vs clock
	work := ProvisioningBatchWork{
		CommandID:      "process:t:1:batch_0:0",
		TournamentID:   "t",
		RoundNumber:    1,
		BatchID:        "batch_0",
		SlotFrom:       "slot_0",
		SlotTo:         "slot_0",
		SlotSize:       1,
		LeaseOwner:     "worker-1",
		LeaseVersion:   1,
		LeaseExpiresAt: leaseExp,
	}

	_, err := svc.ProcessProvisioningBatch(context.Background(), work)
	if !errors.Is(err, store.ErrProvisioningFence) {
		t.Fatalf("want ErrProvisioningFence got %v", err)
	}
	if rooms.CallCount() != 0 {
		t.Fatalf("lost initial heartbeat must make zero Room calls, got %d", rooms.CallCount())
	}
	if prov.hbCalls.Load() < 1 {
		t.Fatal("expected synchronous initial heartbeat")
	}
	gotTTL := time.Duration(prov.lastTTL.Load())
	if gotTTL != 30*time.Second {
		t.Fatalf("ttl must derive from lease remaining, got %v want 30s (not default 5m)", gotTTL)
	}
}

func TestProvisioningHeartbeatTTL_RespectsLeaseFloor(t *testing.T) {
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	if got := provisioningHeartbeatTTL(now.Add(30*time.Second), now); got != 30*time.Second {
		t.Fatalf("got %v", got)
	}
	if got := provisioningHeartbeatTTL(now.Add(10*time.Millisecond), now); got < 10*time.Millisecond {
		t.Fatalf("positive remaining must not collapse: %v", got)
	}
	if got := provisioningHeartbeatTTL(time.Time{}, now); got != store.DefaultProvisioningLease {
		t.Fatalf("zero lease expiry must use default, got %v", got)
	}
	if got := provisioningHeartbeatTTL(now.Add(-time.Second), now); got != store.DefaultProvisioningLease {
		t.Fatalf("expired lease timestamp must use default, got %v", got)
	}
}
