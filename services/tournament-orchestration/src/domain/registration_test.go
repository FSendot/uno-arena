package domain_test

import (
	"testing"

	"unoarena/services/tournament-orchestration/domain"
)

func TestAllocateRegistrationQuotas_SumExact(t *testing.T) {
	for _, capacity := range []int{1, 63, 64, 65, 1000, 1_000_000} {
		quotas := domain.AllocateRegistrationQuotas(capacity)
		if len(quotas) != domain.RegistrationShardCount {
			t.Fatalf("capacity %d: want %d shards, got %d", capacity, domain.RegistrationShardCount, len(quotas))
		}
		sum := 0
		for i, q := range quotas {
			if q < 0 {
				t.Fatalf("capacity %d shard %d: negative quota", capacity, i)
			}
			sum += q
		}
		if sum != capacity {
			t.Fatalf("capacity %d: quota sum %d != capacity", capacity, sum)
		}
		if domain.RegistrationQuotaSum(capacity) != capacity {
			t.Fatalf("RegistrationQuotaSum(%d) = %d", capacity, domain.RegistrationQuotaSum(capacity))
		}
	}
}

func TestRegistrationProbe_Stable(t *testing.T) {
	tid, pid := "tour-a", "player-xyz"
	start := domain.RegistrationStartShard(tid, pid)
	if start < 0 || start >= domain.RegistrationShardCount {
		t.Fatalf("start shard out of range: %d", start)
	}
	order := domain.RegistrationProbeOrder(tid, pid)
	if len(order) != domain.RegistrationShardCount {
		t.Fatalf("probe length %d", len(order))
	}
	if order[0] != start {
		t.Fatalf("probe[0]=%d want start=%d", order[0], start)
	}
	seen := map[int]struct{}{}
	for i, s := range order {
		if s != (start+i)%domain.RegistrationShardCount {
			t.Fatalf("probe[%d]=%d want %d", i, s, (start+i)%domain.RegistrationShardCount)
		}
		if _, ok := seen[s]; ok {
			t.Fatalf("duplicate shard %d in probe", s)
		}
		seen[s] = struct{}{}
	}
	// Stability across calls.
	if domain.RegistrationStartShard(tid, pid) != start {
		t.Fatal("start shard not stable")
	}
	order2 := domain.RegistrationProbeOrder(tid, pid)
	for i := range order {
		if order[i] != order2[i] {
			t.Fatal("probe order not stable")
		}
	}
}

func TestDecideCreateTournament_RejectsBatchSizeBounds(t *testing.T) {
	base := domain.CreateTournamentCommand{
		CommandID: "c1", TournamentID: "t1", Capacity: 8,
	}
	ok := domain.DecideCreateTournament(base)
	if ok.Kind != domain.RegistrationCreate {
		t.Fatalf("default batch ok: %+v", ok)
	}
	over := base
	over.BatchSize = domain.MaxProvisioningBatchSize + 1
	d := domain.DecideCreateTournament(over)
	if d.Kind != domain.RegistrationReject || d.Outcome.Rejection.Code != domain.RejectInvalidCommand {
		t.Fatalf("over max: %+v", d)
	}
	neg := base
	neg.BatchSize = -1
	d = domain.DecideCreateTournament(neg)
	if d.Kind != domain.RegistrationReject || d.Outcome.Rejection.Code != domain.RejectInvalidCommand {
		t.Fatalf("negative: %+v", d)
	}
	exact := base
	exact.BatchSize = domain.MaxProvisioningBatchSize
	d = domain.DecideCreateTournament(exact)
	if d.Kind != domain.RegistrationCreate {
		t.Fatalf("exact max: %+v", d)
	}
}

func TestNormalizedCreateDefaults_RejectsOverMax(t *testing.T) {
	_, _, err := domain.NormalizedCreateDefaults(3, domain.MaxProvisioningBatchSize+1)
	if err == nil {
		t.Fatal("expected error for batchSize > max")
	}
	_, _, err = domain.NormalizedCreateDefaults(3, -1)
	if err == nil {
		t.Fatal("expected error for negative batchSize")
	}
	retry, batch, err := domain.NormalizedCreateDefaults(0, 0)
	if err != nil || retry != domain.DefaultRetryBudget || batch != domain.DefaultBatchSize {
		t.Fatalf("defaults: retry=%d batch=%d err=%v", retry, batch, err)
	}
	retry, batch, err = domain.NormalizedCreateDefaults(2, domain.MaxProvisioningBatchSize)
	if err != nil || retry != 2 || batch != domain.MaxProvisioningBatchSize {
		t.Fatalf("max ok: retry=%d batch=%d err=%v", retry, batch, err)
	}
}
