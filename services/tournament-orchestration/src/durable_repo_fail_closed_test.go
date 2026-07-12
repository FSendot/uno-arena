package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/services/tournament-orchestration/store"
	"unoarena/shared/envelope"
)

func TestDurableRepo_WholeAggregateMutationFailClosedWithoutStoreTouch(t *testing.T) {
	r := &durableRepo{store: nil} // nil store panics if mutation methods dial the store

	_, err := r.BeginExisting(domain.TournamentID("t-fail-closed"))
	if !errors.Is(err, ErrDurableWholeAggregateMutationDisabled) {
		t.Fatalf("BeginExisting: want sentinel, got %v", err)
	}
	_, err = r.BeginCreate(domain.TournamentID("t-fail-closed"))
	if !errors.Is(err, ErrDurableWholeAggregateMutationDisabled) {
		t.Fatalf("BeginCreate: want sentinel, got %v", err)
	}
	err = r.Commit(CommitRequest{CommandID: "cmd-fail-closed", Outcome: envelope.Accepted("cmd-fail-closed", "x", nil, nil)})
	if !errors.Is(err, ErrDurableWholeAggregateMutationDisabled) {
		t.Fatalf("Commit: want sentinel, got %v", err)
	}
}

func TestDurableRepo_WholeAggregateMutationMethodsNeverCallStore(t *testing.T) {
	fn := extractTypeMethods(t, "durable_repo.go", "durableRepo")
	for _, bad := range []string{
		"r.store.BeginExisting",
		"r.store.BeginCreate",
		"r.store.Commit",
		"toStoreCommit",
		"durableUoW",
	} {
		if strings.Contains(fn, bad) {
			t.Fatalf("durableRepo mutation surface must not reference %s", bad)
		}
	}
	for _, name := range []string{"BeginExisting", "BeginCreate"} {
		body := methodBodyForRecv(t, "durable_repo.go", "*durableRepo", name)
		if !strings.Contains(body, "ErrDurableWholeAggregateMutationDisabled") {
			t.Fatalf("%s must return ErrDurableWholeAggregateMutationDisabled", name)
		}
		if strings.Contains(body, "r.store") {
			t.Fatalf("%s must not touch store", name)
		}
	}
	commitBody := methodBodyForRecv(t, "durable_repo.go", "*durableRepo", "Commit")
	if !strings.Contains(commitBody, "ErrDurableWholeAggregateMutationDisabled") {
		t.Fatal("durableRepo.Commit must return ErrDurableWholeAggregateMutationDisabled")
	}
	if strings.Contains(commitBody, "r.store") {
		t.Fatal("durableRepo.Commit must not touch store")
	}
}

func TestService_DurableRepoMissingPort_FailsClosedNoMutation(t *testing.T) {
	counted := &countingFailClosedDurableRepo{durableRepo: &durableRepo{store: nil}}
	svc := NewService(ServiceDeps{
		Repo:  counted,
		Audit: NoopAudit{},
		// Deliberately omit Registrations / all differential ports.
	})
	_, err := svc.SubmitCommand(context.Background(), CommandRequest{
		CommandID:     "t11-missing-port",
		Type:          CmdCreateTournament,
		SchemaVersion: envelope.CurrentSchemaVersion,
		Payload:       json.RawMessage(`{"tournamentId":"t-t11","capacity":12}`),
	}, "corr")
	if !errors.Is(err, ErrDurableWholeAggregateMutationDisabled) {
		t.Fatalf("want sentinel on missing registration port, got %v", err)
	}
	if counted.beginCreate.Load() < 1 {
		t.Fatal("legacy Create path must attempt BeginCreate")
	}
	if counted.commit.Load() != 0 {
		t.Fatalf("Commit must not succeed/mutate; commits=%d", counted.commit.Load())
	}
}

func TestProcessProvisioningBatch_SelectsDifferentialWhenProvisioningWired(t *testing.T) {
	body := extractFuncBody(t, "service.go", "ProcessProvisioningBatch")
	if !strings.Contains(body, "s.provisioning != nil") {
		t.Fatal("ProcessProvisioningBatch must branch on provisioning port")
	}
	if !strings.Contains(body, "processProvisioningBatchDifferential") {
		t.Fatal("wired provisioning must select differential")
	}
	diffIdx := strings.Index(body, "processProvisioningBatchDifferential")
	legacyIdx := strings.Index(body, "processProvisioningBatchLegacy")
	if diffIdx < 0 || legacyIdx < 0 || diffIdx > legacyIdx {
		t.Fatal("differential branch must precede legacy fallback")
	}

	var prepareCalls atomic.Int32
	prov := &countingPrepareProvisioningRepo{prepareCalls: &prepareCalls}
	svc := NewService(ServiceDeps{
		Repo:         &durableRepo{store: nil},
		Provisioning: prov,
		Audit:        NoopAudit{},
		Rooms:        NoopRoomProvisioner{},
	})
	if svc.provisioning == nil {
		t.Fatal("service must have provisioning wired")
	}
	_, err := svc.ProcessProvisioningBatch(context.Background(), ProvisioningBatchWork{
		CommandID:    "t11-prov-diff",
		TournamentID: "t-t11",
		RoundNumber:  1,
		BatchID:      "b1",
		SlotFrom:     "0",
		SlotTo:       "1",
		SlotSize:     4,
		RetryAttempt: 0,
		LeaseOwner:   "owner-1",
		LeaseVersion: 1,
	})
	if err != nil {
		t.Fatalf("differential prepare reject path: %v", err)
	}
	if prepareCalls.Load() != 1 {
		t.Fatalf("want PrepareProvisioningBatch once (differential), got %d", prepareCalls.Load())
	}
}

// countingFailClosedDurableRepo stubs reads so unit tests can exercise legacy mutation
// without a live store, while mutation methods still hit durableRepo fail-closed.
type countingFailClosedDurableRepo struct {
	*durableRepo
	beginExisting atomic.Int32
	beginCreate   atomic.Int32
	commit        atomic.Int32
}

func (r *countingFailClosedDurableRepo) LookupOutcome(string) (envelope.Result, bool) {
	return envelope.Result{}, false
}
func (r *countingFailClosedDurableRepo) Get(domain.TournamentID) (*domain.Tournament, bool) {
	return nil, false
}
func (r *countingFailClosedDurableRepo) BeginExisting(id domain.TournamentID) (TournamentUnitOfWork, error) {
	r.beginExisting.Add(1)
	return r.durableRepo.BeginExisting(id)
}
func (r *countingFailClosedDurableRepo) BeginCreate(id domain.TournamentID) (TournamentUnitOfWork, error) {
	r.beginCreate.Add(1)
	return r.durableRepo.BeginCreate(id)
}
func (r *countingFailClosedDurableRepo) Commit(req CommitRequest) error {
	r.commit.Add(1)
	return r.durableRepo.Commit(req)
}

type countingPrepareProvisioningRepo struct {
	stubProvisioningRepo
	prepareCalls *atomic.Int32
}

func (r *countingPrepareProvisioningRepo) PrepareProvisioningBatch(context.Context, store.PrepareProvisioningBatchInput) (*store.PrepareProvisioningBatchResult, error) {
	r.prepareCalls.Add(1)
	return &store.PrepareProvisioningBatchResult{Rejected: true, RejectReason: "t11_probe"}, nil
}

func methodBodyForRecv(t *testing.T, path, recv, name string) string {
	t.Helper()
	fn := extractTypeMethods(t, path, strings.TrimPrefix(recv, "*"))
	marker := "func (" + recv + ") " + name + "("
	idx := strings.Index(fn, marker)
	if idx < 0 {
		// try without star in search of extract output which includes full signatures
		marker = "func (r " + recv + ") " + name + "("
		idx = strings.Index(fn, marker)
	}
	if idx < 0 {
		t.Fatalf("method %s on %s not found in %s", name, recv, path)
	}
	rest := fn[idx:]
	brace := strings.Index(rest, "{")
	if brace < 0 {
		t.Fatalf("no body for %s", name)
	}
	depth := 0
	for i := brace; i < len(rest); i++ {
		switch rest[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return rest[brace : i+1]
			}
		}
	}
	t.Fatalf("unclosed body for %s", name)
	return ""
}
