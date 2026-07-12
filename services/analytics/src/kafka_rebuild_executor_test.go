package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"unoarena/services/analytics/domain"
	"unoarena/services/analytics/store"
)

type fakeRebuildStore struct {
	job          store.RecoveryJobState
	jobErr       error
	checkpoints  map[string]store.RecoveryPageCheckpoint
	idempotency  map[string]store.RecoveryRequestIdempotency
	owner        string
	leaseOK      bool
	applySeq     []domain.ApplyOutcome
	applyIdx     int
	progress     []store.RecoveryJobState
	reconcileN   int
	activated    bool
	activateErr  error
	ensureCalled bool
	ensureErr    error
	markFailed   int
}

func (f *fakeRebuildStore) key(job, topic, cur string) string {
	return job + "|" + topic + "|" + cur
}

func (f *fakeRebuildStore) LoadRequestIdempotency(ctx context.Context, jobID, topic, pageCursor string) (store.RecoveryRequestIdempotency, bool, error) {
	if f.idempotency == nil {
		return store.RecoveryRequestIdempotency{}, false, nil
	}
	row, ok := f.idempotency[f.key(jobID, topic, pageCursor)]
	return row, ok, nil
}

func (f *fakeRebuildStore) AcquireOrRenewLease(ctx context.Context, jobID, ownerToken string, ttl time.Duration) (store.RecoveryLease, error) {
	if !f.leaseOK {
		return store.RecoveryLease{}, store.ErrRecoveryLeaseLost
	}
	f.owner = ownerToken
	return store.RecoveryLease{RecoveryJobID: jobID, OwnerToken: ownerToken, LeaseEpoch: 1, ExpiresAt: time.Now().Add(ttl)}, nil
}

func (f *fakeRebuildStore) EnsureRecoveryBuildingGeneration(ctx context.Context, spec store.RecoveryJobSpec) (string, error) {
	f.ensureCalled = true
	if f.ensureErr != nil {
		return "", f.ensureErr
	}
	if f.job.GenerationID == "" {
		f.job = store.RecoveryJobState{
			RecoveryJobID: spec.RecoveryJobID, SourceContext: spec.SourceContext, SourceTopic: spec.SourceTopic,
			GenerationID: store.RecoveryGenerationID(spec.RecoveryJobID), Status: store.RecoveryStatusBuilding,
			FromCheckpoint: spec.FromCheckpoint, ToCheckpoint: spec.ToCheckpoint,
			HasCheckpointRng: spec.HasCheckpointRange, HasOccurredRng: spec.HasOccurredRange,
			FromOccurredAt: spec.FromOccurredAt, ToOccurredAt: spec.ToOccurredAt,
		}
	}
	return f.job.GenerationID, nil
}

func (f *fakeRebuildStore) ValidateLeaseOwnership(ctx context.Context, jobID, ownerToken string) error {
	if !f.leaseOK || f.owner != ownerToken {
		return store.ErrRecoveryNotOwned
	}
	return nil
}

func (f *fakeRebuildStore) LoadPageCheckpoint(ctx context.Context, jobID, topic, pageCursor string) (store.RecoveryPageCheckpoint, bool, error) {
	if f.checkpoints == nil {
		return store.RecoveryPageCheckpoint{}, false, nil
	}
	cp, ok := f.checkpoints[f.key(jobID, topic, pageCursor)]
	return cp, ok, nil
}

func (f *fakeRebuildStore) LoadRecoveryJob(ctx context.Context, jobID string) (store.RecoveryJobState, error) {
	if f.jobErr != nil {
		return store.RecoveryJobState{}, f.jobErr
	}
	if f.job.RecoveryJobID == "" {
		return store.RecoveryJobState{}, fmtJobNotFound(jobID)
	}
	return f.job, nil
}

func fmtJobNotFound(jobID string) error {
	return errors.New("recovery job \"" + jobID + "\" not found")
}

func (f *fakeRebuildStore) ApplyToGeneration(ctx context.Context, genID string, evt domain.UpstreamEvent) (domain.ApplyOutcome, error) {
	if f.applyIdx >= len(f.applySeq) {
		return domain.ApplyOutcome{Kind: domain.OutcomeAccepted, EventID: evt.EventID}, nil
	}
	out := f.applySeq[f.applyIdx]
	f.applyIdx++
	return out, nil
}

func (f *fakeRebuildStore) MarkRecoveryFailed(ctx context.Context, ownerToken, jobID, status, code, summary, quarantineKey string) error {
	f.markFailed++
	f.job.Status = status
	return nil
}

func (f *fakeRebuildStore) PersistPageCheckpoint(ctx context.Context, ownerToken string, cp store.RecoveryPageCheckpoint) error {
	if f.checkpoints == nil {
		f.checkpoints = map[string]store.RecoveryPageCheckpoint{}
	}
	f.checkpoints[f.key(cp.RecoveryJobID, cp.SourceTopic, cp.PageCursor)] = cp
	return nil
}

func (f *fakeRebuildStore) ReconcileRecoveryJobProgressFromCheckpoint(ctx context.Context, ownerToken string, cp store.RecoveryPageCheckpoint) error {
	f.reconcileN++
	updated, need := store.ReconcileJobProgressFromCheckpoint(f.job, cp)
	if need {
		f.job = updated
		f.progress = append(f.progress, updated)
	}
	return nil
}

func (f *fakeRebuildStore) ActivateRecoveryGeneration(ctx context.Context, ownerToken, jobID string) error {
	if f.activateErr != nil {
		return f.activateErr
	}
	f.activated = true
	f.job.Status = store.RecoveryStatusComplete
	return nil
}

func (f *fakeRebuildStore) PersistRequestIdempotency(ctx context.Context, ownerToken string, row store.RecoveryRequestIdempotency) error {
	if f.idempotency == nil {
		f.idempotency = map[string]store.RecoveryRequestIdempotency{}
	}
	f.idempotency[f.key(row.RecoveryJobID, row.SourceTopic, row.PageCursor)] = row
	return nil
}

type stubBackfill struct {
	page   AnalyticsBackfillHTTPResponse
	events []domain.UpstreamEvent
	err    error
}

func (s stubBackfill) FetchPage(ctx context.Context, req ParsedAnalyticsProjectionRebuildRequest) (AnalyticsBackfillHTTPResponse, []domain.UpstreamEvent, error) {
	return s.page, s.events, s.err
}

func validRebuildReq(t *testing.T) ParsedAnalyticsProjectionRebuildRequest {
	t.Helper()
	req, err := ParseAnalyticsProjectionRebuildRequested(validRebuildEnvelope())
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func TestExecutor_ReconcilesProgressOnCheckpointRedelivery(t *testing.T) {
	req := validRebuildReq(t)
	gen := store.RecoveryGenerationID(req.RecoveryJobID)
	cp := store.RecoveryPageCheckpoint{
		RecoveryJobID: req.RecoveryJobID, SourceTopic: req.ExpectedSourceTopic,
		PageCursor: "", PageIndex: 0, NextPageCursor: "next",
		RecordsApplied: 4, Status: store.PageStatusApplied, GenerationID: gen,
	}
	f := &fakeRebuildStore{
		leaseOK: true,
		job: store.RecoveryJobState{
			RecoveryJobID: req.RecoveryJobID, SourceContext: req.SourceContext, SourceTopic: req.ExpectedSourceTopic,
			GenerationID: gen, Status: store.RecoveryStatusBuilding,
			FromCheckpoint: req.FromCheckpoint, ToCheckpoint: req.ToCheckpoint, HasCheckpointRng: req.HasCheckpointRange,
			PagesCompleted: 0, AcceptedCount: 0,
		},
		checkpoints: map[string]store.RecoveryPageCheckpoint{
			req.RecoveryJobID + "|" + req.ExpectedSourceTopic + "|": cp,
		},
	}
	exec := &StoreBackedAnalyticsProjectionRebuildExecutor{
		Store: f, Backfill: stubBackfill{}, OwnerToken: "own_test", Clock: systemClock{},
	}
	res, err := exec.ExecutePage(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if f.reconcileN != 1 || f.job.PagesCompleted != 1 || f.job.AcceptedCount != 4 {
		t.Fatalf("reconcileN=%d job=%+v", f.reconcileN, f.job)
	}
	if res.FollowUp == nil || res.FollowUp.PageCursor != "next" {
		t.Fatalf("follow=%+v", res.FollowUp)
	}
}

func TestExecutor_ReconcileAfterProgressAlreadyWrittenIsIdempotent(t *testing.T) {
	req := validRebuildReq(t)
	gen := store.RecoveryGenerationID(req.RecoveryJobID)
	cp := store.RecoveryPageCheckpoint{
		RecoveryJobID: req.RecoveryJobID, SourceTopic: req.ExpectedSourceTopic,
		PageCursor: "", PageIndex: 0, NextPageCursor: "next",
		RecordsApplied: 4, Status: store.PageStatusApplied, GenerationID: gen,
	}
	f := &fakeRebuildStore{
		leaseOK: true,
		job: store.RecoveryJobState{
			RecoveryJobID: req.RecoveryJobID, SourceContext: req.SourceContext, SourceTopic: req.ExpectedSourceTopic,
			GenerationID: gen, Status: store.RecoveryStatusBuilding,
			FromCheckpoint: req.FromCheckpoint, ToCheckpoint: req.ToCheckpoint, HasCheckpointRng: req.HasCheckpointRange,
			PagesCompleted: 1, AcceptedCount: 4, LastPageCursor: "", NextPageCursor: "next",
		},
		checkpoints: map[string]store.RecoveryPageCheckpoint{
			req.RecoveryJobID + "|" + req.ExpectedSourceTopic + "|": cp,
		},
	}
	exec := &StoreBackedAnalyticsProjectionRebuildExecutor{
		Store: f, Backfill: stubBackfill{}, OwnerToken: "own_test", Clock: systemClock{},
	}
	if _, err := exec.ExecutePage(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if f.job.PagesCompleted != 1 || f.job.AcceptedCount != 4 {
		t.Fatalf("must not double-count: %+v", f.job)
	}
}

func TestExecutor_QuarantinedDuplicateNeverActivates(t *testing.T) {
	req := validRebuildReq(t)
	f := &fakeRebuildStore{leaseOK: true, jobErr: fmtJobNotFound(req.RecoveryJobID)}
	f.applySeq = []domain.ApplyOutcome{{
		Kind:      domain.OutcomeDuplicate,
		EventID:   "e1",
		Rejection: &domain.Rejection{Code: domain.RejectInvalidSchema, Message: "cloned quarantine"},
	}}
	exec := &StoreBackedAnalyticsProjectionRebuildExecutor{
		Store: f, OwnerToken: "own_test", Clock: systemClock{},
		Backfill: stubBackfill{
			page: AnalyticsBackfillHTTPResponse{
				RecoveryJobID: req.RecoveryJobID, SourceTopic: req.ExpectedSourceTopic, SchemaVersion: 1,
			},
			events: []domain.UpstreamEvent{{EventID: "e1"}},
		},
	}
	_, err := exec.ExecutePage(context.Background(), req)
	if !errors.Is(err, store.ErrRecoveryQuarantined) {
		t.Fatalf("got %v", err)
	}
	if f.activated || f.markFailed != 1 || f.job.Status != store.RecoveryStatusQuarantined {
		t.Fatalf("activated=%v mark=%d status=%s", f.activated, f.markFailed, f.job.Status)
	}
	cp, ok := f.checkpoints[req.RecoveryJobID+"|"+req.ExpectedSourceTopic+"|"]
	if !ok || cp.QuarantinedCount != 1 || cp.RecordsApplied != 0 {
		t.Fatalf("cp=%+v", cp)
	}
}

func TestExecutor_EmptyPageKeepsEmptyCoverageMarkers(t *testing.T) {
	req := validRebuildReq(t)
	f := &fakeRebuildStore{leaseOK: true, jobErr: fmtJobNotFound(req.RecoveryJobID)}
	exec := &StoreBackedAnalyticsProjectionRebuildExecutor{
		Store: f, OwnerToken: "own_test", Clock: systemClock{},
		Backfill: stubBackfill{
			page: AnalyticsBackfillHTTPResponse{
				RecoveryJobID: req.RecoveryJobID, SourceTopic: req.ExpectedSourceTopic, SchemaVersion: 1,
				// Empty terminal page: no coverage markers, no next cursor.
			},
		},
	}
	res, err := exec.ExecutePage(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Finalize || !f.activated {
		t.Fatalf("res=%+v activated=%v", res, f.activated)
	}
	cp := f.checkpoints[req.RecoveryJobID+"|"+req.ExpectedSourceTopic+"|"]
	if cp.FromCheckpoint != "" || cp.ToCheckpoint != "" {
		t.Fatalf("must not substitute request bounds into empty page: %+v", cp)
	}
}

func TestExecutor_JobIDPoisonAcrossTopics(t *testing.T) {
	req := validRebuildReq(t)
	gen := store.RecoveryGenerationID(req.RecoveryJobID)
	f := &fakeRebuildStore{
		leaseOK: true,
		job: store.RecoveryJobState{
			RecoveryJobID: req.RecoveryJobID, SourceContext: "room", SourceTopic: "room.match.completed",
			GenerationID: gen, Status: store.RecoveryStatusBuilding,
			FromCheckpoint: req.FromCheckpoint, ToCheckpoint: req.ToCheckpoint, HasCheckpointRng: true,
		},
	}
	exec := &StoreBackedAnalyticsProjectionRebuildExecutor{
		Store: f, Backfill: stubBackfill{}, OwnerToken: "own_test", Clock: systemClock{},
	}
	_, err := exec.ExecutePage(context.Background(), req)
	if !errors.Is(err, store.ErrRecoveryJobSpecMismatch) {
		t.Fatalf("got %v", err)
	}
	if f.ensureCalled {
		t.Fatal("must not Ensure on poisoned job")
	}
}

func TestExecutor_ClosedJobUnseenPageRejected(t *testing.T) {
	req := validRebuildReq(t)
	gen := store.RecoveryGenerationID(req.RecoveryJobID)
	f := &fakeRebuildStore{
		leaseOK: true,
		job: store.RecoveryJobState{
			RecoveryJobID: req.RecoveryJobID, SourceContext: req.SourceContext, SourceTopic: req.ExpectedSourceTopic,
			GenerationID: gen, Status: store.RecoveryStatusComplete,
			FromCheckpoint: req.FromCheckpoint, ToCheckpoint: req.ToCheckpoint, HasCheckpointRng: req.HasCheckpointRange,
		},
	}
	exec := &StoreBackedAnalyticsProjectionRebuildExecutor{
		Store: f, Backfill: stubBackfill{}, OwnerToken: "own_test", Clock: systemClock{},
	}
	_, err := exec.ExecutePage(context.Background(), req)
	if !errors.Is(err, store.ErrRecoveryJobClosed) {
		t.Fatalf("got %v", err)
	}
}

func TestClassifyRebuildOutcome_AcceptedSemantics(t *testing.T) {
	// Mirrors executor switch; keeps Accepted()/rejection contract visible in unit tests.
	cases := []struct {
		out               domain.ApplyOutcome
		wantAcc, wantQuar bool
	}{
		{domain.ApplyOutcome{Kind: domain.OutcomeAccepted}, true, false},
		{domain.ApplyOutcome{Kind: domain.OutcomeDuplicate}, true, false},
		{domain.ApplyOutcome{Kind: domain.OutcomeDuplicate, Rejection: &domain.Rejection{Code: "x"}}, false, true},
		{domain.ApplyOutcome{Kind: domain.OutcomeQuarantined, Rejection: &domain.Rejection{Code: "x"}}, false, true},
		{domain.ApplyOutcome{Kind: domain.OutcomeIgnored}, true, false},
	}
	for _, tc := range cases {
		var acc, quar bool
		switch {
		case tc.out.Kind == domain.OutcomeQuarantined || (tc.out.Kind == domain.OutcomeDuplicate && !tc.out.Accepted()):
			quar = true
		case tc.out.Accepted(), tc.out.Kind == domain.OutcomeIgnored:
			acc = true
		}
		if acc != tc.wantAcc || quar != tc.wantQuar {
			t.Fatalf("%+v => acc=%v quar=%v", tc.out, acc, quar)
		}
	}
}

func TestRebuildConsumer_NilFollowPublisherFailsClosed(t *testing.T) {
	req := validRebuildEnvelope()
	rec := ConsumerRecord{Topic: DefaultProjectionRebuildTopic, Key: []byte("job-1"), Value: req, Offset: 3}
	src := &memRebuildSource{}
	followReq, _ := ParseAnalyticsProjectionRebuildRequested(validRebuildEnvelope(func(m map[string]any) {
		m["pageCursor"] = "next"
	}))
	c := &AnalyticsProjectionRebuildKafkaConsumer{
		source: src, dlq: &memRebuildDLQ{}, follow: nil,
		exec:  stubExec{res: RebuildPageResult{FollowUp: &followReq, FollowUpEventID: "evt"}},
		cfg:   ProjectionRebuildKafkaConfig{Group: "g", Topic: DefaultProjectionRebuildTopic, MaxAttempts: 1},
		clock: systemClock{},
	}
	err := c.ProcessBatch(context.Background(), []ConsumerRecord{rec})
	if err == nil || !strings.Contains(err.Error(), "follow-up publisher not configured") {
		t.Fatalf("got %v", err)
	}
	if len(src.commits) != 0 {
		t.Fatal("must not commit when follow publisher missing")
	}
}

func TestRebuildConsumer_FollowPublishFailureDoesNotCommit(t *testing.T) {
	req := validRebuildEnvelope()
	rec := ConsumerRecord{Topic: DefaultProjectionRebuildTopic, Key: []byte("job-1"), Value: req, Offset: 3}
	src := &memRebuildSource{}
	followReq, _ := ParseAnalyticsProjectionRebuildRequested(validRebuildEnvelope(func(m map[string]any) {
		m["pageCursor"] = "next"
	}))
	c := &AnalyticsProjectionRebuildKafkaConsumer{
		source: src, dlq: &memRebuildDLQ{}, follow: &memFollowPub{fail: true},
		exec:  stubExec{res: RebuildPageResult{FollowUp: &followReq}},
		cfg:   ProjectionRebuildKafkaConfig{Group: "g", Topic: DefaultProjectionRebuildTopic, MaxAttempts: 1},
		clock: systemClock{},
	}
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{rec}); err == nil {
		t.Fatal("expected publish failure")
	}
	if len(src.commits) != 0 {
		t.Fatal("must not commit after publish failure")
	}
}
