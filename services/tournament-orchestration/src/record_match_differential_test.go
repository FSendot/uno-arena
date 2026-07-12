package main

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/shared/envelope"
)

type stubRoundMatchRepo struct {
	standaloneCalls atomic.Int32
	beginCalls      atomic.Int32
	outcomes        map[string]envelope.Result
}

func (s *stubRoundMatchRepo) LookupOutcome(commandID string) (envelope.Result, bool) {
	if s.outcomes == nil {
		return envelope.Result{}, false
	}
	out, ok := s.outcomes[commandID]
	return out, ok
}

func (s *stubRoundMatchRepo) BeginStandaloneCommand(ctx context.Context, commandID string) (RoundMatchUnitOfWork, error) {
	s.standaloneCalls.Add(1)
	return &stubRoundMatchUoW{commandID: commandID, repo: s}, nil
}

func (s *stubRoundMatchRepo) BeginRoundMatch(ctx context.Context, tournamentID, roomID string, hintRound int, hintSlot string, commandID string) (RoundMatchUnitOfWork, error) {
	s.beginCalls.Add(1)
	return &stubRoundMatchUoW{
		commandID: commandID,
		repo:      s,
		exists:    true,
		loaded: domain.RoundMatchContext{
			TournamentID: domain.TournamentID(tournamentID),
			Phase:        domain.PhaseInProgress,
			RoundNumber:  1,
			RoundFound:   true,
			SlotFound:    true,
			Slot: domain.RoundMatchSlotState{
				SlotID:             domain.SlotID(hintSlot),
				RoomID:             domain.RoomID(roomID),
				AssignmentResolved: true,
				Status:             domain.SlotAssigned,
			},
		},
	}, nil
}

type stubRoundMatchUoW struct {
	commandID string
	repo      *stubRoundMatchRepo
	exists    bool
	loaded    domain.RoundMatchContext
	committed *envelope.Result
}

func (u *stubRoundMatchUoW) Loaded() domain.RoundMatchContext { return u.loaded }
func (u *stubRoundMatchUoW) Exists() bool                     { return u.exists }
func (u *stubRoundMatchUoW) LookupOutcome(commandID string) (envelope.Result, bool) {
	if commandID != u.commandID {
		return envelope.Result{}, false
	}
	return u.repo.LookupOutcome(commandID)
}
func (u *stubRoundMatchUoW) AttachPriorResult(string, uint64) error { return nil }
func (u *stubRoundMatchUoW) Rollback() error                       { return nil }
func (u *stubRoundMatchUoW) Commit(req RoundMatchCommitRequest) error {
	u.committed = &req.Outcome
	if u.repo.outcomes == nil {
		u.repo.outcomes = map[string]envelope.Result{}
	}
	u.repo.outcomes[req.CommandID] = req.Outcome
	return nil
}

func TestSubmitCommand_RecordMatchRoutesDifferentialWhenWired(t *testing.T) {
	repo := NewMemoryTournamentRepository()
	rm := &stubRoundMatchRepo{}
	svc := NewService(ServiceDeps{Repo: repo, RoundMatches: rm, Audit: NoopAudit{}})

	// Invalid standings → standalone outcome-only path (no BeginRoundMatch / no mu whole rewrite).
	res, err := svc.SubmitCommand(context.Background(), CommandRequest{
		CommandID: "rm-invalid-standings", Type: CmdRecordMatchResult, SchemaVersion: envelope.CurrentSchemaVersion,
		Payload: json.RawMessage(`{"tournamentId":"t1","roomId":"r1","completionVersion":1,"standings":"bad"}`),
	}, "corr")
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != envelope.StatusRejected || res.Reason != "invalid_standings" {
		t.Fatalf("got status=%s reason=%s", res.Status, res.Reason)
	}
	if rm.standaloneCalls.Load() != 1 {
		t.Fatalf("standaloneCalls=%d", rm.standaloneCalls.Load())
	}
	if rm.beginCalls.Load() != 0 {
		t.Fatalf("beginCalls=%d want 0", rm.beginCalls.Load())
	}

	// Invalid identity → standalone, still no tournament begin.
	res, err = svc.SubmitCommand(context.Background(), CommandRequest{
		CommandID: "rm-invalid-id", Type: CmdRecordMatchResult, SchemaVersion: envelope.CurrentSchemaVersion,
		Payload: json.RawMessage(`{"tournamentId":"","roomId":"","completionVersion":1,"standings":[]}`),
	}, "corr")
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != envelope.StatusRejected || res.Reason != string(domain.RejectInvalidIdentity) {
		t.Fatalf("got status=%s reason=%s", res.Status, res.Reason)
	}
	if rm.standaloneCalls.Load() != 2 {
		t.Fatalf("standaloneCalls=%d", rm.standaloneCalls.Load())
	}
}

func TestSubmitCommand_RecordMatchDifferential_InvalidPayloadStableRejection(t *testing.T) {
	repo := NewMemoryTournamentRepository()
	audit := NewMemoryAudit()
	rm := &stubRoundMatchRepo{}
	svc := NewService(ServiceDeps{Repo: repo, RoundMatches: rm, Audit: audit})

	cases := []struct {
		name    string
		cmdID   string
		payload json.RawMessage
	}{
		{"malformed", "rm-bad-json", json.RawMessage(`{not-json`)},
		{"array", "rm-array-payload", json.RawMessage(`[]`)},
		{"string", "rm-string-payload", json.RawMessage(`"x"`)},
		{"null", "rm-null-payload", json.RawMessage(`null`)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			beforeStandalone := rm.standaloneCalls.Load()
			beforeBegin := rm.beginCalls.Load()
			beforeAudit := audit.Len()
			res, err := svc.SubmitCommand(context.Background(), CommandRequest{
				CommandID: tc.cmdID, Type: CmdRecordMatchResult, SchemaVersion: envelope.CurrentSchemaVersion,
				Payload: tc.payload,
			}, "corr")
			if err != nil {
				t.Fatalf("must not return Go error: %v", err)
			}
			if res.Status != envelope.StatusRejected || res.Reason != "invalid_payload" {
				t.Fatalf("got status=%s reason=%s", res.Status, res.Reason)
			}
			if rm.standaloneCalls.Load() != beforeStandalone+1 {
				t.Fatalf("standaloneCalls delta want 1 got %d", rm.standaloneCalls.Load()-beforeStandalone)
			}
			if rm.beginCalls.Load() != beforeBegin {
				t.Fatalf("BeginRoundMatch must not run; beginCalls=%d", rm.beginCalls.Load())
			}
			if audit.Len() != beforeAudit+1 {
				t.Fatalf("audit delta want 1 got %d", audit.Len()-beforeAudit)
			}
			replay, err := svc.SubmitCommand(context.Background(), CommandRequest{
				CommandID: tc.cmdID, Type: CmdRecordMatchResult, SchemaVersion: envelope.CurrentSchemaVersion,
				Payload: json.RawMessage(`{"tournamentId":"t1","roomId":"r1","completionVersion":1,"standings":[]}`),
			}, "corr-replay")
			if err != nil {
				t.Fatal(err)
			}
			if replay.Status != res.Status || replay.Reason != res.Reason || replay.CommandID != res.CommandID {
				t.Fatalf("replay mismatch first=%+v replay=%+v", res, replay)
			}
		})
	}

	// Schema mismatch stays a Go error (parity with other command paths).
	_, err := svc.SubmitCommand(context.Background(), CommandRequest{
		CommandID: "rm-schema", Type: CmdRecordMatchResult, SchemaVersion: envelope.CurrentSchemaVersion + 1,
		Payload: json.RawMessage(`{"tournamentId":"t1","roomId":"r1","completionVersion":1,"standings":[]}`),
	}, "corr")
	if err == nil {
		t.Fatal("schema mismatch must return Go error")
	}
}

func TestSubmitCommand_RecordMatchMemory_ZeroCompletionVersion(t *testing.T) {
	repo := NewMemoryTournamentRepository()
	svc := NewService(ServiceDeps{Repo: repo, Audit: NoopAudit{}})
	create, err := svc.SubmitCommand(context.Background(), CommandRequest{
		CommandID: "mem-cv-create", Type: CmdCreateTournament, SchemaVersion: envelope.CurrentSchemaVersion,
		Payload: json.RawMessage(`{"tournamentId":"t-cv0","capacity":12}`),
	}, "corr")
	if err != nil || create.Status != envelope.StatusAccepted {
		t.Fatalf("create: err=%v res=%+v", err, create)
	}
	for i := 0; i < 12; i++ {
		pid := "p" + itoa(i)
		reg, err := svc.SubmitCommand(context.Background(), CommandRequest{
			CommandID: "mem-cv-reg-" + pid, Type: CmdRegisterPlayer, SchemaVersion: envelope.CurrentSchemaVersion,
			Payload: json.RawMessage(`{"tournamentId":"t-cv0","playerId":"` + pid + `"}`),
		}, "corr")
		if err != nil || reg.Status != envelope.StatusAccepted {
			t.Fatalf("register %s: err=%v res=%+v", pid, err, reg)
		}
	}
	for _, step := range []struct {
		id, typ, payload string
	}{
		{"mem-cv-close", CmdCloseRegistration, `{"tournamentId":"t-cv0"}`},
		{"mem-cv-seed", CmdSeedRound, `{"tournamentId":"t-cv0","roundNumber":1}`},
		{"mem-cv-prov", CmdProvisionRoundMatches, `{"tournamentId":"t-cv0","roundNumber":1}`},
	} {
		res, err := svc.SubmitCommand(context.Background(), CommandRequest{
			CommandID: step.id, Type: step.typ, SchemaVersion: envelope.CurrentSchemaVersion,
			Payload: json.RawMessage(step.payload),
		}, "corr")
		if err != nil || res.Status != envelope.StatusAccepted {
			t.Fatalf("%s: err=%v res=%+v", step.id, err, res)
		}
	}
	tr, ok := repo.Get(domain.TournamentID("t-cv0"))
	if !ok {
		t.Fatal("missing tournament")
	}
	round, _ := tr.Round(1)
	slot := round.Slots[0]
	res, err := svc.SubmitCommand(context.Background(), CommandRequest{
		CommandID: "mem-cv-zero", Type: CmdRecordMatchResult, SchemaVersion: envelope.CurrentSchemaVersion,
		Payload: json.RawMessage(`{"tournamentId":"t-cv0","roomId":"` + string(slot.RoomID) + `","roundNumber":1,"slotId":"` + string(slot.SlotID) + `","completionVersion":0,"standings":[]}`),
	}, "corr")
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != envelope.StatusRejected || res.Reason != string(domain.RejectInvalidCommand) {
		t.Fatalf("want invalid_command, got status=%s reason=%s", res.Status, res.Reason)
	}
}

func TestSubmitCommand_RecordMatchMemoryPathWhenRoundMatchesNil(t *testing.T) {
	repo := NewMemoryTournamentRepository()
	svc := NewService(ServiceDeps{Repo: repo, Audit: NoopAudit{}})
	if svc.UsesRoundMatchDifferential() {
		t.Fatal("expected memory path")
	}
	// Empty tournamentId with valid-shaped standings still goes through memory recordResult.
	res, err := svc.SubmitCommand(context.Background(), CommandRequest{
		CommandID: "mem-rm", Type: CmdRecordMatchResult, SchemaVersion: envelope.CurrentSchemaVersion,
		Payload: json.RawMessage(`{"tournamentId":"missing","roomId":"r1","completionVersion":1,"standings":[]}`),
	}, "corr")
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != envelope.StatusRejected {
		t.Fatalf("expected reject, got %s", res.Status)
	}
}
