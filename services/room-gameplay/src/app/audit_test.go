package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"unoarena/shared/audit"
	"unoarena/shared/envelope"
)

func sampleRejection(commandID string) audit.RejectionRecord {
	return audit.NewRejection(commandID, "corr-"+commandID, "sess", "player", "sequence_mismatch",
		time.Date(2026, 7, 11, 0, 0, 0, 0, time.UTC)).WithRoom("room_a")
}

func TestJSONLAuditSink_ExactDuplicateNoOp(t *testing.T) {
	var buf bytes.Buffer
	sink := NewJSONLAuditSink(&buf)
	rec := sampleRejection("cmd_dup")
	if err := sink.Record(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	if err := sink.Record(context.Background(), rec); err != nil {
		t.Fatalf("exact duplicate must be no-op: %v", err)
	}
	if got := bytes.Count(buf.Bytes(), []byte("\n")); got != 1 {
		t.Fatalf("lines=%d want 1 body=%s", got, buf.String())
	}
}

func TestJSONLAuditSink_ConflictingSameKeyError(t *testing.T) {
	sink := NewJSONLAuditSink(&bytes.Buffer{})
	rec := sampleRejection("cmd_conflict")
	if err := sink.Record(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	other := rec
	other.Reason = "room_full"
	err := sink.Record(context.Background(), other)
	if err == nil || !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("conflicting same commandId must error, err=%v", err)
	}
}

func TestOpenJSONLAuditSink_ScansExistingCommandIDsOnRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.jsonl")
	sink, err := OpenJSONLAuditSink(path)
	if err != nil {
		t.Fatal(err)
	}
	rec := sampleRejection("cmd_restart")
	if err := sink.Record(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenJSONLAuditSink(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.Record(context.Background(), rec); err != nil {
		t.Fatalf("restart exact duplicate must be no-op: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := bytes.Count(raw, []byte("\n")); got != 1 {
		t.Fatalf("restart must not duplicate line: lines=%d body=%s", got, raw)
	}
	conflict := rec
	conflict.Reason = "other"
	if err := reopened.Record(context.Background(), conflict); err == nil || !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("restart conflict err=%v", err)
	}
}

func TestOpenJSONLAuditSink_FailClosedOnCorruptTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "corrupt.jsonl")
	rec := sampleRejection("cmd_ok")
	line, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	// Valid line then partial/malformed JSON tail — must not open for append.
	payload := append(append(line, '\n'), []byte(`{"commandId":"partial`)...)
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	sink, err := OpenJSONLAuditSink(path)
	if err == nil {
		_ = sink.Close()
		t.Fatal("OpenJSONLAuditSink must fail closed on malformed/partial JSON tail")
	}
	if !strings.Contains(err.Error(), "corrupt") && !strings.Contains(err.Error(), "malformed") {
		t.Fatalf("err=%v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, payload) {
		t.Fatal("fail-closed open must not append onto corrupt tail")
	}
}

// Valid JSON without a trailing newline is still parseable by line scanners, but
// O_APPEND would concatenate the next record onto it. Fail closed before append.
func TestOpenJSONLAuditSink_FailClosedOnValidRecordMissingTrailingNewline(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "no_newline.jsonl")
	rec := sampleRejection("cmd_no_nl")
	line, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, line, 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sink, err := OpenJSONLAuditSink(path)
	if err == nil {
		_ = sink.Close()
		t.Fatal("OpenJSONLAuditSink must fail closed when final valid JSON lacks trailing newline")
	}
	if !strings.Contains(err.Error(), "newline") && !strings.Contains(err.Error(), "corrupt") {
		t.Fatalf("err=%v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatalf("fail-closed open must not append or corrupt: before=%q after=%q", before, after)
	}
}

func TestOpenJSONLAuditSink_FailClosedOnMissingCommandID(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "missing.jsonl")
	if err := os.WriteFile(path, []byte(`{"correlationId":"c","reason":"x","timestamp":"2026-07-11T00:00:00Z"}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenJSONLAuditSink(path); err == nil || !strings.Contains(err.Error(), "commandId") {
		t.Fatalf("missing commandId must fail closed, err=%v", err)
	}
}

func TestOpenJSONLAuditSink_FailClosedOnHistoricalConflict(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "conflict.jsonl")
	a := sampleRejection("cmd_hist")
	b := a
	b.Reason = "other_reason"
	la, _ := json.Marshal(a)
	lb, _ := json.Marshal(b)
	body := append(append(la, '\n'), append(lb, '\n')...)
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenJSONLAuditSink(path); err == nil || !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("conflicting historical payloads must fail closed, err=%v", err)
	}
}

func TestOpenJSONLAuditSink_ExactHistoricalDuplicatesOK(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dup.jsonl")
	rec := sampleRejection("cmd_hist_dup")
	line, _ := json.Marshal(rec)
	body := append(append(line, '\n'), append(line, '\n')...)
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	sink, err := OpenJSONLAuditSink(path)
	if err != nil {
		t.Fatalf("exact historical duplicates must be accepted: %v", err)
	}
	defer sink.Close()
	if err := sink.Record(context.Background(), rec); err != nil {
		t.Fatalf("in-memory exact duplicate after scan: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := bytes.Count(raw, []byte("\n")); got != 2 {
		t.Fatalf("must not append another duplicate line: lines=%d", got)
	}
}

func TestFakeAuditSink_ExactDuplicateNoOpAndConflict(t *testing.T) {
	sink := NewFakeAuditSink()
	rec := sampleRejection("cmd_mem")
	if err := sink.Record(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	if err := sink.Record(context.Background(), rec); err != nil {
		t.Fatalf("memory exact duplicate: %v", err)
	}
	if sink.Len() != 1 {
		t.Fatalf("len=%d", sink.Len())
	}
	other := rec
	other.Reason = "changed"
	if err := sink.Record(context.Background(), other); err == nil || !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("memory conflict err=%v", err)
	}
}

func TestAudit_MarkCompleteFailureReplayNoDuplicate_ThenRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pending.jsonl")
	jsonl, err := OpenJSONLAuditSink(path)
	if err != nil {
		t.Fatal(err)
	}
	sessions := NewMemorySessionRepository()
	sessions.FailMarkAudit = errors.New("mark boom")
	svc := NewService(ServiceDeps{
		Sessions:  sessions,
		Integrity: NewFakeGameIntegrity(),
		Publisher: &FakeEventPublisher{},
		SessionsV: AllowAllSessionValidator{},
		Audit:     jsonl,
		Deals:     NewFakeDealSource(),
		Clock:     NewFixedClock(time.Date(2026, 7, 11, 0, 0, 0, 0, time.UTC)),
	})
	sv := envelope.CurrentSchemaVersion
	ctx := context.Background()
	mustAccept(t, svc.HandleCommand(ctx, CommandInput{
		CommandID: "c", Type: CmdCreateRoom, RoomID: "room_audit", PlayerID: "host", SessionID: "s",
		SchemaVersion: sv, Payload: mustRaw(map[string]any{"roomId": "room_audit"}),
	}))

	rej := svc.HandleCommand(ctx, CommandInput{
		CommandID: "bad", Type: CmdJoinRoom, RoomID: "room_audit", PlayerID: "guest", SessionID: "s2",
		SchemaVersion: sv, ExpectedSequenceNumber: int64Ptr(99),
	})
	if rej.Err == nil || !strings.Contains(rej.Err.Error(), "mark boom") {
		t.Fatalf("MarkAuditComplete failure must surface, got %+v", rej)
	}
	if _, ok := sessions.GetPendingAudit(context.Background(), "bad"); !ok {
		t.Fatal("pending audit must remain after mark failure")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := bytes.Count(raw, []byte("\n")); got != 1 {
		t.Fatalf("successful Record before mark failure must write one line, got %d", got)
	}

	sessions.FailMarkAudit = nil
	replay := svc.HandleCommand(ctx, CommandInput{
		CommandID: "bad", Type: CmdJoinRoom, RoomID: "room_audit", PlayerID: "guest", SessionID: "s2",
		SchemaVersion: sv, ExpectedSequenceNumber: int64Ptr(99),
	})
	if replay.Err != nil {
		t.Fatal(replay.Err)
	}
	if replay.Result.Status != envelope.StatusRejected {
		t.Fatalf("replay=%+v", replay.Result)
	}
	if _, ok := sessions.GetPendingAudit(context.Background(), "bad"); ok {
		t.Fatal("pending audit must clear after successful mark")
	}
	raw, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := bytes.Count(raw, []byte("\n")); got != 1 {
		t.Fatalf("replay after mark failure must not duplicate audit line, got %d body=%s", got, raw)
	}
	if err := jsonl.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenJSONLAuditSink(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	var prior audit.RejectionRecord
	if err := json.Unmarshal(bytes.TrimSpace(raw), &prior); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Record(ctx, prior); err != nil {
		t.Fatalf("reopened sink must treat existing commandId as exact duplicate: %v", err)
	}
	raw2, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := bytes.Count(raw2, []byte("\n")); got != 1 {
		t.Fatalf("restart Record must not append duplicate, got %d", got)
	}
}
