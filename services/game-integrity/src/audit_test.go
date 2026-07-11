package main

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

func TestAuditHeadersRequiredAndRecorded(t *testing.T) {
	sink := &MemoryAuditRecorder{}
	repo := NewMemoryStreamRepository()
	srv := NewServerWithAudit(repo, sink, testRoomCredential, testAuditCredential, "offline", "")
	mux := srv.routes()

	room := "room-audit-hdr"
	_ = doJSON(t, mux, http.MethodPost, "/internal/v1/game-logs/"+room+"/append", map[string]any{
		"eventId": "e1", "expectedRevision": 0, "eventType": "PlayCard", "gameId": "g1",
	}, withRoomCred(nil))

	missing := doJSON(t, mux, http.MethodGet, "/internal/v1/game-logs/"+room+"/replay?from=0", nil, map[string]string{
		internalCredentialHeader: testAuditCredential,
	})
	if missing.Code != http.StatusBadRequest {
		t.Fatalf("missing audit headers: %d %s", missing.Code, missing.Body.String())
	}

	ok := doJSON(t, mux, http.MethodGet, "/internal/v1/game-logs/"+room+"/replay?from=0", nil, withAuditCred(map[string]string{
		"X-Correlation-Id": "corr-audit-1",
	}))
	if ok.Code != http.StatusOK {
		t.Fatalf("replay: %d %s", ok.Code, ok.Body.String())
	}
	recs := sink.Snapshot()
	if len(recs) != 1 || recs[0].Action != "replay" || recs[0].Outcome != "success" {
		t.Fatalf("audit records: %+v", recs)
	}
	if recs[0].Actor != "test-operator" || recs[0].Reason != "test-audit-reason" {
		t.Fatalf("actor/reason: %+v", recs[0])
	}
	if recs[0].CorrelationID != "corr-audit-1" {
		t.Fatalf("correlation: %+v", recs[0])
	}
	if len(recs[0].KeyVersions) != 0 || recs[0].KeyVersion != 0 {
		t.Fatalf("memory mode must record no encryption versions: %+v", recs[0])
	}

	again := doJSON(t, mux, http.MethodGet, "/internal/v1/game-logs/"+room+"/replay?from=0", nil, withAuditCred(map[string]string{
		"X-Correlation-Id": "corr-audit-1",
	}))
	if again.Code != http.StatusOK {
		t.Fatalf("replay again: %d %s", again.Code, again.Body.String())
	}
	recs = sink.Snapshot()
	if len(recs) != 2 {
		t.Fatalf("repeated attempt must append second durable record, got %d", len(recs))
	}
	if recs[0].EventID == recs[1].EventID {
		t.Fatalf("attempt ids must differ: %s", recs[0].EventID)
	}
}

func TestAuditAppendFailureFailsClosed(t *testing.T) {
	sink := &MemoryAuditRecorder{Fail: errors.New("audit append failed")}
	repo := NewMemoryStreamRepository()
	srv := NewServerWithAudit(repo, sink, testRoomCredential, testAuditCredential, "offline", "")
	mux := srv.routes()
	room := "room-audit-fail"
	_ = doJSON(t, mux, http.MethodPost, "/internal/v1/game-logs/"+room+"/append", map[string]any{
		"eventId": "e1", "expectedRevision": 0, "eventType": "PlayCard",
	}, withRoomCred(nil))
	w := doJSON(t, mux, http.MethodGet, "/internal/v1/game-logs/"+room+"/replay?from=0", nil, withAuditCred(nil))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("want audit fail-closed 500, got %d %s", w.Code, w.Body.String())
	}
}

func TestContextCancellationPreventsAppend(t *testing.T) {
	repo := NewMemoryStreamRepository()
	svc := NewService(repo)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := svc.Append(ctx, AppendRequest{
		RoomID: "room-cancel-ctx", EventID: "e1", ExpectedRevision: 0, EventType: "PlayCard",
	})
	if err == nil {
		t.Fatal("cancelled context must prevent append")
	}
}
