package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

const testAnalyticsBackfillCred = "analytics-ranking-credential"

func newAnalyticsBackfillHarness(t *testing.T) (*Server, *MemoryAnalyticsBackfillStore) {
	t.Helper()
	restore := SetAnalyticsBackfillCursorMACKeyForTest("http-ranking-analytics-backfill-cursor")
	t.Cleanup(restore)
	mem := NewMemoryAnalyticsBackfillStore()
	srv := NewServerWithScopedCreds(NewMemoryRatingStore(), testInternalCredential, testAnalyticsBackfillCred)
	srv.analyticsBackfill = mem
	return srv, mem
}

func postAnalyticsBackfill(t *testing.T, mux http.Handler, cred string, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/rankings/analytics-backfill", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	if cred != "" {
		req.Header.Set(internalCredentialHeader, cred)
	}
	req.Header.Set("X-Correlation-Id", "corr-analytics-backfill")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}

func TestAnalyticsBackfill_Auth(t *testing.T) {
	srv, _ := newAnalyticsBackfillHarness(t)
	mux := srv.routes()
	body := map[string]any{
		"recoveryJobId": "job-auth", "sourceTopic": TopicPlayerRatingUpdated, "schemaVersion": 1,
		"fromCheckpoint": "1", "toCheckpoint": "100",
	}

	w := postAnalyticsBackfill(t, mux, "", body)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("missing cred: %d", w.Code)
	}
	w = postAnalyticsBackfill(t, mux, "wrong", body)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("wrong cred: %d", w.Code)
	}
	w = postAnalyticsBackfill(t, mux, testInternalCredential, body)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("generic service cred must not authorize: %d", w.Code)
	}
	w = postAnalyticsBackfill(t, mux, testAnalyticsBackfillCred, body)
	if w.Code != http.StatusOK {
		t.Fatalf("analytics cred: %d %s", w.Code, w.Body.String())
	}
}

func TestAnalyticsBackfill_HTTPValidationAndPaging(t *testing.T) {
	srv, mem := newAnalyticsBackfillHarness(t)
	mux := srv.routes()
	at := time.Date(2026, 7, 1, 15, 0, 0, 0, time.UTC)
	var firstID, lastID int64
	for i := 0; i < 3; i++ {
		ts := at.Add(time.Duration(i) * time.Minute)
		id := mem.Append(TopicPlayerRatingUpdated, eventTypePlayerRatingUpdatedMsg,
			playerRatingEnvelope("http-"+strconv.Itoa(i), ts, 1000, 1001, int64(i+1)), &ts)
		if i == 0 {
			firstID = id
		}
		lastID = id
	}

	w := postAnalyticsBackfill(t, mux, testAnalyticsBackfillCred, map[string]any{
		"recoveryJobId": "job-http", "sourceTopic": TopicPlayerRatingUpdated, "schemaVersion": 1,
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("no range: %d %s", w.Code, w.Body.String())
	}

	w = postAnalyticsBackfill(t, mux, testAnalyticsBackfillCred, map[string]any{
		"recoveryJobId": "job-http", "sourceTopic": "room.gameplay.metrics", "schemaVersion": 1,
		"fromCheckpoint": strconv.FormatInt(firstID, 10), "toCheckpoint": strconv.FormatInt(lastID, 10),
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("topic allowlist: %d", w.Code)
	}

	w = postAnalyticsBackfill(t, mux, testAnalyticsBackfillCred, map[string]any{
		"recoveryJobId": "job-http", "sourceTopic": TopicPlayerRatingUpdated, "schemaVersion": 1,
		"fromCheckpoint": strconv.FormatInt(firstID, 10), "toCheckpoint": strconv.FormatInt(lastID, 10),
		"limit": 2,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("page1: %d %s", w.Code, w.Body.String())
	}
	var page1 AnalyticsBackfillResponse
	if err := json.NewDecoder(w.Body).Decode(&page1); err != nil {
		t.Fatal(err)
	}
	if len(page1.Records) != 2 || page1.NextCursor == "" {
		t.Fatalf("page1=%+v", page1)
	}
	if page1.FromCheckpoint != strconv.FormatInt(firstID, 10) {
		t.Fatalf("honest fromCheckpoint=%s", page1.FromCheckpoint)
	}

	w = postAnalyticsBackfill(t, mux, testAnalyticsBackfillCred, map[string]any{
		"recoveryJobId": "job-http", "sourceTopic": TopicPlayerRatingUpdated, "schemaVersion": 1,
		"fromCheckpoint": strconv.FormatInt(firstID, 10), "toCheckpoint": strconv.FormatInt(lastID, 10),
		"limit": 2, "cursor": page1.NextCursor,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("page2: %d %s", w.Code, w.Body.String())
	}
	var page2 AnalyticsBackfillResponse
	_ = json.NewDecoder(w.Body).Decode(&page2)
	if len(page2.Records) != 1 || page2.NextCursor != "" {
		t.Fatalf("page2=%+v", page2)
	}
	before := mem.Count()
	_ = postAnalyticsBackfill(t, mux, testAnalyticsBackfillCred, map[string]any{
		"recoveryJobId": "job-http", "sourceTopic": TopicPlayerRatingUpdated, "schemaVersion": 1,
		"fromCheckpoint": strconv.FormatInt(firstID, 10), "toCheckpoint": strconv.FormatInt(lastID, 10),
	})
	if mem.Count() != before {
		t.Fatal("HTTP backfill must not mutate outbox")
	}
}
