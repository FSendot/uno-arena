package httpx

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWriteJSONAndError(t *testing.T) {
	rec := httptest.NewRecorder()
	if err := WriteJSON(rec, http.StatusOK, map[string]string{"status": "ok"}); err != nil {
		t.Fatalf("WriteJSON: %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("content-type=%q", ct)
	}
	var okBody map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&okBody); err != nil {
		t.Fatalf("decode ok body: %v", err)
	}
	if okBody["status"] != "ok" {
		t.Fatalf("body=%v", okBody)
	}

	rec = httptest.NewRecorder()
	if err := WriteError(rec, http.StatusConflict, "stale_sequence", "sequence mismatch", "corr_1", "cmd_1"); err != nil {
		t.Fatalf("WriteError: %v", err)
	}
	if rec.Code != http.StatusConflict {
		t.Fatalf("status=%d", rec.Code)
	}
	var body ErrorBody
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Code != "stale_sequence" || body.CorrelationID != "corr_1" || body.CommandID != "cmd_1" {
		t.Fatalf("body=%+v", body)
	}
	if body.Message != "sequence mismatch" {
		t.Fatalf("message=%q", body.Message)
	}
}

func TestWriteErrorOmitsEmptyIDs(t *testing.T) {
	rec := httptest.NewRecorder()
	if err := WriteError(rec, http.StatusBadRequest, "bad_request", "invalid", "", ""); err != nil {
		t.Fatalf("WriteError: %v", err)
	}
	raw, _ := io.ReadAll(rec.Body)
	if strings.Contains(string(raw), "correlationId") || strings.Contains(string(raw), "commandId") {
		t.Fatalf("unexpected ids in body: %s", raw)
	}
}

func TestDecodeJSON(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(`{"commandId":"cmd_1"}`))
	var dest struct {
		CommandID string `json:"commandId"`
	}
	if err := DecodeJSON(req, &dest); err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}
	if dest.CommandID != "cmd_1" {
		t.Fatalf("got %+v", dest)
	}
}

func TestDecodeJSONRejectsUnknownFields(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(`{"commandId":"cmd_1","extra":true}`))
	var dest struct {
		CommandID string `json:"commandId"`
	}
	if err := DecodeJSON(req, &dest); err == nil {
		t.Fatal("expected unknown field error")
	}
}

func TestDecodeJSONRejectsMalformed(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(`{`))
	var dest map[string]any
	if err := DecodeJSON(req, &dest); err == nil {
		t.Fatal("expected decode error")
	}
}
