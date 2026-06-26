package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthHandler(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	healthHandler(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestRegisterHandler(t *testing.T) {
	body, _ := json.Marshal(map[string]string{"username": "alice", "password": "pass"})
	req := httptest.NewRequest(http.MethodPost, "/register", bytes.NewReader(body))
	w := httptest.NewRecorder()
	registerHandler(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var resp map[string]string
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["status"] != "ok" {
		t.Errorf("expected status ok, got %s", resp["status"])
	}
	if resp["username"] != "alice" {
		t.Errorf("expected username alice, got %s", resp["username"])
	}
}

func TestWhoamiHandler(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/whoami?user=alice", nil)
	w := httptest.NewRecorder()
	whoamiHandler(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var resp map[string]string
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["username"] != "alice" {
		t.Errorf("expected alice, got %s", resp["username"])
	}
}
