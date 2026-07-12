package bff

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"unoarena/shared/correlation"
)

func TestPublicRoomList_ProxyAndValidation(t *testing.T) {
	room := NewFakeRoom()
	room.PublicListJSON = json.RawMessage(`{
		"schemaVersion":1,
		"rooms":[{"roomId":"r1","status":"waiting","visibility":"public","maxSeats":4,"currentPlayers":1,"hostId":"h1","roomType":"ad_hoc"}]
	}`)
	srv := NewServer(Dependencies{
		Room: room, Identity: NewFakeIdentity(), Ready: true,
	})
	mux := srv.Handler()

	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/rooms", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(room.LastPublicListQuery, "status=waiting") || !strings.Contains(room.LastPublicListQuery, "limit=50") {
		t.Fatalf("query=%q", room.LastPublicListQuery)
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["schemaVersion"].(float64) != 1 {
		t.Fatalf("body=%v", body)
	}

	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/rooms?status=completed", nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", w.Code)
	}

	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/rooms?limit=101", nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", w.Code)
	}
}

func TestPublicRoomList_MalformedUpstreamMaps502(t *testing.T) {
	room := NewFakeRoom()
	room.PublicListJSON = json.RawMessage(`{"schemaVersion":1,"rooms":[{"roomId":"r1","status":"waiting","visibility":"private","maxSeats":4,"currentPlayers":1,"hostId":"h","roomType":"ad_hoc"}]}`)
	srv := NewServer(Dependencies{
		Room: room, Identity: NewFakeIdentity(), Ready: true,
	})
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/rooms", nil))
	if w.Code != http.StatusBadGateway {
		t.Fatalf("want 502, got %d body=%s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), "private") {
		t.Fatal("must not leak upstream detail")
	}
}

func TestPublicRoomList_Upstream400Preserved(t *testing.T) {
	room := NewFakeRoom()
	room.PublicListErr = &httpStatusError{status: http.StatusBadRequest, body: `{"code":"secret"}`}
	srv := NewServer(Dependencies{
		Room: room, Identity: NewFakeIdentity(), Ready: true,
	})
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/rooms?cursor=bad", nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", w.Code)
	}
	if strings.Contains(w.Body.String(), "secret") {
		t.Fatal("must not expose upstream body")
	}
}

func TestPublicRoomList_NetworkMaps502(t *testing.T) {
	room := NewFakeRoom()
	room.PublicListErr = errors.New("dial tcp: connection refused")
	srv := NewServer(Dependencies{
		Room: room, Identity: NewFakeIdentity(), Ready: true,
	})
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/rooms", nil))
	if w.Code != http.StatusBadGateway {
		t.Fatalf("want 502, got %d", w.Code)
	}
}

func TestHTTPRoomClient_PublicList(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/rooms/public-list" {
			t.Fatalf("path=%s", r.URL.Path)
		}
		if r.Header.Get("X-Service-Credential") != "cred" {
			t.Fatalf("cred=%q", r.Header.Get("X-Service-Credential"))
		}
		if r.URL.Query().Get("status") != "waiting" {
			t.Fatalf("query=%v", r.URL.Query())
		}
		_, _ = w.Write([]byte(`{"schemaVersion":1,"rooms":[]}`))
	}))
	defer upstream.Close()

	client := NewHTTPRoomClient(HTTPClientConfig{BaseURL: upstream.URL, ServiceCredential: "cred", HTTPClient: upstream.Client()})
	raw, err := client.PublicList(t.Context(), "status=waiting&limit=50", correlation.Headers{CorrelationID: "corr-pl"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"rooms"`) {
		t.Fatalf("raw=%s", raw)
	}
}
