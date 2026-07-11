package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPGameIntegrityAppend_4xxIsDefinitive(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"code":"revision_mismatch","message":"stale"}`))
	}))
	t.Cleanup(srv.Close)
	gi := NewHTTPGameIntegrity(srv.URL, "cred", srv.Client())
	_, err := gi.Append(context.Background(), AppendRequest{
		RoomID: "r", EventID: "e", ExpectedRevision: 0, EventType: "CreateRoom", Payload: []byte(`{}`),
	})
	if err == nil || !errors.Is(err, ErrIntegrityAppendDefinitive) {
		t.Fatalf("want definitive, got %v", err)
	}
}

func TestHTTPGameIntegrityAppend_5xxIsUncertain(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"code":"boom"}`))
	}))
	t.Cleanup(srv.Close)
	gi := NewHTTPGameIntegrity(srv.URL, "cred", srv.Client())
	_, err := gi.Append(context.Background(), AppendRequest{
		RoomID: "r", EventID: "e", ExpectedRevision: 0, EventType: "CreateRoom", Payload: []byte(`{}`),
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if errors.Is(err, ErrIntegrityAppendDefinitive) {
		t.Fatalf("5xx must not be definitive: %v", err)
	}
}

func TestHTTPGameIntegrityAppend_TransportIsUncertain(t *testing.T) {
	gi := NewHTTPGameIntegrity("http://127.0.0.1:1", "cred", &http.Client{})
	_, err := gi.Append(context.Background(), AppendRequest{
		RoomID: "r", EventID: "e", ExpectedRevision: 0, EventType: "CreateRoom", Payload: []byte(`{}`),
	})
	if err == nil {
		t.Fatal("expected transport error")
	}
	if errors.Is(err, ErrIntegrityAppendDefinitive) {
		t.Fatalf("transport must not be definitive: %v", err)
	}
}

func TestHTTPGameIntegrityAppend_DecodeErrorIsUncertain(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`not-json`))
	}))
	t.Cleanup(srv.Close)
	gi := NewHTTPGameIntegrity(srv.URL, "cred", srv.Client())
	_, err := gi.Append(context.Background(), AppendRequest{
		RoomID: "r", EventID: "e", ExpectedRevision: 0, EventType: "CreateRoom", Payload: []byte(`{}`),
	})
	if err == nil {
		t.Fatal("expected decode error")
	}
	if errors.Is(err, ErrIntegrityAppendDefinitive) {
		t.Fatalf("decode must not be definitive: %v", err)
	}
}

func TestHTTPGameIntegrityAppend_ContextTimeoutIsUncertain(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-block
	}))
	t.Cleanup(func() { close(block); srv.Close() })
	gi := NewHTTPGameIntegrity(srv.URL, "cred", srv.Client())
	ctx, cancel := context.WithTimeout(context.Background(), 1)
	defer cancel()
	_, err := gi.Append(ctx, AppendRequest{
		RoomID: "r", EventID: "e", ExpectedRevision: 0, EventType: "CreateRoom", Payload: []byte(`{}`),
	})
	if err == nil {
		t.Fatal("expected timeout")
	}
	if errors.Is(err, ErrIntegrityAppendDefinitive) {
		t.Fatalf("timeout must not be definitive: %v", err)
	}
}

func TestIsDefinitiveIntegrityAppendFailure_OnlyTyped(t *testing.T) {
	if isDefinitiveIntegrityAppendFailure(fmt.Errorf("generic boom")) {
		t.Fatal("generic must not be definitive")
	}
	if isDefinitiveIntegrityAppendFailure(io.EOF) {
		t.Fatal("EOF must not be definitive")
	}
	if !isDefinitiveIntegrityAppendFailure(fmt.Errorf("%w: 409", ErrIntegrityAppendDefinitive)) {
		t.Fatal("typed definitive must cancel intent")
	}
	if isDefinitiveIntegrityAppendFailure(context.DeadlineExceeded) {
		t.Fatal("deadline must not be definitive")
	}
}

func TestHTTPDealSource_ConfirmAtWithoutMeta(t *testing.T) {
	var sawRoom, sawGame, sawRes string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		s := string(body)
		if strings.Contains(s, `"operation":"confirm"`) {
			sawRoom = strings.TrimPrefix(r.URL.Path, "/internal/v1/game-logs/")
			sawRoom = strings.TrimSuffix(sawRoom, "/deck-operations")
			if i := strings.Index(s, `"gameId":"`); i >= 0 {
				rest := s[i+len(`"gameId":"`):]
				sawGame = rest[:strings.Index(rest, `"`)]
			}
			if i := strings.Index(s, `"reservationId":"`); i >= 0 {
				rest := s[i+len(`"reservationId":"`):]
				sawRes = rest[:strings.Index(rest, `"`)]
			}
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"kind":"ok"}`))
			return
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"kind":"ok"}`))
	}))
	t.Cleanup(srv.Close)

	// Fresh adapter with empty meta (simulates restart).
	deals := NewHTTPDealSource(srv.URL, "cred", srv.Client())
	if err := deals.ConfirmAt(context.Background(), "room-x", "game-y", "res-z"); err != nil {
		t.Fatal(err)
	}
	if sawRoom != "room-x" || sawGame != "game-y" || sawRes != "res-z" {
		t.Fatalf("got room=%q game=%q res=%q", sawRoom, sawGame, sawRes)
	}
	// Convenience Confirm still requires meta.
	if err := deals.Confirm(context.Background(), "res-z"); err == nil {
		t.Fatal("Confirm without meta must fail")
	}
}
