package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/shared/envelope"
)

func TestHTTPRoomProvisioner_NeverLeaksRawBody(t *testing.T) {
	const secret = "SUPER_SECRET_TOKEN_xyz"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `{"error":"`+secret+`"}`)
	}))
	defer srv.Close()

	p := NewHTTPRoomProvisioner(srv.URL, "", srv.Client())
	_, err := p.Provision(context.Background(), RoomProvisionRequest{
		TournamentID: "t1", RoundNumber: 1, SlotID: "slot_0", RoomID: "room-1",
		PlayerIDs: []domain.PlayerID{"p1"}, BatchID: "batch_0",
	})
	if err == nil {
		t.Fatal("expected error")
	}
	msg := err.Error()
	if strings.Contains(msg, secret) {
		t.Fatalf("secret leaked into error: %q", msg)
	}
	if !strings.Contains(msg, "room provision HTTP 5xx") {
		t.Fatalf("want classified 5xx, got %q", msg)
	}
}

func TestHTTPRoomProvisioner_RequiresMatchingRoomID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, _ := json.Marshal(map[string]string{"roomId": "other-room"})
		_ = json.NewEncoder(w).Encode(envelope.Accepted("c", "ProvisionTournamentRoom", nil, payload))
	}))
	defer srv.Close()

	p := NewHTTPRoomProvisioner(srv.URL, "", srv.Client())
	_, err := p.Provision(context.Background(), RoomProvisionRequest{
		TournamentID: "t1", RoundNumber: 1, SlotID: "slot_0", RoomID: "want-room",
		PlayerIDs: []domain.PlayerID{"p1"}, BatchID: "batch_0",
	})
	if !errors.Is(err, ErrRoomIDMismatch) {
		t.Fatalf("want ErrRoomIDMismatch got %v", err)
	}
}

func TestHTTPRoomProvisioner_AcceptsFirstSuccessFactsPlusRoomID(t *testing.T) {
	// Room first-success historically returned facts-only; Tournament requires roomId.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, _ := json.Marshal(map[string]any{
			"facts":  []string{"TournamentRoomProvisioned"},
			"roomId": "want-room",
		})
		_ = json.NewEncoder(w).Encode(envelope.Accepted("c", "ProvisionTournamentRoom", nil, payload))
	}))
	defer srv.Close()

	p := NewHTTPRoomProvisioner(srv.URL, "", srv.Client())
	res, err := p.Provision(context.Background(), RoomProvisionRequest{
		TournamentID: "t1", RoundNumber: 1, SlotID: "slot_0", RoomID: "want-room",
		PlayerIDs: []domain.PlayerID{"p1"}, BatchID: "batch_0",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.RoomID != "want-room" {
		t.Fatalf("roomId=%q", res.RoomID)
	}
}

func TestHTTPRoomProvisioner_RejectsFactsOnlyWithoutRoomID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, _ := json.Marshal(map[string]any{"facts": []string{"TournamentRoomProvisioned"}})
		_ = json.NewEncoder(w).Encode(envelope.Accepted("c", "ProvisionTournamentRoom", nil, payload))
	}))
	defer srv.Close()

	p := NewHTTPRoomProvisioner(srv.URL, "", srv.Client())
	_, err := p.Provision(context.Background(), RoomProvisionRequest{
		TournamentID: "t1", RoundNumber: 1, SlotID: "slot_0", RoomID: "want-room",
		PlayerIDs: []domain.PlayerID{"p1"}, BatchID: "batch_0",
	})
	if err == nil || !strings.Contains(err.Error(), "room provision invalid response") {
		t.Fatalf("want invalid response for facts-only body, got %v", err)
	}
}

func TestHTTPRoomProvisioner_4xxClassifiedWithoutBody(t *testing.T) {
	const secret = "leak-me-please"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, secret)
	}))
	defer srv.Close()
	p := NewHTTPRoomProvisioner(srv.URL, "", srv.Client())
	_, err := p.Provision(context.Background(), RoomProvisionRequest{
		TournamentID: "t1", RoundNumber: 1, SlotID: "slot_0", RoomID: "room-1",
		PlayerIDs: []domain.PlayerID{"p1"}, BatchID: "batch_0",
	})
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("got %v", err)
	}
	if !strings.Contains(err.Error(), "room provision HTTP 4xx") {
		t.Fatalf("got %q", err.Error())
	}
}
