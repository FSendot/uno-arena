package store

import (
	"strings"
	"testing"

	"unoarena/services/room-gameplay/app"
)

func TestSemanticJSONEqual_WhitespaceAndKeyOrderEquivalent(t *testing.T) {
	a := []byte(`{"roomId":"r1","maxSeats":2}`)
	b := []byte(`{ "maxSeats" : 2, "roomId" : "r1" }`)
	ok, err := semanticJSONEqual(a, b)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("semantically equivalent JSON must match")
	}
}

func TestSemanticJSONEqual_ChangedValueFails(t *testing.T) {
	a := []byte(`{"roomId":"r1","maxSeats":2}`)
	b := []byte(`{"roomId":"r1","maxSeats":3}`)
	ok, err := semanticJSONEqual(a, b)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("changed value must not match")
	}
}

func TestSemanticJSONEqual_InvalidJSONRejected(t *testing.T) {
	a := []byte(`{"roomId":"r1"}`)
	b := []byte(`{not-json`)
	_, err := semanticJSONEqual(a, b)
	if err == nil {
		t.Fatal("invalid JSON must be rejected")
	}
	if !strings.Contains(err.Error(), "json") && !strings.Contains(err.Error(), "JSON") {
		t.Fatalf("error should mention JSON: %v", err)
	}
}

func TestAssertReplayIdentity_SemanticPayloadPass(t *testing.T) {
	blob := app.ReconciliationRepairBlob{
		CommandType: "CreateRoom",
		GIPayload:   []byte(`{"roomId":"r1","maxSeats":2}`),
	}
	entry := app.ReplayEntry{
		EventType: "CreateRoom",
		Payload:   []byte(`{ "maxSeats": 2, "roomId": "r1" }`),
	}
	if err := assertReplayIdentity(blob, entry); err != nil {
		t.Fatal(err)
	}
}

func TestAssertReplayIdentity_SemanticPayloadFail(t *testing.T) {
	blob := app.ReconciliationRepairBlob{
		CommandType: "CreateRoom",
		GIPayload:   []byte(`{"roomId":"r1","maxSeats":2}`),
	}
	entry := app.ReplayEntry{
		EventType: "CreateRoom",
		Payload:   []byte(`{"roomId":"r1","maxSeats":9}`),
	}
	if err := assertReplayIdentity(blob, entry); err == nil {
		t.Fatal("expected payload mismatch")
	}
}

func TestAssertReplayIdentity_InvalidPayloadRejected(t *testing.T) {
	blob := app.ReconciliationRepairBlob{
		GIPayload: []byte(`{"ok":true}`),
	}
	entry := app.ReplayEntry{
		Payload: []byte(`[`),
	}
	if err := assertReplayIdentity(blob, entry); err == nil {
		t.Fatal("invalid replay payload must fail closed")
	}
}
