package main

import (
	"encoding/json"
	"net/http"
	"testing"

	"unoarena/services/room-gameplay/app"
	svdomain "unoarena/services/spectator-view/domain"
	"unoarena/shared/envelope"
)

// Cross-service: Room publishes one spectator snapshot per accepted command;
// Spectator View projection applies contiguous room sequences from two player audiences.
func TestReadBridge_CanonicalSpectatorSnapshot_TwoAudiences(t *testing.T) {
	e := newTestEnv(t)
	h := e.auth()
	wireQuickWinDeals(e)

	roomID := "room_bridge"
	w := e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("c", "CreateRoom", nil, "host", "s", roomID, map[string]any{"roomId": roomID}), h)
	if decodeResult(t, w).Status != envelope.StatusAccepted {
		t.Fatalf("create: %s", w.Body.String())
	}
	w = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("j", "JoinRoom", seq(1), "guest", "s2", roomID, map[string]any{}), h)
	if decodeResult(t, w).Status != envelope.StatusAccepted {
		t.Fatalf("join: %s", w.Body.String())
	}
	w = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("l", "LockRoom", seq(2), "host", "s", roomID, map[string]any{}), h)
	if decodeResult(t, w).Status != envelope.StatusAccepted {
		t.Fatalf("lock: %s", w.Body.String())
	}
	w = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("st", "StartMatch", seq(3), "host", "s", roomID, map[string]any{"gameId": "g1"}), h)
	start := decodeResult(t, w)
	if start.Status != envelope.StatusAccepted {
		t.Fatalf("start: %+v", start)
	}
	w = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("p1", "PlayCard", seq(*start.Sequence), "host", "s", roomID, map[string]any{"cardId": "host-w"}), h)
	play := decodeResult(t, w)
	if play.Status != envelope.StatusAccepted {
		t.Fatalf("play: %+v", play)
	}

	var specEvents []app.PublishedEvent
	playerByAudience := map[string][]app.PublishedEvent{}
	for _, ev := range e.publisher.Events {
		switch ev.Stream {
		case app.StreamSpectator:
			specEvents = append(specEvents, ev)
		case app.StreamPlayer:
			playerByAudience[ev.PlayerID+"|"+ev.SessionID] = append(playerByAudience[ev.PlayerID+"|"+ev.SessionID], ev)
		}
	}
	if len(playerByAudience) < 2 {
		t.Fatalf("want 2 player audiences, got %d: %v", len(playerByAudience), keysOf(playerByAudience))
	}
	for aud, evs := range playerByAudience {
		var last int64
		for _, ev := range evs {
			if ev.SequenceNumber <= last {
				t.Fatalf("audience %s player seq not independent/increasing: %d after %d", aud, ev.SequenceNumber, last)
			}
			last = ev.SequenceNumber
		}
	}

	if len(specEvents) < 1 {
		t.Fatal("no spectator events")
	}
	if specEvents[0].SequenceNumber != 1 {
		t.Fatalf("first spectator sequence=%d want 1", specEvents[0].SequenceNumber)
	}

	proj := svdomain.NewSpectatorRoomProjection(svdomain.RoomID(roomID))
	var lastSeq int64
	sawGameCompleteSnap := false
	for i, ev := range specEvents {
		want := int64(i + 1)
		if ev.SequenceNumber != want {
			t.Fatalf("spectator seq[%d]=%d want %d type=%s", i, ev.SequenceNumber, want, ev.EventType)
		}
		if ev.SequenceNumber != lastSeq+1 && lastSeq != 0 {
			t.Fatalf("gap: %d after %d", ev.SequenceNumber, lastSeq)
		}
		lastSeq = ev.SequenceNumber

		var raw map[string]any
		if err := json.Unmarshal(ev.Payload, &raw); err != nil {
			t.Fatal(err)
		}
		data := spectatorSnapshotFromEnvelope(raw)
		for _, bad := range []string{"hand", "hands", "sessionId", "cardId", "privateHand", "deck"} {
			if _, ok := data[bad]; ok {
				t.Fatalf("spectator payload leaked %s: %v", bad, data)
			}
		}
		if s, _ := data["discardTop"].(string); stringsContainsIDLeak(s) {
			t.Fatalf("discardTop looks like card id: %q", s)
		}

		out := proj.Apply(svdomain.SpectatorSafeEvent{
			EventID:       svdomain.EventID(ev.EventID),
			EventType:     svdomain.EventType(ev.EventType),
			SchemaVersion: 1,
			RoomID:        svdomain.RoomID(roomID),
			Sequence:      svdomain.SequenceNumber(ev.SequenceNumber),
			Payload:       data,
		})
		if out.Rejection != nil {
			t.Fatalf("apply %s seq=%d: %+v", ev.EventType, ev.SequenceNumber, out.Rejection)
		}
		if proj.Sequence() != svdomain.SequenceNumber(ev.SequenceNumber) {
			t.Fatalf("projection sequence=%d want %d", proj.Sequence(), ev.SequenceNumber)
		}

		if ev.EventType == app.EventSnapshotSanitized {
			if gc, _ := data["gameCompleted"].(bool); gc {
				sawGameCompleteSnap = true
				if proj.Status() != svdomain.RoomStatusInProgress {
					t.Fatalf("GameCompleted snapshot must keep in_progress, got %s", proj.Status())
				}
				if !proj.Snapshot().GameCompleted {
					t.Fatal("projection gameCompleted flag unset")
				}
			}
		}
	}

	if !sawGameCompleteSnap {
		t.Fatal("expected SnapshotSanitized with gameCompleted after quick-win play")
	}

	// Drive match to terminal via second game win.
	w = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("n1", "StartNextGame", seq(*play.Sequence), "host", "s", roomID, map[string]any{"gameId": "g2"}), h)
	n1 := decodeResult(t, w)
	if n1.Status != envelope.StatusAccepted {
		t.Fatalf("next: %+v", n1)
	}
	w = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("p2", "PlayCard", seq(*n1.Sequence), "host", "s", roomID, map[string]any{"cardId": "host-w"}), h)
	done := decodeResult(t, w)
	if done.Status != envelope.StatusAccepted {
		t.Fatalf("play2: %+v", done)
	}

	// Re-collect spectator events after terminal and apply remaining.
	specEvents = nil
	for _, ev := range e.publisher.Events {
		if ev.Stream == app.StreamSpectator {
			specEvents = append(specEvents, ev)
		}
	}
	proj = svdomain.NewSpectatorRoomProjection(svdomain.RoomID(roomID))
	var terminal bool
	for i, ev := range specEvents {
		if ev.SequenceNumber != int64(i+1) {
			t.Fatalf("terminal path contiguous: seq[%d]=%d", i, ev.SequenceNumber)
		}
		var raw map[string]any
		_ = json.Unmarshal(ev.Payload, &raw)
		data := spectatorSnapshotFromEnvelope(raw)
		out := proj.Apply(svdomain.SpectatorSafeEvent{
			EventID:       svdomain.EventID(ev.EventID),
			EventType:     svdomain.EventType(ev.EventType),
			SchemaVersion: 1,
			RoomID:        svdomain.RoomID(roomID),
			Sequence:      svdomain.SequenceNumber(ev.SequenceNumber),
			Payload:       data,
		})
		if out.Rejection != nil {
			t.Fatalf("reapply %s: %+v", ev.EventType, out.Rejection)
		}
		if ev.EventType == app.EventRoomCompleted || ev.EventType == app.EventRoomCancelled {
			terminal = true
		}
	}
	if !terminal {
		t.Fatal("expected terminal RoomCompleted/RoomCancelled spectator event")
	}
	if proj.Status() != svdomain.RoomStatusCompleted && proj.Status() != svdomain.RoomStatusCancelled {
		t.Fatalf("terminal status=%s", proj.Status())
	}
	if !proj.StreamClosed() {
		t.Fatal("terminal must close spectator stream")
	}
	dec := proj.Admission(svdomain.SpectatorAuth{IsPublicRoom: true})
	if dec.Allowed {
		t.Fatal("admission must deny after terminal close")
	}
}

func spectatorSnapshotFromEnvelope(raw map[string]any) map[string]any {
	if raw == nil {
		return map[string]any{}
	}
	if payload, ok := raw["payload"].(map[string]any); ok {
		return payload
	}
	if data, ok := raw["data"].(map[string]any); ok {
		return data
	}
	return raw
}

func keysOf(m map[string][]app.PublishedEvent) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func stringsContainsIDLeak(s string) bool {
	// Quick-win deal uses card id "host-w"; public face must never be that id.
	return s == "host-w" || s == "disc-top"
}
