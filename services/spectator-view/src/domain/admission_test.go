package domain

import "testing"

func TestAdmission_StatusAndAuth_Table(t *testing.T) {
	t.Parallel()
	roster := map[PlayerID]struct{}{"p1": {}}
	cases := []struct {
		name     string
		status   RoomStatus
		auth     SpectatorAuth
		wantOK   bool
		wantCode RejectionCode
	}{
		{"waiting public", RoomStatusWaiting, SpectatorAuth{IsPublicRoom: true}, true, ""},
		{"locked public", RoomStatusLocked, SpectatorAuth{IsPublicRoom: true}, true, ""},
		{"in_progress public", RoomStatusInProgress, SpectatorAuth{IsPublicRoom: true}, true, ""},
		{"completed denied", RoomStatusCompleted, SpectatorAuth{IsPublicRoom: true, IsOperator: true}, false, RejectSpectatorTerminal},
		{"cancelled denied", RoomStatusCancelled, SpectatorAuth{IsPublicRoom: true, HasInvite: true}, false, RejectSpectatorTerminal},
		{"private no scope", RoomStatusWaiting, SpectatorAuth{IsPublicRoom: false}, false, RejectSpectatorUnauthorized},
		{"private participant", RoomStatusWaiting, SpectatorAuth{IsPublicRoom: false, PlayerID: "p1", SessionID: "s1"}, true, ""},
		{"private participant missing session", RoomStatusWaiting, SpectatorAuth{IsPublicRoom: false, PlayerID: "p1"}, false, RejectSpectatorUnauthorized},
		{"private non-roster", RoomStatusWaiting, SpectatorAuth{IsPublicRoom: false, PlayerID: "p9", SessionID: "s1"}, false, RejectSpectatorUnauthorized},
		{"private invite", RoomStatusWaiting, SpectatorAuth{IsPublicRoom: false, HasInvite: true}, true, ""},
		{"private operator", RoomStatusWaiting, SpectatorAuth{IsPublicRoom: false, IsOperator: true}, true, ""},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dec := EvaluateSpectatorAdmission(tt.status, tt.auth, roster)
			if dec.Allowed != tt.wantOK || dec.Code != tt.wantCode {
				t.Fatalf("admission=%+v want ok=%v code=%q", dec, tt.wantOK, tt.wantCode)
			}
		})
	}
}

func TestAdmission_GameVsMatchTerminalDistinction(t *testing.T) {
	t.Parallel()
	p := NewSpectatorRoomProjection("room_1")

	mustApply(t, p, evt(1, "a1", EventRoomCreated, map[string]any{
		"visibility": "public",
		"seats": []any{
			map[string]any{"seatIndex": 0, "playerId": "p1", "displayName": "Alice", "cardCount": 0},
			map[string]any{"seatIndex": 1, "playerId": "p2", "displayName": "Bob", "cardCount": 0},
		},
	}))
	mustApply(t, p, evt(2, "a2", EventMatchStarted, map[string]any{
		"status": "in_progress",
		"seats": []any{
			map[string]any{"seatIndex": 0, "playerId": "p1", "displayName": "Alice", "cardCount": 7},
			map[string]any{"seatIndex": 1, "playerId": "p2", "displayName": "Bob", "cardCount": 7},
		},
		"discardTop":      "red-7",
		"activeColor":     "red",
		"direction":       "clockwise",
		"currentPlayerId": "p1",
	}))

	dec := p.Admission(SpectatorAuth{})
	if !dec.Allowed {
		t.Fatalf("in_progress must allow admission: %+v", dec)
	}

	out := mustApply(t, p, evt(3, "a3", EventGameCompleted, map[string]any{
		"gameScore":      map[string]any{"p1": 1, "p2": 0},
		"winnerPlayerId": "p1",
	}))
	if hasFact(out.Facts, FactStreamClose) {
		t.Fatal("GameCompleted must not emit StreamClose")
	}
	if p.Status() != RoomStatusInProgress {
		t.Fatalf("status=%s want in_progress after GameCompleted", p.Status())
	}
	dec = p.Admission(SpectatorAuth{})
	if !dec.Allowed {
		t.Fatalf("after GameCompleted admission must remain allowed: %+v", dec)
	}
	snap := p.Snapshot()
	if !snap.GameCompleted || snap.MatchCompleted || snap.StreamClosed {
		t.Fatalf("snap after game: %+v", snap)
	}
	if snap.GameScore["p1"] != 1 {
		t.Fatalf("score=%v", snap.GameScore)
	}

	out = mustApply(t, p, evt(4, "a4", EventRoomCompleted, map[string]any{
		"matchWinner": "p1",
		"gameScore":   map[string]any{"p1": 2, "p2": 1},
	}))
	if !hasFact(out.Facts, FactStreamClose) {
		t.Fatal("RoomCompleted must emit StreamClose")
	}
	assertHasFact(t, out.Facts, FactSpectatorRoomProjectionUpdated)
	if p.Status() != RoomStatusCompleted || !p.StreamClosed() {
		t.Fatalf("status=%s closed=%v", p.Status(), p.StreamClosed())
	}
	dec = p.Admission(SpectatorAuth{IsOperator: true})
	if dec.Allowed || dec.Code != RejectSpectatorTerminal {
		t.Fatalf("after RoomCompleted admission must deny: %+v", dec)
	}
	if p.Snapshot().MatchWinner != "p1" {
		t.Fatalf("winner=%s", p.Snapshot().MatchWinner)
	}
}

func TestAdmission_MatchCompletedMapsTerminal(t *testing.T) {
	t.Parallel()
	p := seededInProgress(t)
	out := mustApply(t, p, evt(3, "mc1", EventMatchCompleted, map[string]any{
		"matchWinner": "p1",
		"matchWins":   map[string]any{"p1": 2, "p2": 1},
		"isAbandoned": false,
	}))
	if !hasFact(out.Facts, FactStreamClose) {
		t.Fatal("MatchCompleted must emit StreamClose")
	}
	if p.Status() != RoomStatusCompleted || !p.StreamClosed() {
		t.Fatalf("status=%s closed=%v", p.Status(), p.StreamClosed())
	}
	snap := p.Snapshot()
	if !snap.MatchCompleted || snap.MatchWinner != "p1" {
		t.Fatalf("snap=%+v", snap)
	}
	// Private fields like forfeits/cardPoints must not appear on snapshot JSON.
	raw, err := p.SnapshotJSON()
	if err != nil {
		t.Fatal(err)
	}
	s := string(raw)
	for _, bad := range []string{"forfeit", "cardPoints", "hand", "deck"} {
		if containsFold(s, bad) {
			t.Fatalf("leaked %q in %s", bad, s)
		}
	}
}

func TestAdmission_ProjectionStatusAndVisibility(t *testing.T) {
	t.Parallel()
	p := NewSpectatorRoomProjection("room_1")
	mustApply(t, p, evt(1, "evt_1", EventRoomCreated, map[string]any{"visibility": "public"}))
	if dec := p.Admission(SpectatorAuth{}); !dec.Allowed {
		t.Fatalf("waiting public: %+v", dec)
	}
	mustApply(t, p, evt(2, "evt_2", EventRoomLocked, map[string]any{}))
	if dec := p.Admission(SpectatorAuth{}); !dec.Allowed {
		t.Fatalf("locked: %+v", dec)
	}
	mustApply(t, p, evt(3, "evt_3", EventMatchStarted, map[string]any{
		"discardTop": "r0", "activeColor": "red", "direction": "clockwise",
	}))
	if dec := p.Admission(SpectatorAuth{}); !dec.Allowed {
		t.Fatalf("in_progress: %+v", dec)
	}

	priv := NewSpectatorRoomProjection("room_priv")
	mustApply(t, priv, evt(1, "p1", EventRoomCreated, map[string]any{
		"visibility": "private",
		"seats": []any{
			map[string]any{"seatIndex": 0, "playerId": "p1", "displayName": "Alice", "cardCount": 0},
		},
	}))
	dec := priv.Admission(SpectatorAuth{})
	if dec.Allowed || dec.Code != RejectSpectatorUnauthorized {
		t.Fatalf("private unauthorized: %+v", dec)
	}
	dec = priv.Admission(SpectatorAuth{PlayerID: "p1", SessionID: "sess_1"})
	if !dec.Allowed {
		t.Fatalf("private participant: %+v", dec)
	}
}

func TestAdmission_CancelledEmitsStreamClose(t *testing.T) {
	t.Parallel()
	p := NewSpectatorRoomProjection("room_c")
	mustApply(t, p, evt(1, "c1", EventRoomCreated, map[string]any{"visibility": "public"}))
	out := mustApply(t, p, evt(2, "c2", EventRoomCancelled, map[string]any{}))
	if !hasFact(out.Facts, FactStreamClose) {
		t.Fatal("RoomCancelled must emit StreamClose")
	}
	dec := p.Admission(SpectatorAuth{IsOperator: true})
	if dec.Allowed || dec.Code != RejectSpectatorTerminal {
		t.Fatalf("cancelled admission: %+v", dec)
	}
}

func TestApplyBatch_AtomicSameSequence(t *testing.T) {
	t.Parallel()
	p := seededWaiting(t)
	out := p.ApplyBatch([]SpectatorSafeEvent{
		evt(2, "b1", EventCardPlayed, map[string]any{"discardTop": "blue-3", "activeColor": "blue"}),
		evt(2, "b2", EventTurnAdvanced, map[string]any{"currentPlayerId": "p2"}),
	})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("batch=%+v", out)
	}
	if p.Sequence() != 2 {
		t.Fatalf("sequence=%d want 1 bump", p.Sequence())
	}
	snap := p.Snapshot()
	if snap.Discard.DiscardTop != "blue-3" || snap.CurrentPlayerID != "p2" {
		t.Fatalf("snap=%+v", snap)
	}
	if !hasFact(out.Facts, FactSpectatorRoomProjectionUpdated) {
		t.Fatal("expected one projection update")
	}
}

func TestRejectNestedEnvelope(t *testing.T) {
	t.Parallel()
	p := NewSpectatorRoomProjection("room_nest")
	out := p.Apply(SpectatorSafeEvent{
		EventID: "n1", EventType: EventRoomCreated, SchemaVersion: 1, RoomID: "room_nest", Sequence: 1,
		Payload: map[string]any{
			"roomId":         "room_nest",
			"eventType":      "RoomCreated",
			"sequenceNumber": 1,
			"payload":        map[string]any{"visibility": "public"},
		},
	})
	if out.Kind != OutcomeDropped && out.Kind != OutcomeQuarantined {
		t.Fatalf("nested envelope must be rejected: %+v", out)
	}
}

func containsFold(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 ||
		(func() bool {
			for i := 0; i+len(sub) <= len(s); i++ {
				ok := true
				for j := 0; j < len(sub); j++ {
					a, b := s[i+j], sub[j]
					if a >= 'A' && a <= 'Z' {
						a += 'a' - 'A'
					}
					if b >= 'A' && b <= 'Z' {
						b += 'a' - 'A'
					}
					if a != b {
						ok = false
						break
					}
				}
				if ok {
					return true
				}
			}
			return false
		})())
}
