package domain

import (
	"bytes"
	"sync"
	"testing"
)

func mustLog(t *testing.T) *GameLog {
	t.Helper()
	g, err := NewGameLog("room-1")
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func TestGameLog_AppendExpectedRevisionAndGapless(t *testing.T) {
	g := mustLog(t)
	if g.Revision() != 0 {
		t.Fatalf("revision=%d", g.Revision())
	}

	a := g.Append(AppendCommand{
		EventID: "e1", EventType: "CardPlayed", Payload: []byte(`{"n":1}`), ExpectedRevision: 0,
	})
	if !a.Accepted() || a.Offset != 0 || a.Revision != 1 {
		t.Fatalf("%+v", a)
	}
	if a.Facts[0].Name != FactGameLogEntryAppended {
		t.Fatalf("fact=%s", a.Facts[0].Name)
	}

	stale := g.Append(AppendCommand{
		EventID: "e2", EventType: "CardPlayed", Payload: []byte(`{"n":2}`), ExpectedRevision: 0,
	})
	if !stale.Rejected() || stale.Rejection.Code != RejectRevisionMismatch {
		t.Fatalf("%+v", stale)
	}
	if g.Revision() != 1 || g.Len() != 1 {
		t.Fatal("stale append mutated log")
	}

	b := g.Append(AppendCommand{
		EventID: "e2", EventType: "CardPlayed", Payload: []byte(`{"n":2}`), ExpectedRevision: 1,
	})
	if !b.Accepted() || b.Offset != 1 || b.Revision != 2 {
		t.Fatalf("%+v", b)
	}

	entries, rej := g.ReplayFrom(0)
	if rej != nil || len(entries) != 2 {
		t.Fatalf("replay: len=%d rej=%v", len(entries), rej)
	}
	if entries[0].Offset != 0 || entries[1].Offset != 1 {
		t.Fatalf("offsets not gapless: %d %d", entries[0].Offset, entries[1].Offset)
	}
}

func TestGameLog_EventIDIdempotencyAndConflict(t *testing.T) {
	g := mustLog(t)
	payload := []byte(`{"card":"r5"}`)
	first := g.Append(AppendCommand{
		EventID: "evt-a", EventType: "CardPlayed", Payload: payload, ExpectedRevision: 0,
	})
	if !first.Accepted() {
		t.Fatalf("%+v", first)
	}
	rev := g.Revision()

	dup := g.Append(AppendCommand{
		EventID: "evt-a", EventType: "CardPlayed", Payload: payload, ExpectedRevision: 99, // revision ignored on idempotent hit
	})
	if dup.Kind != OutcomeDuplicate || !dup.Accepted() {
		t.Fatalf("%+v", dup)
	}
	if dup.Offset != first.Offset || dup.Revision != first.Revision {
		t.Fatalf("dup meta %+v vs %+v", dup, first)
	}
	if g.Revision() != rev || g.Len() != 1 {
		t.Fatal("duplicate append mutated log")
	}

	conflict := g.Append(AppendCommand{
		EventID: "evt-a", EventType: "CardPlayed", Payload: []byte(`{"card":"OTHER"}`), ExpectedRevision: rev,
	})
	if !conflict.Rejected() || conflict.Rejection.Code != RejectConflictingDuplicate {
		t.Fatalf("%+v", conflict)
	}
	if g.Revision() != rev {
		t.Fatal("conflict mutated revision")
	}
}

func TestGameLog_ConcurrentSerializedExpectedRevision(t *testing.T) {
	g := mustLog(t)
	const n = 32
	var wg sync.WaitGroup
	results := make([]CommandOutcome, n)
	expected := g.Revision()
	type attempt struct {
		i   int
		cmd AppendCommand
	}
	attempts := make([]attempt, n)
	for i := 0; i < n; i++ {
		attempts[i] = attempt{i: i, cmd: AppendCommand{
			EventID:          EventID("race-" + itoa(i)),
			EventType:        "TurnAdvanced",
			Payload:          []byte{byte(i)},
			ExpectedRevision: expected,
		}}
	}

	var mu sync.Mutex
	wg.Add(n)
	for _, a := range attempts {
		a := a
		go func() {
			defer wg.Done()
			mu.Lock()
			defer mu.Unlock()
			results[a.i] = g.Append(a.cmd)
		}()
	}
	wg.Wait()

	var accepted int
	for _, out := range results {
		if out.Kind == OutcomeAccepted {
			accepted++
		} else if out.Rejection == nil || out.Rejection.Code != RejectRevisionMismatch {
			t.Fatalf("unexpected outcome %+v", out)
		}
	}
	if accepted != 1 {
		t.Fatalf("accepted=%d want 1 under shared expectedRevision", accepted)
	}
	if g.Revision() != 1 || g.Len() != 1 {
		t.Fatalf("revision=%d len=%d", g.Revision(), g.Len())
	}
}

func TestGameLog_ReplayAndExportDefensiveImmutability(t *testing.T) {
	g := mustLog(t)
	for i, rev := range []Revision{0, 1, 2} {
		out := g.Append(AppendCommand{
			EventID: EventID("e-" + itoa(i)), EventType: "X", Payload: []byte{byte(i)}, ExpectedRevision: rev,
		})
		if !out.Accepted() {
			t.Fatalf("%+v", out)
		}
	}

	replay, rej := g.ReplayFrom(1)
	if rej != nil || len(replay) != 2 {
		t.Fatalf("len=%d rej=%v", len(replay), rej)
	}
	if replay[0].Offset != 1 || replay[1].Offset != 2 {
		t.Fatalf("offsets %d %d", replay[0].Offset, replay[1].Offset)
	}
	replay[0].Payload[0] = 0xff
	replay[0].EventType = "MUTATED"
	alive, _ := g.ReplayFrom(1)
	if alive[0].Payload[0] == 0xff || alive[0].EventType == "MUTATED" {
		t.Fatal("replay entries must be defensive copies")
	}

	export := g.ExportStateInput()
	if export.RoomID != "room-1" || export.Revision != 3 || len(export.Entries) != 3 {
		t.Fatalf("%+v", export)
	}
	export.Entries[0].Payload[0] = 0xaa
	again := g.ExportStateInput()
	if again.Entries[0].Payload[0] == 0xaa {
		t.Fatal("export must be defensive")
	}
	if !bytes.Equal(again.Entries[0].Payload, []byte{0}) {
		t.Fatalf("payload=%v", again.Entries[0].Payload)
	}

	past, rej := g.ReplayFrom(99)
	if past != nil || rej == nil || rej.Code != RejectInvalidOffset {
		t.Fatalf("past offset: %v %v", past, rej)
	}
}

func TestRestoreGameLog_ValidatesAndRebuildsIdempotency(t *testing.T) {
	entries := []GameLogEntry{
		{Offset: 0, EventID: "e1", EventType: "CardPlayed", GameID: "g1", Payload: []byte(`{"n":1}`)},
		{Offset: 1, EventID: "e2", EventType: "CardPlayed", GameID: "g1", Payload: []byte(`{"n":2}`)},
	}
	g, err := RestoreGameLog("room-restore-1", entries)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if g.RoomID() != "room-restore-1" || g.Revision() != 2 || g.Len() != 2 {
		t.Fatalf("room=%s rev=%d len=%d", g.RoomID(), g.Revision(), g.Len())
	}

	dup := g.Append(AppendCommand{
		EventID: "e1", EventType: "CardPlayed", GameID: "g1", Payload: []byte(`{"n":1}`), ExpectedRevision: 99,
	})
	if dup.Kind != OutcomeDuplicate || dup.Offset != 0 || dup.Revision != 1 {
		t.Fatalf("idempotency not restored: %+v", dup)
	}
	if g.Revision() != 2 {
		t.Fatal("duplicate mutated revision")
	}

	conflict := g.Append(AppendCommand{
		EventID: "e1", EventType: "CardPlayed", GameID: "g1", Payload: []byte(`{"n":OTHER}`), ExpectedRevision: 2,
	})
	if !conflict.Rejected() || conflict.Rejection.Code != RejectConflictingDuplicate {
		t.Fatalf("conflict: %+v", conflict)
	}

	next := g.Append(AppendCommand{
		EventID: "e3", EventType: "TurnAdvanced", GameID: "g1", Payload: []byte(`{}`), ExpectedRevision: 2,
	})
	if !next.Accepted() || next.Offset != 2 || next.Revision != 3 {
		t.Fatalf("next: %+v", next)
	}
}

func TestRestoreGameLog_RejectsInvalidInputs(t *testing.T) {
	if _, err := RestoreGameLog("", nil); err == nil {
		t.Fatal("empty roomId must fail")
	}
	if _, err := RestoreGameLog("room-1", []GameLogEntry{
		{Offset: 1, EventID: "e1", EventType: "X", Payload: []byte("a")},
	}); err == nil {
		t.Fatal("non-zero first offset must fail")
	}
	if _, err := RestoreGameLog("room-1", []GameLogEntry{
		{Offset: 0, EventID: "e1", EventType: "X", Payload: []byte("a")},
		{Offset: 2, EventID: "e2", EventType: "X", Payload: []byte("b")},
	}); err == nil {
		t.Fatal("gap must fail")
	}
	if _, err := RestoreGameLog("room-1", []GameLogEntry{
		{Offset: 0, EventID: "e1", EventType: "X", Payload: []byte("a")},
		{Offset: 1, EventID: "e1", EventType: "X", Payload: []byte("b")},
	}); err == nil {
		t.Fatal("duplicate eventId must fail")
	}
	if _, err := RestoreGameLog("room-1", []GameLogEntry{
		{Offset: 0, EventID: "", EventType: "X", Payload: []byte("a")},
	}); err == nil {
		t.Fatal("empty eventId must fail")
	}
}
