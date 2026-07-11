package domain

import (
	"strconv"
	"testing"
	"time"
)

func TestUnoWindow_ExpiresAtAndOpeningSequence(t *testing.T) {
	r := inProgressRoom(t, "uno-open", "host", "p2", "g1")
	now := testNow
	seqBefore := r.Sequence()

	out := r.OpenUnoWindow(OpenUnoWindowCommand{
		CommandID:             "uno-open-1",
		PlayerID:              "host",
		GameID:                "g1",
		TriggeringGameEventID: "evt-9",
		NowUTC:                now,
		ExpectedSequence:      seqBefore,
	})
	assertAcceptedFacts(t, out, FactUnoWindowOpened)

	expiresAt, ok := factData(out.Facts, FactUnoWindowOpened, "expiresAt")
	if !ok {
		t.Fatal("missing expiresAt")
	}
	parsed, err := time.Parse(time.RFC3339Nano, expiresAt)
	if err != nil {
		t.Fatal(err)
	}
	if !parsed.Equal(now.Add(UnoWindowDuration)) {
		t.Fatalf("expiresAt=%s want %s", parsed, now.Add(UnoWindowDuration))
	}
	opening, _ := factData(out.Facts, FactUnoWindowOpened, "openingSequence")
	if opening != strconv.FormatUint(uint64(seqBefore+1), 10) {
		t.Fatalf("openingSequence=%s want %d", opening, seqBefore+1)
	}

	w, ok := r.UnoWindow()
	if !ok || !w.IsOpen() || w.OpeningSequence != seqBefore+1 {
		t.Fatalf("window=%+v ok=%v", w, ok)
	}
	if !w.IsTimely(now.Add(UnoWindowDuration - time.Nanosecond)) {
		t.Fatal("should be timely before expiry")
	}
	if w.IsTimely(now.Add(UnoWindowDuration)) {
		t.Fatal("exact expiresAt must not be timely")
	}
}

func TestUnoWindow_ExactBoundaryExpiry(t *testing.T) {
	now := testNow

	t.Run("expire before deadline rejected", func(t *testing.T) {
		r := inProgressRoom(t, "uno-early", "host", "p2", "g1")
		_ = r.OpenUnoWindow(OpenUnoWindowCommand{
			CommandID: "o", PlayerID: "host", GameID: "g1",
			TriggeringGameEventID: "e1", NowUTC: now, ExpectedSequence: r.Sequence(),
		})
		w, _ := r.UnoWindow()
		out := r.ExpireUnoWindow(ExpireUnoWindowCommand{
			CommandID: "x", PlayerID: "host", GameID: "g1",
			TriggeringGameEventID: "e1",
			OpeningSequence:       w.OpeningSequence,
			NowUTC:                now.Add(UnoWindowDuration - time.Nanosecond),
			ExpectedSequence:      r.Sequence(),
		})
		assertRejected(t, out, RejectUnoWindowNotExpired)
		if w2, ok := r.UnoWindow(); !ok || !w2.IsOpen() {
			t.Fatal("window must remain open")
		}
	})

	t.Run("expire at exact deadline accepted", func(t *testing.T) {
		r := inProgressRoom(t, "uno-exact", "host", "p2", "g1")
		_ = r.OpenUnoWindow(OpenUnoWindowCommand{
			CommandID: "o", PlayerID: "host", GameID: "g1",
			TriggeringGameEventID: "e1", NowUTC: now, ExpectedSequence: r.Sequence(),
		})
		w, _ := r.UnoWindow()
		out := r.ExpireUnoWindow(ExpireUnoWindowCommand{
			CommandID: "x", PlayerID: "host", GameID: "g1",
			TriggeringGameEventID: "e1",
			OpeningSequence:       w.OpeningSequence,
			NowUTC:                now.Add(UnoWindowDuration),
			ExpectedSequence:      r.Sequence(),
		})
		assertAcceptedFacts(t, out, FactUnoWindowExpired)
		opening, _ := factData(out.Facts, FactUnoWindowExpired, "openingSequence")
		if opening != strconv.FormatUint(uint64(w.OpeningSequence), 10) {
			t.Fatalf("openingSequence=%s want %d", opening, w.OpeningSequence)
		}
		if _, ok := r.UnoWindow(); ok {
			t.Fatal("window should be cleared from room after expiry")
		}
	})

	t.Run("mismatch key rejected", func(t *testing.T) {
		r := inProgressRoom(t, "uno-mismatch", "host", "p2", "g1")
		_ = r.OpenUnoWindow(OpenUnoWindowCommand{
			CommandID: "o", PlayerID: "host", GameID: "g1",
			TriggeringGameEventID: "e1", NowUTC: now, ExpectedSequence: r.Sequence(),
		})
		w, _ := r.UnoWindow()
		out := r.ExpireUnoWindow(ExpireUnoWindowCommand{
			CommandID: "x", PlayerID: "host", GameID: "g1",
			TriggeringGameEventID: "other",
			OpeningSequence:       w.OpeningSequence,
			NowUTC:                now.Add(UnoWindowDuration),
			ExpectedSequence:      r.Sequence(),
		})
		assertRejected(t, out, RejectUnoWindowMismatch)
	})

	t.Run("stale opening sequence for superseded window rejected", func(t *testing.T) {
		r := inProgressRoom(t, "uno-stale-open", "host", "p2", "g1")
		_ = r.OpenUnoWindow(OpenUnoWindowCommand{
			CommandID: "o1", PlayerID: "host", GameID: "g1",
			TriggeringGameEventID: "e1", NowUTC: now, ExpectedSequence: r.Sequence(),
		})
		stale, _ := r.UnoWindow()
		_ = r.OpenUnoWindow(OpenUnoWindowCommand{
			CommandID: "o2", PlayerID: "host", GameID: "g1",
			TriggeringGameEventID: "e1", NowUTC: now.Add(time.Second), ExpectedSequence: r.Sequence(),
		})
		current, _ := r.UnoWindow()
		if current.OpeningSequence == stale.OpeningSequence {
			t.Fatal("expected superseded window to bump opening sequence")
		}
		out := r.ExpireUnoWindow(ExpireUnoWindowCommand{
			CommandID: "x-stale", PlayerID: "host", GameID: "g1",
			TriggeringGameEventID: "e1",
			OpeningSequence:       stale.OpeningSequence,
			NowUTC:                now.Add(time.Second + UnoWindowDuration),
			ExpectedSequence:      r.Sequence(),
		})
		assertRejected(t, out, RejectUnoWindowMismatch)
		if w, ok := r.UnoWindow(); !ok || !w.IsOpen() || w.OpeningSequence != current.OpeningSequence {
			t.Fatalf("current window must remain open: %+v ok=%v", w, ok)
		}
		out = r.ExpireUnoWindow(ExpireUnoWindowCommand{
			CommandID: "x-ok", PlayerID: "host", GameID: "g1",
			TriggeringGameEventID: "e1",
			OpeningSequence:       current.OpeningSequence,
			NowUTC:                now.Add(time.Second + UnoWindowDuration),
			ExpectedSequence:      r.Sequence(),
		})
		assertAcceptedFacts(t, out, FactUnoWindowExpired)
	})

	t.Run("duplicate expire command stable", func(t *testing.T) {
		r := inProgressRoom(t, "uno-dup", "host", "p2", "g1")
		_ = r.OpenUnoWindow(OpenUnoWindowCommand{
			CommandID: "o", PlayerID: "host", GameID: "g1",
			TriggeringGameEventID: "e1", NowUTC: now, ExpectedSequence: r.Sequence(),
		})
		w, _ := r.UnoWindow()
		first := r.ExpireUnoWindow(ExpireUnoWindowCommand{
			CommandID: "x-dup", PlayerID: "host", GameID: "g1",
			TriggeringGameEventID: "e1",
			OpeningSequence:       w.OpeningSequence,
			NowUTC:                now.Add(UnoWindowDuration),
			ExpectedSequence:      r.Sequence(),
		})
		assertAcceptedFacts(t, first, FactUnoWindowExpired)
		seq := r.Sequence()
		dup := r.ExpireUnoWindow(ExpireUnoWindowCommand{
			CommandID: "x-dup", PlayerID: "host", GameID: "g1",
			TriggeringGameEventID: "e1",
			OpeningSequence:       w.OpeningSequence,
			NowUTC:                now.Add(UnoWindowDuration),
			ExpectedSequence:      seq,
		})
		if dup.Kind != OutcomeDuplicate || r.Sequence() != seq {
			t.Fatalf("%+v", dup)
		}
	})
}

func TestUnoWindow_CloseByNextTurn(t *testing.T) {
	r := inProgressRoom(t, "uno-next", "host", "p2", "g1")
	now := testNow
	_ = r.OpenUnoWindow(OpenUnoWindowCommand{
		CommandID: "o", PlayerID: "host", GameID: "g1",
		TriggeringGameEventID: "e1", NowUTC: now, ExpectedSequence: r.Sequence(),
	})
	w, _ := r.UnoWindow()
	out := r.CloseUnoWindowByNextTurn(CloseUnoWindowByNextTurnCommand{
		CommandID:        "close-next",
		ExpectedSequence: r.Sequence(),
	})
	assertAcceptedFacts(t, out, FactUnoWindowClosed)
	reason, _ := factData(out.Facts, FactUnoWindowClosed, "reason")
	if reason != "next_turn_began" {
		t.Fatalf("reason=%s", reason)
	}
	if _, ok := r.UnoWindow(); ok {
		t.Fatal("window should be inactive after next-turn close")
	}

	// Expiry after close must fail.
	out = r.ExpireUnoWindow(ExpireUnoWindowCommand{
		CommandID: "x", PlayerID: "host", GameID: "g1",
		TriggeringGameEventID: "e1",
		OpeningSequence:       w.OpeningSequence,
		NowUTC:                now.Add(UnoWindowDuration),
		ExpectedSequence:      r.Sequence(),
	})
	assertRejected(t, out, RejectUnoWindowInactive)
}

func TestUnoWindow_ValueObjectTimeliness(t *testing.T) {
	now := testNow
	w := OpenUnoWindow("p", "g", "t", now, 42)
	if !w.ExpiresAt.Equal(now.Add(5 * time.Second)) {
		t.Fatalf("expiresAt=%s", w.ExpiresAt)
	}
	if w.OpeningSequence != 42 || !w.IsOpen() {
		t.Fatalf("%+v", w)
	}
	if !w.IsTimely(now.Add(5*time.Second - time.Nanosecond)) {
		t.Fatal("before boundary")
	}
	if w.IsTimely(now.Add(5 * time.Second)) {
		t.Fatal("at boundary closed")
	}
	closed := w.CloseByNextTurn()
	if closed.IsOpen() || closed.IsTimely(now) {
		t.Fatal("closed by next turn must not be open/timely")
	}
}
