package domain

import (
	"testing"
	"time"
)

func TestDisconnect_ExactBoundaryAndVersion(t *testing.T) {
	r := inProgressRoom(t, "disc-bound", "host", "p2", "g1")
	now := testNow

	out := r.DisconnectPlayer(DisconnectPlayerCommand{
		CommandID:        "disc-1",
		PlayerID:         "p2",
		NowUTC:           now,
		ExpectedSequence: r.Sequence(),
	})
	assertAcceptedFacts(t, out, FactPlayerDisconnected)
	deadlineStr, ok := factData(out.Facts, FactPlayerDisconnected, "deadlineUTC")
	if !ok {
		t.Fatal("missing deadlineUTC")
	}
	deadline, err := time.Parse(time.RFC3339Nano, deadlineStr)
	if err != nil {
		t.Fatal(err)
	}
	wantDeadline := now.Add(ReconnectWindowDuration)
	if !deadline.Equal(wantDeadline) {
		t.Fatalf("deadline=%s want %s", deadline, wantDeadline)
	}
	verStr, _ := factData(out.Facts, FactPlayerDisconnected, "disconnectVersion")
	if verStr != "1" {
		t.Fatalf("version=%s", verStr)
	}

	state, ok := r.DisconnectState("p2")
	if !ok || state.DisconnectVersion != 1 {
		t.Fatalf("state=%+v ok=%v", state, ok)
	}

	t.Run("reconnect just before deadline", func(t *testing.T) {
		r2 := inProgressRoom(t, "disc-before", "host", "p2", "g1")
		_ = r2.DisconnectPlayer(DisconnectPlayerCommand{
			CommandID: "d", PlayerID: "p2", NowUTC: now, ExpectedSequence: r2.Sequence(),
		})
		out := r2.ReconnectPlayer(ReconnectPlayerCommand{
			CommandID:         "re-before",
			PlayerID:          "p2",
			DisconnectVersion: 1,
			NowUTC:            now.Add(ReconnectWindowDuration - time.Nanosecond),
			ExpectedSequence:  r2.Sequence(),
		})
		assertAcceptedFacts(t, out, FactPlayerReconnected)
	})

	t.Run("reconnect at exact deadline denied", func(t *testing.T) {
		r2 := inProgressRoom(t, "disc-exact", "host", "p2", "g1")
		_ = r2.DisconnectPlayer(DisconnectPlayerCommand{
			CommandID: "d", PlayerID: "p2", NowUTC: now, ExpectedSequence: r2.Sequence(),
		})
		out := r2.ReconnectPlayer(ReconnectPlayerCommand{
			CommandID:         "re-exact",
			PlayerID:          "p2",
			DisconnectVersion: 1,
			NowUTC:            now.Add(ReconnectWindowDuration),
			ExpectedSequence:  r2.Sequence(),
		})
		assertRejected(t, out, RejectReconnectExpired)
	})

	t.Run("forfeit before deadline denied", func(t *testing.T) {
		r2 := inProgressRoom(t, "forfeit-early", "host", "p2", "g1")
		_ = r2.DisconnectPlayer(DisconnectPlayerCommand{
			CommandID: "d", PlayerID: "p2", NowUTC: now, ExpectedSequence: r2.Sequence(),
		})
		out := r2.applyForfeitPlayer(ForfeitPlayerCommand{
			CommandID:         "f-early",
			PlayerID:          "p2",
			DisconnectVersion: 1,
			NowUTC:            now.Add(ReconnectWindowDuration - time.Nanosecond),
			ExpectedSequence:  r2.Sequence(),
		})
		assertRejected(t, out, RejectForfeitNotDue)
	})

	t.Run("forfeit at exact deadline accepted", func(t *testing.T) {
		r2 := inProgressRoom(t, "forfeit-exact", "host", "p2", "g1")
		hostBefore := r2.HostID()
		_ = r2.DisconnectPlayer(DisconnectPlayerCommand{
			CommandID: "d", PlayerID: "p2", NowUTC: now, ExpectedSequence: r2.Sequence(),
		})
		out := r2.applyForfeitPlayer(ForfeitPlayerCommand{
			CommandID:         "f-exact",
			PlayerID:          "p2",
			DisconnectVersion: 1,
			NowUTC:            now.Add(ReconnectWindowDuration),
			ExpectedSequence:  r2.Sequence(),
		})
		assertAcceptedFacts(t, out, FactPlayerForfeited)
		if _, active := r2.DisconnectState("p2"); active {
			t.Fatal("disconnect must clear after forfeit")
		}
		if r2.Roster().IsSeated("p2") {
			t.Fatal("forfeited player must leave occupied roster")
		}
		if r2.Status() != RoomStatusInProgress {
			t.Fatalf("casual room must continue, status=%s", r2.Status())
		}
		if r2.HostID() != hostBefore || HasFact(out.Facts, FactHostReassigned) {
			t.Fatal("forfeit after start must not reassign host")
		}
		if rt, _ := factData(out.Facts, FactPlayerForfeited, "roomType"); rt != string(RoomTypeAdHoc) {
			t.Fatalf("roomType=%s", rt)
		}
	})

	t.Run("stale disconnect version cannot forfeit newer episode", func(t *testing.T) {
		r2 := inProgressRoom(t, "forfeit-ver", "host", "p2", "g1")
		_ = r2.DisconnectPlayer(DisconnectPlayerCommand{
			CommandID: "d1", PlayerID: "p2", NowUTC: now, ExpectedSequence: r2.Sequence(),
		})
		// Reconnect then disconnect again -> version 2
		_ = r2.ReconnectPlayer(ReconnectPlayerCommand{
			CommandID: "re", PlayerID: "p2", DisconnectVersion: 1,
			NowUTC: now.Add(time.Second), ExpectedSequence: r2.Sequence(),
		})
		_ = r2.DisconnectPlayer(DisconnectPlayerCommand{
			CommandID: "d2", PlayerID: "p2", NowUTC: now.Add(2 * time.Second), ExpectedSequence: r2.Sequence(),
		})
		out := r2.applyForfeitPlayer(ForfeitPlayerCommand{
			CommandID:         "f-stale",
			PlayerID:          "p2",
			DisconnectVersion: 1, // stale
			NowUTC:            now.Add(2*time.Second + ReconnectWindowDuration),
			ExpectedSequence:  r2.Sequence(),
		})
		assertRejected(t, out, RejectDisconnectVersion)
		// Correct version still works.
		out = r2.applyForfeitPlayer(ForfeitPlayerCommand{
			CommandID:         "f-ok",
			PlayerID:          "p2",
			DisconnectVersion: 2,
			NowUTC:            now.Add(2*time.Second + ReconnectWindowDuration),
			ExpectedSequence:  r2.Sequence(),
		})
		assertAcceptedFacts(t, out, FactPlayerForfeited)
	})

	t.Run("duplicate forfeit command id stable", func(t *testing.T) {
		r2 := inProgressRoom(t, "forfeit-dup", "host", "p2", "g1")
		_ = r2.DisconnectPlayer(DisconnectPlayerCommand{
			CommandID: "d", PlayerID: "p2", NowUTC: now, ExpectedSequence: r2.Sequence(),
		})
		first := r2.applyForfeitPlayer(ForfeitPlayerCommand{
			CommandID: "f-dup", PlayerID: "p2", DisconnectVersion: 1,
			NowUTC: now.Add(ReconnectWindowDuration), ExpectedSequence: r2.Sequence(),
		})
		assertAcceptedFacts(t, first, FactPlayerForfeited)
		seq := r2.Sequence()
		dup := r2.applyForfeitPlayer(ForfeitPlayerCommand{
			CommandID: "f-dup", PlayerID: "p2", DisconnectVersion: 1,
			NowUTC: now.Add(ReconnectWindowDuration), ExpectedSequence: seq,
		})
		if dup.Kind != OutcomeDuplicate || r2.Sequence() != seq {
			t.Fatalf("%+v seq=%d", dup, r2.Sequence())
		}
	})

	t.Run("host forfeit after start clears seat without host reassignment", func(t *testing.T) {
		r2 := inProgressRoom(t, "forfeit-host", "host", "p2", "g1")
		_ = r2.DisconnectPlayer(DisconnectPlayerCommand{
			CommandID: "d", PlayerID: "host", NowUTC: now, ExpectedSequence: r2.Sequence(),
		})
		out := r2.applyForfeitPlayer(ForfeitPlayerCommand{
			CommandID: "f-host", PlayerID: "host", DisconnectVersion: 1,
			NowUTC: now.Add(ReconnectWindowDuration), ExpectedSequence: r2.Sequence(),
		})
		assertAcceptedFacts(t, out, FactPlayerForfeited)
		if r2.Roster().IsSeated("host") {
			t.Fatal("forfeited host must leave roster")
		}
		if r2.HostID() != "host" || HasFact(out.Facts, FactHostReassigned) {
			t.Fatal("host label must stay; no reassignment after start")
		}
		if r2.Status() != RoomStatusInProgress || !r2.Roster().IsSeated("p2") {
			t.Fatal("casual match continues with remaining player")
		}
	})
}

func TestForfeitPlayer_TournamentFactContext(t *testing.T) {
	r, out := ProvisionTournamentRoom(ProvisionTournamentRoomCommand{
		CommandID: "prov-ff", RoomID: "t-ff", TournamentID: "tourn-ff", HostID: "seed-a",
	})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("%+v", out)
	}
	mustJoin(t, r, "j-b", "seed-b")
	mustLock(t, r, "", true)
	mustStart(t, r, "", "tg-ff", true)
	now := testNow
	_ = r.DisconnectPlayer(DisconnectPlayerCommand{
		CommandID: "d", PlayerID: "seed-b", NowUTC: now, ExpectedSequence: r.Sequence(),
	})
	out = r.applyForfeitPlayer(ForfeitPlayerCommand{
		CommandID: "f", PlayerID: "seed-b", DisconnectVersion: 1,
		NowUTC: now.Add(ReconnectWindowDuration), ExpectedSequence: r.Sequence(),
	})
	assertAcceptedFacts(t, out, FactPlayerForfeited)
	if rt, _ := factData(out.Facts, FactPlayerForfeited, "roomType"); rt != string(RoomTypeTournament) {
		t.Fatalf("roomType=%s", rt)
	}
	if tid, _ := factData(out.Facts, FactPlayerForfeited, "tournamentId"); tid != "tourn-ff" {
		t.Fatalf("tournamentId=%s", tid)
	}
	if r.Roster().IsSeated("seed-b") {
		t.Fatal("forfeited tournament player must leave roster")
	}
}

func TestDisconnectState_ValueObjectBoundaries(t *testing.T) {
	now := testNow
	d := BeginDisconnect("p1", 3, now)
	if !d.DeadlineUTC.Equal(now.Add(ReconnectWindowDuration)) {
		t.Fatalf("deadline=%s", d.DeadlineUTC)
	}
	ok, _ := d.CanReconnect("p1", 3, now.Add(ReconnectWindowDuration-time.Nanosecond))
	if !ok {
		t.Fatal("expected reconnect before deadline")
	}
	ok, code := d.CanReconnect("p1", 3, now.Add(ReconnectWindowDuration))
	if ok || code != RejectReconnectExpired {
		t.Fatalf("exact deadline reconnect ok=%v code=%s", ok, code)
	}
	ok, code = d.CanForfeit("p1", 3, now.Add(ReconnectWindowDuration-time.Nanosecond))
	if ok || code != RejectForfeitNotDue {
		t.Fatalf("early forfeit ok=%v code=%s", ok, code)
	}
	ok, _ = d.CanForfeit("p1", 3, now.Add(ReconnectWindowDuration))
	if !ok {
		t.Fatal("expected forfeit at deadline")
	}
	ok, code = d.CanForfeit("p1", 2, now.Add(ReconnectWindowDuration))
	if ok || code != RejectDisconnectVersion {
		t.Fatalf("version mismatch ok=%v code=%s", ok, code)
	}
}
