package store

import (
	"os"
	"strings"
	"testing"
)

// Static lock-order guards for lifecycle exclusive barrier, quarantine slot→business,
// and RoundMatch AttachPriorResult business-before-result/ledger. Prevents the
// known deadlock (biz waiting slot vs slot waiting biz) and Cancel/hot-path race.

func TestLifecycle_ExclusiveRewriteBarrierOrder(t *testing.T) {
	src, err := os.ReadFile("lifecycle.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(src)
	if !strings.Contains(text, "Exclusive rewrite barrier") && !strings.Contains(text, "exclusive rewrite barrier") {
		t.Fatal("lifecycle comments must document exclusive rewrite barrier")
	}
	for _, name := range []string{"BeginCompleteTournament", "BeginCancelTournament"} {
		body := extractStoreFuncBody(t, text, name)
		excl := strings.Index(body, "acquireRewriteBarrierExclusive")
		shared := strings.Index(body, "acquireRewriteBarrierShared")
		cmd := strings.Index(body, "AcquireCommandLock")
		row := strings.Index(body, "FOR UPDATE")
		if excl < 0 {
			t.Fatalf("%s must take exclusive rewrite barrier", name)
		}
		if shared >= 0 {
			t.Fatalf("%s must not take shared rewrite barrier (lifecycle fences differentials)", name)
		}
		if cmd < 0 || excl > cmd {
			t.Fatalf("%s lock order must be exclusive barrier → command", name)
		}
		if row < 0 || cmd > row {
			t.Fatalf("%s lock order must be command → tournament FOR UPDATE", name)
		}
	}
}

func TestQuarantineResult_LockOrderSharedSlotBeforeBusiness(t *testing.T) {
	src, err := os.ReadFile("quarantine_result.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(src)
	if !strings.Contains(text, "Shared rewrite barrier") && !strings.Contains(text, "shared rewrite barrier") {
		t.Fatal("quarantine comments must document shared rewrite barrier")
	}
	body := extractStoreFuncBody(t, text, "BeginQuarantineTournamentResult")
	if strings.Contains(body, "acquireRewriteBarrierExclusive") {
		t.Fatal("QuarantineTournamentResult is not a lifecycle transition; must keep shared barrier")
	}
	shared := strings.Index(body, "acquireRewriteBarrierShared")
	cmd := strings.Index(body, "AcquireCommandLock")
	biz := strings.Index(body, "quarantineResultBizLockSQL")
	if biz < 0 {
		biz = strings.Index(body, "quarantineResultBusinessLockKey")
	}
	slotLock := strings.Index(body, "FROM bracket_slots")
	if slotLock >= 0 {
		// Prefer the FOR UPDATE slot lock site.
		for _, line := range strings.Split(body, "\n") {
			if strings.Contains(line, "bracket_slots") && strings.Contains(line, "FOR UPDATE") {
				slotLock = strings.Index(body, line)
				break
			}
		}
	}
	resultLock := -1
	for _, line := range strings.Split(body, "\n") {
		trim := strings.TrimSpace(line)
		if strings.Contains(trim, "FROM match_results") && strings.Contains(body[strings.Index(body, line):], "FOR UPDATE") {
			resultLock = strings.Index(body, line)
			break
		}
	}
	if shared < 0 || cmd < 0 || biz < 0 {
		t.Fatal("BeginQuarantineTournamentResult must acquire shared barrier, command, and business lock")
	}
	if !(shared < cmd && cmd < biz) {
		t.Fatal("known order prefix: shared → command → … → business")
	}
	// Must not lock tournaments row FOR UPDATE (plain existence SELECT only).
	for _, line := range strings.Split(body, "\n") {
		trim := strings.TrimSpace(line)
		if strings.HasPrefix(trim, "//") {
			continue
		}
		if strings.Contains(trim, "tournaments") && strings.Contains(trim, "FOR UPDATE") {
			t.Fatalf("quarantine must not FOR UPDATE tournaments row: %s", trim)
		}
	}
	// When slot is locked, it must precede business key.
	if slotLock >= 0 && strings.Contains(body, "bracket_slots") {
		slotFU := -1
		idx := 0
		for {
			i := strings.Index(body[idx:], "bracket_slots")
			if i < 0 {
				break
			}
			abs := idx + i
			window := body[abs:]
			if end := strings.Index(window, ";"); end > 0 {
				window = window[:end]
			}
			if strings.Contains(window, "FOR UPDATE") {
				slotFU = abs
				break
			}
			idx = abs + 1
		}
		if slotFU < 0 {
			t.Fatal("known-room quarantine must FOR UPDATE exact slot before business key")
		}
		if !(slotFU < biz) {
			t.Fatal("known-room order: slot → business key (never business then slot)")
		}
	}
	if resultLock >= 0 && !(biz < resultLock) {
		t.Fatal("business key must precede exact match_results FOR UPDATE")
	}
	// Document both known and unknown orders in header comments.
	header := text[:strings.Index(text, "func ")]
	if !strings.Contains(header, "slot") || !strings.Contains(strings.ToLower(header), "business") {
		t.Fatal("header must document slot-before-business for known rooms")
	}
}

func TestRoundMatch_AttachPriorResultBusinessBeforeResultLedger(t *testing.T) {
	src, err := os.ReadFile("round_match.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(src)
	header := text
	if i := strings.Index(text, "func "); i > 0 {
		header = text[:i]
	}
	if !strings.Contains(strings.ToLower(header), "business") {
		t.Fatal("round_match lock-order header must mention business-key advisory lock")
	}
	attach := extractStoreFuncBody(t, text, "AttachPriorResult")
	biz := strings.Index(attach, "quarantineResultBizLockSQL")
	if biz < 0 {
		biz = strings.Index(attach, "quarantineResultBusinessLockKey")
	}
	result := strings.Index(attach, "FROM match_results")
	ledger := strings.Index(attach, "FROM match_result_quarantines")
	if biz < 0 {
		t.Fatal("AttachPriorResult must acquire business-key advisory lock")
	}
	if result < 0 || ledger < 0 {
		t.Fatal("AttachPriorResult must read match_results and ledger")
	}
	if !(biz < result && biz < ledger) {
		t.Fatal("AttachPriorResult order: business → exact result/ledger (never reverse)")
	}
	ledgerFn := extractStoreFuncBody(t, text, "insertQuarantineLedger")
	// Reentrant re-acquire is OK; must not invent a reverse order vs AttachPriorResult.
	if strings.Contains(ledgerFn, "bracket_slots") && strings.Contains(ledgerFn, "FOR UPDATE") {
		t.Fatal("insertQuarantineLedger must not lock slots after business (reverse order)")
	}
	begin := extractStoreFuncBody(t, text, "BeginRoundMatch")
	for _, line := range strings.Split(begin, "\n") {
		trim := strings.TrimSpace(line)
		if strings.HasPrefix(trim, "//") {
			continue
		}
		if strings.Contains(trim, "quarantineResultBizLock") || strings.Contains(trim, "match-result-quarantine:") {
			t.Fatal("BeginRoundMatch must not take business lock before slot (order is slot → business in AttachPriorResult)")
		}
	}
}

func TestNoPath_BusinessThenSlot(t *testing.T) {
	checks := []struct {
		file string
		fn   string
	}{
		{"quarantine_result.go", "BeginQuarantineTournamentResult"},
		{"round_match.go", "BeginRoundMatch"},
		{"round_match.go", "AttachPriorResult"},
		{"round_match.go", "insertQuarantineLedger"},
		{"lifecycle.go", "BeginCompleteTournament"},
		{"lifecycle.go", "BeginCancelTournament"},
	}
	for _, c := range checks {
		src, err := os.ReadFile(c.file)
		if err != nil {
			t.Fatal(err)
		}
		body := extractStoreFuncBody(t, string(src), c.fn)
		biz := strings.Index(body, "quarantineResultBizLockSQL")
		if biz < 0 {
			biz = strings.Index(body, "quarantineResultBusinessLockKey")
		}
		if biz < 0 {
			continue
		}
		slotFU := -1
		idx := 0
		for {
			i := strings.Index(body[idx:], "bracket_slots")
			if i < 0 {
				break
			}
			abs := idx + i
			end := abs + 400
			if end > len(body) {
				end = len(body)
			}
			if strings.Contains(body[abs:end], "FOR UPDATE") {
				slotFU = abs
				break
			}
			idx = abs + 1
		}
		if slotFU >= 0 && biz < slotFU {
			t.Fatalf("%s in %s: business lock before slot FOR UPDATE (deadlock with RoundMatch)", c.fn, c.file)
		}
	}
}
