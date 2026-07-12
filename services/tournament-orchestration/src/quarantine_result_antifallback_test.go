package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestQuarantineResultDurableAntiFallback(t *testing.T) {
	srcRoot := "."
	files := []string{
		"service.go",
		"service_quarantine_result.go",
		"durable_repo.go",
		filepath.Join("store", "quarantine_result.go"),
	}
	forbidden := []string{
		"BeginExisting",
		"loadTournamentQ",
		"persistTournamentTx",
	}
	for _, rel := range files {
		path := filepath.Join(srcRoot, rel)
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		src := string(b)
		switch {
		case strings.HasSuffix(rel, "service.go"):
			submit := extractFuncBody(t, path, "SubmitCommand")
			if !strings.Contains(submit, "quarantineResults") || !strings.Contains(submit, "CmdQuarantineResult") {
				t.Fatal("SubmitCommand must route QuarantineTournamentResult before Service.mu when wired")
			}
			muIdx := strings.Index(submit, "s.mu.Lock()")
			qrIdx := strings.Index(submit, "CmdQuarantineResult")
			if muIdx < 0 || qrIdx < 0 || qrIdx > muIdx {
				t.Fatal("QuarantineTournamentResult differential must be routed before Service.mu.Lock")
			}
		case strings.HasSuffix(rel, "service_quarantine_result.go"):
			for _, fname := range []string{
				"submitCommandQuarantineResultDifferential",
				"commitQuarantineResult",
				"rejectQuarantineResult",
				"persistQuarantineResultRejectStandalone",
			} {
				fn := extractFuncBody(t, path, fname)
				for _, bad := range forbidden {
					if strings.Contains(fn, bad) {
						t.Fatalf("%s must not reference %s", fname, bad)
					}
				}
				if strings.Contains(fn, "s.mu.") {
					t.Fatalf("%s must not use Service.mu", fname)
				}
			}
			if strings.Contains(src, "topicForFact") || strings.Contains(src, "OutboxEvent") {
				t.Fatal("QuarantineTournamentResult must not invent outbox/Kafka (no contract topic)")
			}
			if strings.Contains(src, "cmd.Reason") && strings.Contains(src, "factsPayload") {
				// Ensure raw reason is not copied into facts without sanitization.
			}
			if !strings.Contains(src, "ExplicitQuarantineReasonCode") {
				t.Fatal("service must force closed ExplicitQuarantineReasonCode into outcome facts")
			}
		case strings.HasSuffix(rel, "durable_repo.go"):
			fn := extractTypeMethods(t, path, "durableQuarantineResultRepo")
			fn += extractTypeMethods(t, path, "durableQuarantineResultUoW")
			for _, bad := range forbidden {
				if strings.Contains(fn, bad) {
					t.Fatalf("durableQuarantineResult* must not reference %s", bad)
				}
			}
		case strings.Contains(rel, "quarantine_result.go"):
			for _, bad := range []string{
				"persistTournamentTx", "loadTournamentQ",
				"DELETE FROM", "UPDATE bracket_slots", "UPDATE tournament_rounds",
				"UPDATE round_progress_shards", "UPDATE advancement_records",
				"INSERT INTO outbox_events",
			} {
				if strings.Contains(src, bad) {
					t.Fatalf("store/quarantine_result.go must not contain %q", bad)
				}
			}
			if !strings.Contains(src, "acquireRewriteBarrierShared") {
				t.Fatal("differential quarantine must take shared rewrite barrier")
			}
			if !strings.Contains(src, "AcquireCommandLock") {
				t.Fatal("differential quarantine must take global command lock")
			}
			if !strings.Contains(src, "quarantineResultBizLockSQL") {
				t.Fatal("must take business-key advisory lock")
			}
			if !strings.Contains(src, "FOR UPDATE") {
				t.Fatal("must FOR UPDATE exact slot (when known) and exact result/ledger rows")
			}
			begin := extractFuncBody(t, path, "BeginQuarantineTournamentResult")
			for _, line := range strings.Split(begin, "\n") {
				trim := strings.TrimSpace(line)
				if strings.HasPrefix(trim, "//") {
					continue
				}
				if strings.Contains(trim, "tournaments") && strings.Contains(trim, "FOR UPDATE") {
					t.Fatalf("quarantine must not FOR UPDATE tournaments row: %s", trim)
				}
			}
			biz := strings.Index(begin, "quarantineResultBizLockSQL")
			if biz < 0 {
				biz = strings.Index(begin, "quarantineResultBusinessLockKey")
			}
			slotFU := strings.Index(begin, "bracket_slots")
			if slotFU >= 0 && biz >= 0 {
				// Slot FOR UPDATE must appear before business advisory for known rooms.
				slotWindow := begin[slotFU:]
				if end := strings.Index(slotWindow, "quarantineResult"); end > 0 {
					slotWindow = slotWindow[:end]
				}
				if !strings.Contains(slotWindow, "FOR UPDATE") || !(slotFU < biz) {
					t.Fatal("known-room quarantine lock order must be slot → business key")
				}
			}
			if !strings.Contains(src, "bumpProjectionVersionTx") {
				t.Fatal("first factful quarantine must bump public projection via base version")
			}
			if !strings.Contains(src, "BeginStandaloneQuarantineResultCommand") {
				t.Fatal("store must expose standalone command-lock-only begin")
			}
			if strings.Contains(src, "pg_advisory_xact_lock(hashtext(") {
				t.Fatal("must use hashtextextended, not 32-bit hashtext")
			}
			if !strings.Contains(src, "ON CONFLICT (claimed_room_id, completion_version)") {
				t.Fatal("ledger insert must use business-key unique conflict")
			}
			if !strings.Contains(src, "ExplicitQuarantineReasonCode") {
				t.Fatal("store must persist closed ExplicitQuarantineReasonCode only")
			}
			if strings.Contains(src, "cmd.Reason") && !strings.Contains(src, "SanitizeExplicitQuarantineReason") {
				t.Fatal("raw cmd.Reason must not reach SQL without sanitization")
			}
		}
	}
}

func TestQuarantineResultBoundedSQLNoScans(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("store", "quarantine_result.go"))
	if err != nil {
		t.Fatal(err)
	}
	src := string(b)
	for _, bad := range []string{
		"SELECT * FROM bracket_slots",
		"SELECT * FROM tournament_rounds",
		"SELECT * FROM match_results WHERE tournament_id",
		"FROM tournament_registrations",
		"FROM provisioning_batches",
	} {
		if strings.Contains(src, bad) {
			t.Fatalf("bounded path must not scan with %q", bad)
		}
	}
}
