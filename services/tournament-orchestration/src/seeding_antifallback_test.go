package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSeedingDifferentialAntiFallback(t *testing.T) {
	files := []string{
		"service.go",
		"durable_repo.go",
		filepath.Join("store", "seeding.go"),
	}
	forbiddenExact := []string{
		"BeginExisting(",
		"loadTournamentQ",
		"persistTournamentTx",
	}
	for _, rel := range files {
		path := rel
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		src := string(b)
		switch {
		case strings.HasSuffix(rel, "service.go"):
			for _, name := range []string{
				"submitCommandSeedingDifferential",
				"seedRoundDifferential",
				"commitSeedingOutcome",
			} {
				fn := extractFuncBody(t, path, name)
				for _, bad := range forbiddenExact {
					if strings.Contains(fn, bad) {
						t.Fatalf("%s must not reference %s", name, bad)
					}
				}
				if strings.Contains(fn, "s.mu.") || strings.Contains(fn, "s.mu.Lock") {
					t.Fatalf("%s must not use Service.mu", name)
				}
			}
		case strings.HasSuffix(rel, "durable_repo.go"):
			fn := extractTypeMethods(t, path, "durableSeedingRepo")
			fn += extractTypeMethods(t, path, "durableSeedingUoW")
			for _, bad := range forbiddenExact {
				if strings.Contains(fn, bad) {
					t.Fatalf("durableSeeding* must not reference %s", bad)
				}
			}
		case strings.Contains(rel, "seeding.go"):
			for _, bad := range forbiddenExact {
				if strings.Contains(src, bad) {
					t.Fatalf("store/seeding.go must not contain %q", bad)
				}
			}
			if !strings.Contains(src, "acquireRewriteBarrierExclusive") {
				t.Fatal("kickoff must take exclusive rewrite barrier")
			}
			if !strings.Contains(src, "acquireRewriteBarrierShared") {
				t.Fatal("chunk path must take shared rewrite barrier")
			}
			if !strings.Contains(src, "AcquireCommandLock") {
				t.Fatal("kickoff must acquire global command lock")
			}
			begin := extractFuncBody(t, path, "BeginSeedRound")
			barrierIdx := strings.Index(begin, "acquireRewriteBarrierExclusive")
			cmdIdx := strings.Index(begin, "AcquireCommandLock")
			if barrierIdx < 0 || cmdIdx < 0 || !(barrierIdx < cmdIdx) {
				t.Fatal("BeginSeedRound lock order must be barrier → command")
			}
			chunk := extractFuncBody(t, path, "ProcessSeedingChunk")
			if strings.Contains(chunk, "OFFSET") {
				t.Fatal("ProcessSeedingChunk must not use OFFSET")
			}
			srcLoad := extractFuncBody(t, path, "loadSeedingSourcePlayers")
			if !strings.Contains(srcLoad, "player_id >") {
				t.Fatal("seeding source load must keyset on player_id")
			}
			if !strings.Contains(chunk, "jsonb_to_recordset") {
				t.Fatal("ProcessSeedingChunk must bulk-insert slots via jsonb_to_recordset")
			}
			if strings.Count(chunk, "INSERT INTO bracket_slots") != 1 {
				t.Fatal("ProcessSeedingChunk must use exactly one bracket_slots INSERT statement")
			}
			if strings.Contains(srcLoad, "FROM tournament_registrations") &&
				(strings.Contains(srcLoad, "count(*) FROM tournament_registrations") ||
					strings.Contains(srcLoad, "count(*)::int FROM tournament_registrations")) {
				t.Fatal("source load must not full-scan tournament_registrations with count(*)")
			}
			fin := extractFuncBody(t, path, "finalizeSeedingInTx")
			if !strings.Contains(fin, "bumpProjectionVersionTx") {
				t.Fatal("finalization must bump base projection once")
			}
			if strings.Contains(fin, "tournament_registrations") {
				t.Fatal("finalize must not scan tournament_registrations")
			}
			if strings.Count(chunk, "bumpProjectionVersionTx") > 0 {
				t.Fatal("ProcessSeedingChunk body must not bump projection directly")
			}
			reap := extractFuncBody(t, path, "ReapExpiredSeedingLeasesBounded")
			updIdx := strings.Index(reap, "UPDATE round_seeding_jobs")
			if updIdx < 0 {
				t.Fatal("reap must UPDATE round_seeding_jobs")
			}
			updTail := reap[updIdx:]
			fromIdx := strings.Index(updTail, "FROM picked")
			if fromIdx < 0 {
				t.Fatal("reap UPDATE must use FROM picked")
			}
			afterFrom := updTail[fromIdx:]
			if strings.Count(afterFrom, "WHERE") != 1 {
				t.Fatalf("reap UPDATE must have exactly one WHERE after FROM picked (no duplicated WHERE), got %d", strings.Count(afterFrom, "WHERE"))
			}
		}
	}
}

func TestSubmitCommandSeedingBypassesMutex(t *testing.T) {
	body := extractFuncBody(t, "service.go", "SubmitCommand")
	if !strings.Contains(body, "submitCommandSeedingDifferential") {
		t.Fatal("SubmitCommand must branch to seeding differential")
	}
	if !strings.Contains(body, "s.mu.Lock()") {
		t.Fatal("non-differential commands must still take Service.mu")
	}
}
