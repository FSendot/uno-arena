package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLifecycleDurableAntiFallback(t *testing.T) {
	srcRoot := "."
	files := []string{
		"service.go",
		"service_lifecycle.go",
		"durable_repo.go",
		filepath.Join("store", "lifecycle.go"),
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
			if !strings.Contains(submit, "lifecycle") || !strings.Contains(submit, "CmdCompleteTournament") {
				t.Fatal("SubmitCommand must route Complete/Cancel before Service.mu when Lifecycle wired")
			}
			muIdx := strings.Index(submit, "s.mu.Lock()")
			ctIdx := strings.Index(submit, "CmdCompleteTournament")
			if muIdx < 0 || ctIdx < 0 || ctIdx > muIdx {
				t.Fatal("Complete/Cancel differential must be routed before Service.mu.Lock")
			}
		case strings.HasSuffix(rel, "service_lifecycle.go"):
			for _, fname := range []string{
				"submitCommandLifecycleDifferential",
				"completeTournamentDifferential",
				"cancelTournamentDifferential",
				"commitLifecycleComplete",
				"commitLifecycleCancel",
				"rejectLifecycle",
				"persistLifecycleRejectStandalone",
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
			if !strings.Contains(src, "BeginStandaloneCommand") {
				t.Fatal("must expose standalone reject path")
			}
		case strings.HasSuffix(rel, "durable_repo.go"):
			fn := extractTypeMethods(t, path, "durableLifecycleRepo")
			fn += extractTypeMethods(t, path, "durableLifecycleUoW")
			for _, bad := range forbidden {
				if strings.Contains(fn, bad) {
					t.Fatalf("durableLifecycle* must not reference %s", bad)
				}
			}
		case strings.Contains(rel, "lifecycle.go"):
			for _, bad := range []string{
				"persistTournamentTx", "loadTournamentQ",
				"DELETE FROM advancement_records", "DELETE FROM match_results",
				"DELETE FROM bracket_slots", "DELETE FROM tournament_rounds",
				"DELETE FROM assigned_matches", "DELETE FROM round_progress_shards",
				"DELETE FROM tournament_registrations", "DELETE FROM provisioning_batches",
				"UPDATE bracket_slots", "UPDATE tournament_registrations",
				"UPDATE tournament_rounds", "UPDATE provisioning_batches",
				"UPDATE round_progress_shards", "UPDATE round_seeding_jobs",
			} {
				if strings.Contains(src, bad) {
					t.Fatalf("store/lifecycle.go must not contain %q", bad)
				}
			}
			if !strings.Contains(src, "acquireRewriteBarrierExclusive") {
				t.Fatal("differential lifecycle must take exclusive rewrite barrier")
			}
			if strings.Contains(src, "acquireRewriteBarrierShared") {
				t.Fatal("Complete/Cancel must not take shared rewrite barrier")
			}
			if !strings.Contains(src, "AcquireCommandLock") {
				t.Fatal("differential lifecycle must take global command lock")
			}
			if !strings.Contains(src, "BeginStandaloneLifecycleCommand") {
				t.Fatal("store must expose standalone command-lock-only begin")
			}
			if !strings.Contains(src, "FOR UPDATE") {
				t.Fatal("must FOR UPDATE tournament (and complete final round/slot)")
			}
			if !strings.Contains(src, "bumpProjectionVersionTx") {
				t.Fatal("success must bump public projection via base version")
			}
			if !strings.Contains(src, "LIMIT 2") {
				t.Fatal("complete must bound final slot load with LIMIT 2")
			}
			if !strings.Contains(src, "disposition = 'recorded'") {
				t.Fatal("complete must join advancement to disposition=recorded match_results")
			}
			beginCancel := extractFuncBody(t, path, "BeginCancelTournament")
			if strings.Contains(beginCancel, "tournament_rounds") ||
				strings.Contains(beginCancel, "bracket_slots") ||
				strings.Contains(beginCancel, "advancement_records") {
				t.Fatal("CancelTournament begin must be O(1) tournament row only")
			}
			applyCancel := extractFuncBody(t, path, "applyCancel")
			if strings.Contains(applyCancel, "bracket_slots") ||
				strings.Contains(applyCancel, "tournament_rounds") ||
				strings.Contains(applyCancel, "tournament_registrations") {
				t.Fatal("CancelTournament commit must not scan/update bracket/player rows")
			}
		}
	}
}
