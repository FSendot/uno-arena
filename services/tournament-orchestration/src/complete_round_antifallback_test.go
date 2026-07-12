package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCompleteRoundDurableAntiFallback(t *testing.T) {
	srcRoot := "."
	files := []string{
		"service.go",
		"service_complete_round.go",
		"durable_repo.go",
		filepath.Join("store", "complete_round.go"),
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
			if !strings.Contains(submit, "completeRounds") || !strings.Contains(submit, "CmdCompleteRound") {
				t.Fatal("SubmitCommand must route CompleteRound before Service.mu when CompleteRounds wired")
			}
			// Ensure CompleteRound route sits before mu.Lock
			muIdx := strings.Index(submit, "s.mu.Lock()")
			crIdx := strings.Index(submit, "CmdCompleteRound")
			if muIdx < 0 || crIdx < 0 || crIdx > muIdx {
				t.Fatal("CompleteRound differential must be routed before Service.mu.Lock")
			}
		case strings.HasSuffix(rel, "service_complete_round.go"):
			for _, fname := range []string{
				"submitCommandCompleteRoundDifferential",
				"TryCompleteReadyRound",
				"completeRoundDifferential",
				"commitCompleteRoundOutcome",
				"rejectCompleteRound",
				"persistCompleteRoundRejectStandalone",
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
			if !strings.Contains(src, "ErrCompleteRoundNotReady") {
				t.Fatal("try path must expose typed retryable not-ready error")
			}
			submit := extractFuncBody(t, path, "submitCommandCompleteRoundDifferential")
			if strings.Contains(submit, "cmd.Validate()") {
				t.Fatal("must not call envelope.Validate before malformed-payload handling (RecordMatch parity)")
			}
		case strings.HasSuffix(rel, "durable_repo.go"):
			fn := extractTypeMethods(t, path, "durableCompleteRoundRepo")
			fn += extractTypeMethods(t, path, "durableCompleteRoundUoW")
			for _, bad := range forbidden {
				if strings.Contains(fn, bad) {
					t.Fatalf("durableCompleteRound* must not reference %s", bad)
				}
			}
		case strings.Contains(rel, "complete_round.go"):
			for _, bad := range []string{
				"persistTournamentTx", "loadTournamentQ",
				"DELETE FROM advancement_records", "DELETE FROM match_results",
				"DELETE FROM bracket_slots", "DELETE FROM tournament_rounds",
				"DELETE FROM assigned_matches", "DELETE FROM round_progress_shards",
			} {
				if strings.Contains(src, bad) {
					t.Fatalf("store/complete_round.go must not contain %q", bad)
				}
			}
			if !strings.Contains(src, "acquireRewriteBarrierShared") {
				t.Fatal("differential CompleteRound must take shared rewrite barrier")
			}
			if !strings.Contains(src, "AcquireCommandLock") {
				t.Fatal("differential CompleteRound must take global command lock")
			}
			if !strings.Contains(src, "BeginStandaloneCompleteRoundCommand") {
				t.Fatal("store must expose standalone command-lock-only begin")
			}
			if !strings.Contains(src, "ORDER BY shard_id") {
				t.Fatal("must lock progress shards in shard_id order")
			}
			if !strings.Contains(src, "generate_series(0") {
				t.Fatal("must zero-init missing progress shards before lock")
			}
			if !strings.Contains(src, "ON CONFLICT") {
				t.Fatal("shard init must be ON CONFLICT DO NOTHING")
			}
			if !strings.Contains(src, "regexp_replace") {
				t.Fatal("must lock provisioning_batches in numeric slot_id order")
			}
			if strings.Contains(src, "ORDER BY COALESCE(slot_id_from") {
				t.Fatal("must not lock batches by lexical slot_id_from")
			}
			if !strings.Contains(src, "FOR UPDATE") {
				t.Fatal("must FOR UPDATE shards/batches/round")
			}
			if !strings.Contains(src, "bumpProjectionVersionTx") {
				t.Fatal("success must bump public projection exactly once via base version")
			}
			// Lock order comment: shards → batches → round
			if !strings.Contains(src, "shards") || !strings.Contains(src, "batches") {
				t.Fatal("must document shards → batches → round lock order")
			}
		}
	}
}
