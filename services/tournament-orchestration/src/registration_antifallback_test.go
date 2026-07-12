package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRegistrationDifferentialAntiFallback(t *testing.T) {
	files := []string{
		"service.go",
		"durable_repo.go",
		filepath.Join("store", "registration.go"),
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
				"submitCommandRegistrationDifferential",
				"createTournamentDifferential",
				"registerPlayerDifferential",
				"closeRegistrationDifferential",
			} {
				fn := extractFuncBody(t, path, name)
				for _, bad := range forbiddenExact {
					if strings.Contains(fn, bad) {
						t.Fatalf("%s must not reference %s", name, bad)
					}
				}
				if containsLegacyBeginCreate(fn) {
					t.Fatalf("%s must not call legacy BeginCreate", name)
				}
				if strings.Contains(fn, "s.mu.") || strings.Contains(fn, "s.mu.Lock") {
					t.Fatalf("%s must not use Service.mu", name)
				}
			}
			regFn := extractFuncBody(t, path, "registerPlayerDifferential")
			if strings.Contains(regFn, "bumpProjectionVersionTx") {
				t.Fatal("registerPlayerDifferential must not call bumpProjectionVersionTx")
			}
		case strings.HasSuffix(rel, "durable_repo.go"):
			fn := extractTypeMethods(t, path, "durableRegistrationRepo")
			fn += extractTypeMethods(t, path, "durableRegistrationUoW")
			for _, bad := range forbiddenExact {
				if strings.Contains(fn, bad) {
					t.Fatalf("durableRegistration* must not reference %s", bad)
				}
			}
			if containsLegacyBeginCreate(fn) {
				t.Fatal("durableRegistration* must not call legacy BeginCreate")
			}
		case strings.Contains(rel, "registration.go"):
			for _, bad := range append(forbiddenExact, "BeginExisting(") {
				if strings.Contains(src, bad) {
					t.Fatalf("store/registration.go must not contain %q", bad)
				}
			}
			if containsLegacyBeginCreate(src) {
				t.Fatal("store/registration.go must not call legacy BeginCreate")
			}
			if !strings.Contains(src, "acquireRewriteBarrierShared") {
				t.Fatal("register path must take shared rewrite barrier")
			}
			if !strings.Contains(src, "acquireRewriteBarrierExclusive") {
				t.Fatal("create/close must take exclusive rewrite barrier")
			}
			if !strings.Contains(src, "AcquireCommandLock") {
				t.Fatal("T-Reg paths must acquire global command lock")
			}
			if !strings.Contains(src, "acquireRegistrationCloseElectionLock") {
				t.Fatal("shard-full auto-close must take registration-close election lock")
			}
			if !strings.Contains(src, "bumpProjectionShardTx") {
				t.Fatal("register path must bump projection shards")
			}
			if !strings.Contains(src, "hashtextextended") {
				t.Fatal("player lock must use hashtextextended")
			}
			begin := extractFuncBody(t, path, "BeginRegisterPlayer")
			for _, line := range strings.Split(begin, "\n") {
				trim := strings.TrimSpace(line)
				if strings.Contains(trim, "tournaments") && strings.Contains(trim, "FOR UPDATE") && !strings.HasPrefix(trim, "//") {
					t.Fatalf("BeginRegisterPlayer must not FOR UPDATE tournaments: %s", trim)
				}
			}
			// Deadlock-safe order: barrier → command → player.
			barrierIdx := strings.Index(begin, "acquireRewriteBarrierShared")
			cmdIdx := strings.Index(begin, "AcquireCommandLock")
			playerIdx := strings.Index(begin, "acquireRegistrationPlayerLock")
			if barrierIdx < 0 || cmdIdx < 0 || playerIdx < 0 {
				t.Fatal("BeginRegisterPlayer must acquire barrier, command, and player locks")
			}
			if !(barrierIdx < cmdIdx && cmdIdx < playerIdx) {
				t.Fatal("BeginRegisterPlayer lock order must be barrier → command → player")
			}
			createBegin := extractFuncBody(t, path, "BeginCreateTournament")
			if strings.Index(createBegin, "acquireRewriteBarrierExclusive") >
				strings.Index(createBegin, "AcquireCommandLock") {
				t.Fatal("BeginCreateTournament lock order must be barrier → command")
			}
			closeBegin := extractFuncBody(t, path, "BeginCloseRegistration")
			if strings.Contains(closeBegin, "acquireRegistrationCloseElectionLock") {
				t.Fatal("manual close must not take registration-close election lock")
			}
			if strings.Index(closeBegin, "acquireRewriteBarrierExclusive") >
				strings.Index(closeBegin, "AcquireCommandLock") {
				t.Fatal("BeginCloseRegistration lock order must be barrier → command")
			}
			standalone := extractFuncBody(t, path, "BeginStandaloneCommand")
			if strings.Contains(standalone, "acquireRewriteBarrier") {
				t.Fatal("standalone command path must not take rewrite barrier")
			}
			if !strings.Contains(standalone, "AcquireCommandLock") {
				t.Fatal("standalone command path must take command lock")
			}
			reserve := extractFuncBody(t, path, "ReserveRegistration")
			if strings.Contains(reserve, "SUM(count)<") || strings.Contains(reserve, "SUM(count) <") {
				t.Fatal("must not use racy SUM(count)<capacity admission")
			}
			if !strings.Contains(reserve, "acquireRegistrationCloseElectionLock") {
				t.Fatal("ReserveRegistration must take election lock on shard-full")
			}
			electionIdx := strings.Index(reserve, "acquireRegistrationCloseElectionLock")
			updateIdx := strings.Index(reserve, "UPDATE tournament_registration_shards")
			if updateIdx < 0 || electionIdx < updateIdx {
				t.Fatal("election lock must be acquired after local shard UPDATE")
			}
			finalize := extractFuncBody(t, path, "FinalizeRegister")
			if !strings.Contains(finalize, "finishWithPrior") {
				t.Fatal("FinalizeRegister must return canonical prior via finishWithPrior, not commit-nil")
			}
			commit := extractFuncBody(t, path, "Commit")
			if !strings.Contains(commit, "finishWithPrior") {
				t.Fatal("Commit must return canonical prior via finishWithPrior, not commit-nil")
			}
		}
	}
}

func TestSubmitCommandDifferentialBypassesMutex(t *testing.T) {
	body := extractFuncBody(t, "service.go", "SubmitCommand")
	if !strings.Contains(body, "submitCommandRegistrationDifferential") {
		t.Fatal("SubmitCommand must branch to registration differential")
	}
	if !strings.Contains(body, "s.mu.Lock()") {
		t.Fatal("non-differential commands must still take Service.mu")
	}
}

// containsLegacyBeginCreate detects repo.BeginCreate / BeginCreate( but not BeginCreateTournament.
func containsLegacyBeginCreate(src string) bool {
	for i := 0; i < len(src); {
		idx := strings.Index(src[i:], "BeginCreate")
		if idx < 0 {
			return false
		}
		i += idx
		rest := src[i:]
		if strings.HasPrefix(rest, "BeginCreateTournament") {
			i += len("BeginCreateTournament")
			continue
		}
		if strings.HasPrefix(rest, "BeginCreate(") || strings.HasPrefix(rest, "BeginCreate ") {
			return true
		}
		i += len("BeginCreate")
	}
	return false
}
