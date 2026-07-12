package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDurableHotPathAntiFallback(t *testing.T) {
	// Structural: differential RoundMatch hot paths must never call BeginExisting /
	// loadTournamentQ / persistTournamentTx, and RoundMatch store must not execute
	// subtree DELETE.
	srcRoot := "."
	files := []string{
		"service.go",
		"durable_repo.go",
		filepath.Join("store", "round_match.go"),
		filepath.Join("store", "barrier.go"),
	}
	forbiddenInHotPath := []string{
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
			for _, fname := range []string{"ingestMatchCompletedDifferential", "submitCommandRecordMatchDifferential"} {
				fn := extractFuncBody(t, path, fname)
				for _, bad := range forbiddenInHotPath {
					if strings.Contains(fn, bad) {
						t.Fatalf("%s must not reference %s", fname, bad)
					}
				}
				if strings.Contains(fn, "s.mu.") {
					t.Fatalf("%s must not use Service.mu", fname)
				}
			}
			httpDiff := extractFuncBody(t, path, "submitCommandRecordMatchDifferential")
			if strings.Contains(httpDiff, "loaded.RoundNumber > 0") && strings.Contains(httpDiff, "roundNumber = loaded.RoundNumber") {
				t.Fatal("HTTP differential must not overwrite nonempty claimed round with resolved")
			}
			kafkaDiff := extractFuncBody(t, path, "ingestMatchCompletedDifferential")
			if strings.Contains(kafkaDiff, "loaded.RoundNumber > 0") && strings.Contains(kafkaDiff, "roundNumber = loaded.RoundNumber") {
				t.Fatal("differential ingest must not overwrite nonempty claimed round with resolved")
			}
			memory := extractFuncBody(t, path, "recordResult")
			if !strings.Contains(memory, "withTournamentSource") && !strings.Contains(memory, "BeginExisting") {
				// recordResult may call withTournamentSource which uses BeginExisting.
				_ = memory
			}
			locked := extractFuncBody(t, path, "submitCommandLocked")
			if strings.Contains(locked, "LEGACY WHOLE-REWRITE") {
				t.Fatal("durable HTTP must not require LEGACY WHOLE-REWRITE guard when RoundMatches is wired")
			}
			if strings.Contains(src, "LegacyRecordMatchResultPath") {
				t.Fatal("LegacyRecordMatchResultPath must not remain as durable HTTP guard")
			}
		case strings.HasSuffix(rel, "durable_repo.go"):
			fn := extractTypeMethods(t, path, "durableRoundMatchRepo")
			fn += extractTypeMethods(t, path, "durableRoundMatchUoW")
			for _, bad := range forbiddenInHotPath {
				if strings.Contains(fn, bad) {
					t.Fatalf("durableRoundMatch* must not reference %s", bad)
				}
			}
			if !strings.Contains(fn, "BeginStandaloneCommand") {
				t.Fatal("durableRoundMatchRepo must expose BeginStandaloneCommand")
			}
		case strings.Contains(rel, "round_match.go"):
			for _, bad := range []string{"persistTournamentTx", "loadTournamentQ", "DELETE FROM advancement_records", "DELETE FROM match_results", "DELETE FROM bracket_slots", "DELETE FROM tournament_rounds", "DELETE FROM assigned_matches"} {
				if strings.Contains(src, bad) {
					t.Fatalf("store/round_match.go must not contain %q", bad)
				}
			}
			if !strings.Contains(src, "acquireRewriteBarrierShared") {
				t.Fatal("differential path must take shared rewrite barrier")
			}
			if !strings.Contains(src, "AcquireCommandLock") {
				t.Fatal("differential path must take global command lock when commandID nonempty")
			}
			if !strings.Contains(src, "BeginStandaloneRoundMatchCommand") {
				t.Fatal("store must expose standalone command-lock-only begin")
			}
			if !strings.Contains(src, "bumpProjectionShardTx") {
				t.Fatal("differential path must bump projection shards, not only base row")
			}
			if strings.Contains(src, "bumpProjectionVersionTx") {
				t.Fatal("differential path must not bump base bracket_projection_versions")
			}
			for _, line := range strings.Split(src, "\n") {
				trim := strings.TrimSpace(line)
				if strings.Contains(trim, "tournaments") && strings.Contains(trim, "FOR UPDATE") && !strings.HasPrefix(trim, "//") {
					t.Fatalf("differential path must not FOR UPDATE tournaments row: %s", trim)
				}
			}
		case strings.Contains(rel, "barrier.go"):
			if !strings.Contains(src, "hashtextextended") {
				t.Fatal("barrier must use hashtextextended")
			}
			if strings.Contains(src, "pg_advisory_xact_lock(hashtext(") {
				t.Fatal("barrier must not use 32-bit hashtext")
			}
		}
	}
}

func extractFuncBody(t *testing.T, path, name string) string {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != name {
			continue
		}
		start := fset.Position(fn.Body.Pos()).Offset
		end := fset.Position(fn.Body.End()).Offset
		b, _ := os.ReadFile(path)
		return string(b[start:end])
	}
	t.Fatalf("function %s not found in %s", name, path)
	return ""
}

func extractTypeMethods(t *testing.T, path, typeName string) string {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	b, _ := os.ReadFile(path)
	var out strings.Builder
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv == nil || len(fn.Recv.List) == 0 {
			continue
		}
		recv := recvTypeName(fn.Recv.List[0].Type)
		if recv != typeName && recv != "*"+typeName {
			continue
		}
		start := fset.Position(fn.Pos()).Offset
		end := fset.Position(fn.End()).Offset
		out.Write(b[start:end])
		out.WriteByte('\n')
	}
	return out.String()
}

func recvTypeName(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.StarExpr:
		return "*" + recvTypeName(e.X)
	case *ast.Ident:
		return e.Name
	default:
		return ""
	}
}

func TestDurableIngestDoesNotTakeServiceMutex(t *testing.T) {
	body := extractFuncBody(t, "service.go", "ingestMatchCompletedDifferential")
	if strings.Contains(body, "s.mu") {
		t.Fatal("durable ingest must not touch Service.mu")
	}
	httpBody := extractFuncBody(t, "service.go", "submitCommandRecordMatchDifferential")
	if strings.Contains(httpBody, "s.mu") {
		t.Fatal("durable HTTP RecordMatchResult must not touch Service.mu")
	}
	legacy := extractFuncBody(t, "service.go", "ingestMatchCompletedLegacy")
	if !strings.Contains(legacy, "s.mu.Lock()") {
		t.Fatal("legacy ingest must retain Service.mu")
	}
}
