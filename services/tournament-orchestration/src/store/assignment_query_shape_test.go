package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSeedingChunk_PopulatesPlayerMappingsSetBased(t *testing.T) {
	path := filepath.Join("seeding.go")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	src := string(b)
	chunk := extractStoreFuncBody(t, src, "ProcessSeedingChunk")
	if !strings.Contains(chunk, "INSERT INTO bracket_slots") {
		t.Fatal("missing bracket_slots INSERT")
	}
	if !strings.Contains(chunk, "INSERT INTO tournament_round_slot_players") {
		t.Fatal("chunk commit must insert tournament_round_slot_players in same transaction")
	}
	// Slots then mappings must be separate statements so FK sees committed-in-tx parents.
	slotIns := strings.Index(chunk, "INSERT INTO bracket_slots")
	mapIns := strings.Index(chunk, "INSERT INTO tournament_round_slot_players")
	if slotIns < 0 || mapIns < 0 || !(slotIns < mapIns) {
		t.Fatal("bracket_slots INSERT must precede tournament_round_slot_players INSERT")
	}
	slotScanRel := strings.Index(chunk[slotIns:], "Scan(&slotConflicts)")
	if slotScanRel < 0 {
		t.Fatal("slot insert statement must Scan slotConflicts")
	}
	slotStmt := chunk[slotIns : slotIns+slotScanRel]
	if strings.Contains(slotStmt, "tournament_round_slot_players") {
		t.Fatal("mappings must not be a sibling CTE of bracket_slots INSERT (FK snapshot)")
	}
	if strings.Count(chunk, "jsonb_to_recordset") != 2 {
		t.Fatal("slot insert/conflict and mapping insert/conflict must each use jsonb_to_recordset (two statements)")
	}
	if !strings.Contains(chunk, "unnest(i.seeded_player_ids) WITH ORDINALITY") &&
		!strings.Contains(chunk, "unnest(i.seeded_player_ids) with ordinality") {
		t.Fatal("mappings must be built set-based from incoming JSON via unnest/ORDINALITY")
	}
	if strings.Contains(chunk, "FROM bracket_slots") && strings.Contains(chunk, "unnest(b.seeded_player_ids)") {
		t.Fatal("must not discover mappings by unnesting persisted bracket_slots arrays")
	}
	if !strings.Contains(chunk, "immutable_player_mapping_conflict") {
		t.Fatal("mapping drift must quarantine with immutable_player_mapping_conflict")
	}
	if !strings.Contains(chunk, "immutable_slot_conflict") {
		t.Fatal("slot drift must quarantine with immutable_slot_conflict")
	}
}

func TestAssignmentLoader_UsesIndexedLatestRoundLookup(t *testing.T) {
	path := filepath.Join("assignment.go")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	src := string(b)
	fn := extractFuncBodyNamed(t, src, "loadPlayerAssignmentQ")
	if !strings.Contains(fn, "FROM tournament_round_slot_players") {
		t.Fatal("assignment discovery must use tournament_round_slot_players")
	}
	if !strings.Contains(fn, "ORDER BY m.round_number DESC") || !strings.Contains(fn, "LIMIT 1") {
		t.Fatal("latest mapping must use round_number DESC LIMIT 1")
	}
	if strings.Contains(fn, "unnest(") || strings.Contains(fn, "seeded_player_ids") {
		t.Fatal("assignment lookup must not scan/unnest seeded_player_ids arrays")
	}
	if strings.Contains(fn, "SELECT * FROM tournament_registrations") {
		t.Fatal("must not scan whole registrations table")
	}
}

func extractFuncBodyNamed(t *testing.T, src, name string) string {
	t.Helper()
	needle := "func " + name + "("
	idx := strings.Index(src, needle)
	if idx < 0 {
		// methods
		needle = "func " + name
		idx = strings.Index(src, "func loadPlayerAssignmentQ")
		if idx < 0 {
			t.Fatalf("function %s not found", name)
		}
	}
	rest := src[idx:]
	brace := strings.Index(rest, "{")
	if brace < 0 {
		t.Fatal("no body")
	}
	depth := 0
	for i := brace; i < len(rest); i++ {
		switch rest[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return rest[:i+1]
			}
		}
	}
	t.Fatal("unbalanced braces")
	return ""
}
