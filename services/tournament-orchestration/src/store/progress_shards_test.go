package store

import (
	"os"
	"strings"
	"testing"
)

func TestLoadRoundProgressReadiness_SingleQueryRowSnapshot(t *testing.T) {
	src, err := os.ReadFile("progress_shards.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(src)
	fnStart := strings.Index(text, "func (s *TournamentStore) LoadRoundProgressReadiness")
	if fnStart < 0 {
		t.Fatal("LoadRoundProgressReadiness not found")
	}
	fnBody := text[fnStart:]
	if end := strings.Index(fnBody, "\nfunc "); end > 0 {
		fnBody = fnBody[:end]
	}
	if n := strings.Count(fnBody, "pool.QueryRow"); n != 1 {
		t.Fatalf("want exactly one pool.QueryRow statement, got %d", n)
	}
	if strings.Contains(fnBody, "pool.Query(") || strings.Contains(fnBody, "pool.Exec(") {
		t.Fatal("readiness must not use additional pool.Query/Exec round-trips")
	}
	if strings.Contains(fnBody, "Begin") || strings.Contains(fnBody, "BeginTx") {
		t.Fatal("prefer one QueryRow snapshot over a multi-statement transaction")
	}
	if !strings.Contains(fnBody, "round_progress_shards") {
		t.Fatal("must SUM shard counters from round_progress_shards")
	}
	if !strings.Contains(fnBody, "tournament_rounds") {
		t.Fatal("must read round status from tournament_rounds")
	}
	if !strings.Contains(fnBody, "provisioning_batches") ||
		!strings.Contains(fnBody, "status = 'quarantined'") {
		t.Fatal("must count quarantined provisioning_batches with indexable predicates")
	}
	if !strings.Contains(fnBody, "advancing_count") {
		t.Fatal("must SUM advancing_count from round_progress_shards")
	}
	if !strings.Contains(fnBody, "RoundInProgress") && !strings.Contains(fnBody, "in_progress") {
		t.Fatal("Ready must require status=in_progress")
	}
	if strings.Contains(fnBody, "outbox") || strings.Contains(fnBody, "INSERT") {
		t.Fatal("must not mutate state or auto-emit RoundCompleted")
	}
}

func TestRebuildRoundProgressShards_DerivesAdvancingCount(t *testing.T) {
	src, err := os.ReadFile("progress_shards.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(src)
	fnStart := strings.Index(text, "func rebuildRoundProgressShardsTx")
	if fnStart < 0 {
		t.Fatal("rebuildRoundProgressShardsTx not found")
	}
	fnBody := text[fnStart:]
	if end := strings.Index(fnBody, "\nfunc "); end > 0 {
		fnBody = fnBody[:end]
	}
	if !strings.Contains(fnBody, "advancement_records") {
		t.Fatal("rebuild must derive advancing_count from advancement_records")
	}
	if !strings.Contains(fnBody, "advancing_count") {
		t.Fatal("rebuild must write advancing_count")
	}
	if !strings.Contains(fnBody, "cardinality(ar.advancing_player_ids)") &&
		!strings.Contains(fnBody, "cardinality(advancing_player_ids)") {
		t.Fatal("rebuild must use advancement_records player cardinality")
	}
}

func TestFindReadyRoundCandidate_HintOnlyStructure(t *testing.T) {
	src, err := os.ReadFile("progress_shards.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(src)
	fnStart := strings.Index(text, "func (s *TournamentStore) FindReadyRoundCandidate")
	if fnStart < 0 {
		t.Fatal("FindReadyRoundCandidate not found")
	}
	fnBody := text[fnStart:]
	if end := strings.Index(fnBody, "\nfunc "); end > 0 {
		fnBody = fnBody[:end]
	}
	if !strings.Contains(fnBody, "LIMIT 1") {
		t.Fatal("candidate query must LIMIT 1")
	}
	if !strings.Contains(fnBody, "status = 'in_progress'") {
		t.Fatal("must filter tournament_rounds status=in_progress")
	}
	if !strings.Contains(fnBody, "JOIN tournaments") && !strings.Contains(fnBody, "JOIN tournaments t") {
		t.Fatal("must JOIN tournaments to exclude terminal phases")
	}
	if !strings.Contains(fnBody, "phase NOT IN ('completed', 'cancelled')") &&
		!strings.Contains(fnBody, `phase NOT IN ('completed','cancelled')`) {
		t.Fatal("must require tournaments.phase NOT IN completed,cancelled")
	}
	if !strings.Contains(fnBody, "cardinality") || !strings.Contains(fnBody, "advancing_player_ids") {
		t.Fatal("must compare shard advancing_count to advancement_records cardinality")
	}
	if !strings.Contains(fnBody, "round_advancing_players") {
		t.Fatal("must compare shard advancing_count to round_advancing_players count")
	}
	if strings.Contains(fnBody, "UPDATE") || strings.Contains(fnBody, "INSERT") || strings.Contains(fnBody, "FOR UPDATE") {
		t.Fatal("FindReadyRoundCandidate is a hint only — no mutation or locks")
	}
}
