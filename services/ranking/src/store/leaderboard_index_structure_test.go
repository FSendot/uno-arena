package store_test

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

func TestMigration_LeaderboardIndexesMatchTop100OrderBy(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("caller")
	}
	path := filepath.Join(filepath.Dir(file), "..", "..", "migrations", "001_init.sql")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sql := string(raw)

	casual := regexp.MustCompile(`(?s)CREATE INDEX IF NOT EXISTS player_ratings_casual_elo_idx\s+ON player_ratings \(([^)]+)\)`)
	tour := regexp.MustCompile(`(?s)CREATE INDEX IF NOT EXISTS player_ratings_tournament_idx\s+ON player_ratings \(([^)]+)\)`)

	cm := casual.FindStringSubmatch(sql)
	if cm == nil {
		t.Fatal("missing player_ratings_casual_elo_idx")
	}
	tm := tour.FindStringSubmatch(sql)
	if tm == nil {
		t.Fatal("missing player_ratings_tournament_idx")
	}

	wantCasual := "casual_elo DESC, player_id ASC"
	wantTour := "tournament_placement_rating DESC, player_id ASC"
	if got := collapseWS(cm[1]); got != wantCasual {
		t.Fatalf("casual index columns=%q want %q (must match top100 ORDER BY rating DESC, player_id ASC)", got, wantCasual)
	}
	if got := collapseWS(tm[1]); got != wantTour {
		t.Fatalf("tournament index columns=%q want %q (must match top100 ORDER BY rating DESC, player_id ASC)", got, wantTour)
	}

	// Guard against single-column rating-only indexes that force equal-score full sorts.
	if strings.Contains(sql, "ON player_ratings (casual_elo DESC);") {
		t.Fatal("casual index must include player_id ASC tie-break")
	}
	if strings.Contains(sql, "ON player_ratings (tournament_placement_rating DESC);") {
		t.Fatal("tournament index must include player_id ASC tie-break")
	}
}

func collapseWS(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
