package store_test

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// BeginCompleteRound must lock provisioning_batches by numeric slot range, not lexical
// text (slot_10 precedes slot_2). Guards >10-batch rounds.
func TestBeginCompleteRound_OrdersBatchesNumerically(t *testing.T) {
	src, err := os.ReadFile("complete_round.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(src)
	fnStart := strings.Index(text, "func (s *TournamentStore) BeginCompleteRound")
	if fnStart < 0 {
		t.Fatal("BeginCompleteRound not found")
	}
	fnBody := text[fnStart:]
	if end := strings.Index(fnBody, "\nfunc "); end > 0 {
		fnBody = fnBody[:end]
	}
	if !strings.Contains(fnBody, "regexp_replace") {
		t.Fatal("expected numeric regexp_replace on slot_id_from/to")
	}
	badLexical := regexp.MustCompile(`(?s)ORDER BY\s+COALESCE\(slot_id_from`)
	if badLexical.MatchString(fnBody) {
		t.Fatal("must not ORDER BY lexical COALESCE(slot_id_from, …)")
	}
	badTextOnly := regexp.MustCompile(`(?s)ORDER BY\s+slot_id_from\s*,`)
	if badTextOnly.MatchString(fnBody) {
		t.Fatal("must not ORDER BY textual slot_id_from alone")
	}
	if !strings.Contains(fnBody, "generate_series") {
		t.Fatal("must insert missing shards 0..63 before lock")
	}
}
