package store_test

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// LoadBracketBatchSummaries is internal chunk rebuild ordering. Textual ORDER BY
// slot_id_from puts slot_10 before slot_2; the query must join bracket_slots and
// order by numeric slot_index without N+1 round-trips.
func TestLoadBracketBatchSummaries_OrdersByNumericSlotIndex(t *testing.T) {
	src, err := os.ReadFile("bracket_page.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(src)
	fnStart := strings.Index(text, "func loadBracketBatchSummaries")
	if fnStart < 0 {
		t.Fatal("loadBracketBatchSummaries not found")
	}
	fnBody := text[fnStart:]
	if end := strings.Index(fnBody, "\nfunc "); end > 0 {
		fnBody = fnBody[:end]
	}
	if !strings.Contains(fnBody, "bracket_slots") {
		t.Fatal("expected bounded join to bracket_slots for numeric slot_index")
	}
	if !strings.Contains(fnBody, "slot_index") {
		t.Fatal("expected ORDER BY slot_index (numeric), not textual slot_id_from alone")
	}
	// Reject the lexical trap: ordering solely by slot_id_from ASC.
	bad := regexp.MustCompile(`(?s)ORDER BY\s+slot_id_from\s+ASC`)
	if bad.MatchString(fnBody) {
		t.Fatal("must not ORDER BY textual slot_id_from ASC (slot_10 precedes slot_2)")
	}
	good := regexp.MustCompile(`(?s)ORDER BY[\s\S]*slot_index`)
	if !good.MatchString(fnBody) {
		t.Fatal("expected ORDER BY ... slot_index")
	}
}
