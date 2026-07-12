package store

import (
	"errors"
	"os"
	"strings"
	"testing"

	"unoarena/services/tournament-orchestration/domain"
)

func TestFinalStandingsFromRankedResult_PreservesOrderAndCaps(t *testing.T) {
	raw := []byte(`{"fingerprint":"fp","standings":[
		{"playerId":"p3","matchWins":1,"cumulativeCardPoints":0,"finalGameCompletedAt":"2026-01-01T00:00:00Z","forfeited":false},
		{"playerId":"p1","matchWins":0,"cumulativeCardPoints":10,"finalGameCompletedAt":"2026-01-01T00:00:01Z","forfeited":false},
		{"playerId":"p2","matchWins":0,"cumulativeCardPoints":20,"finalGameCompletedAt":"2026-01-01T00:00:02Z","forfeited":false}
	]}`)
	got, err := finalStandingsFromRankedResult(raw)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"p3", "p1", "p2"}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order[%d]=%s want %s (must preserve ranked_result array order)", i, got[i], want[i])
		}
	}
}

func TestFinalStandingsFromRankedResult_MalformedFailClosed(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{"invalid json", `{`},
		{"missing standings", `{"fingerprint":"fp"}`},
		{"empty standings", `{"standings":[]}`},
		{"duplicate", `{"standings":[{"playerId":"a"},{"playerId":"a"}]}`},
		{"empty player id", `{"standings":[{"playerId":""}]}`},
		{"over max 10", overTenStandingsJSON()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := finalStandingsFromRankedResult([]byte(tc.raw))
			if !errors.Is(err, ErrMalformedStandings) {
				t.Fatalf("want ErrMalformedStandings, got %v", err)
			}
		})
	}
}

func TestFinalStandingsFromRankedResult_MaxTenAccepted(t *testing.T) {
	ids := make([]string, domain.FinalPlayerThreshold)
	parts := make([]string, domain.FinalPlayerThreshold)
	for i := 0; i < domain.FinalPlayerThreshold; i++ {
		ids[i] = "p" + itoaLocal(i)
		parts[i] = `{"playerId":"` + ids[i] + `"}`
	}
	raw := []byte(`{"standings":[` + strings.Join(parts, ",") + `]}`)
	got, err := finalStandingsFromRankedResult(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != domain.FinalPlayerThreshold {
		t.Fatalf("len=%d", len(got))
	}
}

func TestLoadStandingsProjectionSQL_BoundsMultipleFinals(t *testing.T) {
	// Static guard: fail-closed multiple finals must use bounded LIMIT 2 + count,
	// never ORDER BY completion_version LIMIT 1 (arbitrary pick) or unbounded agg.
	src, err := os.ReadFile("standings_page.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(src)
	if !strings.Contains(body, "LIMIT 2") {
		t.Fatal("final_candidates must LIMIT 2")
	}
	if !strings.Contains(body, "final_count") {
		t.Fatal("must select final_count")
	}
	if !strings.Contains(body, "finalCount > 1") {
		t.Fatal("must fail closed when finalCount > 1")
	}
	if strings.Contains(body, "ORDER BY mr.completion_version") {
		t.Fatal("must not pick an arbitrary final via ORDER BY completion_version")
	}
	if strings.Contains(body, "jsonb_agg") || strings.Contains(body, "json_agg") {
		t.Fatal("must not unbounded JSON-aggregate final results")
	}
}

func overTenStandingsJSON() string {
	parts := make([]string, domain.FinalPlayerThreshold+1)
	for i := range parts {
		parts[i] = `{"playerId":"p` + itoaLocal(i) + `"}`
	}
	return `{"standings":[` + strings.Join(parts, ",") + `]}`
}

func itoaLocal(n int) string {
	if n == 0 {
		return "0"
	}
	var b [16]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
