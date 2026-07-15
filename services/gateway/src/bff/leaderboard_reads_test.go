package bff

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"unoarena/shared/correlation"
)

func TestLeaderboard_ProxyPreservesQuery(t *testing.T) {
	reads := &FakeReads{}
	srv := NewServer(Dependencies{Reads: reads, Ready: true})
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet,
		"/v1/rankings/leaderboards?boardType=casual_elo&limit=25&cursor=next", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if reads.LastLeaderboardQuery != "boardType=casual_elo&limit=25&cursor=next" {
		t.Fatalf("query=%q", reads.LastLeaderboardQuery)
	}
}

func TestHTTPReadModelClient_LeaderboardPreservesQuery(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/rankings/leaderboards" {
			t.Fatalf("path=%s", r.URL.Path)
		}
		if got := r.URL.Query().Get("boardType"); got != "casual_elo" {
			t.Fatalf("boardType=%q", got)
		}
		if got := r.URL.Query().Get("limit"); got != "25" {
			t.Fatalf("limit=%q", got)
		}
		if got := r.Header.Get(correlation.HeaderCorrelationID); got != "corr-lb" {
			t.Fatalf("correlation=%q", got)
		}
		_, _ = w.Write([]byte(`{"boardType":"casual_elo","entries":[]}`))
	}))
	defer upstream.Close()

	client := NewHTTPReadModelClient(upstream.URL, "", upstream.Client())
	if _, err := client.Leaderboard(t.Context(), "boardType=casual_elo&limit=25",
		correlation.Headers{CorrelationID: "corr-lb"}); err != nil {
		t.Fatal(err)
	}
}
