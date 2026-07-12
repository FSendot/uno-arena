package store_test

import (
	"testing"

	"unoarena/services/spectator-view/store"
)

func TestParseResumeStreamID(t *testing.T) {
	cases := []struct {
		in     string
		want   string
		wantOK bool
	}{
		{"seq_1", "1-0", true},
		{"seq_42", "42-0", true},
		{"7-0", "7-0", true},
		{"12-3", "12-3", true},
		{"seq_", "", false},
		{"seq_x", "", false},
		{"missing", "", false},
		{"", "", false},
		{"not-a-stream", "", false},
	}
	for _, tc := range cases {
		got, ok := store.ParseResumeStreamID(tc.in)
		if ok != tc.wantOK || got != tc.want {
			t.Fatalf("ParseResumeStreamID(%q)=(%q,%v) want (%q,%v)", tc.in, got, ok, tc.want, tc.wantOK)
		}
	}
}
