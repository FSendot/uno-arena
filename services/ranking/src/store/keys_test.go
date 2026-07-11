package store

import (
	"sort"
	"strings"
	"testing"
)

func TestPlacementDedupeKey_InjectiveWithDelimiterLikeChars(t *testing.T) {
	cases := []struct {
		player, tournament, placement string
	}{
		{`p"1`, `t,1`, `pe\1`},
		{"p\x00a", "t\x00b", "pe\x00c"},
		{"p]1", "t[2", `pe"3"`},
		{"p\n1", "t\t2", "pe\r3"},
	}
	seen := map[string]struct{}{}
	for _, c := range cases {
		key, err := placementDedupeKey(c.player, c.tournament, c.placement)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		if strings.ContainsRune(key, 0) {
			t.Fatalf("key must not contain NUL (PostgreSQL text rejects it): %q", key)
		}
		again, err := placementDedupeKey(c.player, c.tournament, c.placement)
		if err != nil || again != key {
			t.Fatalf("not stable: %q vs %q err=%v", key, again, err)
		}
		if _, dup := seen[key]; dup {
			t.Fatalf("collision for %+v → %q", c, key)
		}
		seen[key] = struct{}{}
	}
	// Distinct components must not collide even if a NUL-join would.
	a, err := placementDedupeKey("a", "b\x00c", "d")
	if err != nil {
		t.Fatal(err)
	}
	b, err := placementDedupeKey("a\x00b", "c", "d")
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatalf("NUL-ambiguous components collided: %q", a)
	}
}

func TestIngestLockKeys_GlobalSortOrder(t *testing.T) {
	casual := casualIngestLockKeys("e1", "g1")
	place := placementIngestLockKeys(`["p","t","pe"]`, "te1")
	all := append(append([]string{}, casual...), place...)
	sort.Strings(all)
	want := []string{
		lockPrefixCasualEvent + "e1",
		lockPrefixCasualGame + "g1",
		lockPrefixPlacementBiz + `["p","t","pe"]`,
		lockPrefixPlacementEvent + "te1",
	}
	if len(all) != len(want) {
		t.Fatalf("got %v want %v", all, want)
	}
	for i := range want {
		if all[i] != want[i] {
			t.Fatalf("order[%d]=%q want %q (full=%v)", i, all[i], want[i], all)
		}
	}
}
