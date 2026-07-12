package domain

import (
	"strconv"
	"testing"
)

func TestOrderLeaderboard_Deterministic(t *testing.T) {
	entries := []LeaderboardEntry{
		{PlayerID: "carol", Rating: 1200},
		{PlayerID: "alice", Rating: 1500},
		{PlayerID: "bob", Rating: 1500},
		{PlayerID: "dave", Rating: 900},
	}
	a := OrderLeaderboard(entries)
	b := OrderLeaderboard([]LeaderboardEntry{
		{PlayerID: "dave", Rating: 900},
		{PlayerID: "bob", Rating: 1500},
		{PlayerID: "carol", Rating: 1200},
		{PlayerID: "alice", Rating: 1500},
	})
	want := []PlayerID{"alice", "bob", "carol", "dave"}
	for i := range want {
		if a[i].PlayerID != want[i] || b[i].PlayerID != want[i] {
			t.Fatalf("order mismatch at %d: a=%v b=%v", i, a, b)
		}
	}
	if entries[0].PlayerID != "carol" {
		t.Fatal("input mutated")
	}
}

func TestLeaderboardFromSnapshots_SeparatesBoardTypes(t *testing.T) {
	snaps := []PlayerRatingSnapshot{
		{PlayerID: "a", CasualElo: 1400, TournamentPlacementRating: 800},
		{PlayerID: "b", CasualElo: 1000, TournamentPlacementRating: 1600},
	}
	casual := LeaderboardFromSnapshots(snaps, SourceCasualElo)
	if casual[0].PlayerID != "a" || casual[0].Rating != 1400 {
		t.Fatalf("casual=%#v", casual)
	}
	tour := LeaderboardFromSnapshots(snaps, SourceTournamentPlacement)
	if tour[0].PlayerID != "b" || tour[0].Rating != 1600 {
		t.Fatalf("tour=%#v", tour)
	}
}

func TestPublishLeaderboardSnapshot_FactPayload(t *testing.T) {
	out := PublishLeaderboardSnapshot(PublishLeaderboardSnapshotCommand{
		CommandID:  "snap1",
		SnapshotID: "s1",
		BoardType:  SourceCasualElo,
		Entries: []LeaderboardEntry{
			{PlayerID: "bob", Rating: 1100},
			{PlayerID: "alice", Rating: 1200},
		},
	})
	if !out.Accepted() || out.Facts[0].Name != FactLeaderboardSnapshotPublished {
		t.Fatalf("%#v", out)
	}
	f := out.Facts[0].Data
	if f["snapshotId"] != "s1" || f["boardType"] != "casual_elo" {
		t.Fatalf("%v", f)
	}
	if f["rank_1"] != "alice" || f["rating_1"] != "1200" || f["rank_2"] != "bob" {
		t.Fatalf("%v", f)
	}
}

func TestPublishLeaderboardSnapshot_Repeatable(t *testing.T) {
	cmd := PublishLeaderboardSnapshotCommand{
		CommandID: "snap1", SnapshotID: "s1", BoardType: SourceCasualElo,
		Entries: []LeaderboardEntry{{PlayerID: "a", Rating: 1000}},
	}
	a := PublishLeaderboardSnapshot(cmd)
	b := PublishLeaderboardSnapshot(cmd)
	if a.Facts[0].Data["rank_1"] != b.Facts[0].Data["rank_1"] {
		t.Fatal("not stable")
	}
}

func TestPublishLeaderboardSnapshot_CapsAt100(t *testing.T) {
	entries := make([]LeaderboardEntry, 0, 120)
	for i := 0; i < 120; i++ {
		entries = append(entries, LeaderboardEntry{
			PlayerID: PlayerID("p" + strconv.Itoa(i)),
			Rating:   2000 - i,
		})
	}
	out := PublishLeaderboardSnapshot(PublishLeaderboardSnapshotCommand{
		CommandID: "snap-cap", SnapshotID: "s-cap", BoardType: SourceCasualElo, Entries: entries,
	})
	if !out.Accepted() {
		t.Fatalf("%#v", out)
	}
	if out.Facts[0].Data["playerCount"] != "100" {
		t.Fatalf("playerCount=%s", out.Facts[0].Data["playerCount"])
	}
	if _, ok := out.Facts[0].Data["rank_101"]; ok {
		t.Fatal("must not include rank_101")
	}
	if out.Facts[0].Data["rank_100"] == "" {
		t.Fatal("missing rank_100")
	}
}
