//go:build integration

package store_test

import (
	"context"
	"testing"

	"unoarena/services/tournament-orchestration/store"
)

func TestLoadBracketPage_KeysetBounded(t *testing.T) {
	ctx := context.Background()
	_, ts := openStore(t)

	players := make([]string, 0, 40)
	for i := 0; i < 40; i++ {
		players = append(players, "p"+itoa(i))
	}
	_, _ = provisionedTournament(t, ts, "t-page", players)

	page, err := ts.LoadBracketPage(ctx, store.BracketPageQuery{
		TournamentID: "t-page",
		Limit:        2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if page.TournamentID != "t-page" {
		t.Fatalf("id=%s", page.TournamentID)
	}
	if page.ProjectionVersion < 1 {
		t.Fatalf("projectionVersion=%d", page.ProjectionVersion)
	}
	if page.GeneratedAt.IsZero() {
		t.Fatal("generatedAt zero")
	}
	if len(page.Summary.Rounds) == 0 {
		t.Fatal("summary rounds empty")
	}
	if page.Summary.Rounds[0].SlotCount < 2 {
		t.Fatalf("slotCount=%d", page.Summary.Rounds[0].SlotCount)
	}
	if len(page.Slots) != 2 {
		t.Fatalf("slots=%d", len(page.Slots))
	}
	if page.NextCursor == "" {
		t.Fatal("expected nextCursor")
	}
	c, err := store.DecodeBracketCursor(page.NextCursor)
	if err != nil {
		t.Fatal(err)
	}

	page2, err := ts.LoadBracketPage(ctx, store.BracketPageQuery{
		TournamentID: "t-page",
		Cursor:       page.NextCursor,
		Limit:        2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.Slots) == 0 {
		t.Fatal("page2 empty")
	}
	if page2.Slots[0].SlotIndex <= c.SlotIndex && page2.Slots[0].RoundNumber == c.RoundNumber {
		// after cursor must be strictly greater in keyset order
		if !(page2.Slots[0].RoundNumber > c.RoundNumber || page2.Slots[0].SlotIndex > c.SlotIndex) {
			t.Fatalf("page2 did not advance past %+v: %+v", c, page2.Slots[0])
		}
	}

	chunk, err := ts.LoadBatchChunkForProjection(ctx, "t-page", 1, "batch_0")
	if err != nil {
		t.Fatal(err)
	}
	if len(chunk) == 0 || len(chunk) > 1000 {
		t.Fatalf("chunk size=%d", len(chunk))
	}
	if page.Summary.Rounds[0].BatchCount < 1 {
		t.Fatalf("batchCount=%d", page.Summary.Rounds[0].BatchCount)
	}

	_, err = ts.LoadBracketPage(ctx, store.BracketPageQuery{
		TournamentID: "t-page",
		Cursor:       "tampered",
		Limit:        10,
	})
	if err == nil {
		t.Fatal("expected cursor error")
	}
}
