package store

import "testing"

func TestTopicCasualIngest_MatchesAsyncAPI(t *testing.T) {
	if topicCasualIngest != "room.game.completed" {
		t.Fatalf("topicCasualIngest=%q want room.game.completed (AsyncAPI)", topicCasualIngest)
	}
	if topicCasualIngest == "room.gameplay.completed" {
		t.Fatal("rejected drifted spelling room.gameplay.completed")
	}
}

func TestTopicTournamentIngest_MatchesAsyncAPI(t *testing.T) {
	if topicTournamentIngest != "tournament.players.advanced" {
		t.Fatalf("topicTournamentIngest=%q want tournament.players.advanced (AsyncAPI)", topicTournamentIngest)
	}
	if topicTournamentIngest == "tournament.placement" {
		t.Fatal("rejected drifted spelling tournament.placement")
	}
}
