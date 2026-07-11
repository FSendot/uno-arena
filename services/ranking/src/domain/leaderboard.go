package domain

import (
	"sort"
	"strconv"
)

// OrderLeaderboard returns a stable ranking: rating descending, then playerId ascending.
func OrderLeaderboard(entries []LeaderboardEntry) []LeaderboardEntry {
	out := make([]LeaderboardEntry, len(entries))
	copy(out, entries)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Rating != out[j].Rating {
			return out[i].Rating > out[j].Rating
		}
		return string(out[i].PlayerID) < string(out[j].PlayerID)
	})
	return out
}

// LeaderboardFromSnapshots builds ordered leaderboard entries from public snapshots.
func LeaderboardFromSnapshots(snaps []PlayerRatingSnapshot, boardType RatingSourceType) []LeaderboardEntry {
	entries := make([]LeaderboardEntry, 0, len(snaps))
	for _, s := range snaps {
		rating := s.CasualElo
		if boardType == SourceTournamentPlacement {
			rating = s.TournamentPlacementRating
		}
		entries = append(entries, LeaderboardEntry{
			PlayerID: s.PlayerID,
			Rating:   rating,
		})
	}
	return OrderLeaderboard(entries)
}

// PublishLeaderboardSnapshot emits a LeaderboardSnapshotPublished fact for an ordered board.
// Snapshot generation may repeat safely.
func PublishLeaderboardSnapshot(cmd PublishLeaderboardSnapshotCommand) CommandOutcome {
	if !cmd.CommandID.Valid() || !cmd.SnapshotID.Valid() {
		return rejectedOutcome(cmd.CommandID, Rejection{
			Code:    RejectInvalidIdentity,
			Message: "snapshot requires commandId and snapshotId",
		})
	}
	if cmd.BoardType != SourceCasualElo && cmd.BoardType != SourceTournamentPlacement {
		return rejectedOutcome(cmd.CommandID, Rejection{
			Code:    RejectInvalidCommand,
			Message: "boardType must be casual_elo or tournament_placement",
		})
	}
	ordered := OrderLeaderboard(cmd.Entries)
	data := map[string]string{
		"snapshotId":  string(cmd.SnapshotID),
		"boardType":   string(cmd.BoardType),
		"playerCount": strconv.Itoa(len(ordered)),
	}
	if len(ordered) > 0 {
		data["topPlayerId"] = string(ordered[0].PlayerID)
		data["topRating"] = strconv.Itoa(ordered[0].Rating)
	}
	for i, e := range ordered {
		rank := strconv.Itoa(i + 1)
		data["rank_"+rank] = string(e.PlayerID)
		data["rating_"+rank] = strconv.Itoa(e.Rating)
	}
	return acceptedOutcome(cmd.CommandID, []Fact{newFact(FactLeaderboardSnapshotPublished, data)})
}
