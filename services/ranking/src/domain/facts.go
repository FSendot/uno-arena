package domain

// FactName identifies a named domain fact emitted by an accepted transition.
type FactName string

const (
	FactPlayerRatingUpdated              FactName = "PlayerRatingUpdated"
	FactTournamentPlacementRatingUpdated FactName = "TournamentPlacementRatingUpdated"
	FactLeaderboardSnapshotPublished     FactName = "LeaderboardSnapshotPublished"
)

// Fact is a named domain fact produced by an accepted command. Rejected commands never produce facts.
type Fact struct {
	Name FactName
	Data map[string]string
}

func newFact(name FactName, data map[string]string) Fact {
	if data == nil {
		data = map[string]string{}
	}
	// Defensive copy so callers cannot mutate stored fact payloads.
	cp := make(map[string]string, len(data))
	for k, v := range data {
		cp[k] = v
	}
	return Fact{Name: name, Data: cp}
}
