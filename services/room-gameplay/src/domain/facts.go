package domain

// FactName identifies a named domain fact emitted by an accepted transition.
type FactName string

const (
	FactRoomCreated           FactName = "RoomCreated"
	FactPlayerJoinedRoom      FactName = "PlayerJoinedRoom"
	FactPlayerLeftRoom        FactName = "PlayerLeftRoom"
	FactHostReassigned        FactName = "HostReassigned"
	FactRoomLocked            FactName = "RoomLocked"
	FactMatchStarted          FactName = "MatchStarted"
	FactGameStarted           FactName = "GameStarted"
	FactRoomCancelled         FactName = "RoomCancelled"
	FactRoomCompleted         FactName = "RoomCompleted"
	FactMatchCompleted        FactName = "MatchCompleted"
	FactGameCompleted         FactName = "GameCompleted"
	FactMatchScoreUpdated     FactName = "MatchScoreUpdated"
	FactPlayerDisconnected    FactName = "PlayerDisconnected"
	FactPlayerReconnected     FactName = "PlayerReconnected"
	FactPlayerForfeited       FactName = "PlayerForfeited"
	FactUnoWindowOpened       FactName = "UnoWindowOpened"
	FactUnoWindowExpired      FactName = "UnoWindowExpired"
	FactUnoWindowClosed       FactName = "UnoWindowClosed"
	FactUnoCalled             FactName = "UnoCalled"
	FactUnoChallengeIssued    FactName = "UnoChallengeIssued"
	FactUnoPenaltyApplied     FactName = "UnoPenaltyApplied"
	FactCardPlayed            FactName = "CardPlayed"
	FactPenaltyStackIncreased FactName = "PenaltyStackIncreased"
	FactPenaltyStackResolved  FactName = "PenaltyStackResolved"
	FactCardDrawn             FactName = "CardDrawn"
	FactColorChosen           FactName = "ColorChosen"
	FactTurnAdvanced          FactName = "TurnAdvanced"
	FactDrawTurnRetained      FactName = "DrawTurnRetained"
	FactTurnSkipped           FactName = "TurnSkipped"
	FactSpectatorStreamsClose FactName = "SpectatorStreamsClose"
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
	return Fact{Name: name, Data: data}
}
