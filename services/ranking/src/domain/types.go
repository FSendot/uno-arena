package domain

// RoomType mirrors upstream GameCompleted room classification.
type RoomType string

const (
	RoomTypeAdHoc      RoomType = "ad_hoc"
	RoomTypeTournament RoomType = "tournament"
)

func (t RoomType) String() string { return string(t) }

// RatingSourceType distinguishes casual Elo history from tournament placement history.
// Also selects which rating stream a leaderboard snapshot represents.
type RatingSourceType string

const (
	SourceCasualElo           RatingSourceType = "casual_elo"
	SourceTournamentPlacement RatingSourceType = "tournament_placement"
)

// RatingHistoryReason classifies why a rating history entry was recorded.
type RatingHistoryReason string

const (
	ReasonCasualGameCompleted     RatingHistoryReason = "casual_game_completed"
	ReasonTournamentPlacement     RatingHistoryReason = "tournament_placement"
	ReasonTournamentFinalStanding RatingHistoryReason = "tournament_final_standing"
)

// Policy defaults for the PlayerRating aggregate.
const (
	DefaultFloor             = 0
	DefaultInitialCasual     = 1000
	DefaultInitialTournament = 0
	DefaultKFactor           = 32
	EloScale                 = 400
)

// RatingConfig configures floor, initials, and K-factor for a PlayerRating.
type RatingConfig struct {
	Floor                   int
	InitialCasualElo        int
	InitialTournamentRating int
	KFactor                 int
}

// DefaultRatingConfig returns the standard casual Elo defaults.
func DefaultRatingConfig() RatingConfig {
	return RatingConfig{
		Floor:                   DefaultFloor,
		InitialCasualElo:        DefaultInitialCasual,
		InitialTournamentRating: DefaultInitialTournament,
		KFactor:                 DefaultKFactor,
	}
}

func (c RatingConfig) withDefaults() RatingConfig {
	if c.KFactor <= 0 {
		c.KFactor = DefaultKFactor
	}
	// Zero-value InitialCasualElo means "use default"; explicit non-zero is kept.
	if c.InitialCasualElo == 0 {
		c.InitialCasualElo = DefaultInitialCasual
	}
	if c.InitialCasualElo < c.Floor {
		c.InitialCasualElo = c.Floor
	}
	if c.InitialTournamentRating < c.Floor {
		c.InitialTournamentRating = c.Floor
	}
	return c
}

// EloRating is the casual skill score value object.
type EloRating struct {
	Value int
}

// TournamentPlacementRating is the separate tournament achievement score.
type TournamentPlacementRating struct {
	Value int
}

// RatedPlacement is one participant's pre-update rating and final placement (1 = first).
type RatedPlacement struct {
	PlayerID  PlayerID
	Rating    int
	Placement int
}

// RatingHistoryEntry is an immutable audit record of one applied rating change.
type RatingHistoryEntry struct {
	SourceType       RatingSourceType
	PreviousRating   int
	NewRating        int
	Delta            int
	Reason           RatingHistoryReason
	GameID           GameID
	RoomID           RoomID
	EventID          EventID
	TournamentID     TournamentID
	PlacementEventID PlacementEventID
	Placement        int
}

// PlayerRatingSnapshot is a public read of one player's ratings.
type PlayerRatingSnapshot struct {
	PlayerID                  PlayerID
	CasualElo                 int
	TournamentPlacementRating int
}

// LeaderboardEntry is one row in a public leaderboard ordering.
type LeaderboardEntry struct {
	PlayerID PlayerID
	Rating   int
}

// ApplyCasualEloUpdateCommand applies one eligible GameCompleted placement result.
type ApplyCasualEloUpdateCommand struct {
	CommandID     CommandID
	EventID       EventID
	PlayerID      PlayerID
	GameID        GameID
	RoomID        RoomID
	RoomType      RoomType
	IsAbandoned   bool
	Authoritative bool
	Completed     bool
	Participants  []RatedPlacement
}

// ApplyTournamentPlacementUpdateCommand applies a tournament placement/final standing fact.
type ApplyTournamentPlacementUpdateCommand struct {
	CommandID        CommandID
	EventID          EventID
	PlayerID         PlayerID
	TournamentID     TournamentID
	PlacementEventID PlacementEventID
	Placement        int
	Delta            int
	Reason           RatingHistoryReason
}

// PublishLeaderboardSnapshotCommand publishes an ordered public leaderboard snapshot.
// Snapshot generation may repeat safely. BoardType uses RatingSourceType.
type PublishLeaderboardSnapshotCommand struct {
	CommandID  CommandID
	SnapshotID SnapshotID
	BoardType  RatingSourceType
	Entries    []LeaderboardEntry
}
