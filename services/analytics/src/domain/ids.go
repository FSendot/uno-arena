package domain

// Typed identity value objects for PublicAnalyticsProjection.

type EventID string

func (id EventID) String() string { return string(id) }
func (id EventID) Valid() bool    { return id != "" }

type RoomID string

func (id RoomID) String() string { return string(id) }
func (id RoomID) Valid() bool    { return id != "" }

type GameID string

func (id GameID) String() string { return string(id) }
func (id GameID) Valid() bool    { return id != "" }

type TournamentID string

func (id TournamentID) String() string { return string(id) }
func (id TournamentID) Valid() bool    { return id != "" }

type PlayerID string

func (id PlayerID) String() string { return string(id) }
func (id PlayerID) Valid() bool    { return id != "" }

type SnapshotID string

func (id SnapshotID) String() string { return string(id) }
func (id SnapshotID) Valid() bool    { return id != "" }

// ProjectionVersion tracks rebuildable projection generations.
type ProjectionVersion uint64

// CurrentSchemaVersion is the only supported analytics ingestion schema version.
const CurrentSchemaVersion = 1
