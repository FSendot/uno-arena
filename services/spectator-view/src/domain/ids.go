package domain

// Typed identity value objects for SpectatorRoomProjection.

type RoomID string

func (id RoomID) String() string { return string(id) }
func (id RoomID) Valid() bool    { return id != "" }

type EventID string

func (id EventID) String() string { return string(id) }
func (id EventID) Valid() bool    { return id != "" }

type PlayerID string

func (id PlayerID) String() string { return string(id) }
func (id PlayerID) Valid() bool    { return id != "" }

type SequenceNumber uint64

// CurrentSchemaVersion is the only supported spectator-safe schema version.
const CurrentSchemaVersion = 1
