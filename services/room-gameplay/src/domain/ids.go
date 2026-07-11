package domain

// Typed identity value objects for the Room aggregate.

type RoomID string

func (id RoomID) String() string { return string(id) }

func (id RoomID) Valid() bool { return id != "" }

type PlayerID string

func (id PlayerID) String() string { return string(id) }

func (id PlayerID) Valid() bool { return id != "" }

type CommandID string

func (id CommandID) String() string { return string(id) }

func (id CommandID) Valid() bool { return id != "" }

type TournamentID string

func (id TournamentID) String() string { return string(id) }

func (id TournamentID) Valid() bool { return id != "" }

type GameID string

func (id GameID) String() string { return string(id) }

func (id GameID) Valid() bool { return id != "" }

type TriggeringGameEventID string

func (id TriggeringGameEventID) String() string { return string(id) }

func (id TriggeringGameEventID) Valid() bool { return id != "" }

// SeatIndex is the zero-based roster position. Lowest occupied seat wins host reassignment.
type SeatIndex uint8

// SequenceNumber is the monotonic room version used for command serialization.
type SequenceNumber uint64

// DisconnectVersion keys a reconnect/forfeit window for one disconnect episode.
type DisconnectVersion uint64
