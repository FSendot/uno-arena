package domain

// Typed identity value objects for the PlayerRating aggregate.

type PlayerID string

func (id PlayerID) String() string { return string(id) }
func (id PlayerID) Valid() bool    { return id != "" }

type CommandID string

func (id CommandID) String() string { return string(id) }
func (id CommandID) Valid() bool    { return id != "" }

type EventID string

func (id EventID) String() string { return string(id) }
func (id EventID) Valid() bool    { return id != "" }

type GameID string

func (id GameID) String() string { return string(id) }
func (id GameID) Valid() bool    { return id != "" }

type RoomID string

func (id RoomID) String() string { return string(id) }
func (id RoomID) Valid() bool    { return id != "" }

type TournamentID string

func (id TournamentID) String() string { return string(id) }
func (id TournamentID) Valid() bool    { return id != "" }

type PlacementEventID string

func (id PlacementEventID) String() string { return string(id) }
func (id PlacementEventID) Valid() bool    { return id != "" }

type SnapshotID string

func (id SnapshotID) String() string { return string(id) }
func (id SnapshotID) Valid() bool    { return id != "" }
