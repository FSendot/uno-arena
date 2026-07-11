package domain

// Typed identity value objects for Game Integrity aggregates.

type GameID string

func (id GameID) String() string { return string(id) }
func (id GameID) Valid() bool    { return id != "" }

type RoomID string

func (id RoomID) String() string { return string(id) }
func (id RoomID) Valid() bool    { return id != "" }

type EventID string

func (id EventID) String() string { return string(id) }
func (id EventID) Valid() bool    { return id != "" }

type DrawOperationID string

func (id DrawOperationID) String() string { return string(id) }
func (id DrawOperationID) Valid() bool    { return id != "" }

// Card is an opaque card identity for the authoritative draw stream.
type Card string

// Revision is the gapless append count / expected-revision guard for GameLog.
// Empty log has Revision 0; after N appends, Revision equals N.
type Revision uint64

// LogOffset is the zero-based chronological position of a GameLogEntry.
type LogOffset uint64
