package domain

// Typed identity value objects.

type PlayerID string

func (id PlayerID) String() string { return string(id) }
func (id PlayerID) Valid() bool    { return id != "" }

type SessionID string

func (id SessionID) String() string { return string(id) }
func (id SessionID) Valid() bool    { return id != "" }
