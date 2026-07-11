package domain

// DrawCommand requests a sequential draw from an AuthoritativeDeck.
type DrawCommand struct {
	OperationID DrawOperationID
	Count       int
}

// AppendCommand requests an append to a GameLog with expected-revision guard.
// GameID is optional metadata (empty for lifecycle-before-game).
type AppendCommand struct {
	EventID          EventID
	ExpectedRevision Revision
	EventType        string
	GameID           GameID
	Payload          []byte
}
