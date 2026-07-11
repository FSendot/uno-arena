package domain

import "time"

// Player is the registered identity record.
type Player struct {
	ID           PlayerID
	Username     string
	PasswordHash string
	CreatedAt    time.Time
}
