package domain

import "time"

const ReconnectWindowDuration = 60 * time.Second

// DisconnectState tracks one disconnect episode with an absolute UTC deadline
// keyed by disconnectVersion so timer retries cannot forfeit a newer attempt.
type DisconnectState struct {
	PlayerID          PlayerID
	DisconnectVersion DisconnectVersion
	DeadlineUTC       time.Time
	Active            bool
}

// BeginDisconnect opens a reconnect window ending at nowUTC+60s.
func BeginDisconnect(playerID PlayerID, version DisconnectVersion, nowUTC time.Time) DisconnectState {
	nowUTC = nowUTC.UTC()
	return DisconnectState{
		PlayerID:          playerID,
		DisconnectVersion: version,
		DeadlineUTC:       nowUTC.Add(ReconnectWindowDuration),
		Active:            true,
	}
}

// CanReconnect is true while the window is active and nowUTC is strictly before DeadlineUTC.
// At the exact deadline instant reconnect is denied (boundary closed).
func (d DisconnectState) CanReconnect(playerID PlayerID, version DisconnectVersion, nowUTC time.Time) (bool, RejectionCode) {
	if !d.Active || d.PlayerID != playerID {
		return false, RejectDisconnectInactive
	}
	if d.DisconnectVersion != version {
		return false, RejectDisconnectVersion
	}
	if !nowUTC.UTC().Before(d.DeadlineUTC) {
		return false, RejectReconnectExpired
	}
	return true, ""
}

// CanForfeit is true when the window is active, versions match, and nowUTC is at or after DeadlineUTC.
func (d DisconnectState) CanForfeit(playerID PlayerID, version DisconnectVersion, nowUTC time.Time) (bool, RejectionCode) {
	if !d.Active || d.PlayerID != playerID {
		return false, RejectDisconnectInactive
	}
	if d.DisconnectVersion != version {
		return false, RejectDisconnectVersion
	}
	if nowUTC.UTC().Before(d.DeadlineUTC) {
		return false, RejectForfeitNotDue
	}
	return true, ""
}

func (d DisconnectState) Clear() DisconnectState {
	d.Active = false
	return d
}
