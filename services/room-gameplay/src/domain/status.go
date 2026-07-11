package domain

// RoomStatus is the room lifecycle state.
type RoomStatus string

const (
	RoomStatusWaiting    RoomStatus = "waiting"
	RoomStatusLocked     RoomStatus = "locked"
	RoomStatusInProgress RoomStatus = "in_progress"
	RoomStatusCompleted  RoomStatus = "completed"
	RoomStatusCancelled  RoomStatus = "cancelled"
)

func (s RoomStatus) String() string { return string(s) }

func (s RoomStatus) IsTerminal() bool {
	return s == RoomStatusCompleted || s == RoomStatusCancelled
}

func (s RoomStatus) AllowsSpectatorAdmission() bool {
	switch s {
	case RoomStatusWaiting, RoomStatusLocked, RoomStatusInProgress:
		return true
	default:
		return false
	}
}

func (s RoomStatus) HostHasGameplayAuthority() bool {
	// Host may configure/lock/start only before lock/start completes.
	// After lock or start, host labels have no gameplay authority.
	return s == RoomStatusWaiting
}

// RoomType distinguishes ad-hoc casual rooms from tournament-provisioned rooms.
type RoomType string

const (
	RoomTypeAdHoc      RoomType = "ad_hoc"
	RoomTypeTournament RoomType = "tournament"
)

func (t RoomType) String() string { return string(t) }

// Visibility controls spectator authorization requirements.
type Visibility string

const (
	VisibilityPublic  Visibility = "public"
	VisibilityPrivate Visibility = "private"
)

func (v Visibility) String() string { return string(v) }
