package domain

// Seat is a roster position that may be occupied by a player.
type Seat struct {
	Index    SeatIndex
	PlayerID PlayerID
	Occupied bool
}

// Roster is the ordered set of seats for a room.
type Roster struct {
	seats    []Seat
	capacity int
}

func newRoster(capacity int) Roster {
	if capacity < MinMaxSeats || capacity > DefaultMaxSeats {
		capacity = DefaultMaxSeats
	}
	seats := make([]Seat, capacity)
	for i := range seats {
		seats[i] = Seat{Index: SeatIndex(i)}
	}
	return Roster{seats: seats, capacity: capacity}
}

func (r Roster) Capacity() int { return r.capacity }

func (r Roster) OccupiedCount() int {
	n := 0
	for _, s := range r.seats {
		if s.Occupied {
			n++
		}
	}
	return n
}

func (r Roster) Seats() []Seat {
	out := make([]Seat, len(r.seats))
	copy(out, r.seats)
	return out
}

func (r Roster) SeatOf(playerID PlayerID) (Seat, bool) {
	for _, s := range r.seats {
		if s.Occupied && s.PlayerID == playerID {
			return s, true
		}
	}
	return Seat{}, false
}

func (r Roster) IsSeated(playerID PlayerID) bool {
	_, ok := r.SeatOf(playerID)
	return ok
}

func (r *Roster) assignLowestEmpty(playerID PlayerID) (Seat, bool) {
	for i := range r.seats {
		if !r.seats[i].Occupied {
			r.seats[i].Occupied = true
			r.seats[i].PlayerID = playerID
			return r.seats[i], true
		}
	}
	return Seat{}, false
}

func (r *Roster) clear(playerID PlayerID) (Seat, bool) {
	for i := range r.seats {
		if r.seats[i].Occupied && r.seats[i].PlayerID == playerID {
			cleared := r.seats[i]
			r.seats[i].Occupied = false
			r.seats[i].PlayerID = ""
			return cleared, true
		}
	}
	return Seat{}, false
}

// LowestOccupiedSeat returns the occupied seat with the smallest index.
func (r Roster) LowestOccupiedSeat() (Seat, bool) {
	for _, s := range r.seats {
		if s.Occupied {
			return s, true
		}
	}
	return Seat{}, false
}

const (
	MinMaxSeats      = 2
	DefaultMaxSeats  = 10
	MaxMaxSeats      = 10
	MinPlayersToLock = 2
)

// resolveMaxSeats applies documented defaults and inclusive 2..10 bounds.
// Zero/nonpositive means default 10; explicit 1 or >10 is invalid (also prevents uint8 seat wrap).
func resolveMaxSeats(requested int) (int, bool) {
	if requested <= 0 {
		return DefaultMaxSeats, true
	}
	if requested < MinMaxSeats || requested > MaxMaxSeats {
		return 0, false
	}
	return requested, true
}
