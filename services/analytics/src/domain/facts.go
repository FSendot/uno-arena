package domain

// FactName identifies a named domain fact emitted by projection handling.
type FactName string

const (
	FactPublicGameplayMetricProjected      FactName = "PublicGameplayMetricProjected"
	FactPublicTournamentStatisticProjected FactName = "PublicTournamentStatisticProjected"
	FactPublicRatingStatisticProjected     FactName = "PublicRatingStatisticProjected"
	FactProjectionEventQuarantined         FactName = "ProjectionEventQuarantined"
)

// Fact is a named domain fact produced by projection handling.
type Fact struct {
	Name FactName
	Data map[string]string
}

func newFact(name FactName, data map[string]string) Fact {
	return Fact{Name: name, Data: copyStringMap(data)}
}

func copyFacts(in []Fact) []Fact {
	if len(in) == 0 {
		return []Fact{}
	}
	out := make([]Fact, len(in))
	for i, f := range in {
		out[i] = Fact{Name: f.Name, Data: copyStringMap(f.Data)}
	}
	return out
}

func copyStringMap(in map[string]string) map[string]string {
	if in == nil {
		return map[string]string{}
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func u64toa(n uint64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
