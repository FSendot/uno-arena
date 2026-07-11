package game

// Color is a canonical Uno card color. Wild faces use ColorNone.
type Color string

const (
	ColorNone   Color = ""
	ColorRed    Color = "red"
	ColorYellow Color = "yellow"
	ColorGreen  Color = "green"
	ColorBlue   Color = "blue"
)

// AllColors is the four playable colors (excludes ColorNone).
var AllColors = []Color{ColorRed, ColorYellow, ColorGreen, ColorBlue}

// Face is a canonical rank or action symbol.
type Face string

const (
	Face0            Face = "0"
	Face1            Face = "1"
	Face2            Face = "2"
	Face3            Face = "3"
	Face4            Face = "4"
	Face5            Face = "5"
	Face6            Face = "6"
	Face7            Face = "7"
	Face8            Face = "8"
	Face9            Face = "9"
	FaceSkip         Face = "skip"
	FaceReverse      Face = "reverse"
	FaceDrawTwo      Face = "draw_two"
	FaceWild         Face = "wild"
	FaceWildDrawFour Face = "wild_draw_four"
)

// CardID uniquely identifies one physical card instance in a deal.
type CardID string

// Card is an immutable card value object.
// JSON tags match the OpenAPI Card DTO used in player-private snapshots.
type Card struct {
	ID    CardID `json:"id"`
	Color Color  `json:"color"`
	Face  Face   `json:"face"`
}

// PublicFace returns a spectator-safe discard label (color+face, never CardID).
func (c Card) PublicFace() string {
	if c.IsWild() || c.Color == ColorNone {
		return string(c.Face)
	}
	return string(c.Color) + "-" + string(c.Face)
}

func (c Card) IsWild() bool {
	return c.Face == FaceWild || c.Face == FaceWildDrawFour
}

func (c Card) IsDrawCard() bool {
	return c.Face == FaceDrawTwo || c.Face == FaceWildDrawFour
}

func (c Card) DrawValue() int {
	switch c.Face {
	case FaceDrawTwo:
		return 2
	case FaceWildDrawFour:
		return 4
	default:
		return 0
	}
}

// Points returns standard Uno card-point value for scoring.
func (c Card) Points() int {
	switch c.Face {
	case Face0, Face1, Face2, Face3, Face4, Face5, Face6, Face7, Face8, Face9:
		return int(c.Face[0] - '0')
	case FaceSkip, FaceReverse, FaceDrawTwo:
		return 20
	case FaceWild, FaceWildDrawFour:
		return 50
	default:
		return 0
	}
}

// ExactMatch reports jump-in equality: same color and face. Wilds never match.
func ExactMatch(a, b Card) bool {
	if a.IsWild() || b.IsWild() {
		return false
	}
	return a.Color == b.Color && a.Face == b.Face
}

// StandardDeckComposition returns the canonical 108-card Uno multiset without IDs.
// Game Integrity assigns IDs and shuffles; the engine never randomizes.
func StandardDeckComposition() []Card {
	out := make([]Card, 0, 108)
	for _, color := range AllColors {
		out = append(out, Card{Color: color, Face: Face0})
		for _, face := range []Face{
			Face1, Face2, Face3, Face4, Face5, Face6, Face7, Face8, Face9,
			FaceSkip, FaceReverse, FaceDrawTwo,
		} {
			out = append(out, Card{Color: color, Face: face}, Card{Color: color, Face: face})
		}
	}
	for i := 0; i < 4; i++ {
		out = append(out, Card{Color: ColorNone, Face: FaceWild})
		out = append(out, Card{Color: ColorNone, Face: FaceWildDrawFour})
	}
	return out
}

// HandPoints sums card points in a hand.
func HandPoints(hand []Card) int {
	total := 0
	for _, c := range hand {
		total += c.Points()
	}
	return total
}
