package game

import (
	"encoding/json"
	"testing"
)

func TestCard_MarshalJSON_CamelCaseDTO(t *testing.T) {
	card := Card{ID: "card-7", Color: ColorRed, Face: Face7}
	raw, err := json.Marshal(card)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"id":"card-7","color":"red","face":"7"}`
	if string(raw) != want {
		t.Fatalf("marshal=%s want=%s", raw, want)
	}

	wild := Card{ID: "w1", Color: ColorNone, Face: FaceWild}
	raw, err = json.Marshal(wild)
	if err != nil {
		t.Fatal(err)
	}
	want = `{"id":"w1","color":"","face":"wild"}`
	if string(raw) != want {
		t.Fatalf("wild marshal=%s want=%s", raw, want)
	}
}
