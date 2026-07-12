package store

import (
	"os"
	"strings"
	"testing"
)

func TestPublicListSQL_NoOffset(t *testing.T) {
	// Guard the durable public-list query shape: keyset only, never OFFSET.
	src, err := os.ReadFile("public_list.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(src)
	if !strings.Contains(body, "ORDER BY r.room_id ASC") {
		t.Fatal("expected room_id keyset order")
	}
	if !strings.Contains(body, "r.room_id > $2") {
		t.Fatal("expected exclusive keyset boundary")
	}
	if strings.Contains(strings.ToUpper(body), " OFFSET ") || strings.Contains(strings.ToUpper(body), "OFFSET ") {
		t.Fatal("public list must not use OFFSET")
	}
	if !strings.Contains(body, "visibility = 'public'") {
		t.Fatal("expected public visibility filter")
	}
}
