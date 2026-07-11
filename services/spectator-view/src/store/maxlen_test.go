package store_test

import (
	"testing"

	"unoarena/services/spectator-view/store"
)

func TestParseStreamMaxLenEnv(t *testing.T) {
	if _, ok, err := store.ParseStreamMaxLenEnv(""); err != nil || ok {
		t.Fatalf("empty should be unset, ok=%v err=%v", ok, err)
	}
	n, ok, err := store.ParseStreamMaxLenEnv("256")
	if err != nil || !ok || n != 256 {
		t.Fatalf("got n=%d ok=%v err=%v", n, ok, err)
	}
	if _, _, err := store.ParseStreamMaxLenEnv("0"); err == nil {
		t.Fatal("zero must error")
	}
	if _, _, err := store.ParseStreamMaxLenEnv("-1"); err == nil {
		t.Fatal("negative must error")
	}
}
