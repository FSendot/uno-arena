package store_test

import (
	"strconv"
	"testing"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/services/tournament-orchestration/store"
)

func TestSlotSizeFromRange(t *testing.T) {
	got, err := store.SlotSizeFromRange("slot_0", "slot_99")
	if err != nil {
		t.Fatal(err)
	}
	if got != 100 {
		t.Fatalf("got %d want 100", got)
	}
	got, err = store.SlotSizeFromRange("slot_10", "slot_10")
	if err != nil || got != 1 {
		t.Fatalf("got %d err=%v", got, err)
	}
	if _, err := store.SlotSizeFromRange("slot_5", "slot_4"); err == nil {
		t.Fatal("expected inverted range error")
	}
	if _, err := store.SlotSizeFromRange("bad", "slot_1"); err == nil {
		t.Fatal("expected invalid from")
	}
}

func TestSlotSizeFromRange_AtMaxBoundary(t *testing.T) {
	got, err := store.SlotSizeFromRange("slot_0", "slot_"+strconv.Itoa(domain.MaxProvisioningBatchSize-1))
	if err != nil {
		t.Fatal(err)
	}
	if got != domain.MaxProvisioningBatchSize {
		t.Fatalf("got %d want %d", got, domain.MaxProvisioningBatchSize)
	}
	got, err = store.SlotSizeFromRange("slot_0", "slot_"+strconv.Itoa(domain.MaxProvisioningBatchSize))
	if err != nil {
		t.Fatal(err)
	}
	if got != domain.MaxProvisioningBatchSize+1 {
		t.Fatalf("got %d", got)
	}
}
