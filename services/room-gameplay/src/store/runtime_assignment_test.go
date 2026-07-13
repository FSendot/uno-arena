package store

import "testing"

func TestRuntimePodNameIsDeterministicAndDNS1123Safe(t *testing.T) {
	first := RuntimePodName("room/with unsafe characters", 7)
	second := RuntimePodName("room/with unsafe characters", 7)
	if first != second {
		t.Fatalf("pod name must be deterministic: %q != %q", first, second)
	}
	if len(first) > 63 || first[:5] != "room-" {
		t.Fatalf("pod name must be a bounded room DNS label, got %q", first)
	}
	if RuntimePodName("room/with unsafe characters", 8) == first {
		t.Fatal("generation must fence a replacement pod name")
	}
}

func TestRuntimeAssignmentStateValidation(t *testing.T) {
	if !RuntimeDesiredRunning.Valid() || !RuntimeDesiredTerminal.Valid() {
		t.Fatal("documented desired states must be valid")
	}
	if RuntimeDesiredState("anything").Valid() {
		t.Fatal("unknown desired state must fail closed")
	}
}
