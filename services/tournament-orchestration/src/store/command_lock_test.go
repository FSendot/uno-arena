package store_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"unoarena/services/tournament-orchestration/store"
	"unoarena/shared/envelope"
)

func TestCommandLockUsesHashtextextended(t *testing.T) {
	if !store.CommandLockUsesHashtextextended() {
		t.Fatal("command lock must use hashtextextended 64-bit keys")
	}
	b, err := os.ReadFile(filepath.Join("command_lock.go"))
	if err != nil {
		t.Fatal(err)
	}
	src := string(b)
	if !strings.Contains(src, "tournament:command:") {
		t.Fatal("command lock namespace must be tournament:command:")
	}
	if strings.Contains(src, "pg_advisory_xact_lock(hashtext(") {
		t.Fatal("command lock must not use 32-bit hashtext")
	}
}

func TestRegistrationCloseElectionUsesHashtextextended(t *testing.T) {
	if !store.RegistrationCloseElectionUsesHashtextextended() {
		t.Fatal("registration-close election must use hashtextextended 64-bit keys")
	}
	b, err := os.ReadFile(filepath.Join("registration_close_election.go"))
	if err != nil {
		t.Fatal(err)
	}
	src := string(b)
	if !strings.Contains(src, "tournament:registration-close:") {
		t.Fatal("election namespace must be tournament:registration-close:")
	}
}

func TestPriorCommandOutcomeSentinel(t *testing.T) {
	prior := &store.PriorCommandOutcome{
		Outcome: envelope.Accepted("cmd-1", "RegisterPlayer", nil, nil),
	}
	got, ok := store.AsPriorCommandOutcome(prior)
	if !ok || got.CommandID != "cmd-1" {
		t.Fatalf("AsPriorCommandOutcome: ok=%v got=%#v", ok, got)
	}
	if !strings.Contains(prior.Error(), "prior") {
		t.Fatalf("error=%q", prior.Error())
	}
}
