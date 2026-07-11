package main

import (
	"context"
	"reflect"
	"testing"
)

func TestDecryptTrace_CollectsSortedUniqueVersions(t *testing.T) {
	ctx, trace := withDecryptTrace(context.Background())
	if fromContextDecryptTrace(ctx) != trace {
		t.Fatal("trace must round-trip via context")
	}
	trace.noteKeyVersion(3)
	trace.noteKeyVersion(1)
	trace.noteKeyVersion(3)
	trace.noteKeyVersion(2)
	got := trace.keyVersions()
	want := []int{1, 2, 3}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("versions=%v want %v", got, want)
	}
}

func TestDecryptTrace_EmptyWhenNoVersions(t *testing.T) {
	_, trace := withDecryptTrace(context.Background())
	if got := trace.keyVersions(); len(got) != 0 {
		t.Fatalf("empty trace versions=%v", got)
	}
	if !trace.unknown() {
		t.Fatal("empty trace must report unknown")
	}
}

func TestNewAuditRecord_RandomAttemptIDsAndKeyVersions(t *testing.T) {
	a := newAuditRecord("replay", "op", "reason", "r1", "", "stream", "c1", []int{2, 1}, "success", "")
	b := newAuditRecord("replay", "op", "reason", "r1", "", "stream", "c1", []int{2, 1}, "success", "")
	if a.EventID == "" || b.EventID == "" {
		t.Fatal("attempt ids required")
	}
	if a.EventID == b.EventID {
		t.Fatalf("repeated attempts must not share event id: %s", a.EventID)
	}
	if !reflect.DeepEqual(a.KeyVersions, []int{1, 2}) {
		t.Fatalf("sorted unique keyVersions: %+v", a.KeyVersions)
	}
	if a.KeyVersion != 0 {
		t.Fatalf("keyVersion must be unset when multiple versions, got %d", a.KeyVersion)
	}
	single := newAuditRecord("export", "op", "r", "r1", "g1", "s", "c", []int{7}, "success", "")
	if single.KeyVersion != 7 || !reflect.DeepEqual(single.KeyVersions, []int{7}) {
		t.Fatalf("single version compat: %+v", single)
	}
	unknown := newAuditRecord("replay", "op", "r", "r1", "", "s", "c", nil, "failure", "before decrypt")
	if len(unknown.KeyVersions) != 0 || unknown.KeyVersion != 0 {
		t.Fatalf("unknown versions must be empty: %+v", unknown)
	}
}
