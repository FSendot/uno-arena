package main

import (
	"testing"

	"github.com/google/uuid"
	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
)

func TestAuthenticateEnvelopeMetadata_FieldGroups(t *testing.T) {
	stream := "gi.room.v1.test"
	meta := envelopeMetadataV1{
		EnvelopeVersion:   envelopeVersionV1,
		KeyVersion:        1,
		WrappedDEK:        "aa",
		WrapNonce:         "bb",
		PayloadNonce:      "cc",
		OriginalEventID:   "evt-1",
		OriginalEventType: "PlayCard",
		Stream:            stream,
		RoomID:            "r1",
		GameID:            "g1",
		KurrentRevision:   0,
		DomainRevision:    1,
		EventUUID:         deterministicEventUUID(stream, "evt-1").String(),
	}
	ev := &kurrentdb.ResolvedEvent{Event: &kurrentdb.RecordedEvent{
		EventID:     deterministicEventUUID(stream, "evt-1"),
		EventType:   "PlayCard",
		EventNumber: 0,
	}}
	if err := authenticateEnvelopeMetadata(meta, stream, 0, ev); err != nil {
		t.Fatalf("valid meta: %v", err)
	}

	badStream := meta
	badStream.Stream = "other"
	if err := authenticateEnvelopeMetadata(badStream, stream, 0, ev); err == nil {
		t.Fatal("stream mismatch must fail")
	}

	badType := meta
	badType.OriginalEventType = "JoinRoom"
	if err := authenticateEnvelopeMetadata(badType, stream, 0, ev); err == nil {
		t.Fatal("event type mismatch must fail")
	}

	badUUID := meta
	badUUID.EventUUID = uuid.NewString()
	if err := authenticateEnvelopeMetadata(badUUID, stream, 0, ev); err == nil {
		t.Fatal("uuid metadata mismatch must fail")
	}

	badRev := meta
	if err := authenticateEnvelopeMetadata(badRev, stream, 1, ev); err == nil {
		t.Fatal("revision mismatch must fail")
	}

	badKey := meta
	badKey.WrappedDEK = ""
	if err := authenticateEnvelopeMetadata(badKey, stream, 0, ev); err == nil {
		t.Fatal("missing wrapped DEK must fail")
	}

	missingUUID := meta
	missingUUID.EventUUID = ""
	if err := authenticateEnvelopeMetadata(missingUUID, stream, 0, ev); err == nil {
		t.Fatal("missing eventUuid must fail closed")
	}

	badDomain := meta
	badDomain.DomainRevision = 99
	if err := authenticateEnvelopeMetadata(badDomain, stream, 0, ev); err == nil {
		t.Fatal("domainRevision mismatch must fail")
	}

	wrongRoom := meta
	if err := authenticateEnvelopeMetadataForAggregate(wrongRoom, stream, 0, ev, "other-room", "g1"); err == nil {
		t.Fatal("roomId mismatch must fail")
	}
	wrongGame := meta
	if err := authenticateEnvelopeMetadataForAggregate(wrongGame, stream, 0, ev, "r1", "other-game"); err == nil {
		t.Fatal("gameId mismatch must fail")
	}
}
