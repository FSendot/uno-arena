package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"

	"unoarena/services/game-integrity/domain"
)

const unitTestMasterKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestDecryptRoomEntry_RejectsSelfConsistentWrongRoomMetadata(t *testing.T) {
	keys, err := ParseDevKeyring("1:" + unitTestMasterKey)
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewDevKeyProviderFromKeyring(keys, 1)
	if err != nil {
		t.Fatal(err)
	}
	repo := &KurrentStreamRepository{provider: provider, dekCache: map[string]cachedStreamDEK{}}

	requested := domain.RoomID("room-requested-identity")
	stream := roomStreamName(requested)
	forgedRoom := "room-forged-other"

	ev, err := sealRoomEventForTest(provider, stream, forgedRoom, "evt-forged-room", "PlayCard", 0, 1, map[string]any{"n": 1})
	if err != nil {
		t.Fatal(err)
	}

	_, err = repo.decryptRoomEntry(context.Background(), stream, requested, 0, ev)
	if err == nil {
		t.Fatal("decryptRoomEntry must fail closed when metadata roomId != requested roomID")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "room") {
		t.Fatalf("expected room identity error, got: %v", err)
	}
}

func TestDecryptRoomEntry_RejectsStreamNameMismatchForRoom(t *testing.T) {
	keys, err := ParseDevKeyring("1:" + unitTestMasterKey)
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewDevKeyProviderFromKeyring(keys, 1)
	if err != nil {
		t.Fatal(err)
	}
	repo := &KurrentStreamRepository{provider: provider, dekCache: map[string]cachedStreamDEK{}}

	requested := domain.RoomID("room-stream-check")
	wrongStream := roomStreamName(domain.RoomID("room-other-stream"))
	ev, err := sealRoomEventForTest(provider, wrongStream, string(requested), "evt-stream-mismatch", "PlayCard", 0, 1, map[string]any{"n": 1})
	if err != nil {
		t.Fatal(err)
	}

	_, err = repo.decryptRoomEntry(context.Background(), wrongStream, requested, 0, ev)
	if err == nil {
		t.Fatal("decryptRoomEntry must fail when stream != roomStreamName(requestedRoomID)")
	}
}

func sealRoomEventForTest(provider KeyProvider, stream, roomID, eventID, eventType string, kurrentRev, domainRev uint64, payload any) (*kurrentdb.ResolvedEvent, error) {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	rec := roomPlaintextV1{
		EventID:   eventID,
		EventType: eventType,
		Payload:   payloadJSON,
	}
	plain, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}
	dek := make([]byte, dekSizeBytes)
	if _, err := io.ReadFull(rand.Reader, dek); err != nil {
		return nil, err
	}
	wrapped, wrapNonce, err := provider.WrapDEK(context.Background(), dek)
	if err != nil {
		return nil, err
	}
	payloadNonce := make([]byte, gcmNonceSize)
	if _, err := io.ReadFull(rand.Reader, payloadNonce); err != nil {
		return nil, err
	}
	eventUUID := deterministicEventUUID(stream, eventID)
	meta := envelopeMetadataV1{
		EnvelopeVersion:   envelopeVersionV1,
		KeyVersion:        provider.KeyVersion(),
		WrappedDEK:        hexBytes(wrapped),
		WrapNonce:         hexBytes(wrapNonce),
		PayloadNonce:      hexBytes(payloadNonce),
		OriginalEventID:   eventID,
		OriginalEventType: eventType,
		Stream:            stream,
		RoomID:            roomID,
		KurrentRevision:   kurrentRev,
		DomainRevision:    domainRev,
		EventUUID:         eventUUID.String(),
	}
	ct, err := SealPayloadWithNonce(dek, meta.canonicalAAD(), payloadNonce, plain)
	if err != nil {
		return nil, err
	}
	metaBytes, err := meta.marshal()
	if err != nil {
		return nil, err
	}
	return &kurrentdb.ResolvedEvent{Event: &kurrentdb.RecordedEvent{
		EventID:      eventUUID,
		EventType:    eventType,
		EventNumber:  kurrentRev,
		Data:         ct,
		UserMetadata: metaBytes,
	}}, nil
}
