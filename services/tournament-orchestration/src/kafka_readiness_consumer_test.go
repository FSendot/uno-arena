package main

import (
	"context"
	"errors"
	"testing"
)

type fakeReadinessIngester struct {
	calls []RoomRuntimeReadyEvent
	err   error
}

func (f *fakeReadinessIngester) IngestRoomRuntimeReady(_ context.Context, evt RoomRuntimeReadyEvent) (bool, error) {
	f.calls = append(f.calls, evt)
	return f.err == nil, f.err
}

func readinessRecord() ConsumerRecord {
	return ConsumerRecord{
		Topic: DefaultRoomRuntimeReadyTopic, Partition: 1, Offset: 9, Key: []byte("r1"),
		Value: []byte(`{"schemaVersion":1,"eventId":"room-runtime-ready:r1","eventType":"RoomRuntimeReady","correlationId":"corr-1","occurredAt":"2026-07-13T10:00:00Z","roomId":"r1","tournamentId":"t1","roundNumber":2,"slotId":"s7","generation":1}`),
	}
}

func TestRoomRuntimeReadyConsumer_AppliesThenCommits(t *testing.T) {
	src, dlq, ready := &fakeSource{}, &fakeDLQ{}, &fakeReadinessIngester{}
	c := newTestConsumer(src, dlq, &fakeHandler{})
	c.readiness = ready
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{readinessRecord()}); err != nil {
		t.Fatal(err)
	}
	if len(ready.calls) != 1 || ready.calls[0].RoomID != "r1" {
		t.Fatalf("calls=%+v", ready.calls)
	}
	if got := src.committedOffsets(); len(got) != 1 || got[0] != 9 {
		t.Fatalf("commits=%v", got)
	}
	if len(dlq.publications()) != 0 {
		t.Fatal("successful readiness must not publish DLQ")
	}
}

func TestRoomRuntimeReadyConsumer_TerminalFailureUsesReadinessSource(t *testing.T) {
	src, dlq := &fakeSource{}, &fakeDLQ{}
	ready := &fakeReadinessIngester{err: newTerminalKafkaError(KafkaFailurePayloadInvalid, errors.New("assignment mismatch"))}
	c := newTestConsumer(src, dlq, &fakeHandler{})
	c.readiness = ready
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{readinessRecord()}); err != nil {
		t.Fatal(err)
	}
	pubs := dlq.publications()
	if len(pubs) != 1 || pubs[0].Meta.SourceTopic != DefaultRoomRuntimeReadyTopic || pubs[0].Meta.AttemptCount != 1 {
		t.Fatalf("DLQ=%+v", pubs)
	}
}
