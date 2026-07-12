package app

import (
	"testing"
	"time"

	"unoarena/services/room-gameplay/domain"
)

func TestBuildFeedEvents_OutboxClassification(t *testing.T) {
	room, out := domain.CreateRoom(domain.CreateRoomCommand{
		CommandID: "c0", RoomID: "room_ob_cls", HostID: "host",
		Visibility: domain.VisibilityPublic, MaxSeats: 2,
	})
	if out.Rejection != nil {
		t.Fatal(out.Rejection)
	}
	sess := domain.OpenSession(room)
	evs, _ := BuildFeedEvents(sess, 1, 1, "corr", "c0", out.Facts, []FeedAudience{
		{PlayerID: "host", SessionID: "s1"},
	}, 0, time.Unix(1, 0).UTC())
	if len(evs) < 2 {
		t.Fatalf("expected player + spectator events, got %d", len(evs))
	}

	var realtime, integration int
	for _, ev := range evs {
		kind, err := ClassifyOutboxEvent(ev)
		if err != nil {
			t.Fatalf("classify %#v: %v", ev, err)
		}
		switch kind {
		case OutboxRealtime:
			realtime++
			if ev.Stream != StreamPlayer {
				t.Fatalf("realtime event stream=%q", ev.Stream)
			}
			if ev.Topic != "" {
				t.Fatalf("player realtime must not carry kafka topic, got %q", ev.Topic)
			}
		case OutboxIntegration:
			integration++
			if ev.Stream != StreamSpectator {
				t.Fatalf("integration feed event stream=%q", ev.Stream)
			}
			if ev.Topic != TopicSpectatorSafe {
				t.Fatalf("spectator topic=%q", ev.Topic)
			}
		default:
			t.Fatalf("unexpected kind %q for %#v", kind, ev)
		}
	}
	if realtime < 1 {
		t.Fatalf("realtime=%d want >=1", realtime)
	}
	if integration != 1 {
		t.Fatalf("integration(spectator)=%d want 1", integration)
	}

	topicOnly := PublishedEvent{EventID: "e3", EventType: "GameCompleted", Topic: TopicGameCompleted, RoomID: "room_ob_cls"}
	kind, err := ClassifyOutboxEvent(topicOnly)
	if err != nil || kind != OutboxIntegration {
		t.Fatalf("topic-only classify=%q err=%v", kind, err)
	}

	_, err = ClassifyOutboxEvent(PublishedEvent{Stream: "mystery", EventID: "bad"})
	if err == nil {
		t.Fatal("unknown nonempty stream without topic must fail closed")
	}
}

func TestClassifyOutboxEvent_PlayerOnlyRealtime(t *testing.T) {
	kind, err := ClassifyOutboxEvent(PublishedEvent{Stream: StreamPlayer, EventID: "p1"})
	if err != nil || kind != OutboxRealtime {
		t.Fatalf("got %q err=%v", kind, err)
	}
	kind, err = ClassifyOutboxEvent(PublishedEvent{Stream: StreamSpectator, Topic: TopicSpectatorSafe, EventID: "s1"})
	if err != nil || kind != OutboxIntegration {
		t.Fatalf("spectator got %q err=%v", kind, err)
	}
}
