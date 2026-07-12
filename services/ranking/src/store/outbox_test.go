package store

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"unoarena/services/ranking/domain"
)

func TestOutboxFromFact_CasualPlayerRatingUpdated(t *testing.T) {
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	ev, err := outboxFromFact(domain.Fact{
		Name: domain.FactPlayerRatingUpdated,
		Data: map[string]string{
			"playerId": "p1", "gameId": "g1", "eventId": "e1",
			"previousRating": "1000", "newRating": "1016",
		},
	}, outboxMeta{
		UpstreamEventID: "e1",
		CorrelationID:   "corr-body",
		CausationID:     "cmd-1",
		Now:             now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ev.EventType != msgPlayerRatingUpdated || ev.Topic != topicPlayerRatingUpdated {
		t.Fatalf("event_type/topic: %s %s", ev.EventType, ev.Topic)
	}
	assertRequiredEventMetadata(t, ev.Payload, msgPlayerRatingUpdated)
	if ev.Payload["correlationId"] != "corr-body" {
		t.Fatalf("correlationId=%v", ev.Payload["correlationId"])
	}
	if ev.Payload["causationId"] != "cmd-1" {
		t.Fatalf("causationId=%v", ev.Payload["causationId"])
	}
	if ev.Payload["gameId"] != "g1" || ev.Payload["playerId"] != "p1" {
		t.Fatalf("body fields: %+v", ev.Payload)
	}
}

func TestOutboxFromFact_PlacementMapsToPlayerRatingUpdated(t *testing.T) {
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	ev, err := outboxFromFact(domain.Fact{
		Name: domain.FactTournamentPlacementRatingUpdated,
		Data: map[string]string{
			"playerId": "p1", "tournamentId": "t1", "placementEventId": "pe1",
			"eventId": "te1", "previousRating": "0", "newRating": "50",
		},
	}, outboxMeta{
		UpstreamEventID: "te1",
		CorrelationID:   "corr-place",
		CausationID:     "tp1",
		Now:             now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ev.EventType != msgPlayerRatingUpdated {
		t.Fatalf("external event_type want PlayerRatingUpdated, got %s", ev.EventType)
	}
	if ev.Topic != topicPlayerRatingUpdated {
		t.Fatalf("topic=%s", ev.Topic)
	}
	assertRequiredEventMetadata(t, ev.Payload, msgPlayerRatingUpdated)
	if ev.Payload["tournamentId"] != "t1" || ev.Payload["placementEventId"] != "pe1" {
		t.Fatalf("placement fields: %+v", ev.Payload)
	}
	if strings.Contains(ev.EventID, "TournamentPlacementRatingUpdated") {
		t.Fatalf("outbox event_id must use external message name: %s", ev.EventID)
	}
}

func TestOutboxFromFact_CorrelationFallbackRootedInEventID(t *testing.T) {
	ev, err := outboxFromFact(domain.Fact{
		Name: domain.FactPlayerRatingUpdated,
		Data: map[string]string{
			"playerId": "p1", "gameId": "g1", "eventId": "e-fallback",
			"previousRating": "1000", "newRating": "1000",
		},
	}, outboxMeta{UpstreamEventID: "e-fallback", Now: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	assertRequiredEventMetadata(t, ev.Payload, msgPlayerRatingUpdated)
	if corr, _ := ev.Payload["correlationId"].(string); corr == "" || !strings.Contains(corr, "e-fallback") {
		t.Fatalf("fallback correlationId rooted in eventId, got %q", corr)
	}
}

func TestOutboxFromFact_LeaderboardSnapshotSatisfiesEventMetadata(t *testing.T) {
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	ev, err := outboxFromFact(domain.Fact{
		Name: domain.FactLeaderboardSnapshotPublished,
		Data: map[string]string{
			"snapshotId": "snap-1", "boardType": "casual_elo", "playerCount": "2",
			"rank_1": "alice", "rating_1": "1200",
			"rank_2": "bob", "rating_2": "1100",
		},
	}, outboxMeta{
		CorrelationID: "corr-snap",
		CausationID:   "rebuild-1",
		Now:           now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ev.EventType != msgLeaderboardSnapshotPublished {
		t.Fatalf("event_type=%s", ev.EventType)
	}
	assertRequiredEventMetadata(t, ev.Payload, msgLeaderboardSnapshotPublished)
	if ev.Payload["snapshotId"] != "snap-1" || ev.Payload["boardType"] != "casual_elo" {
		t.Fatalf("snapshot fields: %+v", ev.Payload)
	}
	entries, ok := ev.Payload["entries"].([]map[string]any)
	if !ok {
		t.Fatalf("entries type %T", ev.Payload["entries"])
	}
	if len(entries) != 2 {
		t.Fatalf("entries=%d", len(entries))
	}
	if entries[0]["playerId"] != "alice" || entries[0]["rating"] != 1200 || entries[0]["rank"] != 1 {
		t.Fatalf("entry0=%+v", entries[0])
	}
	if entries[1]["playerId"] != "bob" || entries[1]["rank"] != 2 {
		t.Fatalf("entry1=%+v", entries[1])
	}
}

func TestLeaderboardEntriesFromFact_CapsAt100(t *testing.T) {
	data := map[string]string{"playerCount": "150"}
	for i := 1; i <= 150; i++ {
		rk := "rank_" + strconv.Itoa(i)
		data[rk] = "p" + strconv.Itoa(i)
		data["rating_"+strconv.Itoa(i)] = "1000"
	}
	entries := leaderboardEntriesFromFact(data)
	if len(entries) != LeaderboardSnapshotTopN {
		t.Fatalf("want %d got %d", LeaderboardSnapshotTopN, len(entries))
	}
}

func TestStampPlayerRatingProjectionVersion(t *testing.T) {
	ev, err := outboxFromFact(domain.Fact{
		Name: domain.FactPlayerRatingUpdated,
		Data: map[string]string{
			"playerId": "p1", "gameId": "g1", "eventId": "e1",
			"previousRating": "1000", "newRating": "1016",
		},
	}, outboxMeta{UpstreamEventID: "e1", Now: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	snap, err := outboxFromFact(domain.Fact{
		Name: domain.FactLeaderboardSnapshotPublished,
		Data: map[string]string{"snapshotId": "s1", "boardType": "casual_elo", "playerCount": "0"},
	}, outboxMeta{Now: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	events := []OutboxEvent{ev, snap}
	stampPlayerRatingProjectionVersion(events, 17)
	if events[0].Payload["projectionVersion"] != int64(17) {
		t.Fatalf("rating payload version=%v", events[0].Payload["projectionVersion"])
	}
	if _, ok := events[1].Payload["projectionVersion"]; ok {
		t.Fatal("snapshot must not receive projectionVersion")
	}
}

func assertRequiredEventMetadata(t *testing.T, payload map[string]any, wantType string) {
	t.Helper()
	for _, key := range []string{"eventId", "eventType", "schemaVersion", "correlationId", "occurredAt"} {
		if _, ok := payload[key]; !ok {
			t.Fatalf("missing required EventMetadata field %q in %+v", key, payload)
		}
	}
	if payload["eventType"] != wantType {
		t.Fatalf("payload.eventType=%v want %s", payload["eventType"], wantType)
	}
	if corr, _ := payload["correlationId"].(string); strings.TrimSpace(corr) == "" {
		t.Fatal("correlationId must be nonempty")
	}
	if sv, ok := payload["schemaVersion"].(int); !ok || sv != 1 {
		t.Fatalf("schemaVersion=%v", payload["schemaVersion"])
	}
}
