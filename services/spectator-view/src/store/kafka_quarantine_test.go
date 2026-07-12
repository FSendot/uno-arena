package store_test

import (
	"strings"
	"testing"

	"unoarena/services/spectator-view/domain"
	"unoarena/services/spectator-view/store"
)

func TestClassificationForDomainOutcome(t *testing.T) {
	priv := store.ClassificationForDomainOutcome(domain.ApplyOutcome{
		Kind:      domain.OutcomeDropped,
		Rejection: &domain.Rejection{Code: domain.RejectForbiddenField},
	})
	if priv != store.QuarantineClassPrivacy {
		t.Fatalf("got %s", priv)
	}
	gap := store.ClassificationForDomainOutcome(domain.ApplyOutcome{
		Kind:      domain.OutcomeQuarantined,
		Rejection: &domain.Rejection{Code: domain.RejectOutOfOrderSequence},
	})
	if gap != store.QuarantineClassApplication {
		t.Fatalf("got %s", gap)
	}
}

func TestKafkaQuarantineKey_SharesRoomHashTag(t *testing.T) {
	ks := store.NewKeySpace("spectator:")
	room := domain.RoomID("roomA")
	q := ks.KafkaQuarantine(room)
	meta := ks.Meta(room)
	if !strings.Contains(q, "room:{roomA}") {
		t.Fatalf("quarantine key missing hash tag: %s", q)
	}
	tagQ, okQ := redisHashTag(q)
	tagM, okM := redisHashTag(meta)
	if !okQ || !okM || tagQ != tagM {
		t.Fatalf("hash tags diverge: q=%q meta=%q", tagQ, tagM)
	}
}
