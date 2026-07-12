package main

import (
	"testing"
	"time"
)

func TestDLQHeaders_SanitizedOperationalMetadata(t *testing.T) {
	meta := DLQFailureMeta{
		Consumer:        DefaultSpectatorKafkaGroup,
		SourceTopic:     DefaultSpectatorSafeTopic,
		SourcePartition: 2,
		SourceOffset:    9,
		AttemptCount:    4,
		Classification:  KafkaFailureDependency,
		FirstFailureAt:  time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC),
		LastFailureAt:   time.Date(2026, 7, 11, 12, 1, 0, 0, time.UTC),
		CorrelationID:   "corr-dlq",
		ErrorSummary:    "connection reset",
	}
	headers := dlqHeaders(meta)
	got := map[string]string{}
	for _, h := range headers {
		got[h.Key] = string(h.Value)
	}
	want := map[string]string{
		dlqHeaderConsumer:        DefaultSpectatorKafkaGroup,
		dlqHeaderSourceTopic:     DefaultSpectatorSafeTopic,
		dlqHeaderSourcePartition: "2",
		dlqHeaderSourceOffset:    "9",
		dlqHeaderAttemptCount:    "4",
		dlqHeaderClassification:  KafkaFailureDependency,
		dlqHeaderCorrelationID:   "corr-dlq",
		dlqHeaderErrorSummary:    "connection reset",
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("%s=%q want %q", k, got[k], v)
		}
	}
	if got[dlqHeaderFirstFailureAt] == "" || got[dlqHeaderLastFailureAt] == "" {
		t.Fatalf("missing timestamps: %+v", got)
	}
}

func TestFranzSpectatorSafeClient_ConfigRequiresIdentity(t *testing.T) {
	_, err := newFranzSpectatorSafeClient(SpectatorSafeKafkaConfig{})
	if err == nil {
		t.Fatal("expected error")
	}
}
