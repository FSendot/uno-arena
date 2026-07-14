package main

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestConsumerRecordFromKgoPreservesTraceContextHeaders(t *testing.T) {
	rec := consumerRecordFromKgo(&kgo.Record{Headers: []kgo.RecordHeader{
		{Key: "traceparent", Value: []byte("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")},
		{Key: "tracestate", Value: []byte("vendor=value")},
	}})
	if rec.Headers["traceparent"] == "" || rec.Headers["tracestate"] != "vendor=value" {
		t.Fatalf("trace headers not preserved: %#v", rec.Headers)
	}
}

func TestDLQHeaders_SanitizedOperationalMetadata(t *testing.T) {
	meta := DLQFailureMeta{
		Consumer:        DefaultAnalyticsKafkaGroup,
		SourceTopic:     "room.gameplay.metrics",
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
		dlqHeaderConsumer:        DefaultAnalyticsKafkaGroup,
		dlqHeaderSourceTopic:     "room.gameplay.metrics",
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

func TestAnalyticsKafkaConfig_DLQTopicRouting(t *testing.T) {
	cfg := AnalyticsKafkaConfig{
		DLQByTopic: map[string]string{
			"room.gameplay.metrics": "custom.dlq",
		},
	}
	if got := cfg.DLQTopic("room.gameplay.metrics"); got != "custom.dlq" {
		t.Fatalf("override=%q", got)
	}
	if got := cfg.DLQTopic("room.match.completed"); got != "room.match.completed.analytics.dlq" {
		t.Fatalf("default=%q", got)
	}
}
