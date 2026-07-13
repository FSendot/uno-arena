package main

import (
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	DefaultTournamentKafkaGroup           = "tournament-orchestration"
	DefaultMatchCompletedTopic            = "room.match.completed"
	DefaultRoomRuntimeReadyTopic          = "room.runtime.ready"
	DefaultRoomRuntimeReadyDLQTopic       = "room.runtime.ready.tournament-orchestration.dlq"
	DefaultMatchCompletedDLQTopic         = "room.match.completed.tournament-orchestration.dlq"
	defaultMatchCompletedMaxAttempt       = 5
	defaultMatchCompletedRetryBack        = 200 * time.Millisecond
	defaultMatchCompletedPartitionWorkers = 8
	maxDLQErrorSummaryLen                 = 256

	KafkaFailureSchemaInvalid        = "schema_invalid"
	KafkaFailurePayloadInvalid       = "payload_invalid"
	KafkaFailureDependency           = "dependency_error"
	KafkaFailureApplication          = "application_error"
	KafkaFailureAggregateQuarantined = "aggregate_quarantined"
)

// MatchCompletedKafkaConfig is the Tournament-owned Kafka consumer settings.
type MatchCompletedKafkaConfig struct {
	Brokers              []string
	Group                string
	Topic                string
	RuntimeReadyTopic    string
	DLQTopic             string
	RuntimeReadyDLQTopic string
	MaxAttempts          int
	RetryBackoff         time.Duration
	MaxPartitionWorkers  int
}

// LoadMatchCompletedKafkaConfigFromEnv loads Kafka settings. enabled is false when
// KAFKA_BROKERS is empty (consumer stays offline). Non-empty brokers with blank
// group/topic/DLQ fail closed.
func LoadMatchCompletedKafkaConfigFromEnv() (MatchCompletedKafkaConfig, bool, error) {
	rawBrokers := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	if rawBrokers == "" {
		return MatchCompletedKafkaConfig{}, false, nil
	}
	brokers := splitCSVBrokers(rawBrokers)
	if len(brokers) == 0 {
		return MatchCompletedKafkaConfig{}, false, fmt.Errorf("KAFKA_BROKERS is invalid")
	}

	group, err := envOrDefaultRequired("KAFKA_CONSUMER_GROUP", DefaultTournamentKafkaGroup)
	if err != nil {
		return MatchCompletedKafkaConfig{}, false, err
	}
	topic, err := envOrDefaultRequired("KAFKA_MATCH_COMPLETED_TOPIC", DefaultMatchCompletedTopic)
	if err != nil {
		return MatchCompletedKafkaConfig{}, false, err
	}
	readyTopic, err := envOrDefaultRequired("KAFKA_ROOM_RUNTIME_READY_TOPIC", DefaultRoomRuntimeReadyTopic)
	if err != nil {
		return MatchCompletedKafkaConfig{}, false, err
	}
	readyDLQ, err := envOrDefaultRequired("KAFKA_ROOM_RUNTIME_READY_DLQ_TOPIC", DefaultRoomRuntimeReadyDLQTopic)
	if err != nil {
		return MatchCompletedKafkaConfig{}, false, err
	}
	dlq, err := envOrDefaultRequired("KAFKA_MATCH_COMPLETED_DLQ_TOPIC", DefaultMatchCompletedDLQTopic)
	if err != nil {
		return MatchCompletedKafkaConfig{}, false, err
	}

	maxAttempts := defaultMatchCompletedMaxAttempt
	if v := strings.TrimSpace(os.Getenv("KAFKA_MATCH_COMPLETED_MAX_ATTEMPTS")); v != "" {
		n, err := parsePositiveInt(v)
		if err != nil {
			return MatchCompletedKafkaConfig{}, false, fmt.Errorf("KAFKA_MATCH_COMPLETED_MAX_ATTEMPTS: %w", err)
		}
		maxAttempts = n
	}

	maxWorkers := defaultMatchCompletedPartitionWorkers
	if v := strings.TrimSpace(os.Getenv("KAFKA_MATCH_COMPLETED_MAX_PARTITION_WORKERS")); v != "" {
		n, err := parsePositiveInt(v)
		if err != nil {
			return MatchCompletedKafkaConfig{}, false, fmt.Errorf("KAFKA_MATCH_COMPLETED_MAX_PARTITION_WORKERS: %w", err)
		}
		maxWorkers = n
	}

	return MatchCompletedKafkaConfig{
		Brokers:              brokers,
		Group:                group,
		Topic:                topic,
		RuntimeReadyTopic:    readyTopic,
		DLQTopic:             dlq,
		RuntimeReadyDLQTopic: readyDLQ,
		MaxAttempts:          maxAttempts,
		RetryBackoff:         defaultMatchCompletedRetryBack,
		MaxPartitionWorkers:  maxWorkers,
	}, true, nil
}

func envOrDefaultRequired(name, def string) (string, error) {
	if _, set := os.LookupEnv(name); !set {
		return def, nil
	}
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return "", fmt.Errorf("%s must not be blank when set", name)
	}
	return v, nil
}

func splitCSVBrokers(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func parsePositiveInt(v string) (int, error) {
	var n int
	_, err := fmt.Sscanf(v, "%d", &n)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("must be a positive integer")
	}
	return n, nil
}
