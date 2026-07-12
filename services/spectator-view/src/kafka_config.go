package main

import (
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	DefaultSpectatorKafkaGroup           = "spectator-view"
	DefaultSpectatorSafeTopic            = "room.spectator-safe.events"
	DefaultSpectatorSafeDLQTopic         = "room.spectator-safe.events.spectator-view.dlq"
	defaultSpectatorSafeMaxAttempt       = 5
	defaultSpectatorSafeRetryBack        = 200 * time.Millisecond
	defaultSpectatorSafePartitionWorkers = 8
	maxDLQErrorSummaryLen                = 256

	KafkaFailureSchemaInvalid        = "schema_invalid"
	KafkaFailurePayloadInvalid       = "payload_invalid"
	KafkaFailurePrivacyViolation     = "privacy_violation"
	KafkaFailureDependency           = "dependency_error"
	KafkaFailureApplication          = "application_error"
	KafkaFailureAggregateQuarantined = "aggregate_quarantined"
)

// SpectatorSafeKafkaConfig is the Spectator-owned Kafka consumer settings.
type SpectatorSafeKafkaConfig struct {
	Brokers             []string
	Group               string
	Topic               string
	DLQTopic            string
	MaxAttempts         int
	RetryBackoff        time.Duration
	MaxPartitionWorkers int
}

// LoadSpectatorSafeKafkaConfigFromEnv loads Kafka settings. enabled is false when
// KAFKA_BROKERS is empty (consumer stays offline). Non-empty brokers with blank
// group/topic/DLQ fail closed.
func LoadSpectatorSafeKafkaConfigFromEnv() (SpectatorSafeKafkaConfig, bool, error) {
	rawBrokers := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	if rawBrokers == "" {
		return SpectatorSafeKafkaConfig{}, false, nil
	}
	brokers := splitCSVBrokers(rawBrokers)
	if len(brokers) == 0 {
		return SpectatorSafeKafkaConfig{}, false, fmt.Errorf("KAFKA_BROKERS is invalid")
	}

	group, err := envOrDefaultRequired("KAFKA_CONSUMER_GROUP", DefaultSpectatorKafkaGroup)
	if err != nil {
		return SpectatorSafeKafkaConfig{}, false, err
	}
	topic, err := envOrDefaultRequired("KAFKA_SPECTATOR_SAFE_TOPIC", DefaultSpectatorSafeTopic)
	if err != nil {
		return SpectatorSafeKafkaConfig{}, false, err
	}
	dlq, err := envOrDefaultRequired("KAFKA_SPECTATOR_SAFE_DLQ_TOPIC", DefaultSpectatorSafeDLQTopic)
	if err != nil {
		return SpectatorSafeKafkaConfig{}, false, err
	}

	maxAttempts := defaultSpectatorSafeMaxAttempt
	if v := strings.TrimSpace(os.Getenv("KAFKA_SPECTATOR_SAFE_MAX_ATTEMPTS")); v != "" {
		n, err := parsePositiveInt(v)
		if err != nil {
			return SpectatorSafeKafkaConfig{}, false, fmt.Errorf("KAFKA_SPECTATOR_SAFE_MAX_ATTEMPTS: %w", err)
		}
		maxAttempts = n
	}

	maxWorkers := defaultSpectatorSafePartitionWorkers
	if v := strings.TrimSpace(os.Getenv("KAFKA_SPECTATOR_SAFE_MAX_PARTITION_WORKERS")); v != "" {
		n, err := parsePositiveInt(v)
		if err != nil {
			return SpectatorSafeKafkaConfig{}, false, fmt.Errorf("KAFKA_SPECTATOR_SAFE_MAX_PARTITION_WORKERS: %w", err)
		}
		maxWorkers = n
	}

	return SpectatorSafeKafkaConfig{
		Brokers:             brokers,
		Group:               group,
		Topic:               topic,
		DLQTopic:            dlq,
		MaxAttempts:         maxAttempts,
		RetryBackoff:        defaultSpectatorSafeRetryBack,
		MaxPartitionWorkers: maxWorkers,
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
