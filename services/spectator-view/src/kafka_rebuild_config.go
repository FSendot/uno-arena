package main

import (
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	defaultProjectionRebuildMaxAttempt = 5
	defaultProjectionRebuildRetryBack  = 200 * time.Millisecond
)

// ProjectionRebuildKafkaConfig is the rebuild-request worker Kafka settings.
type ProjectionRebuildKafkaConfig struct {
	Brokers      []string
	Group        string
	Topic        string
	DLQTopic     string
	HeldDLQTopic string
	MaxAttempts  int
	RetryBackoff time.Duration
}

// LoadProjectionRebuildKafkaConfigFromEnv loads rebuild worker Kafka settings.
// Requires KAFKA_BROKERS (fail closed when blank in worker role).
func LoadProjectionRebuildKafkaConfigFromEnv() (ProjectionRebuildKafkaConfig, error) {
	rawBrokers := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	if rawBrokers == "" {
		return ProjectionRebuildKafkaConfig{}, fmt.Errorf("KAFKA_BROKERS is required")
	}
	brokers := splitCSVBrokers(rawBrokers)
	if len(brokers) == 0 {
		return ProjectionRebuildKafkaConfig{}, fmt.Errorf("KAFKA_BROKERS is invalid")
	}

	group, err := envOrDefaultRequired("KAFKA_PROJECTION_REBUILD_GROUP", DefaultProjectionRebuildGroup)
	if err != nil {
		return ProjectionRebuildKafkaConfig{}, err
	}
	if group == DefaultSpectatorKafkaGroup {
		return ProjectionRebuildKafkaConfig{}, fmt.Errorf("KAFKA_PROJECTION_REBUILD_GROUP must not equal live group %q", DefaultSpectatorKafkaGroup)
	}
	topic, err := envOrDefaultRequired("KAFKA_PROJECTION_REBUILD_TOPIC", DefaultProjectionRebuildTopic)
	if err != nil {
		return ProjectionRebuildKafkaConfig{}, err
	}
	dlq, err := envOrDefaultRequired("KAFKA_PROJECTION_REBUILD_DLQ_TOPIC", DefaultProjectionRebuildDLQTopic)
	if err != nil {
		return ProjectionRebuildKafkaConfig{}, err
	}
	heldDLQ, err := envOrDefaultRequired("KAFKA_SPECTATOR_SAFE_DLQ_TOPIC", DefaultSpectatorSafeDLQTopic)
	if err != nil {
		return ProjectionRebuildKafkaConfig{}, err
	}

	maxAttempts := defaultProjectionRebuildMaxAttempt
	if v := strings.TrimSpace(os.Getenv("KAFKA_PROJECTION_REBUILD_MAX_ATTEMPTS")); v != "" {
		n, err := parsePositiveInt(v)
		if err != nil {
			return ProjectionRebuildKafkaConfig{}, fmt.Errorf("KAFKA_PROJECTION_REBUILD_MAX_ATTEMPTS: %w", err)
		}
		maxAttempts = n
	}

	return ProjectionRebuildKafkaConfig{
		Brokers:      brokers,
		Group:        group,
		Topic:        topic,
		DLQTopic:     dlq,
		HeldDLQTopic: heldDLQ,
		MaxAttempts:  maxAttempts,
		RetryBackoff: defaultProjectionRebuildRetryBack,
	}, nil
}

// ToSpectatorSafeKafkaConfig adapts rebuild config to the shared franz client shape.
func (c ProjectionRebuildKafkaConfig) ToSpectatorSafeKafkaConfig() SpectatorSafeKafkaConfig {
	return SpectatorSafeKafkaConfig{
		Brokers:             c.Brokers,
		Group:               c.Group,
		Topic:               c.Topic,
		DLQTopic:            c.DLQTopic,
		MaxAttempts:         c.MaxAttempts,
		RetryBackoff:        c.RetryBackoff,
		MaxPartitionWorkers: 1,
	}
}
