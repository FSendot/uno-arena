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
	MaxAttempts  int
	RetryBackoff time.Duration
}

// LoadProjectionRebuildKafkaConfigFromEnv loads rebuild worker Kafka settings.
func LoadProjectionRebuildKafkaConfigFromEnv() (ProjectionRebuildKafkaConfig, error) {
	rawBrokers := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	if rawBrokers == "" {
		return ProjectionRebuildKafkaConfig{}, fmt.Errorf("KAFKA_BROKERS is required")
	}
	brokers := splitCSV(rawBrokers)
	if len(brokers) == 0 {
		return ProjectionRebuildKafkaConfig{}, fmt.Errorf("KAFKA_BROKERS is invalid")
	}

	group, err := envOrDefaultRequired("KAFKA_PROJECTION_REBUILD_GROUP", DefaultProjectionRebuildGroup)
	if err != nil {
		return ProjectionRebuildKafkaConfig{}, err
	}
	if group == DefaultAnalyticsKafkaGroup {
		return ProjectionRebuildKafkaConfig{}, fmt.Errorf("KAFKA_PROJECTION_REBUILD_GROUP must not equal live group %q", DefaultAnalyticsKafkaGroup)
	}
	topic, err := envOrDefaultRequired("KAFKA_PROJECTION_REBUILD_TOPIC", DefaultProjectionRebuildTopic)
	if err != nil {
		return ProjectionRebuildKafkaConfig{}, err
	}
	dlq, err := envOrDefaultRequired("KAFKA_PROJECTION_REBUILD_DLQ_TOPIC", DefaultProjectionRebuildDLQTopic)
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
		MaxAttempts:  maxAttempts,
		RetryBackoff: defaultProjectionRebuildRetryBack,
	}, nil
}

// ToAnalyticsKafkaConfig adapts rebuild config for the shared franz client shape.
func (c ProjectionRebuildKafkaConfig) ToAnalyticsKafkaConfig() AnalyticsKafkaConfig {
	return AnalyticsKafkaConfig{
		Brokers:             c.Brokers,
		Group:               c.Group,
		Topics:              []string{c.Topic},
		DLQByTopic:          map[string]string{c.Topic: c.DLQTopic},
		MaxAttempts:         c.MaxAttempts,
		RetryBackoff:        c.RetryBackoff,
		MaxPartitionWorkers: 1,
	}
}
