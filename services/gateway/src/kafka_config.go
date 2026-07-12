package main

import (
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	DefaultGatewayKafkaGroup                 = "gateway"
	DefaultSessionInvalidatedTopic           = "identity.session.invalidated"
	DefaultSessionInvalidatedDLQTopic        = "identity.session.invalidated.gateway.dlq"
	defaultSessionInvalidatedMaxAttempt      = 5
	defaultSessionInvalidatedRetryBack       = 200 * time.Millisecond
	defaultSessionInvalidatedPartitionWorker = 8
	maxDLQErrorSummaryLen                    = 256

	KafkaFailureSchemaInvalid        = "schema_invalid"
	KafkaFailurePayloadInvalid       = "payload_invalid"
	KafkaFailureDependency           = "dependency_error"
	KafkaFailureApplication          = "application_error"
	KafkaFailureAggregateQuarantined = "aggregate_quarantined"
)

// SessionInvalidatedKafkaConfig is the Gateway-owned Kafka consumer settings.
type SessionInvalidatedKafkaConfig struct {
	Brokers             []string
	Group               string
	Topic               string
	DLQTopic            string
	MaxAttempts         int
	RetryBackoff        time.Duration
	MaxPartitionWorkers int
}

// LoadSessionInvalidatedKafkaConfigFromEnv loads Kafka settings. enabled is false
// when KAFKA_BROKERS is empty (consumer stays offline). Non-empty brokers with
// blank group/topic/DLQ fail closed.
func LoadSessionInvalidatedKafkaConfigFromEnv() (SessionInvalidatedKafkaConfig, bool, error) {
	rawBrokers := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	if rawBrokers == "" {
		return SessionInvalidatedKafkaConfig{}, false, nil
	}
	brokers := splitCSVBrokers(rawBrokers)
	if len(brokers) == 0 {
		return SessionInvalidatedKafkaConfig{}, false, fmt.Errorf("KAFKA_BROKERS is invalid")
	}

	group, err := envOrDefaultRequired("KAFKA_CONSUMER_GROUP", DefaultGatewayKafkaGroup)
	if err != nil {
		return SessionInvalidatedKafkaConfig{}, false, err
	}
	topic, err := envOrDefaultRequired("KAFKA_SESSION_INVALIDATED_TOPIC", DefaultSessionInvalidatedTopic)
	if err != nil {
		return SessionInvalidatedKafkaConfig{}, false, err
	}
	dlq, err := envOrDefaultRequired("KAFKA_SESSION_INVALIDATED_DLQ_TOPIC", DefaultSessionInvalidatedDLQTopic)
	if err != nil {
		return SessionInvalidatedKafkaConfig{}, false, err
	}

	maxAttempts := defaultSessionInvalidatedMaxAttempt
	if v := strings.TrimSpace(os.Getenv("KAFKA_SESSION_INVALIDATED_MAX_ATTEMPTS")); v != "" {
		n, err := parsePositiveInt(v)
		if err != nil {
			return SessionInvalidatedKafkaConfig{}, false, fmt.Errorf("KAFKA_SESSION_INVALIDATED_MAX_ATTEMPTS: %w", err)
		}
		maxAttempts = n
	}

	maxWorkers := defaultSessionInvalidatedPartitionWorker
	if v := strings.TrimSpace(os.Getenv("KAFKA_SESSION_INVALIDATED_MAX_PARTITION_WORKERS")); v != "" {
		n, err := parsePositiveInt(v)
		if err != nil {
			return SessionInvalidatedKafkaConfig{}, false, fmt.Errorf("KAFKA_SESSION_INVALIDATED_MAX_PARTITION_WORKERS: %w", err)
		}
		maxWorkers = n
	}

	return SessionInvalidatedKafkaConfig{
		Brokers:             brokers,
		Group:               group,
		Topic:               topic,
		DLQTopic:            dlq,
		MaxAttempts:         maxAttempts,
		RetryBackoff:        defaultSessionInvalidatedRetryBack,
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
