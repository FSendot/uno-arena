package main

import (
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	DefaultAnalyticsKafkaGroup       = "analytics"
	defaultAnalyticsMaxAttempt       = 5
	defaultAnalyticsRetryBack        = 200 * time.Millisecond
	defaultAnalyticsPartitionWorkers = 8
	maxDLQErrorSummaryLen            = 256
	analyticsDLQSuffix               = ".analytics.dlq"

	KafkaFailureSchemaInvalid  = "schema_invalid"
	KafkaFailurePayloadInvalid = "payload_invalid"
	KafkaFailureDependency     = "dependency_error"
	KafkaFailureApplication    = "application_error"
)

// DefaultAnalyticsTopics is the ordered set of AsyncAPI channels Analytics consumes.
var DefaultAnalyticsTopics = []string{
	"room.gameplay.metrics",
	"room.match.completed",
	"tournament.match.assigned",
	"tournament.match.result_recorded",
	"tournament.players.advanced",
	"tournament.round.completed",
	"tournament.completed",
	"ranking.player_rating_updated",
	"ranking.leaderboard_snapshot_published",
}

// AnalyticsKafkaConfig is the Analytics-owned multi-topic Kafka consumer settings.
type AnalyticsKafkaConfig struct {
	Brokers             []string
	Group               string
	Topics              []string
	DLQByTopic          map[string]string
	MaxAttempts         int
	RetryBackoff        time.Duration
	MaxPartitionWorkers int
}

// DLQTopic returns the Analytics DLQ topic for a source topic.
func (c AnalyticsKafkaConfig) DLQTopic(sourceTopic string) string {
	sourceTopic = strings.TrimSpace(sourceTopic)
	if c.DLQByTopic != nil {
		if dlq, ok := c.DLQByTopic[sourceTopic]; ok && strings.TrimSpace(dlq) != "" {
			return dlq
		}
	}
	if sourceTopic == "" {
		return ""
	}
	return sourceTopic + analyticsDLQSuffix
}

// LoadAnalyticsKafkaConfigFromEnv loads Kafka settings. enabled is false when
// KAFKA_BROKERS is empty (consumer stays offline). Non-empty brokers with blank
// group/topics fail closed.
func LoadAnalyticsKafkaConfigFromEnv() (AnalyticsKafkaConfig, bool, error) {
	rawBrokers := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	if rawBrokers == "" {
		return AnalyticsKafkaConfig{}, false, nil
	}
	brokers := splitCSV(rawBrokers)
	if len(brokers) == 0 {
		return AnalyticsKafkaConfig{}, false, fmt.Errorf("KAFKA_BROKERS is invalid")
	}

	group, err := envOrDefaultRequired("KAFKA_CONSUMER_GROUP", DefaultAnalyticsKafkaGroup)
	if err != nil {
		return AnalyticsKafkaConfig{}, false, err
	}

	topics, err := loadAnalyticsTopicsFromEnv()
	if err != nil {
		return AnalyticsKafkaConfig{}, false, err
	}

	dlqByTopic, err := loadAnalyticsDLQOverrides(topics)
	if err != nil {
		return AnalyticsKafkaConfig{}, false, err
	}

	maxAttempts := defaultAnalyticsMaxAttempt
	if v := strings.TrimSpace(os.Getenv("KAFKA_MAX_ATTEMPTS")); v != "" {
		n, err := parsePositiveInt(v)
		if err != nil {
			return AnalyticsKafkaConfig{}, false, fmt.Errorf("KAFKA_MAX_ATTEMPTS: %w", err)
		}
		maxAttempts = n
	}

	maxWorkers := defaultAnalyticsPartitionWorkers
	if v := strings.TrimSpace(os.Getenv("KAFKA_MAX_PARTITION_WORKERS")); v != "" {
		n, err := parsePositiveInt(v)
		if err != nil {
			return AnalyticsKafkaConfig{}, false, fmt.Errorf("KAFKA_MAX_PARTITION_WORKERS: %w", err)
		}
		maxWorkers = n
	}

	return AnalyticsKafkaConfig{
		Brokers:             brokers,
		Group:               group,
		Topics:              topics,
		DLQByTopic:          dlqByTopic,
		MaxAttempts:         maxAttempts,
		RetryBackoff:        defaultAnalyticsRetryBack,
		MaxPartitionWorkers: maxWorkers,
	}, true, nil
}

func loadAnalyticsTopicsFromEnv() ([]string, error) {
	if raw := strings.TrimSpace(os.Getenv("KAFKA_TOPICS")); raw != "" {
		topics := splitCSV(raw)
		if len(topics) == 0 {
			return nil, fmt.Errorf("KAFKA_TOPICS must not be blank when set")
		}
		return topics, nil
	}
	// Prefer explicit per-topic env vars when present; otherwise default all nine.
	explicit := make([]string, 0, len(DefaultAnalyticsTopics))
	anyExplicit := false
	for i := range DefaultAnalyticsTopics {
		envName := analyticsTopicEnvName(i)
		if _, set := os.LookupEnv(envName); !set {
			continue
		}
		anyExplicit = true
		v := strings.TrimSpace(os.Getenv(envName))
		if v == "" {
			return nil, fmt.Errorf("%s must not be blank when set", envName)
		}
		explicit = append(explicit, v)
	}
	if anyExplicit {
		if len(explicit) != len(DefaultAnalyticsTopics) {
			return nil, fmt.Errorf("partial per-topic Kafka env overrides are not allowed; set all or use KAFKA_TOPICS")
		}
		return explicit, nil
	}
	out := make([]string, len(DefaultAnalyticsTopics))
	copy(out, DefaultAnalyticsTopics)
	return out, nil
}

func analyticsTopicEnvName(i int) string {
	names := []string{
		"KAFKA_TOPIC_ROOM_GAMEPLAY_METRICS",
		"KAFKA_TOPIC_ROOM_MATCH_COMPLETED",
		"KAFKA_TOPIC_TOURNAMENT_MATCH_ASSIGNED",
		"KAFKA_TOPIC_TOURNAMENT_MATCH_RESULT_RECORDED",
		"KAFKA_TOPIC_TOURNAMENT_PLAYERS_ADVANCED",
		"KAFKA_TOPIC_TOURNAMENT_ROUND_COMPLETED",
		"KAFKA_TOPIC_TOURNAMENT_COMPLETED",
		"KAFKA_TOPIC_RANKING_PLAYER_RATING_UPDATED",
		"KAFKA_TOPIC_RANKING_LEADERBOARD_SNAPSHOT_PUBLISHED",
	}
	if i < 0 || i >= len(names) {
		return ""
	}
	return names[i]
}

func loadAnalyticsDLQOverrides(topics []string) (map[string]string, error) {
	out := make(map[string]string, len(topics))
	for _, t := range topics {
		out[t] = t + analyticsDLQSuffix
	}
	if raw := strings.TrimSpace(os.Getenv("KAFKA_DLQ_TOPICS")); raw != "" {
		// Format: source=dlq,source2=dlq2
		for _, part := range splitCSV(raw) {
			kv := strings.SplitN(part, "=", 2)
			if len(kv) != 2 {
				return nil, fmt.Errorf("KAFKA_DLQ_TOPICS entries must be source=dlq")
			}
			src := strings.TrimSpace(kv[0])
			dlq := strings.TrimSpace(kv[1])
			if src == "" || dlq == "" {
				return nil, fmt.Errorf("KAFKA_DLQ_TOPICS entries must be source=dlq")
			}
			out[src] = dlq
		}
	}
	return out, nil
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

func splitCSV(raw string) []string {
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
