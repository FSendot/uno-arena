package main

import (
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	DefaultRankingKafkaGroup           = "ranking"
	DefaultGameCompletedTopic          = "room.game.completed"
	DefaultGameCompletedDLQTopic       = "room.game.completed.ranking.dlq"
	DefaultPlayersAdvancedTopic        = "tournament.players.advanced"
	DefaultPlayersAdvancedDLQTopic     = "tournament.players.advanced.ranking.dlq"
	DefaultTournamentCompletedTopic    = "tournament.completed"
	DefaultTournamentCompletedDLQTopic = "tournament.completed.ranking.dlq"
	DefaultPlayerRatingUpdatedTopic    = "ranking.player_rating_updated"
	DefaultPlayerRatingUpdatedDLQTopic = "ranking.player_rating_updated.ranking.dlq"
	defaultRankingMaxAttempt           = 5
	defaultRankingRetryBack            = 200 * time.Millisecond
	defaultRankingPartitionWorkers     = 8
	maxDLQErrorSummaryLen              = 256
	rankingDLQSuffix                   = ".ranking.dlq"

	KafkaFailureSchemaInvalid        = "schema_invalid"
	KafkaFailurePayloadInvalid       = "payload_invalid"
	KafkaFailureDependency           = "dependency_error"
	KafkaFailureApplication          = "application_error"
	KafkaFailureAggregateQuarantined = "aggregate_quarantined"
	KafkaFailureBusinessKeyConflict  = "business_key_conflict"
)

// DefaultRankingTopics is the ordered set of AsyncAPI channels Ranking consumes.
var DefaultRankingTopics = []string{
	DefaultGameCompletedTopic,
	DefaultPlayersAdvancedTopic,
	DefaultTournamentCompletedTopic,
	DefaultPlayerRatingUpdatedTopic,
}

// RankingKafkaConfig is the Ranking-owned multi-topic Kafka consumer settings.
type RankingKafkaConfig struct {
	Brokers             []string
	Group               string
	Topics              []string
	DLQByTopic          map[string]string
	MaxAttempts         int
	RetryBackoff        time.Duration
	MaxPartitionWorkers int

	// Topic / DLQTopic are single-topic test/compat shims; prefer Topics + DLQByTopic.
	Topic    string
	DLQTopic string
}

// GameCompletedKafkaConfig is retained as an alias for older call sites/tests.
type GameCompletedKafkaConfig = RankingKafkaConfig

// DLQTopicFor returns the Ranking DLQ topic for a source topic.
func (c RankingKafkaConfig) DLQTopicFor(sourceTopic string) string {
	sourceTopic = strings.TrimSpace(sourceTopic)
	if c.DLQByTopic != nil {
		if dlq, ok := c.DLQByTopic[sourceTopic]; ok && strings.TrimSpace(dlq) != "" {
			return dlq
		}
	}
	if sourceTopic != "" && strings.TrimSpace(c.Topic) == sourceTopic && strings.TrimSpace(c.DLQTopic) != "" {
		return c.DLQTopic
	}
	if sourceTopic == "" {
		return ""
	}
	return sourceTopic + rankingDLQSuffix
}

func (c RankingKafkaConfig) normalizedTopics() []string {
	if len(c.Topics) > 0 {
		return c.Topics
	}
	if t := strings.TrimSpace(c.Topic); t != "" {
		return []string{t}
	}
	return nil
}

// rankingTopicKind identifies which Ranking consumer handler owns a source topic.
type rankingTopicKind string

const (
	topicKindGameCompleted       rankingTopicKind = "game_completed"
	topicKindPlayersAdvanced     rankingTopicKind = "players_advanced"
	topicKindTournamentCompleted rankingTopicKind = "tournament_completed"
	topicKindPlayerRatingUpdated rankingTopicKind = "player_rating_updated"
)

// classifyTopic maps a record topic to a handler using configured topic names
// (Topics[0]=game, [1]=advanced, [2]=completed, [3]=player_rating_updated), with
// Default* fallbacks for older single-topic shims/tests.
func (c RankingKafkaConfig) classifyTopic(topic string) rankingTopicKind {
	topic = strings.TrimSpace(topic)
	topics := c.normalizedTopics()
	if len(topics) >= 4 {
		switch topic {
		case topics[1]:
			return topicKindPlayersAdvanced
		case topics[2]:
			return topicKindTournamentCompleted
		case topics[3]:
			return topicKindPlayerRatingUpdated
		case topics[0]:
			return topicKindGameCompleted
		}
	}
	switch topic {
	case DefaultPlayersAdvancedTopic:
		return topicKindPlayersAdvanced
	case DefaultTournamentCompletedTopic:
		return topicKindTournamentCompleted
	case DefaultPlayerRatingUpdatedTopic:
		return topicKindPlayerRatingUpdated
	default:
		return topicKindGameCompleted
	}
}

// LoadRankingKafkaConfigFromEnv loads multi-topic Kafka settings. enabled is false when
// KAFKA_BROKERS is empty. Non-empty brokers with blank group/topics/DLQ fail closed.
func LoadRankingKafkaConfigFromEnv() (RankingKafkaConfig, bool, error) {
	rawBrokers := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	if rawBrokers == "" {
		return RankingKafkaConfig{}, false, nil
	}
	brokers := splitCSVBrokers(rawBrokers)
	if len(brokers) == 0 {
		return RankingKafkaConfig{}, false, fmt.Errorf("KAFKA_BROKERS is invalid")
	}

	group, err := envOrDefaultRequired("KAFKA_CONSUMER_GROUP", DefaultRankingKafkaGroup)
	if err != nil {
		return RankingKafkaConfig{}, false, err
	}

	topics, dlqByTopic, err := loadRankingTopicsAndDLQs()
	if err != nil {
		return RankingKafkaConfig{}, false, err
	}

	maxAttempts := defaultRankingMaxAttempt
	if v := strings.TrimSpace(os.Getenv("KAFKA_GAME_COMPLETED_MAX_ATTEMPTS")); v != "" {
		n, err := parsePositiveInt(v)
		if err != nil {
			return RankingKafkaConfig{}, false, fmt.Errorf("KAFKA_GAME_COMPLETED_MAX_ATTEMPTS: %w", err)
		}
		maxAttempts = n
	}
	if v := strings.TrimSpace(os.Getenv("KAFKA_MAX_ATTEMPTS")); v != "" {
		n, err := parsePositiveInt(v)
		if err != nil {
			return RankingKafkaConfig{}, false, fmt.Errorf("KAFKA_MAX_ATTEMPTS: %w", err)
		}
		maxAttempts = n
	}

	maxWorkers := defaultRankingPartitionWorkers
	if v := strings.TrimSpace(os.Getenv("KAFKA_GAME_COMPLETED_MAX_PARTITION_WORKERS")); v != "" {
		n, err := parsePositiveInt(v)
		if err != nil {
			return RankingKafkaConfig{}, false, fmt.Errorf("KAFKA_GAME_COMPLETED_MAX_PARTITION_WORKERS: %w", err)
		}
		maxWorkers = n
	}
	if v := strings.TrimSpace(os.Getenv("KAFKA_MAX_PARTITION_WORKERS")); v != "" {
		n, err := parsePositiveInt(v)
		if err != nil {
			return RankingKafkaConfig{}, false, fmt.Errorf("KAFKA_MAX_PARTITION_WORKERS: %w", err)
		}
		maxWorkers = n
	}

	return RankingKafkaConfig{
		Brokers:             brokers,
		Group:               group,
		Topics:              topics,
		DLQByTopic:          dlqByTopic,
		MaxAttempts:         maxAttempts,
		RetryBackoff:        defaultRankingRetryBack,
		MaxPartitionWorkers: maxWorkers,
	}, true, nil
}

// LoadGameCompletedKafkaConfigFromEnv preserves the prior name for lifecycle wiring.
func LoadGameCompletedKafkaConfigFromEnv() (RankingKafkaConfig, bool, error) {
	return LoadRankingKafkaConfigFromEnv()
}

func loadRankingTopicsAndDLQs() ([]string, map[string]string, error) {
	gameTopic, err := envOrDefaultRequired("KAFKA_GAME_COMPLETED_TOPIC", DefaultGameCompletedTopic)
	if err != nil {
		return nil, nil, err
	}
	gameDLQ, err := envOrDefaultRequired("KAFKA_GAME_COMPLETED_DLQ_TOPIC", DefaultGameCompletedDLQTopic)
	if err != nil {
		return nil, nil, err
	}
	advTopic, err := envOrDefaultRequired("KAFKA_PLAYERS_ADVANCED_TOPIC", DefaultPlayersAdvancedTopic)
	if err != nil {
		return nil, nil, err
	}
	advDLQ, err := envOrDefaultRequired("KAFKA_PLAYERS_ADVANCED_DLQ_TOPIC", DefaultPlayersAdvancedDLQTopic)
	if err != nil {
		return nil, nil, err
	}
	compTopic, err := envOrDefaultRequired("KAFKA_TOURNAMENT_COMPLETED_TOPIC", DefaultTournamentCompletedTopic)
	if err != nil {
		return nil, nil, err
	}
	compDLQ, err := envOrDefaultRequired("KAFKA_TOURNAMENT_COMPLETED_DLQ_TOPIC", DefaultTournamentCompletedDLQTopic)
	if err != nil {
		return nil, nil, err
	}
	ratingTopic, err := envOrDefaultRequired("KAFKA_PLAYER_RATING_UPDATED_TOPIC", DefaultPlayerRatingUpdatedTopic)
	if err != nil {
		return nil, nil, err
	}
	ratingDLQ, err := envOrDefaultRequired("KAFKA_PLAYER_RATING_UPDATED_DLQ_TOPIC", DefaultPlayerRatingUpdatedDLQTopic)
	if err != nil {
		return nil, nil, err
	}

	topics := []string{gameTopic, advTopic, compTopic, ratingTopic}
	dlq := map[string]string{
		gameTopic:   gameDLQ,
		advTopic:    advDLQ,
		compTopic:   compDLQ,
		ratingTopic: ratingDLQ,
	}
	return topics, dlq, nil
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
