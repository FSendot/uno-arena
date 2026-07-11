package app

import "fmt"

// OutboxKind selects realtime (SSE) vs integration (Kafka) transactional outbox.
type OutboxKind string

const (
	OutboxRealtime    OutboxKind = "realtime"
	OutboxIntegration OutboxKind = "integration"
	OutboxSkip        OutboxKind = ""
)

// ClassifyOutboxEvent routes published events:
//   - StreamPlayer → realtime only
//   - any non-player event with a Kafka topic (incl. StreamSpectator) → integration
//   - unknown nonempty stream without topic → fail closed
//   - empty stream and empty topic → skip
func ClassifyOutboxEvent(ev PublishedEvent) (OutboxKind, error) {
	if ev.Stream == StreamPlayer {
		return OutboxRealtime, nil
	}
	if ev.Topic != "" {
		return OutboxIntegration, nil
	}
	if ev.Stream != "" {
		return "", fmt.Errorf("unknown stream %q without kafka topic", ev.Stream)
	}
	return OutboxSkip, nil
}
