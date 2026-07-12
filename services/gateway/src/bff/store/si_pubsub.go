package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/redis/go-redis/v9"
)

// SessionInvalidationHandler is invoked for Pub/Sub wake notifications.
type SessionInvalidationHandler func(ctx context.Context, n SessionInvalidationNotify) error

// SessionInvalidationSubscriber listens for wake signals on every durable replica.
// Pub/Sub is a wake only; LiveFeed admission/recheck against Redis remains authoritative
// for cross-replica closure when a message is missed.
type SessionInvalidationSubscriber struct {
	rdb     redis.UniversalClient
	channel string
	handler SessionInvalidationHandler
}

// NewSessionInvalidationSubscriber constructs a replica wake subscriber.
func NewSessionInvalidationSubscriber(rdb redis.UniversalClient, channel string, handler SessionInvalidationHandler) *SessionInvalidationSubscriber {
	if strings.TrimSpace(channel) == "" {
		channel = SessionInvalidationNotifyChannel
	}
	return &SessionInvalidationSubscriber{rdb: rdb, channel: channel, handler: handler}
}

// Run blocks until ctx is cancelled or the subscription fails.
func (s *SessionInvalidationSubscriber) Run(ctx context.Context) error {
	if s == nil || s.rdb == nil {
		return fmt.Errorf("session invalidation subscriber not configured")
	}
	if s.handler == nil {
		return fmt.Errorf("session invalidation handler required")
	}
	pubsub := s.rdb.Subscribe(ctx, s.channel)
	defer func() { _ = pubsub.Close() }()

	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return fmt.Errorf("session invalidation pubsub channel closed")
			}
			if msg == nil {
				continue
			}
			var n SessionInvalidationNotify
			if err := json.Unmarshal([]byte(msg.Payload), &n); err != nil {
				log.Printf(`{"level":"warn","service":"gateway","event":"si_notify_decode_failed","error":%q}`, err.Error())
				continue
			}
			n.EventID = strings.TrimSpace(n.EventID)
			n.SessionID = strings.TrimSpace(n.SessionID)
			if n.EventID == "" || n.SessionID == "" {
				continue
			}
			if err := s.handler(ctx, n); err != nil {
				log.Printf(`{"level":"warn","service":"gateway","event":"si_notify_apply_failed","error":%q}`, err.Error())
			}
		}
	}
}
