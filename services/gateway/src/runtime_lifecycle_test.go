package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"unoarena/services/gateway/bff"
)

func TestWorkerReady_DurableWithoutKafkaFailClosed(t *testing.T) {
	rt := &gatewayRuntime{mode: "durable-redis"}
	err := rt.workerReady(context.Background())
	if err == nil || !strings.Contains(err.Error(), "kafka_consumer_not_configured") {
		t.Fatalf("err=%v", err)
	}
}

func TestWorkerReady_CapabilityIgnoresMissingKafka(t *testing.T) {
	rt := &gatewayRuntime{mode: "capability"}
	if err := rt.workerReady(context.Background()); err != nil {
		t.Fatalf("capability must ignore kafka: %v", err)
	}
	rt = &gatewayRuntime{mode: "demo-fakes"}
	if err := rt.workerReady(context.Background()); err != nil {
		t.Fatalf("fakes must ignore kafka: %v", err)
	}
}

func TestWorkerReady_KafkaConfiguredButNotStarted(t *testing.T) {
	rt := &gatewayRuntime{
		mode:  "durable-redis",
		kafka: &sessionInvalidatedKafkaLifecycle{},
		sub:   &sessionInvalidationSubLifecycle{},
	}
	err := rt.workerReady(context.Background())
	if err == nil || !strings.Contains(err.Error(), "kafka_consumer_stopped") {
		t.Fatalf("before start must fail closed err=%v", err)
	}
}

func TestWorkerReady_UnexpectedKafkaExit(t *testing.T) {
	life := &sessionInvalidatedKafkaLifecycle{}
	life.healthy.Store(true)
	rt := &gatewayRuntime{mode: "durable-redis", kafka: life, sub: &sessionInvalidationSubLifecycle{}}
	rt.sub.healthy.Store(true)
	if err := rt.workerReady(context.Background()); err != nil {
		t.Fatalf("healthy workers: %v", err)
	}
	life.healthy.Store(false)
	err := rt.workerReady(context.Background())
	if err == nil || !strings.Contains(err.Error(), "kafka_consumer_stopped") {
		t.Fatalf("unexpected exit err=%v", err)
	}
}

func TestStopWorkers_DoubleStopAndCloseIdempotent(t *testing.T) {
	done := make(chan struct{})
	close(done)
	life := &sessionInvalidatedKafkaLifecycle{done: done}
	life.healthy.Store(true)
	subDone := make(chan struct{})
	close(subDone)
	sub := &sessionInvalidationSubLifecycle{done: subDone}
	sub.healthy.Store(true)

	rt := &gatewayRuntime{
		mode:  "durable-redis",
		kafka: life,
		sub:   sub,
		closeFns: []func(){
			func() {},
		},
	}
	rt.stopWorkers()
	rt.stopWorkers() // second stop must not hang
	rt.close()       // close calls stopWorkers again
	if rt.kafka != nil || rt.sub != nil {
		t.Fatal("workers must be cleared")
	}
	if life.Healthy() || sub.Healthy() {
		t.Fatal("stopped workers must report unhealthy")
	}
}

func TestHandleReady_MapsKafkaConsumerNotConfigured(t *testing.T) {
	srv := bff.NewServer(bff.Dependencies{
		Ready: true,
		WorkerReady: func(context.Context) error {
			return errKafkaConsumerNotConfigured
		},
	})
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "kafka_consumer_not_configured") {
		t.Fatalf("body=%s", w.Body.String())
	}
}

func TestBuildGatewayRuntime_DurableMissingKafkaFailClosedOnReady(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "")
	cfg := gatewayConfig{
		RedisURL:            "redis://127.0.0.1:1/6",
		PlayerFeedRedisURL:  "redis://127.0.0.1:1/2",
		SpectatorRedisURL:   "redis://127.0.0.1:1/5",
		EdgeRateLimit:       1000,
		EdgeRateWindow:      time.Minute,
		PrincipalRateLimit:  1000,
		PrincipalRateWindow: time.Minute,
	}
	rt, err := buildGatewayRuntime(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.close)
	if rt.mode != "durable-redis" {
		t.Fatalf("mode=%s", rt.mode)
	}
	if rt.kafka != nil {
		t.Fatal("absent KAFKA_BROKERS must not create kafka lifecycle")
	}
	err = rt.workerReady(context.Background())
	if err == nil || !strings.Contains(err.Error(), "kafka_consumer_not_configured") {
		t.Fatalf("err=%v", err)
	}
}

// errKafkaConsumerNotConfigured mirrors the durable fail-closed workerReady message.
var errKafkaConsumerNotConfigured = errString("kafka_consumer_not_configured")

type errString string

func (e errString) Error() string { return string(e) }
