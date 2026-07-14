package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"unoarena/services/room-gameplay/app"
	"unoarena/services/room-gameplay/store"
)

type fakeAssignmentStore struct {
	mu                       sync.Mutex
	assignments              []store.RuntimeAssignment
	ready, deleted, advanced []string
	claimLimit               int
	advanceErr               error
}

func (s *fakeAssignmentStore) ClaimRuntimeAssignments(_ context.Context, _ string, limit int, _ time.Duration) ([]store.RuntimeAssignment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claimLimit = limit
	return append([]store.RuntimeAssignment(nil), s.assignments...), nil
}
func (s *fakeAssignmentStore) MarkRuntimePodReady(_ context.Context, room string, generation int64, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ready = append(s.ready, room+":"+strconv.FormatInt(generation, 10))
	return nil
}
func (s *fakeAssignmentStore) MarkRuntimePodCreating(context.Context, string, int64) error {
	return nil
}
func (s *fakeAssignmentStore) MarkRuntimePodDeleted(_ context.Context, room string, generation int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleted = append(s.deleted, room+":"+strconv.FormatInt(generation, 10))
	return nil
}
func (s *fakeAssignmentStore) AdvanceRuntimeGeneration(_ context.Context, room string, generation int64) (store.RuntimeAssignment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.advanced = append(s.advanced, room+":"+strconv.FormatInt(generation, 10))
	if s.advanceErr != nil {
		return store.RuntimeAssignment{}, s.advanceErr
	}
	return store.RuntimeAssignment{RoomID: room, Generation: generation + 1}, nil
}

type fakeKubePods struct {
	mu               sync.Mutex
	pods             map[string]runtimePod
	created, deleted []string
	blockGet         <-chan struct{}
	getStarted       chan<- struct{}
	activeGets       int
	maxActiveGets    int
}

func (k *fakeKubePods) List(context.Context) ([]runtimePod, error) { return nil, nil }
func (k *fakeKubePods) Get(_ context.Context, name string) (runtimePod, error) {
	k.mu.Lock()
	k.activeGets++
	if k.activeGets > k.maxActiveGets {
		k.maxActiveGets = k.activeGets
	}
	p, ok := k.pods[name]
	block, started := k.blockGet, k.getStarted
	k.mu.Unlock()
	if started != nil {
		started <- struct{}{}
	}
	if block != nil {
		<-block
	}
	k.mu.Lock()
	k.activeGets--
	k.mu.Unlock()
	if !ok {
		return runtimePod{}, errKubeNotFound
	}
	return p, nil
}
func (k *fakeKubePods) Create(_ context.Context, spec runtimePodSpec) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.created = append(k.created, spec.Name)
	return nil
}
func (k *fakeKubePods) Delete(_ context.Context, name string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.deleted = append(k.deleted, name)
	return nil
}

func TestRoomRuntimeQueueRejectsOverflow(t *testing.T) {
	q := NewRoomMutationQueue(0)
	entered := make(chan struct{})
	release := make(chan struct{})
	go func() { _ = q.Do(func() { close(entered); <-release }) }()
	<-entered
	if err := q.Do(func() {}); err != ErrRoomBusy {
		t.Fatalf("queue overflow = %v, want ErrRoomBusy", err)
	}
	close(release)
}

func TestPodDirectoryRoutesOnlyReadyMatchingGeneration(t *testing.T) {
	d := NewPodDirectory()
	d.Replace([]runtimePod{{RoomHash: runtimeRoomHash("r-1"), Generation: 2, IP: "10.0.0.2", Ready: true}})
	if got, ok := d.Lookup("r-1", 2); !ok || got.IP != "10.0.0.2" {
		t.Fatalf("ready lookup = %#v, %v", got, ok)
	}
	if _, ok := d.Lookup("r-1", 1); ok {
		t.Fatal("stale generation must never route")
	}
}

func TestPodDirectoryWaitsForNetworkWarmupAfterKubeReady(t *testing.T) {
	d := NewPodDirectory()
	pod := runtimePod{RoomHash: runtimeRoomHash("r-warm"), Generation: 1, IP: "10.0.0.9", Ready: true, ReadyAt: time.Now()}
	d.Replace([]runtimePod{pod})
	if _, ok := d.LookupCurrent("r-warm"); ok {
		t.Fatal("fresh Kube Ready pod must not route before network warmup")
	}
	pod.ReadyAt = time.Now().Add(-runtimePodNetworkWarmup - time.Second)
	d.Replace([]runtimePod{pod})
	if _, ok := d.LookupCurrent("r-warm"); !ok {
		t.Fatal("warmed Ready pod must route")
	}
}

func TestKubernetesPodStatusDecodesReadyEndpoint(t *testing.T) {
	created := "2026-07-13T10:00:00Z"
	readyAt := "2026-07-13T10:00:05Z"
	var raw kubePod
	if err := json.Unmarshal([]byte(`{"metadata":{"name":"room-x","creationTimestamp":"`+created+`","labels":{"unoarena.io/room-hash":"abc","unoarena.io/room-generation":"2"}},"status":{"podIP":"10.0.0.2","phase":"Running","conditions":[{"type":"Ready","status":"True","lastTransitionTime":"`+readyAt+`"}]}}`), &raw); err != nil {
		t.Fatal(err)
	}
	pod := podFromKube(raw)
	if !pod.Ready || pod.IP != "10.0.0.2" || pod.Phase != "Running" || pod.Generation != 2 || pod.CreatedAt.Format(time.RFC3339) != created || pod.ReadyAt.Format(time.RFC3339) != readyAt {
		t.Fatalf("decoded pod = %#v", pod)
	}
}

func TestDedicatedRuntimeRejectsStaleInternalGenerationBeforeDispatch(t *testing.T) {
	svc := app.NewService(app.ServiceDeps{
		Sessions: app.NewMemorySessionRepository(), Integrity: app.NewFakeGameIntegrity(),
		Publisher: app.NewFakeEventPublisher(), Audit: app.NewFakeAuditSink(), Deals: app.NewFakeDealSource(),
		Clock: app.SystemClock{}, SessionsV: app.AllowAllSessionValidator{},
	})
	srv := NewServer(svc, "gateway-secret", "room-gameplay")
	srv.ConfigureDedicatedRuntime("r-fenced", 2, "router-secret", 0)
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/commands", bytes.NewBufferString(`{"commandId":"c1","type":"PlayCard","schemaVersion":1,"roomId":"r-fenced","payload":{}}`))
	req.Header.Set(internalCredentialHeader, "gateway-secret")
	req.Header.Set(runtimeCredentialHeader, "router-secret")
	req.Header.Set(runtimeGenerationHeader, "1")
	w := httptest.NewRecorder()
	srv.routes().ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("stale generation status = %d body=%s", w.Code, w.Body.String())
	}
}

func TestRoomRouterDoesNotRetryUnknownMutationOutcome(t *testing.T) {
	d := NewPodDirectory()
	d.Replace([]runtimePod{{RoomHash: runtimeRoomHash("r-1"), Generation: 1, IP: "127.0.0.1", Ready: true}})
	calls := 0
	router := NewRoomRouter(d, "router-secret", func(*http.Request) (*http.Response, error) {
		calls++
		return nil, http.ErrHandlerTimeout
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/rooms/r-1/commands", nil)
	req.Header.Set(runtimeGenerationHeader, "1")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if calls != 1 || w.Code != http.StatusBadGateway {
		t.Fatalf("calls=%d status=%d; mutation must not be blindly retried", calls, w.Code)
	}
}

func TestRoomRouterExtractsRoomFromInternalCommand(t *testing.T) {
	d := NewPodDirectory()
	d.Replace([]runtimePod{{RoomHash: runtimeRoomHash("r-internal"), Generation: 3, IP: "127.0.0.1", Ready: true}})
	var got *http.Request
	router := NewRoomRouter(d, "router-secret", func(r *http.Request) (*http.Response, error) {
		got = r
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: http.NoBody}, nil
	})
	body := []byte(`{"type":"PlayCard","roomId":"r-internal"}`)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/internal/v1/commands", bytes.NewReader(body)))
	if w.Code != http.StatusOK || got == nil {
		t.Fatalf("status=%d request=%v", w.Code, got)
	}
	if got.Header.Get(runtimeRoomIDHeader) != "r-internal" || got.Header.Get(runtimeGenerationHeader) != "3" {
		t.Fatalf("runtime fencing headers = %#v", got.Header)
	}
}

func TestRoomRouterStartingIsRetryable(t *testing.T) {
	router := NewRoomRouter(NewPodDirectory(), "router-secret", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/internal/v1/commands", bytes.NewBufferString(`{"roomId":"r-starting"}`)))
	if w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") != "1" {
		t.Fatalf("status=%d retry=%q", w.Code, w.Header().Get("Retry-After"))
	}
}

func TestRuntimeControllerCreatesDistinctPodsAndDeletesTerminal(t *testing.T) {
	assignments := &fakeAssignmentStore{assignments: []store.RuntimeAssignment{
		{RoomID: "r-1", Generation: 1, Desired: store.RuntimeDesiredRunning, PodName: store.RuntimePodName("r-1", 1)},
		{RoomID: "r-2", Generation: 1, Desired: store.RuntimeDesiredRunning, PodName: store.RuntimePodName("r-2", 1)},
		{RoomID: "r-done", Generation: 3, Desired: store.RuntimeDesiredTerminal, PodName: store.RuntimePodName("r-done", 3)},
	}}
	kube := &fakeKubePods{pods: map[string]runtimePod{}}
	controller := NewRuntimeController(assignments, kube, "test", "room:local", "router-secret")
	if err := controller.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(kube.created) != 2 || kube.created[0] == kube.created[1] {
		t.Fatalf("created pods = %#v", kube.created)
	}
	if len(kube.deleted) != 1 || len(assignments.deleted) != 1 {
		t.Fatalf("terminal assignment was not deleted: kube=%#v store=%#v", kube.deleted, assignments.deleted)
	}
}

func TestRuntimeControllerDoesNotReplacePendingPod(t *testing.T) {
	name := store.RuntimePodName("r-pending", 1)
	assignments := &fakeAssignmentStore{assignments: []store.RuntimeAssignment{{RoomID: "r-pending", Generation: 1, Desired: store.RuntimeDesiredRunning, PodName: name}}}
	kube := &fakeKubePods{pods: map[string]runtimePod{name: {Name: name, Phase: "Pending"}}}
	if err := NewRuntimeController(assignments, kube, "test", "room:local", "router-secret").ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(kube.deleted) != 0 || len(assignments.advanced) != 0 {
		t.Fatalf("pending pod was replaced: deleted=%v advanced=%v", kube.deleted, assignments.advanced)
	}
}

func TestRuntimeControllerAdvancesMissingReadyGeneration(t *testing.T) {
	name := store.RuntimePodName("r-replace", 4)
	assignments := &fakeAssignmentStore{assignments: []store.RuntimeAssignment{{RoomID: "r-replace", Generation: 4, Desired: store.RuntimeDesiredRunning, Observed: store.RuntimeObservedReady, PodName: name}}}
	kube := &fakeKubePods{pods: map[string]runtimePod{}}
	if err := NewRuntimeController(assignments, kube, "test", "room:local", "router-secret").ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(kube.created) != 0 || len(assignments.advanced) != 1 || assignments.advanced[0] != "r-replace:4" {
		t.Fatalf("replacement = created=%v advanced=%v", kube.created, assignments.advanced)
	}
}

func TestRuntimeControllerClaimBatchAndConcurrencyAreBoundedConfigurable(t *testing.T) {
	assignments := &fakeAssignmentStore{}
	for i := 0; i < 6; i++ {
		room := "r-concurrent-" + strconv.Itoa(i)
		assignments.assignments = append(assignments.assignments, store.RuntimeAssignment{
			RoomID: room, Generation: 1, Desired: store.RuntimeDesiredRunning,
			PodName: store.RuntimePodName(room, 1),
		})
	}
	release := make(chan struct{})
	started := make(chan struct{}, len(assignments.assignments))
	kube := &fakeKubePods{pods: map[string]runtimePod{}, blockGet: release, getStarted: started}
	controller := NewRuntimeController(assignments, kube, "test", "room:local", "router-secret")
	controller.ConfigureLimits(6, 2, time.Minute)
	done := make(chan error, 1)
	go func() { done <- controller.ReconcileOnce(context.Background()) }()
	<-started
	<-started
	select {
	case <-started:
		t.Fatal("controller exceeded configured Kubernetes concurrency")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if assignments.claimLimit != 6 || kube.maxActiveGets != 2 {
		t.Fatalf("claimLimit=%d maxConcurrent=%d", assignments.claimLimit, kube.maxActiveGets)
	}
	controller.ConfigureLimits(1000, 1000, time.Minute)
	if controller.batch != maxRuntimeControllerClaimBatch || controller.concurrency != maxRuntimeControllerConcurrency {
		t.Fatalf("limits not clamped: batch=%d concurrency=%d", controller.batch, controller.concurrency)
	}
}

func TestRuntimeControllerReplacesPendingOrRunningPodPastReadinessDeadline(t *testing.T) {
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	for _, phase := range []string{"Pending", "Running"} {
		t.Run(phase, func(t *testing.T) {
			name := store.RuntimePodName("r-stale-"+phase, 3)
			assignments := &fakeAssignmentStore{assignments: []store.RuntimeAssignment{{RoomID: "r-stale-" + phase, Generation: 3, Desired: store.RuntimeDesiredRunning, PodName: name}}}
			kube := &fakeKubePods{pods: map[string]runtimePod{name: {Name: name, Phase: phase, Ready: false, CreatedAt: now.Add(-61 * time.Second)}}}
			controller := NewRuntimeController(assignments, kube, "test", "room:local", "router-secret")
			controller.ConfigureLimits(1, 1, time.Minute)
			controller.now = func() time.Time { return now }
			if err := controller.ReconcileOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			if len(kube.deleted) != 1 || len(assignments.advanced) != 1 {
				t.Fatalf("stale runtime not replaced: deleted=%v advanced=%v", kube.deleted, assignments.advanced)
			}
		})
	}
}

func TestRuntimeControllerKeepsNonReadyPodBeforeDeadline(t *testing.T) {
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	name := store.RuntimePodName("r-starting", 1)
	assignments := &fakeAssignmentStore{assignments: []store.RuntimeAssignment{{RoomID: "r-starting", Generation: 1, Desired: store.RuntimeDesiredRunning, PodName: name}}}
	kube := &fakeKubePods{pods: map[string]runtimePod{name: {Name: name, Phase: "Pending", CreatedAt: now.Add(-59 * time.Second)}}}
	controller := NewRuntimeController(assignments, kube, "test", "room:local", "router-secret")
	controller.ConfigureLimits(1, 1, time.Minute)
	controller.now = func() time.Time { return now }
	if err := controller.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(kube.deleted) != 0 || len(assignments.advanced) != 0 {
		t.Fatalf("runtime replaced before deadline: deleted=%v advanced=%v", kube.deleted, assignments.advanced)
	}
}

func TestRuntimeControllerStaleGenerationAfterDeleteIsConverged(t *testing.T) {
	now := time.Now().UTC()
	name := store.RuntimePodName("r-raced", 2)
	assignments := &fakeAssignmentStore{advanceErr: app.ErrRuntimeGenerationStale, assignments: []store.RuntimeAssignment{{RoomID: "r-raced", Generation: 2, Desired: store.RuntimeDesiredRunning, PodName: name}}}
	kube := &fakeKubePods{pods: map[string]runtimePod{name: {Name: name, Phase: "Running", CreatedAt: now.Add(-2 * time.Minute)}}}
	controller := NewRuntimeController(assignments, kube, "test", "room:local", "router-secret")
	controller.ConfigureLimits(1, 1, time.Minute)
	controller.now = func() time.Time { return now }
	if err := controller.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("conditional generation race must converge: %v", err)
	}
}

func TestKubeCreateTreatsTypedAlreadyExistsAsIdempotent(t *testing.T) {
	for _, tc := range []struct {
		name, reason string
		wantErr      bool
	}{
		{name: "typed already exists", reason: "AlreadyExists"},
		{name: "other conflict", reason: "Conflict", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost {
					t.Fatalf("method=%s", r.Method)
				}
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(map[string]string{"kind": "Status", "reason": tc.reason})
			}))
			defer srv.Close()
			client := newKubeHTTPClient(srv.URL, "test", "", nil)
			err := client.Create(context.Background(), runtimePodSpec{RoomID: "r1", Generation: 1, Name: "room-r1-g1", Image: "room:test"})
			if (err != nil) != tc.wantErr {
				t.Fatalf("Create error=%v wantErr=%v", err, tc.wantErr)
			}
		})
	}
}

func TestKubeCreateAddsRuntimeTelemetryContract(t *testing.T) {
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()
	client := newKubeHTTPClient(srv.URL, "test", "", nil)
	err := client.Create(context.Background(), runtimePodSpec{
		RoomID: "r1", Generation: 1, Name: "room-r1-g1", Image: "room:test",
		Env: map[string]string{"TELEMETRY_MODE": "required", "UNOARENA_COMPONENT": "room-runtime", "METRICS_ADDR": ":9090"},
	})
	if err != nil {
		t.Fatal(err)
	}
	metadata := body["metadata"].(map[string]any)
	labels := metadata["labels"].(map[string]any)
	if labels["unoarena.io/metrics-scrape"] != "pod" || labels["unoarena.io/metrics-exposed"] != "true" {
		t.Fatalf("telemetry labels = %#v", labels)
	}
	spec := body["spec"].(map[string]any)
	if spec["serviceAccountName"] != "room-gameplay-runtime" || spec["automountServiceAccountToken"] != false {
		t.Fatalf("runtime identity = %#v", spec)
	}
	container := spec["containers"].([]any)[0].(map[string]any)
	ports := container["ports"].([]any)
	foundMetrics := false
	for _, raw := range ports {
		port := raw.(map[string]any)
		if port["name"] == "metrics" && port["containerPort"] == float64(9090) {
			foundMetrics = true
		}
	}
	if !foundMetrics {
		t.Fatalf("runtime ports = %#v", ports)
	}
	env := container["env"].([]any)
	foundUID := false
	for _, raw := range env {
		entry := raw.(map[string]any)
		if entry["name"] == "POD_UID" {
			foundUID = true
		}
	}
	if !foundUID {
		t.Fatalf("runtime env lacks POD_UID: %#v", env)
	}
	resources := container["resources"].(map[string]any)
	limits := resources["limits"].(map[string]any)
	if limits["cpu"] != "250m" || limits["memory"] != "256Mi" {
		t.Fatalf("runtime default limits = %#v", limits)
	}
	liveness := container["livenessProbe"].(map[string]any)
	if liveness["timeoutSeconds"] != float64(1) || liveness["failureThreshold"] != float64(3) {
		t.Fatalf("runtime default liveness = %#v", liveness)
	}
}

func TestKubeCreateUsesConfiguredRuntimePodProfile(t *testing.T) {
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()
	client := newKubeHTTPClient(srv.URL, "test", "", nil)
	err := client.Create(context.Background(), runtimePodSpec{
		RoomID: "r1", Generation: 1, Name: "room-r1-g1", Image: "room:test",
		Profile: runtimePodProfile{CPURequest: "20m", MemoryRequest: "48Mi", CPULimit: "500m", MemoryLimit: "384Mi", ProbeTimeoutSeconds: 3, ProbeFailureThreshold: 6},
	})
	if err != nil {
		t.Fatal(err)
	}
	container := body["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)
	resources := container["resources"].(map[string]any)
	requests := resources["requests"].(map[string]any)
	limits := resources["limits"].(map[string]any)
	if requests["cpu"] != "20m" || requests["memory"] != "48Mi" || limits["cpu"] != "500m" || limits["memory"] != "384Mi" {
		t.Fatalf("runtime resources = %#v", resources)
	}
	for _, probeName := range []string{"readinessProbe", "livenessProbe"} {
		probe := container[probeName].(map[string]any)
		if probe["timeoutSeconds"] != float64(3) || probe["failureThreshold"] != float64(6) {
			t.Fatalf("%s = %#v", probeName, probe)
		}
	}
}

func TestRuntimeControllerCadenceDefaultsAndClamps(t *testing.T) {
	for _, tc := range []struct {
		in, want time.Duration
	}{
		{in: 0, want: time.Second},
		{in: time.Millisecond, want: minRuntimeControllerCadence},
		{in: 750 * time.Millisecond, want: 750 * time.Millisecond},
		{in: time.Minute, want: maxRuntimeControllerCadence},
	} {
		if got := boundedRuntimeControllerCadence(tc.in); got != tc.want {
			t.Fatalf("cadence(%s)=%s want=%s", tc.in, got, tc.want)
		}
	}
}

func TestRuntimeControllerConfigurationLoadsAdmissionControls(t *testing.T) {
	t.Setenv("ROOM_RUNTIME_CONTROLLER_CLAIM_BATCH", "23")
	t.Setenv("ROOM_RUNTIME_CONTROLLER_CONCURRENCY", "7")
	t.Setenv("ROOM_RUNTIME_CONTROLLER_CADENCE_MILLIS", "750")
	t.Setenv("ROOM_RUNTIME_READINESS_TIMEOUT_SECONDS", "45")
	t.Setenv("ROOM_RUNTIME_CPU_REQUEST", "20m")
	t.Setenv("ROOM_RUNTIME_MEMORY_REQUEST", "48Mi")
	t.Setenv("ROOM_RUNTIME_CPU_LIMIT", "500m")
	t.Setenv("ROOM_RUNTIME_MEMORY_LIMIT", "384Mi")
	t.Setenv("ROOM_RUNTIME_PROBE_TIMEOUT_SECONDS", "3")
	t.Setenv("ROOM_RUNTIME_PROBE_FAILURE_THRESHOLD", "6")
	cfg := loadRoomRuntimeConfig()
	if cfg.RuntimeControllerClaimBatch != 23 || cfg.RuntimeControllerConcurrency != 7 ||
		cfg.RuntimeControllerCadence != 750*time.Millisecond || cfg.RuntimeReadinessTimeout != 45*time.Second ||
		cfg.RuntimeCPURequest != "20m" || cfg.RuntimeMemoryRequest != "48Mi" || cfg.RuntimeCPULimit != "500m" || cfg.RuntimeMemoryLimit != "384Mi" ||
		cfg.RuntimeProbeTimeoutSeconds != 3 || cfg.RuntimeProbeFailureThreshold != 6 {
		t.Fatalf("controller config=%+v", cfg)
	}
}

func TestDedicatedRuntimeDurableValidationSkipsUnservedScopedSecrets(t *testing.T) {
	cfg := roomRuntimeConfig{
		WorkerRole:                  "room-runtime",
		DatabaseURL:                 "postgres://room",
		RedisURL:                    "redis://room",
		IdentityURL:                 "http://identity",
		IdentityCred:                "identity-cred",
		GameIntegrityURL:            "http://gi",
		GameIntegrityCred:           "gi-cred",
		SpectatorRecoveryCredential: "spectator-recovery-cred",
		ServiceCredential:           "gateway-room-cred",
	}
	if missing := roomDurableMissing(cfg); len(missing) != 0 {
		t.Fatalf("dedicated runtime required unserved scoped secrets: %v", missing)
	}
}

func TestDedicatedRuntimeDurableValidationDoesNotRequireGatewayCredential(t *testing.T) {
	cfg := roomRuntimeConfig{
		WorkerRole:        "room-runtime",
		DatabaseURL:       "postgres://room",
		RedisURL:          "redis://room",
		IdentityURL:       "http://identity",
		IdentityCred:      "identity-cred",
		GameIntegrityURL:  "http://gi",
		GameIntegrityCred: "gi-cred",
	}
	missing := roomDurableMissing(cfg)
	if len(missing) != 0 {
		t.Fatalf("dedicated runtime inherited stable Gateway authority: missing=%v", missing)
	}
}

func TestRoomProcessRolesDoNotMultiplyGlobalMaintenance(t *testing.T) {
	for _, tc := range []struct {
		role                 string
		wantTimerRebuild     bool
		wantIntegrityRepairs bool
	}{
		{role: ""},
		{role: "room-router"},
		{role: "room-runtime"},
		{role: "room-runtime-controller"},
		{role: "room-timer", wantTimerRebuild: true},
		{role: "room-integrity-reconciler", wantIntegrityRepairs: true},
	} {
		t.Run(tc.role, func(t *testing.T) {
			got := responsibilitiesForRoomRole(tc.role)
			if got.TimerIndexRebuild != tc.wantTimerRebuild || got.IntegrityReconciliation != tc.wantIntegrityRepairs {
				t.Fatalf("role=%q responsibilities=%+v", tc.role, got)
			}
		})
	}
}

func TestMaintenanceRolesRequireOnlyOwnedDurableDependencies(t *testing.T) {
	timer := roomRuntimeConfig{
		WorkerRole: "room-timer", DatabaseURL: "postgres://room", RedisURL: "redis://room",
		TimerCredential: "timer-cred",
	}
	if missing := roomDurableMissing(timer); len(missing) != 0 {
		t.Fatalf("timer inherited unrelated credentials: %v", missing)
	}
	reconciler := roomRuntimeConfig{
		WorkerRole: "room-integrity-reconciler", DatabaseURL: "postgres://room", RedisURL: "redis://room",
		GameIntegrityURL: "http://gi", GameIntegrityCred: "gi-cred",
	}
	if missing := roomDurableMissing(reconciler); len(missing) != 0 {
		t.Fatalf("integrity reconciler inherited unrelated credentials: %v", missing)
	}
}

func TestKubeClientConstructionDoesNotAssumeDefaultTransportType(t *testing.T) {
	original := http.DefaultTransport
	http.DefaultTransport = roundTripper(func(*http.Request) (*http.Response, error) {
		return nil, context.Canceled
	})
	t.Cleanup(func() { http.DefaultTransport = original })

	client := newKubeHTTPClient("https://kubernetes.default.svc", "uno-arena", "token", nil)
	if client == nil || client.client == nil || client.client.Transport == nil {
		t.Fatal("Kubernetes client was not constructed with an explicit base transport")
	}
}
