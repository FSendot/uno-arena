package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"unoarena/services/room-gameplay/app"
	"unoarena/services/room-gameplay/store"
	"unoarena/shared/httpx"
)

const (
	runtimeCredentialHeader = "X-Room-Runtime-Credential"
	runtimeRoomIDHeader     = "X-Room-Runtime-Room-Id"
	runtimeGenerationHeader = "X-Room-Runtime-Generation"
)

var ErrRoomBusy = errors.New("room mutation queue is busy")

// RoomMutationQueue is intentionally one worker per state-machine process.
// A caller either gets admission or a deterministic overload response; an
// accepted command is never transparently replayed after an unknown outcome.
type RoomMutationQueue struct {
	tasks   chan mutationTask
	permits chan struct{}
}
type mutationTask struct {
	fn   func()
	done chan struct{}
}

func NewRoomMutationQueue(capacity int) *RoomMutationQueue {
	if capacity < 0 {
		capacity = 0
	}
	q := &RoomMutationQueue{tasks: make(chan mutationTask), permits: make(chan struct{}, capacity+1)}
	go func() {
		for task := range q.tasks {
			task.fn()
			close(task.done)
		}
	}()
	return q
}

func (q *RoomMutationQueue) Do(fn func()) error {
	select {
	case q.permits <- struct{}{}:
	default:
		return ErrRoomBusy
	}
	defer func() { <-q.permits }()
	t := mutationTask{fn: fn, done: make(chan struct{})}
	q.tasks <- t
	<-t.done
	return nil
}

type runtimePod struct {
	RoomHash            string
	Generation          int64
	Name                string
	IP                  string
	Phase               string
	Ready               bool
	Unschedulable       bool
	AdmissionReason     string
	CreatedAt           time.Time
	ReadyAt             time.Time
	PrincipalKeyVersion string
}

// Kubelet can report a pod Ready just before the ambient data plane has
// converged on its new IP. Keep that narrow startup race out of request paths.
const runtimePodNetworkWarmup = 3 * time.Second

func runtimeRoomHash(roomID string) string {
	sum := sha256.Sum256([]byte(roomID))
	return fmt.Sprintf("%x", sum[:12])
}

// PodDirectory is a bounded, periodically-refreshed directory. The router
// never makes an unbounded Kubernetes list/watch call on a request path.
type PodDirectory struct {
	mu     sync.RWMutex
	byRoom map[string]runtimePod
}

func NewPodDirectory() *PodDirectory { return &PodDirectory{byRoom: map[string]runtimePod{}} }
func (d *PodDirectory) Replace(pods []runtimePod) {
	next := make(map[string]runtimePod, len(pods))
	now := time.Now()
	for _, p := range pods {
		warmed := p.ReadyAt.IsZero() || !now.Before(p.ReadyAt.Add(runtimePodNetworkWarmup))
		if p.Ready && warmed && p.RoomHash != "" {
			if current, ok := next[p.RoomHash]; !ok || p.Generation > current.Generation {
				next[p.RoomHash] = p
			}
		}
	}
	d.mu.Lock()
	d.byRoom = next
	d.mu.Unlock()
}
func (d *PodDirectory) Lookup(roomID string, generation int64) (runtimePod, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	p, ok := d.byRoom[runtimeRoomHash(roomID)]
	return p, ok && p.Generation == generation && p.Ready
}
func (d *PodDirectory) LookupCurrent(roomID string) (runtimePod, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	p, ok := d.byRoom[runtimeRoomHash(roomID)]
	return p, ok && p.Ready
}

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// RoomRouter forwards only to a cached Ready current pod. It is suitable for
// the stable router deployment (Gateway may use the same directory protocol).
type RoomRouter struct {
	directory  *PodDirectory
	credential string
	client     *http.Client
}

func NewRoomRouter(directory *PodDirectory, credential string, roundTrip func(*http.Request) (*http.Response, error)) *RoomRouter {
	client := &http.Client{Timeout: 10 * time.Second}
	if roundTrip != nil {
		client.Transport = roundTripper(roundTrip)
	}
	return &RoomRouter{directory: directory, credential: credential, client: client}
}
func (rr *RoomRouter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		_ = httpx.WriteError(w, http.StatusBadRequest, "bad_request", "unable to read request", "", "")
		return
	}
	roomID, ok := roomIDFromRouterRequest(r.URL.Path, body)
	if !ok {
		_ = httpx.WriteError(w, http.StatusNotFound, "not_found", "route not found", "", "")
		return
	}
	pod, ready := rr.directory.LookupCurrent(roomID)
	if !ready {
		w.Header().Set("Retry-After", "1")
		_ = httpx.WriteError(w, http.StatusServiceUnavailable, "room_starting", "room runtime is not ready", "", "")
		return
	}
	upstream, err := http.NewRequestWithContext(r.Context(), r.Method, "http://"+pod.IP+":8080"+r.URL.RequestURI(), bytes.NewReader(body))
	if err != nil {
		_ = httpx.WriteError(w, http.StatusBadGateway, "room_runtime_unavailable", "invalid runtime address", "", "")
		return
	}
	upstream.Header = r.Header.Clone()
	// The stable Room boundary authenticates the original caller. Dedicated
	// runtimes receive only the scoped router hop plus any already-derived
	// principal fields; upstream service credentials never fan out to room pods.
	upstream.Header.Del(internalCredentialHeader)
	upstream.Header.Set(runtimeCredentialHeader, rr.credential)
	upstream.Header.Set(runtimeRoomIDHeader, roomID)
	upstream.Header.Set(runtimeGenerationHeader, strconv.FormatInt(pod.Generation, 10))
	resp, err := rr.client.Do(upstream)
	if err != nil {
		_ = httpx.WriteError(w, http.StatusBadGateway, "room_runtime_unknown", "room runtime outcome unknown", "", "")
		return
	}
	defer resp.Body.Close()
	for key, vals := range resp.Header {
		for _, val := range vals {
			w.Header().Add(key, val)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// NewStableRoomHandler composes the stable Postgres-backed Room boundary with
// dedicated runtime routing. Original route authority is checked before the
// pod directory is consulted. Stable-only recovery reads never enter routing.
func NewStableRoomHandler(server *Server, router *RoomRouter) http.Handler {
	base := server.routes()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		if roomID, action, ok := splitRoomPath(path, "/v1/rooms/"); ok {
			switch action {
			case "snapshot":
				serveStableAuthorizedSnapshot(w, r, server, router, roomID)
				return
			case "commands":
				if !server.authorizeInternal(r) {
					_ = httpx.WriteError(w, http.StatusUnauthorized, "unauthorized", "invalid service credential", "", "")
					return
				}
				router.ServeHTTP(w, r)
				return
			}
		}

		if roomID, action, ok := splitRoomPath(path, "/internal/v1/rooms/"); ok {
			switch action {
			case "spectator-recovery-snapshot":
				base.ServeHTTP(w, r)
				return
			case "timer-commands":
				if !server.authorizeTimer(r) {
					_ = httpx.WriteError(w, http.StatusUnauthorized, "unauthorized", "invalid timer service credential", "", "")
					return
				}
				_ = roomID
				router.ServeHTTP(w, r)
				return
			}
		}

		if path == "/internal/v1/commands" && r.Method == http.MethodPost {
			body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
			if err != nil {
				_ = httpx.WriteError(w, http.StatusBadRequest, "bad_request", "unable to read request", "", "")
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			var command struct {
				Type string `json:"type"`
			}
			if json.Unmarshal(body, &command) == nil && command.Type != app.CmdCreateRoom {
				if !server.authorizeInternal(r) {
					_ = httpx.WriteError(w, http.StatusUnauthorized, "unauthorized", "invalid service credential", "", "")
					return
				}
				router.ServeHTTP(w, r)
				return
			}
		}

		base.ServeHTTP(w, r)
	})
}

func splitRoomPath(path, prefix string) (roomID, action string, ok bool) {
	if !strings.HasPrefix(path, prefix) {
		return "", "", false
	}
	parts := strings.Split(strings.TrimPrefix(path, prefix), "/")
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func serveStableAuthorizedSnapshot(w http.ResponseWriter, r *http.Request, server *Server, router *RoomRouter, roomID string) {
	if !server.authorizeInternal(r) {
		_ = httpx.WriteError(w, http.StatusUnauthorized, "unauthorized", "invalid service credential", "", "")
		return
	}
	playerID := strings.TrimSpace(r.Header.Get("X-Player-Id"))
	if playerID == "" {
		_ = httpx.WriteError(w, http.StatusUnauthorized, "unauthorized", "trusted player identity required", "", "")
		return
	}
	snapshot, err := server.svc.PlayerSnapshot(r.Context(), roomID, playerID)
	if err != nil {
		switch {
		case errors.Is(err, app.ErrNotFound):
			_ = httpx.WriteError(w, http.StatusNotFound, "not_found", "room not found", "", "")
		case errors.Is(err, app.ErrForbidden):
			_ = httpx.WriteError(w, http.StatusForbidden, "forbidden", "not a room member", "", "")
		default:
			_ = httpx.WriteError(w, http.StatusInternalServerError, "internal_error", "snapshot authorization failed", "", "")
		}
		return
	}
	status, _ := snapshot["status"].(string)
	if status == "completed" || status == "cancelled" {
		_ = httpx.WriteJSON(w, http.StatusOK, snapshot)
		return
	}
	router.ServeHTTP(w, r)
}
func roomIDFromRouterRequest(path string, body []byte) (string, bool) {
	if path == "/internal/v1/commands" {
		var command struct {
			RoomID string `json:"roomId"`
		}
		if json.Unmarshal(body, &command) != nil || strings.TrimSpace(command.RoomID) == "" {
			return "", false
		}
		return command.RoomID, true
	}
	prefix := "/v1/rooms/"
	if strings.HasPrefix(path, "/internal/v1/rooms/") {
		prefix = "/internal/v1/rooms/"
	}
	parts := strings.Split(strings.TrimPrefix(path, prefix), "/")
	return parts[0], len(parts) >= 2 && strings.TrimSpace(parts[0]) != ""
}

// KubePodClient is the small standard-library Kubernetes REST surface needed
// by Room. It deliberately exposes only Pod operations; no dynamic client or
// CRD dependency is required.
type KubePodClient interface {
	List(context.Context) ([]runtimePod, error)
	Get(context.Context, string) (runtimePod, error)
	Create(context.Context, runtimePodSpec) error
	Delete(context.Context, string) error
}
type runtimePodSpec struct {
	RoomID              string
	Generation          int64
	Name, Image         string
	SecretName          string
	SecretEnv           map[string]string
	Env                 map[string]string
	Profile             runtimePodProfile
	PrincipalKeyVersion string
}

type runtimePodProfile struct {
	CPURequest, MemoryRequest string
	CPULimit, MemoryLimit     string
	ProbeTimeoutSeconds       int
	ProbeFailureThreshold     int
	NodeSelector              map[string]string
	TopologySpreadEnabled     bool
	TopologyKey               string
	TopologyMaxSkew           int
	TopologyWhenUnsatisfiable string
}

func normalizeRuntimePodProfile(profile runtimePodProfile) runtimePodProfile {
	if strings.TrimSpace(profile.CPURequest) == "" {
		profile.CPURequest = "10m"
	}
	if strings.TrimSpace(profile.MemoryRequest) == "" {
		profile.MemoryRequest = "32Mi"
	}
	if strings.TrimSpace(profile.CPULimit) == "" {
		profile.CPULimit = "250m"
	}
	if strings.TrimSpace(profile.MemoryLimit) == "" {
		profile.MemoryLimit = "256Mi"
	}
	if profile.ProbeTimeoutSeconds < 1 {
		profile.ProbeTimeoutSeconds = 1
	}
	if profile.ProbeFailureThreshold < 1 {
		profile.ProbeFailureThreshold = 3
	}
	profile.NodeSelector = cloneStringMap(profile.NodeSelector)
	if strings.TrimSpace(profile.TopologyKey) == "" {
		profile.TopologyKey = "kubernetes.io/hostname"
	}
	if profile.TopologyMaxSkew < 1 {
		profile.TopologyMaxSkew = 1
	}
	if profile.TopologyWhenUnsatisfiable != "ScheduleAnyway" {
		profile.TopologyWhenUnsatisfiable = "DoNotSchedule"
	}
	return profile
}

func cloneStringMap(source map[string]string) map[string]string {
	if len(source) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(source))
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
}

type kubeHTTPClient struct {
	baseURL, namespace string
	client             *http.Client
}

func newKubeHTTPClient(baseURL, namespace, token string, caPEM []byte) *kubeHTTPClient {
	tr := roomBaseHTTPTransport.Clone()
	if len(caPEM) > 0 {
		roots := x509.NewCertPool()
		if roots.AppendCertsFromPEM(caPEM) {
			tr.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
		}
	}
	client := &http.Client{Timeout: 10 * time.Second, Transport: roundTripper(func(r *http.Request) (*http.Response, error) {
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		return tr.RoundTrip(r)
	})}
	return &kubeHTTPClient{baseURL: strings.TrimRight(baseURL, "/"), namespace: namespace, client: client}
}
func (k *kubeHTTPClient) podsURL(name string) string {
	base := k.baseURL + "/api/v1/namespaces/" + k.namespace + "/pods"
	if name != "" {
		return base + "/" + name
	}
	return base
}
func (k *kubeHTTPClient) do(ctx context.Context, method, url string, body any, out any) error {
	var in io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		in = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, in)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := k.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return errKubeNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if resp.StatusCode == http.StatusConflict {
			var status struct {
				Reason string `json:"reason"`
			}
			if err := json.NewDecoder(io.LimitReader(resp.Body, maxBodyBytes)).Decode(&status); err == nil && status.Reason == "AlreadyExists" {
				return errKubeAlreadyExists
			}
		}
		return fmt.Errorf("kubernetes pods %s: %s", method, resp.Status)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

var errKubeNotFound = errors.New("kubernetes pod not found")
var errKubeAlreadyExists = errors.New("kubernetes pod already exists")

type kubePodList struct {
	Items []kubePod `json:"items"`
}
type kubePod struct {
	Metadata struct {
		Name              string            `json:"name"`
		Labels            map[string]string `json:"labels"`
		CreationTimestamp time.Time         `json:"creationTimestamp"`
	} `json:"metadata"`
	Status struct {
		PodIP      string `json:"podIP"`
		Phase      string `json:"phase"`
		Conditions []struct {
			Type               string    `json:"type"`
			Status             string    `json:"status"`
			Reason             string    `json:"reason"`
			Message            string    `json:"message"`
			LastTransitionTime time.Time `json:"lastTransitionTime"`
		} `json:"conditions"`
	} `json:"status"`
}

func podFromKube(p kubePod) runtimePod {
	gen, _ := strconv.ParseInt(p.Metadata.Labels["unoarena.io/room-generation"], 10, 64)
	ready := p.Status.Phase == "Running"
	unschedulable := false
	admissionReason := ""
	var readyAt time.Time
	for _, c := range p.Status.Conditions {
		if c.Type == "Ready" {
			ready = ready && c.Status == "True"
			readyAt = c.LastTransitionTime.UTC()
		}
		if c.Type == "PodScheduled" && c.Status == "False" {
			unschedulable = c.Reason == "Unschedulable"
			admissionReason = strings.TrimSpace(c.Reason)
		}
	}
	return runtimePod{RoomHash: p.Metadata.Labels["unoarena.io/room-hash"], Generation: gen, Name: p.Metadata.Name, IP: p.Status.PodIP, Phase: p.Status.Phase, Ready: ready, Unschedulable: unschedulable, AdmissionReason: admissionReason, CreatedAt: p.Metadata.CreationTimestamp.UTC(), ReadyAt: readyAt, PrincipalKeyVersion: p.Metadata.Labels["unoarena.io/principal-key-version"]}
}
func (k *kubeHTTPClient) List(ctx context.Context) ([]runtimePod, error) {
	var list kubePodList
	if err := k.do(ctx, http.MethodGet, k.podsURL("")+"?labelSelector=unoarena.io%2Fmanaged-by%3Droom-runtime-controller", nil, &list); err != nil {
		return nil, err
	}
	out := make([]runtimePod, 0, len(list.Items))
	for _, p := range list.Items {
		out = append(out, podFromKube(p))
	}
	return out, nil
}
func (k *kubeHTTPClient) Get(ctx context.Context, name string) (runtimePod, error) {
	var pod kubePod
	if err := k.do(ctx, http.MethodGet, k.podsURL(name), nil, &pod); err != nil {
		return runtimePod{}, err
	}
	return podFromKube(pod), nil
}
func (k *kubeHTTPClient) Create(ctx context.Context, spec runtimePodSpec) error {
	profile := normalizeRuntimePodProfile(spec.Profile)
	env := []any{
		map[string]string{"name": "WORKER_ROLE", "value": "room-runtime"},
		map[string]string{"name": "ROOM_RUNTIME_ROOM_ID", "value": spec.RoomID},
		map[string]string{"name": "ROOM_RUNTIME_GENERATION", "value": strconv.FormatInt(spec.Generation, 10)},
		map[string]any{"name": "POD_UID", "valueFrom": map[string]any{"fieldRef": map[string]string{"fieldPath": "metadata.uid"}}},
	}
	names := make([]string, 0, len(spec.Env))
	for name := range spec.Env {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		value := spec.Env[name]
		env = append(env, map[string]string{"name": name, "value": value})
	}
	secretNames := make([]string, 0, len(spec.SecretEnv))
	for name := range spec.SecretEnv {
		secretNames = append(secretNames, name)
	}
	sort.Strings(secretNames)
	for _, name := range secretNames {
		key := spec.SecretEnv[name]
		if name == "" || key == "" || spec.SecretName == "" {
			continue
		}
		env = append(env, map[string]any{"name": name, "valueFrom": map[string]any{"secretKeyRef": map[string]string{"name": spec.SecretName, "key": key}}})
	}
	container := map[string]any{"name": "room-runtime", "image": spec.Image, "ports": []any{map[string]any{"name": "http", "containerPort": 8080}, map[string]any{"name": "metrics", "containerPort": 9090}}, "env": env}
	container["securityContext"] = map[string]any{"allowPrivilegeEscalation": false, "readOnlyRootFilesystem": true, "runAsNonRoot": true, "runAsUser": 65532, "runAsGroup": 65532, "capabilities": map[string]any{"drop": []string{"ALL"}}}
	container["resources"] = map[string]any{
		"requests": map[string]string{"cpu": profile.CPURequest, "memory": profile.MemoryRequest},
		"limits":   map[string]string{"cpu": profile.CPULimit, "memory": profile.MemoryLimit},
	}
	container["readinessProbe"] = map[string]any{"httpGet": map[string]any{"path": "/ready", "port": 8080}, "periodSeconds": 2, "timeoutSeconds": profile.ProbeTimeoutSeconds, "failureThreshold": profile.ProbeFailureThreshold}
	container["livenessProbe"] = map[string]any{"httpGet": map[string]any{"path": "/health", "port": 8080}, "periodSeconds": 10, "timeoutSeconds": profile.ProbeTimeoutSeconds, "failureThreshold": profile.ProbeFailureThreshold}
	labels := map[string]string{"app.kubernetes.io/name": "room-gameplay", "unoarena.io/managed-by": "room-runtime-controller", "unoarena.io/room-hash": runtimeRoomHash(spec.RoomID), "unoarena.io/room-generation": strconv.FormatInt(spec.Generation, 10), "unoarena.io/component": "room-runtime", "unoarena.io/metrics-scrape": "pod", "unoarena.io/metrics-exposed": "true", "unoarena.io/principal-key-version": spec.PrincipalKeyVersion}
	podSpec := map[string]any{"restartPolicy": "Always", "serviceAccountName": "room-gameplay-runtime", "automountServiceAccountToken": false, "securityContext": map[string]any{"runAsNonRoot": true, "runAsUser": 65532, "runAsGroup": 65532, "seccompProfile": map[string]string{"type": "RuntimeDefault"}}, "containers": []any{container}}
	if len(profile.NodeSelector) > 0 {
		podSpec["nodeSelector"] = profile.NodeSelector
	}
	if profile.TopologySpreadEnabled {
		podSpec["topologySpreadConstraints"] = []any{map[string]any{"maxSkew": profile.TopologyMaxSkew, "topologyKey": profile.TopologyKey, "whenUnsatisfiable": profile.TopologyWhenUnsatisfiable, "labelSelector": map[string]any{"matchLabels": map[string]string{"unoarena.io/component": "room-runtime"}}}}
	}
	body := map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": spec.Name, "labels": labels}, "spec": podSpec}
	err := k.do(ctx, http.MethodPost, k.podsURL(""), body, nil)
	if errors.Is(err, errKubeAlreadyExists) {
		return nil
	}
	return err
}
func (k *kubeHTTPClient) Delete(ctx context.Context, name string) error {
	err := k.do(ctx, http.MethodDelete, k.podsURL(name), nil, nil)
	if errors.Is(err, errKubeNotFound) {
		return nil
	}
	return err
}

// RuntimeController owns only bare Pod convergence. A failed/missing current
// pod advances the generation before the replacement is created; terminal rows
// only delete and never recreate a runtime.
type RuntimeController struct {
	store                    RuntimeAssignmentStore
	kube                     KubePodClient
	owner, image, credential string
	batch                    int
	concurrency              int
	readinessTimeout         time.Duration
	now                      func() time.Time
	secretName               string
	secretEnv                map[string]string
	env                      map[string]string
	runtimeProfile           runtimePodProfile
	principalKeyVersion      string
	jitter                   func(time.Duration) time.Duration
}

type RuntimeAssignmentStore interface {
	ClaimRuntimeAssignments(context.Context, string, int, time.Duration) ([]store.RuntimeAssignment, error)
	MarkRuntimePodReady(context.Context, string, int64, string) error
	MarkRuntimePodCreating(context.Context, string, int64) error
	MarkRuntimePodDeleted(context.Context, string, int64) error
	AdvanceRuntimeGeneration(context.Context, string, int64) (store.RuntimeAssignment, error)
}

func NewRuntimeController(s RuntimeAssignmentStore, kube KubePodClient, owner, image, credential string) *RuntimeController {
	return &RuntimeController{store: s, kube: kube, owner: owner, image: image, credential: credential, batch: 16, concurrency: 4, readinessTimeout: time.Minute, now: time.Now, runtimeProfile: normalizeRuntimePodProfile(runtimePodProfile{}), jitter: randomJitterDuration}
}

const (
	maxRuntimeControllerClaimBatch  = 64
	maxRuntimeControllerConcurrency = 16
	minRuntimeControllerCadence     = 100 * time.Millisecond
	maxRuntimeControllerCadence     = 30 * time.Second
)

func boundedRuntimeControllerCadence(cadence time.Duration) time.Duration {
	if cadence <= 0 {
		return time.Second
	}
	if cadence < minRuntimeControllerCadence {
		return minRuntimeControllerCadence
	}
	if cadence > maxRuntimeControllerCadence {
		return maxRuntimeControllerCadence
	}
	return cadence
}

func (c *RuntimeController) nextCadence(cadence time.Duration) time.Duration {
	if c.jitter == nil {
		return cadence
	}
	return c.jitter(cadence)
}

// ConfigureLimits provides bounded per-replica admission controls. Values are
// clamped so a configuration typo cannot create unbounded Kubernetes API load.
func (c *RuntimeController) ConfigureLimits(batch, concurrency int, readinessTimeout time.Duration) {
	if batch < 1 {
		batch = 16
	}
	if batch > maxRuntimeControllerClaimBatch {
		batch = maxRuntimeControllerClaimBatch
	}
	if concurrency < 1 {
		concurrency = 4
	}
	if concurrency > maxRuntimeControllerConcurrency {
		concurrency = maxRuntimeControllerConcurrency
	}
	if concurrency > batch {
		concurrency = batch
	}
	if readinessTimeout <= 0 {
		readinessTimeout = time.Minute
	}
	c.batch, c.concurrency, c.readinessTimeout = batch, concurrency, readinessTimeout
}

func (c *RuntimeController) ConfigureRuntimePodProfile(profile runtimePodProfile) {
	c.runtimeProfile = normalizeRuntimePodProfile(profile)
}

func (c *RuntimeController) ConfigureRuntimePod(secretName string, secretEnv, env map[string]string, principalKeyVersion string) {
	c.secretName, c.secretEnv, c.env, c.principalKeyVersion = secretName, secretEnv, env, strings.TrimSpace(principalKeyVersion)
}
func (c *RuntimeController) ReconcileOnce(ctx context.Context) error {
	assignments, err := c.store.ClaimRuntimeAssignments(ctx, c.owner, c.batch, 20*time.Second)
	if err != nil {
		return err
	}
	if len(assignments) == 0 {
		return nil
	}
	workers := c.concurrency
	if workers < 1 {
		workers = 1
	}
	if workers > len(assignments) {
		workers = len(assignments)
	}
	jobs := make(chan store.RuntimeAssignment)
	errs := make(chan error, len(assignments))
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for assignment := range jobs {
				if err := c.reconcileAssignment(ctx, assignment); err != nil {
					errs <- err
				}
			}
		}()
	}
	for _, assignment := range assignments {
		jobs <- assignment
	}
	close(jobs)
	wg.Wait()
	close(errs)
	var combined error
	for err := range errs {
		combined = errors.Join(combined, err)
	}
	return combined
}

func (c *RuntimeController) reconcileAssignment(ctx context.Context, a store.RuntimeAssignment) error {
	pod, err := c.kube.Get(ctx, a.PodName)
	if a.Desired == store.RuntimeDesiredTerminal {
		if err == nil || errors.Is(err, errKubeNotFound) {
			if err := c.kube.Delete(ctx, a.PodName); err != nil {
				return err
			}
			return c.store.MarkRuntimePodDeleted(ctx, a.RoomID, a.Generation)
		}
		return err
	}
	if errors.Is(err, errKubeNotFound) {
		if a.Observed == store.RuntimeObservedReady {
			return c.advanceGeneration(ctx, a)
		}
		if err := c.kube.Create(ctx, runtimePodSpec{RoomID: a.RoomID, Generation: a.Generation, Name: a.PodName, Image: c.image, SecretName: c.secretName, SecretEnv: c.secretEnv, Env: c.env, Profile: c.runtimeProfile, PrincipalKeyVersion: c.principalKeyVersion}); err != nil {
			return err
		}
		return c.store.MarkRuntimePodCreating(ctx, a.RoomID, a.Generation)
	}
	if err != nil {
		return err
	}
	if pod.PrincipalKeyVersion != c.principalKeyVersion {
		if err := c.kube.Delete(ctx, a.PodName); err != nil {
			return err
		}
		return c.advanceGeneration(ctx, a)
	}
	if pod.Ready {
		return c.store.MarkRuntimePodReady(ctx, a.RoomID, a.Generation, pod.IP)
	}
	if pod.Phase == "Unknown" {
		if err := c.kube.Delete(ctx, a.PodName); err != nil {
			return err
		}
		return c.advanceGeneration(ctx, a)
	}
	// Scheduler admission failure is distinct from a pod that was admitted but
	// is still starting. Both remain bounded, but the condition is preserved for
	// diagnosis and never promoted to Ready.
	if pod.Phase == "Pending" && pod.Unschedulable {
		if !c.readinessExpired(pod) {
			return nil
		}
		if err := c.kube.Delete(ctx, a.PodName); err != nil {
			return err
		}
		return c.advanceGeneration(ctx, a)
	}
	if (pod.Phase == "Pending" || pod.Phase == "Running") && c.readinessExpired(pod) {
		if err := c.kube.Delete(ctx, a.PodName); err != nil {
			return err
		}
		return c.advanceGeneration(ctx, a)
	}
	// Failed is intentionally represented by a non-ready observed pod after
	// the controller has seen it; deletion plus a generation advance fences it.
	if pod.Phase == "Failed" || pod.Phase == "Succeeded" {
		if err := c.kube.Delete(ctx, a.PodName); err != nil {
			return err
		}
		return c.advanceGeneration(ctx, a)
	}
	return nil
}

func (c *RuntimeController) readinessExpired(pod runtimePod) bool {
	if pod.CreatedAt.IsZero() {
		return false
	}
	now := time.Now()
	if c.now != nil {
		now = c.now()
	}
	return !now.Before(pod.CreatedAt.Add(c.readinessTimeout))
}

func (c *RuntimeController) advanceGeneration(ctx context.Context, a store.RuntimeAssignment) error {
	_, err := c.store.AdvanceRuntimeGeneration(ctx, a.RoomID, a.Generation)
	if errors.Is(err, app.ErrRuntimeGenerationStale) {
		return nil
	}
	return err
}
