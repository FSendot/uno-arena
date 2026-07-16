{{- define "uno-observability.namespace" -}}
{{- default .Release.Namespace .Values.namespaceOverride -}}
{{- end -}}

{{- define "uno-observability.labels" -}}
app.kubernetes.io/part-of: uno-arena
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "uno-observability.requireConfig" -}}
{{- $s3 := .Values.storage.s3 -}}
{{- if empty $s3.region }}{{ fail "storage.s3.region is required" }}{{ end -}}
{{- if empty $s3.existingSecret }}{{ fail "storage.s3.existingSecret is required" }}{{ end -}}
{{- if empty $s3.lokiBucket }}{{ fail "storage.s3.lokiBucket is required" }}{{ end -}}
{{- if empty $s3.tempoBucket }}{{ fail "storage.s3.tempoBucket is required" }}{{ end -}}
{{- if eq $s3.lokiBucket $s3.tempoBucket }}{{ fail "Loki and Tempo must use separate S3 buckets" }}{{ end -}}
{{- if empty .Values.grafana.existingAdminSecret }}{{ fail "grafana.existingAdminSecret is required" }}{{ end -}}
{{- $receiver := .Values.alertmanager.receiver -}}
{{- if and (empty $receiver.inlineUrl) (empty $receiver.existingSecret) }}{{ fail "alertmanager receiver inlineUrl or existingSecret is required" }}{{ end -}}
{{- if and (not (empty $receiver.inlineUrl)) (not (empty $receiver.existingSecret)) }}{{ fail "alertmanager receiver inlineUrl and existingSecret are mutually exclusive" }}{{ end -}}
{{- if and $receiver.externalSecret.enabled (empty $receiver.existingSecret) }}{{ fail "alertmanager externalSecret requires existingSecret" }}{{ end -}}
{{- if and $receiver.externalSecret.enabled (empty $receiver.externalSecret.secretStoreRef.name) }}{{ fail "alertmanager externalSecret secretStoreRef.name is required" }}{{ end -}}
{{- if and $receiver.externalSecret.enabled (empty $receiver.externalSecret.remoteKey) }}{{ fail "alertmanager externalSecret remoteKey is required" }}{{ end -}}
{{- if and (not (empty $receiver.inlineUrl)) (not .Values.alertmanager.webhookSink.enabled) }}{{ fail "inline alert receiver URLs are allowed only with the local webhook sink" }}{{ end -}}
{{- $redis := .Values.redisExporter -}}
{{- if and (empty $redis.address) (empty $redis.existingSecret) }}{{ fail "redisExporter address or existingSecret is required" }}{{ end -}}
{{- if and (not (empty $redis.address)) (not (empty $redis.existingSecret)) }}{{ fail "redisExporter address and existingSecret are mutually exclusive" }}{{ end -}}
{{- if and (not (empty $redis.existingSecret)) (empty $redis.addressKey) }}{{ fail "redisExporter addressKey is required with existingSecret" }}{{ end -}}
{{- if .Values.postSyncEvidence.enabled -}}
  {{- if lt (int .Values.postSyncEvidence.activeDeadlineSeconds) 1800 }}{{ fail "postSyncEvidence.activeDeadlineSeconds must tolerate independent service reconciliation" }}{{ end -}}
  {{- if lt (int .Values.postSyncEvidence.gatewayWaitAttempts) 1 }}{{ fail "postSyncEvidence.gatewayWaitAttempts must be positive" }}{{ end -}}
  {{- if lt (int .Values.postSyncEvidence.evidenceWaitAttempts) 1 }}{{ fail "postSyncEvidence.evidenceWaitAttempts must be positive" }}{{ end -}}
  {{- if lt (int .Values.postSyncEvidence.pollIntervalSeconds) 1 }}{{ fail "postSyncEvidence.pollIntervalSeconds must be positive" }}{{ end -}}
{{- end -}}
{{- range $name, $image := .Values.images -}}
  {{- if and (ne $name "pullPolicy") (not (regexMatch "@sha256:[a-f0-9]{64}$" (toString $image))) -}}
    {{- fail (printf "images.%s must use an immutable sha256 digest" $name) -}}
  {{- end -}}
{{- end -}}
{{- end -}}

{{- define "uno-observability.podSecurityContext" -}}
runAsNonRoot: true
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{- define "uno-observability.containerSecurityContext" -}}
allowPrivilegeEscalation: false
capabilities:
  drop: ["ALL"]
readOnlyRootFilesystem: true
{{- end -}}
