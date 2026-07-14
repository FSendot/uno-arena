{{- define "tournament-orchestration.image" -}}
{{- $kind := false -}}
{{- if not (empty .Values.kind) -}}
{{- if not (kindIs "bool" .Values.kind) -}}
{{- fail "kind must be a boolean when set" -}}
{{- end -}}
{{- $kind = .Values.kind -}}
{{- end -}}
{{- $deploymentEnv := "" -}}
{{- if and .Values.env .Values.env.DEPLOYMENT_ENV -}}
{{- $deploymentEnv = toString .Values.env.DEPLOYMENT_ENV -}}
{{- end -}}
{{- $localKind := and $kind (eq $deploymentEnv "local") -}}
{{- if and .Values.image.digest (ne .Values.image.digest "") -}}
{{- if not (kindIs "string" .Values.image.digest) -}}
{{- fail "image.digest must be a string" -}}
{{- end -}}
{{- if not (regexMatch "^sha256:[a-f0-9]{64}$" .Values.image.digest) -}}
{{- fail "image.digest must be sha256: followed by exactly 64 lowercase hex characters" -}}
{{- end -}}
{{ printf "%s@%s" .Values.image.repository .Values.image.digest }}
{{- else if $localKind -}}
{{- if and .Values.image.tag (ne .Values.image.tag "") -}}
{{ printf "%s:%s" .Values.image.repository .Values.image.tag }}
{{- else -}}
{{- fail "kind+local mode requires image.tag when image.digest is empty" -}}
{{- end -}}
{{- else -}}
{{- fail "image.digest is required unless kind=true and env.DEPLOYMENT_ENV=local; image.tag alone is not allowed" -}}
{{- end -}}
{{- end -}}

{{- define "tournament-orchestration.imagePullPolicy" -}}
{{- if .Values.image.pullPolicy -}}
{{ .Values.image.pullPolicy }}
{{- else -}}
IfNotPresent
{{- end -}}
{{- end -}}

{{- define "tournament-orchestration.telemetryEnv" -}}
- name: TELEMETRY_MODE
  value: {{ .root.Values.telemetry.mode | quote }}
- name: SERVICE_VERSION
  value: {{ .root.Chart.AppVersion | quote }}
- name: UNOARENA_COMPONENT
  value: {{ .component | quote }}
- name: POD_UID
  valueFrom:
    fieldRef:
      fieldPath: metadata.uid
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ .root.Values.telemetry.otlpEndpoint | quote }}
- name: OTEL_EXPORTER_OTLP_PROTOCOL
  value: "grpc"
- name: OTEL_TRACES_SAMPLER
  value: {{ .root.Values.telemetry.tracesSampler | quote }}
- name: OTEL_TRACES_SAMPLER_ARG
  value: {{ .root.Values.telemetry.tracesSamplerArg | quote }}
- name: OTEL_GO_X_OBSERVABILITY
  value: "true"
- name: METRICS_ADDR
  value: ":9090"
{{- end -}}
