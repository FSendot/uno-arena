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
