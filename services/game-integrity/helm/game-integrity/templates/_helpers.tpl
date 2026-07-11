{{- define "game-integrity.image" -}}
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

{{- define "game-integrity.imagePullPolicy" -}}
{{- if .Values.image.pullPolicy -}}
{{ .Values.image.pullPolicy }}
{{- else if and .Values.image.digest (ne .Values.image.digest "") -}}
IfNotPresent
{{- else -}}
IfNotPresent
{{- end -}}
{{- end -}}
