{{- define "room-gameplay.image" -}}
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

{{- define "room-gameplay.validateTopologySecurity" -}}
{{- $deploymentEnv := default "" .Values.env.DEPLOYMENT_ENV | toString -}}
{{- if lt (int .Values.topology.controller.replicaCount) 1 -}}
{{- fail "topology.controller.replicaCount must be at least 1" -}}
{{- end -}}
{{- if lt (int .Values.topology.controller.dbPoolMaxConns) 1 -}}
{{- fail "topology.controller.dbPoolMaxConns must be at least 1" -}}
{{- end -}}
{{- if or (lt (int .Values.topology.controller.claimBatch) 1) (gt (int .Values.topology.controller.claimBatch) 64) -}}
{{- fail "topology.controller.claimBatch must be between 1 and 64" -}}
{{- end -}}
{{- if or (lt (int .Values.topology.controller.concurrency) 1) (gt (int .Values.topology.controller.concurrency) 16) -}}
{{- fail "topology.controller.concurrency must be between 1 and 16" -}}
{{- end -}}
{{- if gt (int .Values.topology.controller.concurrency) (int .Values.topology.controller.claimBatch) -}}
{{- fail "topology.controller.concurrency must not exceed claimBatch" -}}
{{- end -}}
{{- if lt (int .Values.topology.controller.readinessTimeoutSeconds) 1 -}}
{{- fail "topology.controller.readinessTimeoutSeconds must be at least 1" -}}
{{- end -}}
{{- if and .Values.topology.enabled .Values.topology.pgbouncer.enabled -}}
{{- if lt (int .Values.topology.pgbouncer.replicaCount) 1 -}}
{{- fail "topology.pgbouncer.replicaCount must be at least 1" -}}
{{- end -}}
{{- if lt (int .Values.topology.pgbouncer.maxClientConn) 1 -}}
{{- fail "topology.pgbouncer.maxClientConn must be at least 1" -}}
{{- end -}}
{{- if lt (int .Values.topology.pgbouncer.defaultPoolSize) 1 -}}
{{- fail "topology.pgbouncer.defaultPoolSize must be at least 1" -}}
{{- end -}}
{{- if lt (int .Values.topology.pgbouncer.reservePoolSize) 0 -}}
{{- fail "topology.pgbouncer.reservePoolSize must not be negative" -}}
{{- end -}}
{{- if ne .Values.topology.pgbouncer.poolMode "transaction" -}}
{{- fail "Room-owned PgBouncer poolMode must remain transaction" -}}
{{- end -}}
{{- end -}}
{{- if or (eq $deploymentEnv "staging") (eq $deploymentEnv "production") -}}
{{- if not .Values.topology.enabled -}}
{{- fail "staging/production requires the Room runtime topology" -}}
{{- end -}}
{{- if not .Values.topology.pgbouncer.enabled -}}
{{- fail "staging/production requires Room-owned PgBouncer" -}}
{{- end -}}
{{- if ne .Values.topology.pgbouncer.authType "scram-sha-256" -}}
{{- fail "staging/production PgBouncer authType must be scram-sha-256" -}}
{{- end -}}
{{- if not .Values.topology.pgbouncer.tls.enabled -}}
{{- fail "staging/production PgBouncer TLS must be enabled" -}}
{{- end -}}
{{- if empty .Values.topology.pgbouncer.tls.secretName -}}
{{- fail "staging/production PgBouncer TLS secretName is required" -}}
{{- end -}}
{{- if or (empty .Values.topology.pgbouncer.tls.caKey) (empty .Values.topology.pgbouncer.tls.certKey) (empty .Values.topology.pgbouncer.tls.privateKeyKey) -}}
{{- fail "staging/production PgBouncer TLS CA, certificate, and private-key secret keys are required" -}}
{{- end -}}
{{- if ne .Values.topology.pgbouncer.tls.clientMode "require" -}}
{{- fail "staging/production PgBouncer client TLS mode must be require" -}}
{{- end -}}
{{- if ne .Values.topology.pgbouncer.tls.serverMode "verify-full" -}}
{{- fail "staging/production PgBouncer server TLS mode must be verify-full" -}}
{{- end -}}
{{- if not .Values.topology.pgbouncer.mesh.enforceStrictMTLS -}}
{{- fail "staging/production PgBouncer requires strict mesh mTLS enforcement" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "room-gameplay.imagePullPolicy" -}}
{{- if .Values.image.pullPolicy -}}
{{ .Values.image.pullPolicy }}
{{- else -}}
IfNotPresent
{{- end -}}
{{- end -}}
