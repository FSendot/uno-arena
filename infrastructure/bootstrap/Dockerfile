# Repo-root buildable bootstrap image.
# Embeds services/*/migrations as the single source of schema SQL.
# Fingerprints are generated at build time from those migrations (no SQL duplication).
# Build: docker build -f infrastructure/bootstrap/Dockerfile -t uno-arena/bootstrap:local .
FROM postgres:18.4-alpine3.24

RUN apk add --no-cache bash curl ruby

WORKDIR /bootstrap

# Service migrations (copied at build time from repo root context).
# Root .dockerignore must NOT exclude these paths (see !services/*/migrations exceptions).
COPY services/identity/migrations/ /bootstrap/migrations/identity/
COPY services/room-gameplay/migrations/ /bootstrap/migrations/room-gameplay/
COPY services/tournament-orchestration/migrations/ /bootstrap/migrations/tournament-orchestration/
COPY services/ranking/migrations/ /bootstrap/migrations/ranking/
COPY services/analytics/migrations/ /bootstrap/migrations/analytics/

# Infrastructure-owned wrappers / fingerprint generator (do not rewrite service migrations).
COPY infrastructure/bootstrap/sql/ /bootstrap/sql/
COPY infrastructure/bootstrap/bin/ /bootstrap/bin/
COPY infrastructure/bootstrap/lib/ /bootstrap/lib/

# Generate fingerprints from the embedded migrations (paths remapped for image layout).
RUN mkdir -p /tmp/fp-gen/services/identity/migrations \
             /tmp/fp-gen/services/room-gameplay/migrations \
             /tmp/fp-gen/services/tournament-orchestration/migrations \
             /tmp/fp-gen/services/ranking/migrations \
             /tmp/fp-gen/services/analytics/migrations \
             /tmp/fp-gen/infrastructure/bootstrap \
  && cp /bootstrap/migrations/identity/001_init.sql /tmp/fp-gen/services/identity/migrations/ \
  && cp /bootstrap/migrations/room-gameplay/001_init.sql /tmp/fp-gen/services/room-gameplay/migrations/ \
  && cp /bootstrap/migrations/tournament-orchestration/001_init.sql /tmp/fp-gen/services/tournament-orchestration/migrations/ \
  && cp /bootstrap/migrations/ranking/001_init.sql /tmp/fp-gen/services/ranking/migrations/ \
  && cp /bootstrap/migrations/analytics/001_init.sql /tmp/fp-gen/services/analytics/migrations/ \
  && cp -R /bootstrap/lib /tmp/fp-gen/infrastructure/bootstrap/lib \
  && cp /bootstrap/bin/generate-fingerprints.rb /tmp/fp-gen/infrastructure/bootstrap/bin-generate-fingerprints.rb \
  && ruby -e "\
root='/tmp/fp-gen'; \
\$LOAD_PATH.unshift(File.join(root,'infrastructure/bootstrap/lib')); \
require 'fingerprint'; \
require 'fileutils'; \
out='/bootstrap/fingerprints'; FileUtils.mkdir_p(out); \
{ \
  'identity'=>['postgres','services/identity/migrations/001_init.sql'], \
  'room-gameplay'=>['postgres','services/room-gameplay/migrations/001_init.sql'], \
  'tournament-orchestration'=>['postgres','services/tournament-orchestration/migrations/001_init.sql'], \
  'ranking'=>['postgres','services/ranking/migrations/001_init.sql'], \
  'analytics'=>['clickhouse','services/analytics/migrations/001_init.sql'] \
}.each do |ctx,(engine,rel)| \
  path=File.join(root,rel); \
  doc=BootstrapFingerprint.fingerprint_for(path, context: ctx, engine: engine); \
  BootstrapFingerprint.write_json(File.join(out,\"#{ctx}.json\"), doc); \
  File.write(File.join(out,\"#{ctx}.env\"), [ \
    \"EXPECTED_CHECKSUM=#{doc['checksum']}\", \
    \"EXPECTED_TABLES=#{doc['tables'].join(',')}\", \
    \"EXPECTED_VIEWS=#{Array(doc['views']).join(',')}\", \
    \"EXPECTED_MATERIALIZED_VIEWS=#{Array(doc['materialized_views']).join(',')}\", \
    \"EXPECTED_SEQUENCES=#{Array(doc['sequences']).join(',')}\", \
    \"EXPECTED_INDEXES=#{Array(doc['indexes']).join(',')}\" \
  ].join(\"\\n\")+\"\\n\"); \
end"

RUN chmod +x /bootstrap/bin/*.sh \
  && chmod +x /bootstrap/bin/*.rb || true

ENTRYPOINT ["/bootstrap/bin/bootstrap-entrypoint.sh"]
