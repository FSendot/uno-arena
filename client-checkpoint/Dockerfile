# Client Checkpoint CLI image. Build from this directory:
#   docker build -t uno-arena/client:local .
# Or from the repository root:
#   docker build -f client-checkpoint/Dockerfile -t uno-arena/client:local ./client-checkpoint
#
# Target any BFF with UNOARENA_API_URL, e.g.:
#   docker run --rm -e UNOARENA_API_URL=http://host.docker.internal:8080 uno-arena/client:local health
FROM alpine:3.24.1

RUN apk add --no-cache bash curl python3 \
  && adduser -D -H -u 10001 unoarena

COPY bin/unoarena /usr/local/bin/unoarena
COPY lib/game_client.py /usr/local/lib/game_client.py
RUN chmod 755 /usr/local/bin/unoarena /usr/local/lib/game_client.py

USER unoarena
WORKDIR /home/unoarena

ENTRYPOINT ["unoarena"]
