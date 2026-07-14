# Build from repository root:
#   docker build -f services/game-integrity/Dockerfile -t uno-arena/game-integrity:local .
FROM golang:1.26.5-alpine3.24 AS builder
WORKDIR /src
COPY services/game-integrity/src/ ./services/game-integrity/src/
COPY platform/telemetry/ ./platform/telemetry/
WORKDIR /src/services/game-integrity/src
ENV GOWORK=off
ENV CGO_ENABLED=0
RUN go build -o /service .

FROM alpine:3.24.1
WORKDIR /app
COPY --from=builder /service .
EXPOSE 8080 9090
CMD ["./service"]
