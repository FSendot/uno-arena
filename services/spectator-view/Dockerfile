# Build from repository root:
#   docker build -f services/spectator-view/Dockerfile -t uno-arena/spectator-view:local .
FROM golang:1.26.5-alpine3.24 AS builder
WORKDIR /src
COPY services/spectator-view/src/ ./services/spectator-view/src/
COPY platform/telemetry/ ./platform/telemetry/
WORKDIR /src/services/spectator-view/src
ENV GOWORK=off
ENV CGO_ENABLED=0
RUN go build -o /service .

FROM alpine:3.24.1
WORKDIR /app
COPY --from=builder /service .
EXPOSE 8080
EXPOSE 9090
CMD ["./service"]
