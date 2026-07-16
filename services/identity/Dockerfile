# Build from repository root:
#   docker build -f services/identity/Dockerfile -t uno-arena/identity:local .
FROM golang:1.26.5-alpine3.24 AS builder
WORKDIR /src
COPY services/identity/src/ ./services/identity/src/
COPY shared/ ./shared/
COPY platform/telemetry/ ./platform/telemetry/
WORKDIR /src/services/identity/src
ENV GOWORK=off
ENV CGO_ENABLED=0
RUN go build -o /service .

FROM alpine:3.24.1
WORKDIR /app
COPY --from=builder /service .
EXPOSE 8080 9090
CMD ["./service"]
