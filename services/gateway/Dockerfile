# Build from repository root:
#   docker build -f services/gateway/Dockerfile -t uno-arena/gateway:local .
FROM golang:1.26.5-alpine3.24 AS builder
WORKDIR /src
COPY services/gateway/src/ ./services/gateway/src/
COPY shared/ ./shared/
COPY platform/telemetry/ ./platform/telemetry/
WORKDIR /src/services/gateway/src
ENV GOWORK=off
ENV CGO_ENABLED=0
RUN go build -o /service .

FROM alpine:3.24.1
WORKDIR /app
COPY --from=builder /service .
EXPOSE 8080 9090
CMD ["./service"]
