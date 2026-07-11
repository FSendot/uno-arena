# Build from repository root:
#   docker build -f services/ranking/Dockerfile -t uno-arena/ranking:local .
FROM golang:1.26.5-alpine3.24 AS builder
WORKDIR /src
COPY services/ranking/src/ ./services/ranking/src/
WORKDIR /src/services/ranking/src
ENV GOWORK=off
ENV CGO_ENABLED=0
RUN go build -o /service .

FROM alpine:3.24.1
WORKDIR /app
COPY --from=builder /service .
EXPOSE 8080
CMD ["./service"]
