FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY src/ .
RUN go build -o service .

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/service .
EXPOSE 8080
CMD ["./service"]
