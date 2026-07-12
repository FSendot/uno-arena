module unoarena/services/gateway

go 1.26.0

toolchain go1.26.5

require (
	github.com/redis/go-redis/v9 v9.14.1
	github.com/twmb/franz-go v1.21.5
	unoarena/shared v0.0.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
)

replace unoarena/shared => ../../../shared
