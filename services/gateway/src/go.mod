module unoarena/services/gateway

go 1.26.0

toolchain go1.26.5

require (
	github.com/redis/go-redis/v9 v9.14.1
	unoarena/shared v0.0.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
)

replace unoarena/shared => ../../../shared
