module unoarena/services/room-gameplay

go 1.26.0

toolchain go1.26.5

require (
	unoarena/services/spectator-view v0.0.0
	unoarena/shared v0.0.0
)

replace unoarena/shared => ../../../shared

replace unoarena/services/spectator-view => ../../spectator-view/src
