package main

import (
	"encoding/base64"

	"unoarena/services/game-integrity/domain"
)

func encodeStreamPart(id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(id))
}

func roomStreamName(roomID domain.RoomID) string {
	return "gi.room.v1." + encodeStreamPart(string(roomID))
}

func deckStreamName(roomID domain.RoomID, gameID domain.GameID) string {
	return "gi.deck.v1." + encodeStreamPart(string(roomID)) + "." + encodeStreamPart(string(gameID))
}

func gameBindStreamName(gameID domain.GameID) string {
	return "gi.gamebind.v1." + encodeStreamPart(string(gameID))
}
