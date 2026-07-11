package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// envelopeMetadataV1 is readable Kurrent user metadata for an encrypted event.
// All fields in canonicalAAD are known before Seal (payload nonce is pre-generated).
type envelopeMetadataV1 struct {
	EnvelopeVersion   int    `json:"envelopeVersion"`
	KeyVersion        int    `json:"keyVersion"`
	WrappedDEK        string `json:"wrappedDek"`
	WrapNonce         string `json:"wrapNonce"`
	PayloadNonce      string `json:"payloadNonce"`
	OriginalEventID   string `json:"originalEventId"`
	OriginalEventType string `json:"originalEventType"`
	Stream            string `json:"stream"`
	RoomID            string `json:"roomId,omitempty"`
	GameID            string `json:"gameId,omitempty"`
	KurrentRevision   uint64 `json:"kurrentRevision"`
	DomainRevision    uint64 `json:"domainRevision"`
	EventUUID         string `json:"eventUuid"`
}

func (m envelopeMetadataV1) marshal() ([]byte, error) {
	return json.Marshal(m)
}

func parseEnvelopeMetadata(raw []byte) (envelopeMetadataV1, error) {
	var m envelopeMetadataV1
	if err := json.Unmarshal(raw, &m); err != nil {
		return envelopeMetadataV1{}, fmt.Errorf("envelope metadata: %w", err)
	}
	if m.EnvelopeVersion != envelopeVersionV1 {
		return envelopeMetadataV1{}, fmt.Errorf("unsupported envelope version %d", m.EnvelopeVersion)
	}
	return m, nil
}

func (m envelopeMetadataV1) wrappedDEKBytes() ([]byte, error) {
	return hex.DecodeString(m.WrappedDEK)
}

func (m envelopeMetadataV1) wrapNonceBytes() ([]byte, error) {
	return hex.DecodeString(m.WrapNonce)
}

func (m envelopeMetadataV1) payloadNonceBytes() ([]byte, error) {
	return hex.DecodeString(m.PayloadNonce)
}

// canonicalAAD binds the full immutable envelope metadata (no ciphertext).
func (m envelopeMetadataV1) canonicalAAD() []byte {
	var buf []byte
	buf = appendLengthPrefixed(buf, []byte(fmt.Sprintf("%d", m.EnvelopeVersion)))
	buf = appendLengthPrefixed(buf, []byte(fmt.Sprintf("%d", m.KeyVersion)))
	buf = appendLengthPrefixed(buf, []byte(m.WrappedDEK))
	buf = appendLengthPrefixed(buf, []byte(m.WrapNonce))
	buf = appendLengthPrefixed(buf, []byte(m.PayloadNonce))
	buf = appendLengthPrefixed(buf, []byte(m.OriginalEventID))
	buf = appendLengthPrefixed(buf, []byte(m.OriginalEventType))
	buf = appendLengthPrefixed(buf, []byte(m.Stream))
	buf = appendLengthPrefixed(buf, []byte(m.RoomID))
	buf = appendLengthPrefixed(buf, []byte(m.GameID))
	buf = appendLengthPrefixed(buf, []byte(fmt.Sprintf("%d", m.KurrentRevision)))
	buf = appendLengthPrefixed(buf, []byte(fmt.Sprintf("%d", m.DomainRevision)))
	buf = appendLengthPrefixed(buf, []byte(m.EventUUID))
	return buf
}

func hexBytes(b []byte) string { return hex.EncodeToString(b) }
