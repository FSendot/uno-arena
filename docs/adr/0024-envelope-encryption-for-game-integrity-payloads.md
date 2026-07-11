# Envelope Encryption for Game Integrity Payloads

## Status
Accepted

## Context
Game Integrity stores deck seeds, shuffled order, private card identities, reservations, and replay material for high-stakes audit and dispute resolution. Transport mTLS and encrypted volumes protect network links and physical media, but they do not prevent a database administrator, backup operator, storage credential holder, or compromised KurrentDB process from reading plaintext payloads.

## Decision
Encrypt replay-sensitive Game Integrity event data at the application boundary before KurrentDB append. Use an AES-256-GCM data-encryption key per game stream. Wrap each data key with a versioned key-encryption provider; production requires a managed KMS/HSM-backed provider, while local tests and `kind` use an explicitly non-production development provider. Event type, stream identity, expected revision, event ID, key version, nonce, and cryptographic seed/order commitments remain readable metadata; deck seed/order, reservations, private cards, and other hidden replay payloads remain ciphertext.

Authorized replay/audit code obtains decryption through the key provider under scoped service identity and records every decrypt/export request with actor, reason, stream/game, correlation ID, key version(s), and outcome. Each stream generates and wraps its DEK once on the first successful append and reuses the identical wrapped DEK / wrap nonce / key-version identity on every subsequent event; replay unwraps the first event once and requires later events to carry the same stream key identity. Key rotation creates new streams under the current wrapping key; existing immutable streams continue on their original DEK and key version unless an explicit rewrap mechanism is added later. Historical key versions remain available only to authorized replay/audit paths. Spectator View, Analytics, public clients, and ordinary operators never receive decryption capability.

## Consequences
KurrentDB, volume, snapshot, and backup access alone cannot reveal unreleased deck material. Replay and audit now depend on key-provider availability and retained historical key versions, so key loss is permanent audit-data loss and must be covered by the RPO/RTO, restore drills, rotation, access control, and key-history recovery in ADR-0034. Encryption/decryption latency and failures are part of the Game Integrity write/replay budgets. Mesh mTLS and encrypted storage remain complementary controls rather than substitutes.
