# Pinned platform sources

`sources.lock.tsv` is the official-source ledger. `SHA256SUMS` is the local
artifact ledger. `verify.sh` checks exact inventory, rejects symlinks, verifies
every artifact, and also runs the independent Istio 1.30.2 chart verifier.

Acquisition is a deliberate operator action, separate from cluster bootstrap:

```bash
# Networked: fetch the exact locked URLs.
./infrastructure/local-production/vendor/fetch.sh

# Offline import: upstream basenames in a reviewed directory.
./infrastructure/local-production/vendor/fetch.sh --source-dir /reviewed/releases
```

`fetch.sh` verifies each candidate before replacing the repository copy. All
installation scripts are offline-only and fail closed through `verify.sh`.
Istio remains vendored at `infrastructure/istio`; its official archive release
and checksum are recorded here, while its extracted charts have their own
aggregate checksum ledger.
