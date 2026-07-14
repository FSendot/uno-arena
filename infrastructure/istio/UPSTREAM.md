# Vendored Istio charts

These chart sources are copied without modification from
`istio-1.30.2-osx-arm64.tar.gz`, published with the Istio 1.30.2 GitHub release.
The downloaded release archive SHA-256 was
`56deb84b26fefbf425eadc6b71cc9a32da5d8d1a62560c74968d27af80ba18d7`.

`verify.sh` hashes every file in each chart in sorted path order and fails closed
if its deterministic aggregate differs from the reviewed source:

| chart | aggregate SHA-256 |
|---|---|
| base | `c7ba96fff51bdaf2f875adb68510965ecf3c66432d1a773d4163f7bcd90531b4` |
| istiod | `8c8fbe372281b0287d1ce6e3a4b7e0c1f8b74bbd11595df090224567a6c1355c` |
| cni | `df7b368ee1b9bf1e7aab0e6992f013eb044a6316e0f390601bcd49285a06ab83` |
| ztunnel | `26bb678de639fbc0262f903a3db13fdf862eea122c7ed870a471632f92137c07` |

