# Market-data adapter and L3-B compatibility

The adapter accepts strict provider packets for quotes, trades, and aggregated DOM snapshots and emits frozen L3-B `QuoteEvent`, `TradeEvent`, and `BookSnapshotEvent` records. Every record retains resolved concrete MNQ contract identity, provider sequence when supplied, provider timestamp, receipt timestamp, source identity, raw-event identity, and a raw payload hash.

| L3-B requirement | Status |
| --- | --- |
| Trade tape | SUPPORTED WITH LIMITATION — tick-chart/packet schema must be commissioned |
| Best bid/ask | SUPPORTED WITH LIMITATION — entitlement/schema unverified |
| DOM | SUPPORTED WITH LIMITATION — aggregated depth only, not order-by-order CME depth |
| Timestamps / sequence | SUPPORTED WITH LIMITATION — provider timestamp/sequence preserved when supplied |
| Volume | SUPPORTED WITH LIMITATION — provider size only; no fabricated totals |
| Contract / session identity | FULLY MODELED, uncommissioned |

Actual entitlement, payload completeness, ordering, and reconnection remain **UNKNOWN**. Malformed, non-tick-aligned, wrong-contract, unsequenced-as-sequenced, or timestamp-free observations fail closed.
