# Market-data adapter and L3-B compatibility

The adapter accepts strict provider packets for quotes, trades, and aggregated DOM snapshots and emits frozen L3-B `QuoteEvent`, `TradeEvent`, and `BookSnapshotEvent` records. Every record retains resolved concrete MNQ contract identity, provider sequence when supplied, provider timestamp, receipt timestamp, source identity, raw-event identity, and a raw payload hash.

| L3-B requirement | Status |
| --- | --- |
| Trade tape | AUTHENTICALLY OBSERVED — 4,224 L1-capture trades and 869 L2-capture trades |
| Best bid/ask | AUTHENTICALLY OBSERVED — 21,739 L1-capture quotes and 3,433 L2-capture quotes |
| DOM | AUTHENTICALLY OBSERVED WITH LIMITATION — 46,214 aggregated depth frames; not order-by-order CME depth |
| Timestamps / sequence | SUPPORTED WITH LIMITATION — provider timestamp/sequence preserved when supplied |
| Volume | AUTHENTICALLY OBSERVED WITH LIMITATION — provider size only; no fabricated totals |
| Contract / session identity | PARTIALLY AUTHENTICATED — native `MNQ SEP26` and per-channel session guarding; separate expiry/exchange/tick/point metadata unverified |

L1 and Level 2 entitlement and the observation payload shapes are authenticated. Depth ordering remains local bridge ordering rather than exchange sequence; disconnect/reconnect lifecycle is tested but the fresh 2026-08-23 provider attempt was unavailable at the NinjaTrader Welcome screen. Malformed, non-tick-aligned, wrong-contract, or falsely sequenced observations fail closed; timestamp absence remains explicit rather than fabricated.
