# Canonical event model

Every canonical event has an `EventHeader` containing a canonical event ID, provider/feed identity, `MNQContract`, stream, three-clock timestamps, optional provider sequence/event ID, raw-event ID, and raw-payload SHA-256.  `MNQContract` requires `strategy_instrument=MNQ`, exchange `CME`, a concrete month-code contract such as `MNQU6`, and a matching `YYYY-MM` expiry.

Supported event records are:

| Record | Normalized fields |
| --- | --- |
| Trade | tick-aligned price, positive size, aggressor side, aggressor provenance, optional quote derivation ID |
| Quote | best bid/ask prices and positive sizes, spread derivable in ticks |
| Book snapshot | complete ordered bid and ask level tuples |
| Book delta | side, tick-aligned price, `UPSERT` positive aggregate quantity or `REMOVE` |
| Derivatives extension | underlying, expiry, strike, put/call, optional OI/volume, explicit data-vintage time |

Prices are `Decimal`, must be finite, positive, and aligned to the MNQ 0.25 tick. Quantities are integer and positive except explicitly optional derivative zero values. Crossed quotes/books, duplicate depth levels, invalid month/expiry identities, NaN/infinity, and malformed timestamps are refused before they reach reconstructed state.

`AggressorSide.UNKNOWN` is preserved as unknown. A known side must say whether a provider supplied it or L3-B derived it from a named quote event; neither case is represented as an unqualified exchange fact.
