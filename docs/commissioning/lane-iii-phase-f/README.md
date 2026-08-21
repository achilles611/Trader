# Lane III Phase F (`l3f`) — Tradovate observation boundary

`l3f` adds a concrete, **read-only** Lucid Trading / Tradovate integration boundary. It is downstream of frozen `l3b → l3c → l3d` and may feed frozen `l3e` only as simulation input.

Status on 2026-08-20: `l3f3` authenticated the NinjaTrader Desktop observation path. The loopback receiver accepted 27,492 native observations with no rejections: connection, account, quote, trade, and explicit position/order snapshot records. The evaluation account is bound only to the persistent alias `Lucid25kflex01`; Sim101 is separately classified as local simulation. The authoritative snapshot confirmed the Lucid alias is flat with no working orders.

The Level 2 re-commissioning capture subsequently accepted 46,214 genuine `DEPTH` observations from NinjaTrader alongside quotes and trades. It exposes aggregated price-level L2 snapshots, not MBO/order-by-order depth. This is still deliberately **not a freeze**: provider reconnect, authenticated contract metadata beyond the native name, and the downstream live shadow path remain incomplete. No real order, modification, cancellation, flatten operation, or capital touch occurred.

Fresh l3f3 commissioning on 2026-08-21 listened for 60 seconds on `127.0.0.1:48135` and accepted zero frames. The sanitized result is [receiver-observation-2026-08-21.json](receiver-observation-2026-08-21.json). It proves the new receiver was available but does not prove a currently connected NinjaTrader/Lucid source, so it leaves the runtime freeze gate blocked.

The implementation is [tradovate_observation.py](../../../src/l3f_provider/tradovate_observation.py). It sits outside the frozen Lane III package and contains a named read-only REST transport, a read-only WebSocket subscription client, strict L3-B normalization, account/position/order observation, reconciliation, health tracking, secret redaction, and future-only compliance diagnostics. It contains no real order authority.

See [closure audit](closure-audit.md) for the current hard-gate result.
