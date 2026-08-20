# DOM reconstruction and liquidity measurements

`OrderBookReconstructor` is a single-threaded snapshot-plus-delta state machine. A sequenced full snapshot establishes `HEALTHY` bid/ask state. A next sequence delta updates aggregate displayed quantity; duplicate and late deltas do not mutate state. A sequence gap stops incremental application and marks state `GAPPED`; after reconnect it is `RECOVERING`. Only a new valid full snapshot can restore `HEALTHY`.

The reconstructed state carries source/contract identity, snapshot origin, latest event/receipt/sequence, the source-event chain, gap flag, quality, and canonical state hash. It is explicitly derived state rather than a raw observation.

Mechanical change records are limited to `ADD`, `REDUCE`, `PULL`, `REPLENISH`, and duration-based `PERSIST`. A disappearance starts as `REDUCE`; it becomes `PULL` only when the caller supplies the relevant observed-trade window and no matching execution is present. A reduction becomes `EXECUTE` only when a matching observed aggressive trade at the same price is supplied. No record uses terms such as spoofing, genuine intent, institutional behavior, support, resistance, or iceberg detection.
