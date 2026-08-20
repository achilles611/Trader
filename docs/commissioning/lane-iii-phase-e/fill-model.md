# Fill model

The model is deterministic, intentionally pessimistic, and intentionally modest. A marketable buy fills against the post-latency best ask and displayed ask quantity; a sell fills against the post-latency best bid and displayed bid quantity. The currently processed replay observation supplies the capacity. Capacity is consumed in deterministic risk/stop/exit/entry priority order within that observation.

Fill price includes configurable conservative whole-tick slippage: ask plus ticks for buys and bid minus ticks for sells. A zero quantity at the relevant top of book does not fabricate a fill. A quantity larger than available capacity creates a partial fill and leaves an explicit remaining order.

No queue position, hidden liquidity, exchange priority, or exchange-grade matching behavior is claimed. The model is suitable for mechanical lifecycle testing, not for asserting executable profitability.
