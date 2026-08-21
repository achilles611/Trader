# Market-data fidelity

| Dimension | Status | Semantics |
| --- | --- | --- |
| Trade tape | AUTHENTICALLY_OBSERVED | Native `Last` callbacks reached the bridge (869 accepted observations in the fresh Level 2 capture; 4,224 in the earlier L1/account capture). |
| Best bid/ask | AUTHENTICALLY_OBSERVED | Native Bid/Ask callbacks reached the bridge (3,433 accepted quote observations in the fresh Level 2 capture). This remains L1 best bid/ask, separate from the depth stream. |
| DOM | AUTHENTICALLY_OBSERVED | After the Lucid/Tradovate Level 2 entitlement became active, NinjaTrader `OnMarketDepth` produced 46,214 accepted `DEPTH` observations in one fresh post-restart capture, with no bridge rejections. Each frame is an aggregated price-level bid/ask snapshot after one Add/Update/Remove delta; its numeric price/size arrays are ascending by price on both sides and carry the triggering side/operation. It has no order ID, queue position, or per-order quantity, so it is market-by-price L2—not MBO or individual-order queue depth. |
| Provider timestamp | SUPPORTED_WITH_LIMITATION | Market callbacks supply NinjaTrader callback time; account snapshots have no provider time. It is not asserted to be an exchange timestamp. |
| Exchange timestamp | NOT_SUPPORTED | No exchange timestamp was exposed by the authenticated bridge. |
| Provider sequence | NOT_SUPPORTED | Always `UNAVAILABLE`; local monotonic ordering is explicitly local only. |
| Volume | SUPPORTED_WITH_LIMITATION | Callback volume only. |
| Contract/session identity | PARTIALLY_AUTHENTICATED | Native identity `MNQ SEP26` was observed. Expiration/exchange/tick-size/point-value have not yet been emitted as authenticated metadata; per-channel session guarding is active. |

The previous L1-only/`UnknownSymbol` result is superseded by the authenticated Level 2 capture. Depth snapshot ordering is local bridge ordering; it does not establish an exchange sequence or an exchange timestamp.
