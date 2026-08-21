# NinjaScript AddOn boundary

Sources: [AddOn](../../../ninjatrader/NinjaScript/AddOns/BeelzebubReadOnlyAddOn.cs) and [market observer](../../../ninjatrader/NinjaScript/Indicators/BeelzebubReadOnlyMarketObserver.cs).

The AddOn only subscribes to account item, execution, order, and position updates. The indicator only observes trade/bid/ask/depth callbacks for the resolved chart instrument. No execution API call, ATM API, follower selection, command deserializer, or inbound protocol is in either source.

Compilation/install validation inside NinjaTrader is still required before treating these sources as an authentic provider.
