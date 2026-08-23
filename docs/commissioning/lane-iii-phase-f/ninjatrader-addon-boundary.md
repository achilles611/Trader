# NinjaScript AddOn boundary

Sources: [AddOn](../../../ninjatrader/NinjaScript/AddOns/BeelzebubReadOnlyAddOn.cs) and [market observer](../../../ninjatrader/NinjaScript/Indicators/BeelzebubReadOnlyMarketObserver.cs).

The AddOn only subscribes to account item, execution, order, and position updates. The indicator only observes trade/bid/ask/depth callbacks for the resolved chart instrument. No execution API call, ATM API, follower selection, command deserializer, or inbound protocol is in either source.

The sources were installed, compiled, and used for the authentic 2026-08-20 captures. On 2026-08-23 the installed and repository AddOn SHA-256 values both remained `587EC0D324E1587BE22A132273FA91F6D659F5A0D517B413E5F39D3064DA8CB6`; the installed and repository indicator values both remained `194A61F189B27276042E5E1E9AC0536CB287F5868A13B74639E7BE88F8023CD5`.
