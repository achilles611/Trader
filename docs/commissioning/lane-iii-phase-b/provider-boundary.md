# Provider boundary

`MarketDataProviderAdapter` is a protocol with only `source` and `normalize(raw_event) -> canonical events`. `DerivativesProviderAdapter` is separately scoped to vintage-bearing derivatives observations. No Rithmic, Tradovate, Bookmap, broker-native, or execution provider is selected or contacted by this phase.

Adapters own packet parsing and must create `RawProviderEvent` before producing strict canonical records. Bad provider data is rejected/quarantined rather than coerced. The core package imports no HTTP, WebSocket, broker, execution, Lane II, copy-trade, or Phase E module.

The optional derivatives record preserves expiry, strike, put/call, OI, volume, observation provenance, and data vintage. No provider is bundled, no options value is fabricated, and old OI cannot masquerade as newly observed data merely because it was read now.
