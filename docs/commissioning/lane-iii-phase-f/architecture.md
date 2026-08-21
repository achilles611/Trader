# Architecture

```text
Lucid / CQG → NinjaTrader 8 Desktop → read-only AddOn → localhost-only one-way bridge
                                                        ↓
    L3-F3 loopback receiver + provider-neutral market-data adapter
                 ↓ canonical L3-B events
            frozen L3-B → L3-C → L3-D
                 ↓
             shadow signals / optional frozen L3-E simulation
                 X
           no provider execution path
```

Direct Tradovate remains retained as a structurally useful but unavailable account path. NinjaTrader is a separately modeled provider seam. Neither surface exposes a generic provider request function and no strategy receives either client.

Provider account/order/position observations are reconciled separately from market evidence. They never become L3-C evidence.

The receiver admits only complete, bounded, sanitized observation frames through `NinjaTraderObservation.from_wire()` and `NinjaTraderSessionLedger`. It has no response writes and no access to a NinjaTrader `Account` object. Transport loss is recorded as `LOCAL_BRIDGE` provider health, independently of account and market streams.
