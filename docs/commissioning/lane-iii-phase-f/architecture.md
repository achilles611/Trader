# Architecture

```text
Tradovate REST / WebSocket (explicit DEMO or LIVE)
                 ↓ read-only named operations
    L3-F observation service + market-data adapter
                 ↓ canonical L3-B events
            frozen L3-B → L3-C → L3-D
                 ↓
             shadow signals / optional frozen L3-E simulation
                 X
           no provider execution path
```

`RequestsTradovateReadOnlyClient` exposes only authentication plus named account, contract, position, and order reads. `TradovateReadOnlyWebSocket` exposes only authorization, quote/DOM/tick-chart subscription, and user synchronization. There is no generic provider request function and no strategy receives either client.

Provider account/order/position observations are reconciled separately from market evidence. They never become L3-C evidence.
