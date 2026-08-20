# Authority matrix

| Capability / authority | `l3e` |
| --- | --- |
| Consume frozen L3-D signals | YES |
| Admit exact Trader V0 artifact | YES |
| Create simulated orders/fills/partial fills | YES |
| Maintain simulated positions/P&L | YES |
| Model latency, slippage, stops, risk, replay | YES |
| Persist and recover simulation state | YES |
| Submit real broker order | NO |
| Contact broker execution or prop account | NO |
| Control copier | NO |
| Alter Trader V0 or L3-C confidence | NO |
| Tune from P&L | NO |
| Scientific authority | NO |
| Live-capital authority | NO |

The source imports only Lane III contracts, frozen market-data types, and frozen Trader V0 signal types; static commissioning tests reject provider, broker, copy-trade, scientific Phase E, and network dependencies.
