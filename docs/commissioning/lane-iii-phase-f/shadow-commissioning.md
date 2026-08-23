# Shadow commissioning protocol

When direct API access is verified, run only:

```text
authenticated provider observations → L3-F → frozen L3-B → frozen L3-C → frozen L3-D
                                                        ↓ optional frozen L3-E simulation
```

Record capability status, resolved contract, sanitized packet hashes, stream health, reconciliation state, and signal count. Label all artifacts `OBSERVED_EXTERNAL` or `SIMULATED`; `LIVE_EXECUTION` cannot be produced by l3f.

Current result: authentic NinjaTrader bridge observation is commissioned for the persistent Lucid alias, separate Sim101 classification, native `MNQ SEP26`, quote, trade, and aggregated L2 callbacks. Driving the downstream live `l3b → l3c → l3d` shadow path is intentionally outside the observer freeze and has not been used to claim decision or execution readiness. Real orders: 0.
