# Shadow commissioning protocol

When direct API access is verified, run only:

```text
authenticated provider observations → L3-F → frozen L3-B → frozen L3-C → frozen L3-D
                                                        ↓ optional frozen L3-E simulation
```

Record capability status, resolved contract, sanitized packet hashes, stream health, reconciliation state, and signal count. Label all artifacts `OBSERVED_EXTERNAL` or `SIMULATED`; `LIVE_EXECUTION` cannot be produced by l3f.

Current result: **not performed**. Provider credentials, account mapping, contract selection, and entitlement are absent. Real provider requests: 0. Real orders: 0.
