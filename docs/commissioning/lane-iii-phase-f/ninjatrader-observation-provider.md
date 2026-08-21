# NinjaTrader observation provider

The provider emits typed records only: connection, instrument, trade, quote, depth, account, position, order, execution, health, and snapshot-complete. All records state `NINJATRADER / LUCID_CQG / PROP_SIM`; the evaluation account is `PROVIDER_EVALUATION`, not `LIVE_EXECUTION`.

The native contract is resolved rather than trusting display text. The expected result is MNQ, expiry `2026-09`, CME, 0.25 tick, and NinjaTrader internal contract identity. The exact observed native identity is not yet commissioned.
