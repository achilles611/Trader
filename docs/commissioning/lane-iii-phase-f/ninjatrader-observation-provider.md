# NinjaTrader observation provider

The provider emits typed records only: connection, instrument, trade, quote, depth, account, position, order, execution, health, and snapshot-complete. All records state `NINJATRADER / LUCID_CQG / PROP_SIM`; the evaluation account is `PROVIDER_EVALUATION`, not `LIVE_EXECUTION`.

The authentic observed native identity is `MNQ SEP26`. Emission of expiry, exchange, tick size, and point value as separately authenticated bridge fields remains unverified. Those fields matter before future downstream decision/execution readiness; they do not create another listener owner and are not required to freeze the observation transport itself.
