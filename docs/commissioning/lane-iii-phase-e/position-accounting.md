# Position accounting

Simulated position is derived from simulated fills only. It is `FLAT`, `LONG quantity`, `SHORT quantity`, or explicitly `UNKNOWN`; a signal cannot make it directional and a restart cannot make unknown state flat.

The state tracks quantity, average entry price, realized P&L, fees paid, open orders, and fill references. MNQ P&L is exact decimal arithmetic: `(exit - average entry) / 0.25 × $0.50 × quantity × direction`, less configured commission per fill. Fees are neutral configurable defaults, not an invented prop-firm schedule.

MAE and MFE are recorded as deterministic mark-to-bid (long) / mark-to-ask (short) currency diagnostics while exposure exists. They do not feed Trader V0, confidence, stops, or any optimization.
