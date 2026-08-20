# Latency and slippage

All timing is replay/event time. Default entry/exit eligibility is signal time plus 25 ms processing, 50 ms submission, and 25 ms simulated venue delay. Cancellation has a separately configurable 25 ms delay. A zero-delay fixture can be configured explicitly, but it is not the commissioned default.

The simulator uses the first valid market observation at or after order eligibility. It never fills an order at the source price merely because the decision was generated there. This makes both favorable and adverse movement during latency visible.

Slippage is an explicit configuration parameter, not a fitted result. The default is one tick on entry and exit, applied in the adverse direction from post-latency best bid/ask. It is combined with displayed top-of-book quantity and never tuned from simulated P&L.
