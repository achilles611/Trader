# Simulation contract

The public contract is deliberately named `Simulated*` throughout: `SimulatedOrder`, `SimulatedFill`, `SimulatedPosition`, and `SimulatedMarketState`. These represent no broker order, real fill, real position, or real P&L.

Scope is exactly strategy root `MNQ` plus one configured concrete CME contract. Quantity is `SimulationConfig.configured_quantity`, never a signal field. The default is one MNQ. Configuration also owns maximum exposure, open order count, working-order age, loss ceiling, commission, stop distance, slippage, and latency.

Supported actions are narrowly market entry, market exit, protective stop, cancel request, and simulated operator flatten. `l3e` does not expose a universal exchange-order language, account selection, account credentials, live transport, or a broker submission method.
