# Signal admission

Only an exact immutable `SignalDecision` from commissioned Trader V0 is admitted. The simulator verifies the deterministic decision identity, `l3-strategy-f6f549ced930c2b411b965984ffff555`, and artifact hash `9cc6526264ca340dbff8ca32f680d05563bed0e617c746c5af1bdceb4ccbc90a` before making a simulated action.

Admission is event-time based. It visibly records `ADMITTED`, `REJECTED`, `DUPLICATE`, or `IGNORED` in the ledger. Rejections cover malformed input, unknown identity, artifact mismatch, expiry, degraded/missing market state, disarm/pause, loss ceiling, existing exposure, and lifecycle/risk limits. An L3-D `NO_TRADE` is retained as an ignored non-execution fact.

The decision ID is the replay idempotency key. A duplicate can never create another simulated entry order. L3-D contains no contract or quantity command, so contract identity and quantity are independently controlled by `SimulationConfig`.
