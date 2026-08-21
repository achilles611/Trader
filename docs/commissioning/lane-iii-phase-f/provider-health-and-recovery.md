# Provider health and recovery

Authentication, market data, and user/account data have independent health states: `UNKNOWN`, `CONNECTING`, `HEALTHY`, `STALE`, `DISCONNECTED`, and authentication expiry. Loss of market data does not erase separately healthy account truth; loss of user/account truth blocks any future downstream authority.

Token renewal discards old session authority, marks dependent streams disconnected, and reauthenticates explicitly. Renewal failure remains visible; it cannot leave a stale session looking current. Restart requires fresh authentication and reconciliation.

For NinjaTrader, L3-F2 separately models process, Lucid connection, market-data, depth, account, position, order, and local-bridge health. A healthy bridge does not imply a healthy Lucid connection or current account truth.
