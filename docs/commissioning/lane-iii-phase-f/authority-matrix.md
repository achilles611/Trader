# Authority matrix

| Capability | L3-F |
| --- | --- |
| Authenticate / discover / observe configured provider | YES, read-only boundary |
| Receive MNQ quotes, trades, DOM | YES if account-entitled and commissioned |
| Normalize into L3-B | YES |
| Observe master account, positions, orders / reconcile | YES |
| Shadow L3-B→L3-C→L3-D / feed L3-E | YES |
| Submit, modify, cancel, liquidate, flatten, reverse real order | **NO** |
| Control followers / copier fan-out | **NO** |
| Alter Trader V0, hard risk, science, frozen phases | **NO** |
| Live-capital execution authority | **NO** |
