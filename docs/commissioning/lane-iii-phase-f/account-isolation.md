# Account isolation

`L3F_NT_LUCID_ACCOUNT_ID` is runtime-only. The AddOn prefers its process environment and, if NinjaTrader did not inherit that value, reads the same key from `%USERPROFILE%\Documents\NinjaTrader 8\l3f2.local.config`. This file is user-local, outside the repository, and must contain exactly one `L3F_NT_LUCID_ACCOUNT_ID=<value>` line. The AddOn emits only `Lucid25kflex01`, never the provider identifier. Zero or multiple matching bindings fail closed.

Sim101 is observed only as `LOCAL_SIMULATION`; it retains independent provenance/state and cannot satisfy authoritative Lucid reconciliation. Its balance, P&L, position, orders, and executions are never merged with `PROVIDER_EVALUATION` truth.
