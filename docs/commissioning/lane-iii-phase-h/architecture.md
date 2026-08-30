# L3H architecture

```text
local signed capability -> authority gates -> write-ahead event store
                                           -> isolated gateway -> NinjaTrader Live AddOn
broker snapshots --------> reconciliation -> risk/protection -> projections/UI
```

`src/l3h_live` owns L3H contracts, gate derivation, canonical event evidence,
reconciliation, canary risk, and the fail-closed runtime. It does not import
`src/l3g_paper`; L3G remains an independently sealed Sim101 capability.

The event store writes `COMMAND_SEALED` before the gateway may dispatch. A
network failure after seal creates `UNKNOWN` and quarantines the runtime. It
never retries an unresolved entry. A `FLAT` result requires complete, fresh
broker position and order snapshots with no foreign or unclassified activity.

The default gateway is `NoDispatchLiveGateway`; it always refuses. A concrete
gateway may be installed only after repository/installed AddOn parity, local
capability verification, and all runtime gates have passed.
