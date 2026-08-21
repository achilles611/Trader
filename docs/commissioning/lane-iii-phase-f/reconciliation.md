# Reconciliation and startup

Startup order is:

```text
authenticate → explicitly select master account → resolve explicit MNQ expiry
→ observe account + position + orders → reconcile → observation ready or blocked
```

Provider truth is never overwritten by local state. Reconciliation yields `FLAT_CONFIRMED`, `POSITION_CONFIRMED`, `ORDER_WORKING`, `MISMATCH`, `UNKNOWN`, or `STALE`. Any local disagreement, unknown position, incoherent snapshot, or stale data blocks readiness.

No empty local persistence is treated as flat.

L3-F2 reuses these states. A NinjaTrader bridge session ID and local monotonic ordering sequence prevent prior-session or late callbacks from replacing newer truth. Sim101 state is never an input to Lucid reconciliation.

## Authenticated L3-F3 result

On 2026-08-20 the AddOn emitted four `SNAPSHOT_COMPLETE` records: position and order snapshots for the Lucid alias and separately for Sim101. The Lucid snapshots explicitly reported zero open positions and zero nonterminal orders, so reconciliation yielded `FLAT_CONFIRMED` and `NONE_WORKING_CONFIRMED`. No value was derived from missing callbacks.

The observation receiver keeps independent market-data and account-state session ledgers. This is required because a chart indicator may remain active while the AddOn reloads. A retired session is rejected within its own channel; one channel's reload cannot incorrectly retire the other.
