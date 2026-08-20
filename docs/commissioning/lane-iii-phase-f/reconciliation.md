# Reconciliation and startup

Startup order is:

```text
authenticate → explicitly select master account → resolve explicit MNQ expiry
→ observe account + position + orders → reconcile → observation ready or blocked
```

Provider truth is never overwritten by local state. Reconciliation yields `FLAT_CONFIRMED`, `POSITION_CONFIRMED`, `ORDER_WORKING`, `MISMATCH`, `UNKNOWN`, or `STALE`. Any local disagreement, unknown position, incoherent snapshot, or stale data blocks readiness.

No empty local persistence is treated as flat.
