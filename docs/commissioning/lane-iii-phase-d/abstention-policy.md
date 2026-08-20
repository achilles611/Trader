# Abstention policy

`NO_TRADE` is a successful first-class output. It is returned with a stable
reason code when no active thesis exists and entry is unsafe or unauthorized.

| Condition | Result/reason |
| --- | --- |
| No candidate | `NO_ELIGIBLE_HYPOTHESIS` |
| Archetype not commissioned | `UNAUTHORIZED_HYPOTHESIS` |
| Not active | `HYPOTHESIS_NOT_ACTIVE` |
| Invalidated or expired | `HYPOTHESIS_INVALIDATED` / `HYPOTHESIS_EXPIRED` |
| Stale hypothesis/evidence | `HYPOTHESIS_STALE` / `EVIDENCE_STALE` |
| Any quality not healthy | `DATA_QUALITY_DEGRADED` |
| Relative support below 0.65 | `BELOW_ENTRY_THRESHOLD` |
| Lead below 0.10 | `INSUFFICIENT_DOMINANCE` |
| Missing required independent families | `INSUFFICIENT_FAMILY_BREADTH` |
| Any contradiction | `BLOCKING_CONTRADICTION` |
| Hypothesis already signaled | `ALREADY_SIGNALED_HYPOTHESIS` |
| Cooldown active | `REENTRY_COOLDOWN` |

When an active thesis remains valid, the result is `NO_TRADE` with
`ACTIVE_THESIS_RETAINED`; this means no new entry signal, not a broker hold
instruction. Exact duplicate source states return the already-created decision
identity and increment duplicate-suppression diagnostics.
