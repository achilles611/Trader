# Beelzebub scalper V2

This is the active experimental `Sim101 / LOCAL_SIMULATION / MNQ SEP26 /
quantity 1` paper profile. It restores the short-horizon scalper tuning on top
of the later lifecycle and provenance hardening. It is not an edge claim, is
not scientifically commissioned, and cannot authorize live capital.

## Immutable profile contract

- Profile: `BEELZEBUB_SCALPER`
- Version: `BEELZEBUB_SCALPER_V2`
- Policy ID: `l3g-beelzebub-scalper-policy-v3`
- Policy hash: `872d9079f710ebaa58dbdec88db0d13b34822ed7b49ac69b9af63bffb1b280bf`
- Risk ID: `l3g-beelzebub-scalper-risk-v3`
- Risk hash: `2c0f54dc63da79f1b25e8d7ef3add0e2ae7386705b75e22c4ae8fbb99b060c6c`
- Execution schedule: `CONTINUOUS_EXCHANGE_TRADEABLE`
- `ASIA`, `LONDON`, `NEW_YORK_RTH`, `NY_AFTER`, and `OFF_SESSION` are
  timezone-aware reporting labels only. They do not own, lease, arm, stop, or
  flatten Scalper V2 execution.
- Minimum support: `0.55`
- Minimum winner-over-loser dominance: `0.025`
- Required positive families: all three of `STRUCTURAL_CONTEXT`, `ORDER_FLOW`,
  and `RESTING_LIQUIDITY`
- Blocking contradictions: denied
- Retention support: `0.525` with two positive families
- Re-entry cooldown: `10` seconds

## Risk contract

- Maximum position: one MNQ contract; no pyramiding, averaging, or same-event
  reversal
- Protective stop distance: `25.00` MNQ points / `$50.00` maximum trade risk
- Maximum position age: `540` seconds
- Session and exchange-trade-date entry quotas: disabled by the explicit
  `DISABLED_COUNTING_RETAINED` policy. Entry counters and all historical values
  remain durable reporting facts.
- Consecutive-loss lockout: `4`
- Daily loss limit: `$200.00`
- Maximum hold remains `540` seconds. Reporting-session entry cutoffs and
  hard-flat deadlines are inert for this profile; actual exchange maintenance,
  weekend closure, and the equity-index halt still deny new entries.

## Reporting and P&L attribution

Every new decision, command, fill, and risk record keeps its canonical UTC
identity, account, instrument, profile, exchange trade date, and current
reporting-session identity. `OFF_SESSION` means only that no named reporting
window classified the callback; it is not an execution refusal.

A position retains one trade identity across reporting-label transitions.
`EXECUTION_REALIZED_PNL` records both entry and exit reporting labels and the
`cross_reporting_session` fact. Realized P&L and the entry count are attributed
once to the entry reporting session (`ENTRY_REPORTING_SESSION`); no synthetic
boundary fill or close/reopen is created. Session-grouped history is available
through the existing paper ledger-history API filters and the canonical ledger.

## Provenance and commissioning

Every new ledger envelope carries the explicit profile, version, entry support,
dominance, family count, retention threshold, policy hash, and risk hash. This
prevents a mechanically complete lifecycle from being attributed to a different
default profile.

Atomic Commissioning remains a one-shot ownership path and is not used to start
persistent operation. It waits for a fresh profile-qualified directional
decision and seals that decision's source observations into the commissioned
entry. Persistent paper trading uses the separate guarded operational-start
path. Both paths remain `Sim101` only and fail closed on stale data, continuity,
reconciliation, ledger, provenance, exchange availability, capacity, or
identity failures. Operational start is runtime-scoped and persists across
reporting-label transitions without rearming.

The archived V1 scalper remains at Git ref
`archive/beelzebub-scalper-v1-20260903`. V2 intentionally retains the later
signal-gated commissioning behavior and corrects the historical profile-name
spelling rather than replaying V1 byte-for-byte.
