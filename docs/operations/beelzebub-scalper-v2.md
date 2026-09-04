# Beelzebub scalper V2

This is the active experimental `Sim101 / LOCAL_SIMULATION / MNQ SEP26 /
quantity 1` paper profile. It restores the short-horizon scalper tuning on top
of the later lifecycle and provenance hardening. It is not an edge claim, is
not scientifically commissioned, and cannot authorize live capital.

## Immutable profile contract

- Profile: `BEELZEBUB_SCALPER`
- Version: `BEELZEBUB_SCALPER_V2`
- Policy ID: `l3g-beelzebub-scalper-policy-v2`
- Policy hash: `7077c30bdeb3b8017b5d8049ed32b92eb3acca67393ef101b217538da30d6659`
- Risk ID: `l3g-beelzebub-scalper-risk-v2`
- Risk hash: `c86c2c2b39f7fef9fcdaed06978ab22141eeacd2cf1590f2a8cddee0c5b17404`
- Sessions: `ASIA`, `LONDON`, `NEW_YORK_RTH`, and `NY_AFTER`, each under its
  own exact session identity, generation, entry window, and hard-flat boundary
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
- Maximum entries: `12` per exact session and trade date
- Consecutive-loss lockout: `4`
- Daily loss limit: `$200.00`
- Exact session entry cutoff and hard-flat deadlines remain authoritative

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
reconciliation, ledger, provenance, session, capacity, or identity failures.

The archived V1 scalper remains at Git ref
`archive/beelzebub-scalper-v1-20260903`. V2 intentionally retains the later
signal-gated commissioning behavior and corrects the historical profile-name
spelling rather than replaying V1 byte-for-byte.
