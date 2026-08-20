# Adversarial and scenario tests

The targeted suite contains 37 tests in `tests/test_lane_iii_phase_d.py`.

## Required scenarios

| Scenario | Evidence |
| --- | --- |
| Clean bullish reversal | `LONG` once; exact duplicate keeps the same ID; next state is retained/no new entry |
| Clean bearish continuation | `SHORT` under the exact three-family policy |
| High support, close competition | `NO_TRADE` for insufficient dominance |
| Correlated flow-only support | Four evidence IDs remain one family and abstain |
| Confidence decay | `EXIT` below 0.58 |
| Invalidating contradiction | `EXIT` with thesis invalidation |
| Degraded data | all named degraded states block entry; active degradation emits strategy `EXIT` only |
| Immediate opposing thesis | `EXIT`; no same-event short/reversal |

## Adversarial coverage

Tests also prove:

- exact identity/hash registration; wrong ID/hash refusal; changed threshold
  changes hash/identity and has no authority;
- below threshold, unauthorized, stale, invalidated, contradicted, insufficient
  breadth, and insufficient dominance states abstain;
- a retained hypothesis whose evidence has been evicted abstains explicitly
  without fabricating provenance;
- multiple same-family evidence IDs do not create extra family votes;
- confidence decay, contradiction, invalidation, expiry, dominance loss,
  maximum thesis age, and opposing dominance create deterministic exits;
- entry/retention hysteresis, cooldown, once-per-hypothesis entry, and bounded
  history prevent uncontrolled churn;
- identical replay yields identical decision sequences and hashes;
- provenance reaches family contribution, evidence, source observation, event
  identity, and payload hash;
- configuration/time mismatch and backward time refuse evaluation; and
- no signal field/import/public method provides execution, sizing, account,
  broker, risk, copier, Phase E, network, P&L, probability, or live-capital
  authority.

Final focused and repository-wide results are recorded in `closure-audit.md`.
