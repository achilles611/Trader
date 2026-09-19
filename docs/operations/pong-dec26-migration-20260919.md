# Pong DEC26 topology migration — 2026-09-19

## Status

`DEC26_MIGRATION_IMPLEMENTED_VERIFICATION_PENDING`

The migration implementation is complete in the isolated worktree named
`lane-iii-dec26-migration-luna`, remains disarmed, and emitted zero native
directives during verification. The final implementation commit is recorded in
the delivery receipt alongside this file.

The operating checkout remains at `c236255bf834c1b57ff898e68ea74f70dd4e828c`
with its seven pre-existing user edits preserved. It was not modified by this
execution, and no paper or live order path was started. This receipt does not
claim native DEC26 replay qualification, paper readiness, or live authority.

## Direct request and handoff boundary

The direct user request was `execute`. The attached handoff was treated as the
operational specification, not as a request to arm capital or bypass a block.
Its fail-closed requirements were retained: exact identities, separate state
epoch, no mixed bundles, no SEP26 evidence relabeling, disarmed Sim101/local
simulation only, and no strategy, cadence, trading-hours, CPU/GPU, or FHB
changes.

## Exact topology identities

| Field | Historical route | Migration route |
| --- | --- | --- |
| Version | `PONG_CONTRACT_TOPOLOGY_SEP26_V1` | `PONG_CONTRACT_TOPOLOGY_DEC26_V1` |
| Native instrument | `MNQ SEP26` | `MNQ DEC26` |
| Canonical contract | `MNQU6` | `MNQZ6` |
| Expiry | `2026-09` | `2026-12` |
| State namespace | `pong/MNQU6/v1` | `pong/MNQZ6/v1` |
| Binding hash | `46da3d6506e1feb8c321e6f21dfa13a3ef6e87ca30b6263940bc9f2666e58ccc` | `a339cb56a2dc1280272062d8bd73a4f85829f3ebf92bacc1a6256904fa87e5e7` |

Both routes remain exact and selectable. The selector defaults to DEC26 and
rejects unknown values; it never falls back to SEP26. Startup bundle checks
require topology version, binding hash, native/canonical identity, state
namespace, account scope, disarmed/live-denied scope, and quantity cap to
agree before DEC26 processing.

## Implemented boundaries

- `src/l3g_paper/pong_contract.py` defines the immutable versioned topology,
  exact selector, binding hash, isolated namespace, and mixed-bundle refusal.
- Warm-start/history admission, collection status, microstructure mapping,
  discernment snapshots, quantity/reconciliation proofs, runtime identity,
  controller observation, state-machine identity checks, and production status
  now carry the selected topology.
- DEC26 production state is rooted below `pong/MNQZ6/v1`; SEP26’s historical
  default path retains its existing state layout for compatibility.
- `BEELZEBUB_PONG_CONTRACT_TOPOLOGY` crosses the launcher boundary and is
  resolved once by the Control Center composition root.
- The read-only NinjaTrader producer maps only `MNQ SEP26`→`MNQU6` and
  `MNQ DEC26`→`MNQZ6`; unknown mappings return no canonical contract.
  DEC26 receives its topology-specific warm-start policy hash.
- No order-capable native AddOn, paper activation, live capital, strategy,
  cadence, trading-hours, FHB, or periodic-controller redesign was performed.

## Verification

| Check | Result |
| --- | --- |
| Documented baseline/regression selection plus migration tests | 130 passed, 0 failed, 0 errors |
| Focused protected Pong regression | 58 passed, 0 failed, 0 errors |
| Native source and launcher tests | 81 passed, 0 failed, 0 errors |
| DEC26 exact/mixed/selector/state/zero-dispatch tests | 4 passed, 0 failed, 0 errors |
| Python compile check for changed modules | Passed |
| `git diff --check` | Passed |
| Native NinjaTrader SDK build | Not available in this workspace; source-contract tests only |
| Native DEC26 protected replay | Not verified; no qualifying replay corpus/build pair was available |

The DEC26 test constructed and stopped the protected host with an old SEP26
runtime snapshot. It retained `DISARMED`, isolated state, blocked authority,
and zero native dispatch. A partial or mixed DEC26 runtime bundle is refused.

The existing protected SEP26 receipt remains historical evidence and was not
overwritten or relabeled:

- `H:\PongPeriodic\20260918\operational-natural-v1\result.json` —
  `F3E1658CFECFA4C8E4DB5D80DF8AE8B6757B2D5BD0A14767A314052C376911C1`
- `H:\PongPeriodic\20260918\newer-capture-audit.json` —
  `CC9A3B143398E8C974136EF783EFFA3709C8114E053263F3758593BF4087EC41`

That receipt records 150,016 signed/durable SEP26 records, zero native
directives, and no runtime start. The DEC26 audit records 335,551 authenticated
`MNQ DEC26`/`MNQZ6` observations with source fingerprint
`d9f7737c1e101ec7d5a88b6197b8afa5cdd25ede3f3742c5a3c568fd6c458fd4`, but it
also records the prior hardcoded-runtime incompatibility and is not itself a
fresh qualifying migration replay.

## Blocked integration boundary and next task

The installed generic Lane III paper/risk runtime remains explicitly sealed to
SEP26. It was not silently rebound to DEC26. The current DEC26 observer audit
also predates the new read-only producer fingerprint, so it cannot authenticate
the new native build by relabeling. A fresh DEC26 live-context qualification is
required: build/install the exact read-only DEC26 observer, collect a fresh
signed warm-start/history and complete market stream, run the protected
DEC26 replay with the matching source/build fingerprints, prove account/risk
and recovery continuity, then perform a separate reviewed integration step.
Until that work is complete, selection remains disarmed and blocked.
