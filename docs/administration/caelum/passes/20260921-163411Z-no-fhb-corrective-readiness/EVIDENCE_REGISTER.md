# Evidence register — no-FHB corrective readiness

Evidence is historical unless explicitly labelled current observation. Old
identity-bound evidence may diagnose a source change but cannot qualify it.

## Current repository and process observation

| Observed UTC | Item | Result |
|---|---|---|
| 2026-09-21T16:35:19Z | Git HEAD / branch | `2e2c743e6205f09f10901ad6af4d8375fd0a4ffb`; `codex/lane-iii-perpetual-corrective-20260906` |
| 2026-09-21T16:35:19Z | `c76cccc...` ancestry | PASS, exit 0 |
| 2026-09-21T16:35:19Z | Ports 8090/48135/48136/48137 | no listeners |
| 2026-09-21T16:35:19Z | Relevant persistent process | NinjaTrader PID 17208 only; operator-owned, not touched |
| 2026-09-21T16:37:08Z | Intake worktree | only pre-existing unrelated modified/untracked audit/evidence paths; preserved |

## Preserved failed R2 evidence

| Artifact | Bytes | SHA-256 | Meaning |
|---|---:|---|---|
| `H:\PongPeriodic\NoFHBQualificationReadiness-20260921\R2-commission-585a-20260921T153900Z.json` | 10,508 | `df530f25afee14474898cb516ff8ad37c0a5e12cede908bf2f0870e6439522e7` | Receiver-first distinct identity passed; `verified=false`, host fault and reject growth prevent clean R2 |
| `H:\PongPeriodic\NoFHBQualificationReadiness-20260921\R2-short-natural-chain-20260921T154100Z.json` | 12,448 | `14662f1c4efe58c9c443944c2f74c80d1da74acde678b6e670efb398e218f7eb` | 24.1676 s failed diagnostic; not Gate B |
| `N:\Beelzebub\runtime\pong-production\pong\MNQZ6\v1\host-telemetry\first-fault.json` | 10,436 | `e52cdbf8ae1a2f61ab62a3ad21308301ea0ad212942bfb065cf8a74eab1de68a` | First host fault receipt |
| `N:\Beelzebub\runtime\pong-production\pong\MNQZ6\v1\host-telemetry\final.json` | 10,563 | `deba075a3bbf9a7ff3dc6b3117943614755f999d3c6b53dc0bf020a62fd352ea` | Final failed-epoch host telemetry |
| `N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\audit\ledger-verification\20260921T154300321406Z-no-fhb-r2-canonical-fault-closeout-20260921T1542Z.json` | 2,673 | `1b46e2800dbdac23b2bb0a0909cf85355bcd3d991892711705800222deaf6994` | Narrow ledger closeout PASS through 233998; not R2 or readiness |

## Authenticated failure seam

- Prior current-source identity: source
  `585a57d4c8138665b6c5734fcb84608f16c64668b028ec960371d78957fa2344`,
  build `70a1762e68d34ef12ff98c4ab1ca2b86619b4d02f52a2136454cd14e8c5eb759`,
  session `46adca83becf42e59e4356bb5829c88b`, generation 1.
- Independent direct inspection of preserved signed records 146–150 at
  `2026-09-21T16:40Z` supersedes the intake claim that record 149 itself is the
  fatal application:

| Journal / source | Status | Mutation | Shape | Signature / record hash |
|---|---|---|---|---|
| 146 / 141 | available | Ask position 9, resolved 30599.25 | 10x10 | true / `0b9b5a3e79d5730071c50b6dbf30b70b441b18ef60ea8b5ab6a91061f0cdc825` |
| 147 / 142 | `UNAVAILABLE / NATIVE_DEPTH_REMOVE_IDENTITY_UNRESOLVED` | Ask Remove position 10, resolved null | 10x10 retained snapshot | true / `f52830f2f2f79ec3902b6f1da44b7f617ae7b74bb9059b397a9dbebdac15154e` |
| 148 / 143 | available | Ask Add position 10, resolved 30599.5 | exact retained 10x10; added row is outside top ten | true / `826afdf3e3390e62f0cbea69f18fe8f74c3de1c3c54a22946356af85dc8908e0` |
| 149 / 144 | `UNAVAILABLE / NATIVE_DEPTH_REMOVE_IDENTITY_UNRESOLVED` | Ask Remove position 11, resolved null | 10x10 retained snapshot | true / `42430b5f26e032356796ea48d02f6f643bc344efe994eccc935060402b331867` |
| 150 / 145 | available | Ask Add position 11, resolved 30599.75 | exact retained 10x10; added row is outside top ten | true / `028e5a3c219b3f1351d1af5dd679040b1a6cada1c761bbb424118f6b0fddbeaa` |

- Production source already returns early on explicit unavailable input. Record
  148 is therefore the evidence-backed likely trigger for the post-book
  membership fault: its resolved position-10 price is not supposed to appear in
  the retained visible top ten. Owner A must still reproduce this through
  actual production host logic before Caelum marks the cause OBSERVED.
- Records 147 and 149 remain required non-regression evidence for truthful
  fail-closed unavailable behavior and later coherent recovery.
- Host fault: `PONG_CANONICAL_DEPTH_MUTATION_NOT_IN_POST_BOOK`.
- Commissioning checkpoint reject delta: `+2381`; no clean corpus boundary.
- Diagnostic: DEPTH 305.078/s; opportunities 43.985/s; full evaluations
  0/s; late/protection supersessions 907; obsolete 156; producer overflow
  `+6335`; transport `0`; host queue 58→5 and age 110→0 ms (max 265 ms);
  source age ended 81.321 s and was increasing.
- Final producer overflow: `+45772`. The recovered host queue plus worsening
  producer/source age is evidence against assuming the Python host is the first
  bottleneck.

## Evidence intake rules

For every specialist or Sol receipt, record: producer/source/build/commit,
environment, command or runtime interval, exact path/hash, counters and clock
domain, and whether the claim is source-implemented, test-observed,
installed, loaded, runtime-exercised, or accepted. Runtime claims without a
current source/build/session/generation are not acceptance evidence.

## Phase 2 instrumentation evidence

- Owner B files: both checked-in read-only NinjaScript producers and
  `tests/test_l3g_ninjascript_source.py`.
- Owner C files: `src/l3f_provider/ninjatrader_commission.py` and
  `tests/test_lane_iii_phase_f3.py`.
- Native compiler receipt reported by Owner B: zero errors, 1,329 warnings.
- Owner B derived pre-embedding source fingerprint:
  `051808624d0d91ac04e91cae27b7e8765d887774053192b909c7b7434f652a14`;
  Sol must embed/rederive/verify it during integration.
- Caelum independent source test:
  `.\.venv312\Scripts\python.exe -m pytest tests/test_l3g_ninjascript_source.py -q`
  -> `46 passed in 0.52s`.
- Caelum independent receiver test after schema correction:
  `.\.venv312\Scripts\python.exe -m pytest tests/test_lane_iii_phase_f3.py -q`
  -> `29 passed in 12.64s`.
- `git diff --check` for Owner B paths was clean except line-ending notices.
- Disposition: **ACCEPTED SOURCE/OFFLINE**; internal bottleneck remains
  `INFERRED / needs runtime proof`; zero performance-repair attempts spent.

## Gate A delta evidence

- Frozen candidate: `767f8506077344baf3911ff9d08df4df4e81a39a`.
- Phase 1 ancestor: `4630230161dbbab9355c86c1f2da68aae9f85903`.
- Affected composite: 221 passed + 62 subtests; two C: low-free-space ledger
  refusals; exact ledger rerun 9/9 PASS on N: (211.9 GB free).
- Native compile: success, zero errors, 1,329 warnings.
- Callback ordering PASS; generated wrapper PASS, repaired errors zero,
  idempotent.
- Observer source identity:
  `051808624d0d91ac04e91cae27b7e8765d887774053192b909c7b7434f652a14`.
- AddOn SHA-256:
  `89700b8a4eee0bfd4c437f9ed1fdf6401e8e78d5d0c198edb8d80811ba167be7`.
- Indicator SHA-256:
  `38659a9907c02923d6f96c6fa48920dde0f4f886a8b0283b326ef07663f2d310`.
- `PONG_LOADED_ARTIFACT_HASH`:
  `103d817809f601dbedea24bc411b74b15481da3f264d692afaa0902b79b92802`.
- FHB unloaded/absent from qualifying hash and archived; E4 paths absent.
- Decision: **GATE A DELTA PASS**; one Sol-operated fresh R2 attempt released.

## Fresh R2 failed epoch evidence

- Bounded diagnostic and causal receipt:
  `H:\PongPeriodic\NoFHBQualificationCorrective-20260921\R2-bounded-natural-diagnostic-20260921T171332Z.json`,
  7,932 bytes, SHA-256
  `47b37cb3847f128a7c2a90d7c36ae65645c2cd5a22b9edde3d30e5d56f5ffcea`.
  It contains the exact captured callback/publication/dequeue/socket/receiver/
  downstream metrics, requested scheduler/queue/age/loss chain, identities,
  source receipt hashes, `OBSERVED` causal conclusion and stopped state.
- Commission receipt:
  `H:\PongPeriodic\NoFHBQualificationCorrective-20260921\R2-commission-0518-20260921T1709Z.json`,
  10,322 bytes, SHA-256
  `61f49a152e545587b72834afdd35fbf2ad788ba9245f1eec51ec5a19bea6df79`.
- Final host telemetry:
  `N:\Beelzebub\runtime\pong-production\pong\MNQZ6\v1\host-telemetry\final.json`,
  10,588 bytes, SHA-256
  `2a999ff7ef8d6a136260f2c30caaa37c37822d5624652b71e3c93842d608c88f`.
- Final collector status:
  `N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\audit\pong-authentic-hour\collectors\pong-collector-20260921T170754935836Z-01b256397627\status.json`,
  4,048 bytes, SHA-256
  `482e2d4990419b456cdb49107916c25a031b04b40fd978a394aab3225fa5f1c2`.
- Collector journal at the same collector root:
  `authenticated-observations.jsonl`, 208,984,166 bytes, SHA-256
  `9d1030ca2dfb9a7db2dbcb251c7d77ad502539370cd9d636da95ae8ff77a664c`.
- Incremental ledger verifier:
  `N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\audit\ledger-verification\20260921T171611505725Z-no-fhb-corrective-r2-stop-20260921T171332Z.json`,
  SHA-256
  `606ebf91f3dfc06dd5e598fb1e9d97fd9463914c5754f7c019c7864bd84cad39`,
  PASS through 277,856 from checkpoint 233,998.
- Decision: **R2 FAIL / HOLD**. Gate B not run. One unconditional R2 attempt
  spent; one conditional attempt preserved for a demonstrated correction.

## Phase 1 corrective evidence

- Files: `src/l3g_paper/pong_production.py`,
  `tests/test_l3g_pong_production.py`, and
  `tests/fixtures/pong_record_149_authentic_seam.jsonl`.
- Fixture SHA-256:
  `13e017841660736632683698526db100744ff21f0464cb22fabb3d7c4b50c7b8`;
  five lines exactly equal to preserved collector journal records 146–150.
- Independent command:
  `.\.venv312\Scripts\python.exe -m pytest tests/test_l3g_pong_production.py tests/test_l3g_pong_inputs.py tests/test_l3g_pong_microstructure.py -q`.
- Independent result at `2026-09-21T16:44Z`: `42 passed, 8 subtests
  passed in 4.16s`.
- Review disposition: **ACCEPTED SOURCE/OFFLINE**. This is not integration,
  install, loaded-binary, fresh R2, or runtime evidence.
- Sol integration commit:
  `4630230161dbbab9355c86c1f2da68aae9f85903`; no native install or runtime
  acceptance is implied.
