# Evidence register — receiver periodic corrective

Status: final administrative register. Large artifacts remain under
`N:/Beelzebub/receiver-periodic-corrective-evidence`; this record links rather
than copies them.

## Repository identities

| Item | Identity | Disposition |
| --- | --- | --- |
| Corrective source commit | `448081aa055279c0ce516b5fb948c4b2530c2de7` | Implemented and integrated; one corrective revision |
| Corrective source tree | `d6c30b299afb18722133955be189dfcccc1c6935` | Exact source/test tree at corrective commit |
| Comparison-harness reconciliation | `ada2420ca1182006bbdffc6ced0969307f16ea32` | Tool-only; not a second behavioral correction |
| Final acceptance-tool reconciliation / evidence HEAD | `28fa75290151c4716f9c40bfef91eac647bfbdf1` | Tool-only; stale periodic fixed target retired |
| Final evidence tree | `403ad98c3058cb6375664f0d8af006cc5df1a63a` | Root tree before this uncommitted administrative record |
| Prior receiver repair | `0cc1a86576a553d10237db37dfeef3b773c175a3` | Ancestor; accepted earlier correction retained |

The corrective commit changes four application files and four focused tests.
The comparison-harness commit changes only `tools/pong_receiver_comparison.py`.
The final reconciliation changes only `tools/pong_periodic_paced.py` and
`tools/pong_periodic_fidelity.py`. Tool reconciliation is evidence semantics,
not receiver behavior or a second corrective revision.

## Current relevant file hashes at evidence HEAD

| File | SHA-256 |
| --- | --- |
| `src/copytrade/control_center.py` | `1938e2fac0c1d61130f3478bd64d282e702bf28068768fdcecf0eafbce83d7a8` |
| `src/l3g_paper/runtime.py` | `780dd815b28b649201d074282cf749ec690fa2897394a5db8e90283beb2a21c5` |
| `src/l3g_paper/policy.py` | `585cd5a474785c8595046a272779cef4c740272a9e5d819b97b5b16f65800d54` |
| `src/l3g_paper/ledger.py` | `93666c246ff46bfed232392ad6507338ef378526e7c35c13a0d7c6653d747ccc` |
| `tools/pong_receiver_comparison.py` | `54f79fed8183ce0ef0b84e3f875b286d41db58ba44c73009ba1cc27ee196e301` |
| `tools/pong_periodic_paced.py` | `59abf20023e7ae6cbe0e1db3791c88ba1484cd9793dc371015f8b7fb3f568117` |
| `tools/pong_periodic_fidelity.py` | `76e0c0d296ec0744632ddfda17e3d7022949635da406fa89870c6959a4989495` |

## Aligned pair

| Artifact | SHA-256 | Key use |
| --- | --- | --- |
| `PAIR_1000_COMPARISON.md` | `cf9d7e22a8b7ea2614425ca0dd665b79f757995e4d2e5082788c10b8f1072eeb` | Human-readable pair disposition and final HEAD provenance |
| `pair-1000-full/result.json` | `60f2844c1c140e86ff452d1397a76d6f598eb885404012c69aebe09fbdc91330` | Full legacy paper-path result |
| `pair-1000-fast/result.json` | `372d8d28efc8495808fb3526a9341870f53749999be43564828666b7aa227746` | Exact qualifying fast-path result |
| `pair-1000-full/input-frames.jsonl` | `bf4c66c02f2e2ec8b526b45ca4c8ae8d5effd5155ef30a15a5315d15cbd64556` | Shared 10,000 signed input |
| `pair-1000-fast/input-frames.jsonl` | `bf4c66c02f2e2ec8b526b45ca4c8ae8d5effd5155ef30a15a5315d15cbd64556` | Independently verified byte-identical input |

Both result receipts report ordered-wire digest
`68e1bd83a64ebade48f7f08211ba2724f6eab8ac0135c5f7b09d54306480c1d5`.

### Raw result review

| Predicate | Full | Fast | Administrative disposition |
| --- | ---: | ---: | --- |
| Records completed | 10,000 | 10,000 | PASS narrow completeness |
| Completion rate | 241.513589/s | 283.860640/s | +17.534%; diagnostic only |
| Receiver end lag | 31.406538 s | 25.229554 s | Improvement; still material failure |
| Paper fanout total | 12,615.942 ms | 6,631.989 ms | Named correction benefit observed |
| Fast/fallback | 0 / 0 | 10,000 / 0 | Exact route selected in fast run |
| Durable collector records | 10,000 | 10,000 | PASS narrow durability |
| Overflow/transport delta | 0 / 0 | 0 / 0 | PASS narrow loss counters |
| Collector maximum queue age | 11,937 ms | 10,938 ms | FAIL unchanged 1,000 ms contract |
| Ordinary periodic evaluations | 0 | 0 | Ratio unavailable |
| Protection | `RUNTIME_BRIDGE_UNAVAILABLE` | same | Both required protected cases |
| Shutdown | clean | clean | Necessary, not service qualification |

The explicit offered rate of 1,000/s is only a comparison coordinate. It does
not replace the retired 2,111/s floor and is not a new requirement.

## Verification receipts

These outcomes are parent/implementation-owner reported; no standalone test
transcript was placed in the final N: evidence root for Caelum to hash.

- Initial isolated selected suite: **48 passed, 1 failed, 20 subtests passed**.
  The exact failure reproduced on the pre-change root. The test used
  `tempfile.TemporaryDirectory` on C:, and the ledger correctly checked the
  database parent. Approximately 4.95 GB free crossed the 8 GiB warning floor
  (4 GiB minimum), producing `AUTHORITY_LEDGER_DISK_FREE_WARNING`.
- Capacity-aligned rerun with `TEMP`, `TMP` and `TMPDIR` at
  `N:/Beelzebub/receiver-periodic-corrective-evidence/pytest-temp`: **49 passed,
  20 subtests passed in 5.33 seconds**. N: had approximately 226.76 GB free.
- Integrated root focused corrective suite: **8 passed in 3.04 seconds**.
- Integrated root combined selected suite: **48 passed, 1 baseline-identical
  capacity-guard failure, 20 subtests passed**.
- Final tool reconciliation: owner reports `py_compile`, help and diff checks
  passed. Caelum independently observed no `2111`, `2,111`, `5.15` or
  `target_events_per_second` match in the three affected tools.

The capacity failure was preserved, not skipped by weakening the disk
requirement. The clean rerun establishes source/offline regression coverage on
the intended evidence volume only.

## Capacity-ledger and lockout evidence

These details are parent/runtime-owner reported and were not mutated by Caelum:

- trace artifact:
  `N:/Beelzebub/receiver-periodic-corrective-evidence/CAPACITY_PATH_TRACE.md`,
  SHA-256
  `6363be5959cc91f2b1b924fc56e2b1958c7b4bb1689000f1c941e48d4a40c6f4`;

- actual runtime ledger:
  `N:/Beelzebub/runtime/profiles/beelzebub_pong_discernment_v1/runs/20260920T211544Z-ca2ceba7da11/hot/lane_iii_paper.sqlite3`;
- exact observed ledger size / approximate free space: 1,498,558,464 bytes /
  226.76 GB;
- identity/epoch/tip:
  `l3g-ledger-3ec065663f4b45eeb5570a8dcba63dda` /
  `L3G-PAPER-EPOCH-BEELZEBUB_PONG_DISCERNMENT_V1-20260920T211544Z-ca2ceba7da11`
  / 277856;
- historical selected-writer receipt `paper-before-stop.json:628-667`:
  `HEALTHY`, database 930,816,000 bytes, free 228,571,398,144 bytes, warnings
  empty, growth zero;
- preserved latest C:-temp test ledger:
  `C:/Users/atlas/AppData/Local/Temp/tmp0k577py1/paper.sqlite3`, capacity lock
  sequence 2 at `2026-09-21T22:58:13.963705Z`; approximately 4.614 GiB free was
  above the 4 GiB hard minimum but below the 8 GiB warning floor, so
  `DEGRADED` was expected;
- conclusion: the actual ledger already resides on N:; no migration is needed
  or authorized. Audit migration moves audits only, profile switch creates a
  new identity/epoch, and reset imports nothing; the ledger must not be copied
  or moved;
- historical capacity lockout: sequence 134169, still uncleared; original
  `DEGRADED` subcause not persisted;
- later historical flat reconciliation: sequence 274023 at
  `2026-09-21T17:09:26Z`, Sim101 `LOCAL_SIMULATION`, MNQ DEC26, quantity zero,
  zero working/entry orders, complete snapshots, no foreign activity;
- limitation: that reconciliation and healthy writer receipt are historical.
  With no listener on ports 8090, 48135 or 48136 and no current authenticated
  reconciled bridge, supported lockout recovery prerequisites are not currently
  confirmed. NinjaTrader remains operator-owned and untouched. No clear was
  authorized or run.

## Excluded and limited evidence

- `profile-current-2000` is incomplete and excluded.
- `profile-current-2000b` is a completed offline diagnostic, not part of the
  predeclared aligned pair and not acceptance evidence.
- Neither pair run had a native producer or execution transport and neither
  spent R2.
- Both runs used offline fixed timestamps and remained protected; neither proves
  current source freshness or the accepted periodic ratio.
- Operator-owned NinjaTrader remained active and untouched, so the timing pair
  is not a controlled-host qualification.
- Account, position, order and arming state was not observed.

## Point-in-time closeout observation

Parent observation at `2026-09-21T23:15:46.2724963Z`:

- port 8090 listener count: 0;
- Python process count: 0;
- NinjaTrader process count: 1, PID 17208, operator-owned and untouched;
- account/positions/orders/arming: `UNKNOWN`.

This is a point observation, not continuous monitoring.
