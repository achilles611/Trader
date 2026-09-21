# Beelzebub no-FHB qualifying cut and brief natural-feed proof — 2026-09-21

## Verdict

The surgical qualifying-build cut is **SOURCE/STRUCTURAL PASS** at commit
`efea61561e128bc44632def5d7c135e6ea7bacd9`. The explicit identity is
`FULL_NATIVE_DEPTH_PERIODIC_20MS_NO_FHB_V1`; inherited FHB configuration is
rejected, FHB is absent from the clean launcher/Control Center/production-host
import graph and loaded-artifact hash, and runtime status classifies it as
archived, disabled, and not required. The archived FHB implementation and its
tests remain recoverable at `ce7f64d669284aadada4344af68fb53f159c7070`.

The first current-build natural-feed proof is **FAIL / HOLD**. It was stopped
after 67.598476 seconds rather than extended. The collector durably retained
52,517 authentic records, but the native producer counters increased by 44,135
queue-overflow frames and 32,686 transport-failed frames. The retained streams
therefore contained 40,756 missing DEPTH sequence identities, 1,238 missing
QUOTE identities, and 1,661 missing TRADE identities. Source receipt age also
reached 291,938.355 ms because a pre-start native backlog was delivered into
the new host.

The first producer record that violated the qualifying canonical-depth contract
was collector record 47 / DEPTH source sequence 955867: the producer declared
the snapshot available while publishing 10 bids and only 1 ask. The qualifying
host deterministically latches `PONG_CANONICAL_DEPTH_10X10_REQUIRED` there and
does not admit later records into analytical state. Consequently this epoch
does not establish stable full-path periodic analytics and is not qualification
or paper-start evidence.

## Exact identity

| Field | Value |
|---|---|
| Source commit | `efea61561e128bc44632def5d7c135e6ea7bacd9` |
| Pong loaded-artifact hash | `18ad28ca6feaed14a8776fb53057d0df16ff55c5c0996e127551f9da2c73da2f` |
| Qualifying architecture | `FULL_NATIVE_DEPTH_PERIODIC_20MS_NO_FHB_V1` |
| Schedule | `PERIODIC_FLAT_PROTECTED`, 20 ms |
| FHB profile/policy | `null` / `null` |
| FHB disposition | `EXPERIMENTAL_ARCHIVED_NOT_PART_OF_QUALIFYING_BUILD` |
| Activation | `false` |
| Host and native topology | MNQ DEC26 / MNQZ6 / `PONG_CONTRACT_TOPOLOGY_DEC26_V1` |
| Account and authority | Sim101 / LOCAL_SIMULATION / disarmed / live capital denied |
| Native DLL reused | `C:\Users\atlas\Documents\NinjaTrader 8\bin\Custom\NinjaTrader.Custom.dll`; SHA-256 `9f8beffb5b34e7c37c9f2488320d5076565bbc77c724720b07f5197fec8fdb76`; native source was unchanged by this cut |

The runtime binding used DEC26-derived paper-policy hash
`333f3c07bc6c7340e67b9238532eb1d9538c4e9b22644f6ecd23d4d49c8773e0`
and risk hash
`404251db8a372641f3a25d7edbb921cf5da6a4277bc3a6fd2fab5a2768269962`.
The new qualifying certificate identity binds architecture, topology, schedule,
FHB-null disposition, activation state, live-authority state, and the current
loaded-artifact hash. Pre-cut certificates are preserved but intentionally not
eligible for this artifact.

## Structural verification

- Clean subprocess imports of `beez_console`, `src.copytrade.control_center`,
  and `src.l3g_paper.pong_production` leave `src.l3g_paper.fhb` absent from
  `sys.modules`; `src/l3g_paper/fhb.py` is absent from the qualifying loaded
  source hashes.
- Focused launcher/host/FHB regression: 78 tests passed.
- Exact affected integration set, including native positional continuity,
  NinjaScript source, observation parsing, archived FHB, collection, launcher,
  production host, periodic clock/scheduler, authorization, volume history,
  and DEC26 topology: 179 tests passed in 11.962 seconds.
- Python compilation and scoped `git diff --check` passed.
- A broader 287-test run passed 284 tests. Three unrelated Control Center tests
  stopped before app construction on the retained established-run guard
  `PROFILE_SELECTION_CHECKOUT_MISMATCH`, as expected after changing the source
  commit. That established selection and its evidence were not altered.
- Caelum's bounded source/requirements review returned
  `PASS_TO_VERIFICATION`; its separate administrative closeout is under
  `docs/administration/caelum/passes/`.

## Brief natural-feed measurement

Collector receipt window:
`2026-09-21T04:32:28.520784Z` through
`2026-09-21T04:33:36.119260Z` (67.598476 seconds).

| Requested measure | Result |
|---|---:|
| DEPTH | 49,423 durable; **731.125950/s** |
| Full analytical evaluations/s | **Unavailable / not acceptable**: host status snapshot was not persisted before the terminal 10x1 canonical-input latch; no full-path rate is claimed |
| Periodic opportunities/s | **Unavailable** for the same failed epoch |
| Late / obsolete / missed periodic ticks | **Unavailable** for the same failed epoch |
| Host queue age | **Unavailable**; the collector metric below is not relabelled as host age |
| Collector queue age | last 0 ms; maximum 610 ms |
| Source age | minimum 0.011 ms; maximum **291,938.355 ms** (collector receipt minus NinjaTrader receipt) |
| Producer transport-failure delta | **+32,686 — FAIL** (`334,731 -> 367,417`) |
| Producer queue-overflow delta | **+44,135 — FAIL** (`575,177 -> 619,312`) |
| Collector queue start / peak / end | 0 / 869 / 0 records; capacity 262,144 |

Additional retained facts:

| Measure | Result |
|---|---:|
| QUOTE | 1,497; 22.145470/s |
| TRADE | 1,597; 23.624793/s |
| Enqueued / written / durable | 52,517 / 52,517 / 52,517 |
| Collector rejected / duplicate / overflow | 0 / 0 / 0 |
| DEPTH available / unavailable | 47,701 / 1,722 |
| Available exact 10+10 snapshots | 43,748 |
| Duplicate or out-of-order stream identities | 0 |
| Process sample | PID 4180; CPU time 47.921875 s; working set 153,788,416 bytes; private memory 136,634,368 bytes |

Natural traffic establishes only the measured retained ingress rate. The
positive producer loss deltas, stale backlog, and terminal canonical-depth
fault prevent any capacity, periodic-cadence, 2,111/s, certificate, or
qualification claim.

## Authority, shutdown, and ledger

The startup reconciliation at `2026-09-21T04:32:28.756341Z` bound Sim101,
LOCAL_SIMULATION, MNQ DEC26, position quantity 0, working order count 0,
working entry count 0, complete order/position snapshots, and no foreign
activity. The proof window added zero commands, zero order events, and zero
executions. The supported guarded stop acknowledged and completed cleanly;
no force termination was used. Its checkpoint sealed expected and durable tip
sequence 165,842 with identical tip hash
`cfc6baa0482c4b5200c8e4b2abe53c37aa1fd1a87d51f227bb440a2bb0fdcf85`.

Post-stop incremental verification passed through sequence 165,842 with a
valid chain, checkpoint, and risk-continuity guard:

`N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\audit\ledger-verification\20260921T043538455710Z-no-fhb-brief-closeout-20260921T0433Z.json`

## Preserved receipts

- Collector journal:
  `N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\audit\pong-authentic-hour\collectors\pong-collector-20260921T043228465785Z-0fefc92586ff\authenticated-observations.jsonl`
  — SHA-256 `361d9f5a612090475c4fa7a2a7a72d1fb6fb54defb537895dc58957d81f36087`.
- Collector status:
  `N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\audit\pong-authentic-hour\collectors\pong-collector-20260921T043228465785Z-0fefc92586ff\status.json`
  — SHA-256 `46f9d937cf04d52dc4ddedf4f69a71e7c3ebcdf52f0309f9fb09622c8f658b8d`.
- Ledger verifier receipt above — SHA-256
  `4bf5aeff8ee41100d56b9c35065a9d6ac4b98baf49143121f4f79f04bcdb9fd5`.

All earlier warm-start faults, failed epochs, certificates, FHB evidence, and
depth receipts remain in place and were not cleared, reset, or relabelled.

## Release condition and stopped state

Do not qualify or paper-start from this epoch. A distinct supported epoch must
first establish all of the following without changing thresholds or authority:

1. producer queue-overflow and transport-failure deltas remain zero from the
   admitted baseline;
2. DEPTH, QUOTE, and TRADE source sequences remain continuous;
3. stale pre-start backlog does not enter the measurement window;
4. a partial native rebuild is explicitly unavailable until the canonical book
   is coherent, or the host receives coherent 10+10 available snapshots without
   latching an input error;
5. a persisted host start/end snapshot supplies evaluations, periodic
   opportunities, late/obsolete/missed ticks, host queue age, source age, and
   bounded/drained host queue values;
6. the fresh certificate matches the current artifact identity; Sim101 remains
   flat/disarmed with zero commands/orders; and the ledger closes verified.

There is no valid qualification or paper-start time while these conditions are
open. The failed brief cannot be extended, overlapped, or relabelled as a
qualifying hour. No automated restart is authorized.

After closeout, ports 8090, 48135, and 48136 are free. The only remaining
relevant process is operator-owned NinjaTrader PID 17208. The supported host
stop remains `scripts\stop_beezconsole.ps1 -Port 8090`.
