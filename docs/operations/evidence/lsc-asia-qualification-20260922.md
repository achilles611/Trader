# LSC Asia qualification disposition — 2026-09-22

Decision time: `2026-09-22T13:00:00-06:00` / `2026-09-22T19:00:00Z`

Fixed target retained: `2026-09-22T18:00:00-06:00` / `2026-09-23T00:00:00Z`

Preflight latest start retained: `17:15 MDT`. Source/configuration freeze retained:
`17:40 MDT`.

Controlling disposition:

`HOLD_LSC_ASIA_QUALIFICATION_NOT_PROVEN`

The fixed qualification window is closed pre-start. It was not moved or
relabelled. No legacy fallback, runtime start, ledger write, latch recovery,
NinjaTrader reload, installation, arming, paper start, canary, or order was
performed. The clock does not release any gate.

## Exact source and checkpoint

- Neptune requirements checkpoint: `5455ad00cef07a042a2d607303cb404c70acc9d6`.
- Exact checkpoint parent: `64adc337b97f7d4b757176a18dc11235cef67897`.
- Pushed remote branch containing it: `origin/codex/lsc-v1-20260922`.
- Requirements matrix: `docs/architecture/lsc-v1-pre-edit-requirements.md`,
  11,498 bytes, SHA-256
  `FFDC2D24EEBD9D326B81486DBC71472EE8F2918CB30C48C3804C852DD13EF385`.
- Current isolated branch: `codex/lsc-v1-current-20260922`.
- Final reviewed implementation HEAD:
  `99f43c5d44cfc7c583c77676e2a3a6a6b86048af`.
- Final tree: `3cf03f74482e185e4e56101b7c38965870973b96`.
- Operational Atlas branch remains unchanged at
  `5d5a603dce1d806da153613fce16678af618130c`; its pre-existing unrelated
  modifications remain preserved.

## Gate status

| Required return item | Status | Evidence / reason |
| --- | --- | --- |
| Neptune checkpoint sealed | **PASS** | Exact one-file commit `5455ad0...`, pushed. |
| Matrix transferred and hash-verified | **PASS** | 11,498 bytes; SHA-256 `FFDC2D...F385`. |
| Atlas base verified | **PASS** | Original base `64adc337...`; current-source port based on operational `5d5a603...` in isolated worktree. |
| LSC implemented | **PASS, source** | Codec, bounded segment store, independent shadow commitments, decision coordinates, verifier, and default-off runtime seam. |
| Focused tests passed | **PASS** | 121 cases plus 54 subtests at final candidate; JUnit SHA-256 `6DC14CD61648EB715F63CBAE547F622699DCD7DD6E3D50DBF9FA714DCB6F2976`. |
| Full tests passed | **PASS** | 1,707 cases plus 608 subtests in 280.09 s; 2,315/2,315 nodes clean. JUnit SHA-256 `B4B761EF30B1E109DD63E72C2DA66522DA74473F48A149A3F562779FF670EA56`. |
| Lossless parity passed | **HOLD, partial** | All 1,885 retained authenticated observations match exactly, but both corpora lack evidence families, hypotheses, decisions, reason codes, final replay state, and final replay hash. Full-gate report correctly returns `FAIL`. |
| Crash recovery passed | **HOLD, partial** | Store/commitment fault tests and one external forced-process kill/recovery/continuation probe pass. The complete required external write/flush/rename/commit/completion boundary matrix is not proven. |
| Performance requirement passed | **HOLD / UNKNOWN** | No three sustained plus three burst like-for-like clean runs at 2,111 events/s. Threshold is unchanged. |
| Integrated | **PASS, default-off source only** | Caelum accepted callback isolation and supported unclean recovery at `99f43c5...`. No production factory is selected. |
| Installed | **NO** | Not performed. |
| Loaded | **NO** | Not performed. |
| Preflight passed | **NO / NOT STARTED** | Upstream proof gates failed before operational preflight. |
| Qualification started | **NO** | Fixed window closed pre-start. |
| Qualification completed | **NO** | Not started. |
| Qualification passed or held | **HELD** | `HOLD_LSC_ASIA_QUALIFICATION_NOT_PROVEN`. |
| Account identity | **UNKNOWN current** | No runtime/account observation was authorized in this pass. Historical configuration is not current proof. |
| Position | **UNKNOWN current** | No current reconciliation was performed. |
| Working orders | **UNKNOWN current** | No current reconciliation was performed. |
| Arming state | **UNKNOWN current; no arming performed** | Runtime ports were closed at the read-only check. |
| Live-capital authority | **DENIED** | No live-capital operation is authorized. |

## Immutable receipts

- Focused integration JUnit:
  `N:\Beelzebub\lsc-v1-tests-20260922\current-integration-corrected-focused\results.xml`.
- Exact-head Full JUnit:
  `N:\Beelzebub\lsc-v1-tests-20260922\parent-current-full-integrated\results.xml`.
- External crash probe:
  `N:\Beelzebub\lsc-v1-tests-20260922\external-crash-probe-20260922T1900Z\report.json`,
  SHA-256
  `62A57D08969F386020BB46134E332328355E6C02534539EE6CBFC57A14C7855B`.
- Retained-corpus full parity gate:
  `N:\Beelzebub\lsc-v1-tests-20260922\retained-observation-parity-20260922\parity-full-gate-report-v2.json`,
  SHA-256
  `9D5A0834980D30588F74F1838C67D7FB1D530FCC9FCA2E0C5CF9E09A5476EAD1`.
- Caelum review:
  `docs/administration/caelum/passes/20260922-184107Z-lsc-integration-review/LSC_INTEGRATION_REVIEW.md`,
  pre-commit SHA-256
  `0FDA4B3525EEDED80A09D103246FE0676D6F6B96395BDFE1E5DADFA55CE5811C`.
- Adam admissions review:
  `docs/administration/adam/passes/20260922-190205Z-lsc-asia-admissions/ADMISSIONS_REPORT.md`,
  pre-commit SHA-256
  `00D3E080E1EC63FBE29617FAC964BF81A7BE96A29C4DF3B570AD452007616A1C`.
- Caelum superior disposition:
  `AFFIRM_ADAM_ADMISSIONS_REPORT_NO_SUPERSESSION`.

## Capacity and latch facts

The capacity sampler uses `shutil.disk_usage(self.path.parent).free`; therefore
it measures the filesystem containing the actual resolved ledger destination,
not a hard-coded C: volume. The 8 GiB warning floor and 4 GiB minimum floor are
unchanged.

Read-only destination check for the historical operational ledger path:

- Ledger:
  `N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\hot\lane_iii_paper.sqlite3`.
- Size: `1,769,721,856` bytes.
- Destination free: `225,612,677,120` bytes.
- Warning floor: `8,589,934,592` bytes.
- Minimum floor: `4,294,967,296` bytes.

The earlier C: refusal came from test-created ledgers whose actual parent was on
C:. LSC tests and evidence in this pass used N:. No operational storage
migration was necessary or performed.

Supported capacity-lock recovery exists as a separate loopback-authenticated,
disarmed operation, but it requires a current Full verification plus exact
Sim101, flat, zero-order, reconciled runtime proof. Those current facts are
unknown because the runtime is stopped and the upstream LSC gates are held.
Therefore capacity latch recovery was not called and no ledger row was written.

## Commands

Focused verification:

```powershell
python -m pytest tests/lsc tests/test_receiver_repair_dispatch.py tests/test_l3g_paper_control_center.py tests/test_lane_iii_phase_f3.py tests/test_lane_iii_live_shadow.py -q --junitxml=N:\Beelzebub\lsc-v1-tests-20260922\current-integration-corrected-focused\results.xml
```

Exact-head Full:

```powershell
python -m pytest -q --junitxml=N:\Beelzebub\lsc-v1-tests-20260922\parent-current-full-integrated\results.xml
```

No benchmark receipt was manufactured from the lossy retained capture. No
missing proof family was filled synthetically.

## Release conditions

Do not reopen or move this qualification window. A future, separately
authorized and predeclared window requires one unchanged candidate with:

1. full decision/evidence/final-replay parity;
2. the complete external crash-boundary matrix;
3. at least three sustained and three burst passes at 2,111 events/s with zero
   overflow, transport loss, gaps, unexplained loss, or queue growth;
4. an explicit N:-resident production LSC factory and resolved identity;
5. installed and loaded binary/source/profile/route identity; and
6. fresh Full verification, exact Sim101, flat/no-orders/reconciled proof,
   disarmed state, trusted ledger tip, and adequate destination capacity.
