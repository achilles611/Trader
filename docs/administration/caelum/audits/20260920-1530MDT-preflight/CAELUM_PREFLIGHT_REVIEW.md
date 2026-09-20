# Caelum preflight review — DEC26 PONG / non-FHB

Review time: 2026-09-20 15:35 MDT (`2026-09-20T21:35:13Z`)

## Outcome

**Administrative result: PASS for continued disarmed DEC26 qualification.** No
source defect among the four assigned issues requires stopping collection or
fresh-hour qualification. The old 2026-09-17 ledger remains ineligible for
current policy reuse; Sol's fresh ledger is the correct path and has a current
PASS verification receipt.

One bounded defect remains: before a qualifying-hour certificate exists, the
Pong status publishes an unavailable, unconfigured daily-bias sentinel whose
`instrument` field says `MNQ SEP26`. This is an observability-label defect, not
entry authority: the sentinel is fail-closed, bridge creation requires a valid
certificate, and both certificate construction and reload rebuild the bias with
the selected topology's native instrument. **It does not HOLD current
qualification.** It must not be mistaken for DEC26 qualification evidence.

Activation remains held by the existing qualification and authority gates until
a fresh, unexpired certificate is loaded and the status carries `MNQ DEC26`.

## Scope and ownership

- Caelum was authorized as one bounded administrator reviewer for four named
  issues. Sol remained the sole integrator and sole runtime/process/account
  operator.
- Caelum's only writable path was this report. No application, test,
  configuration, runtime, process, account, order, ledger, benchmark, or
  collector action was authorized or performed.
- Evidence classes below are `OBSERVED` when independently read or executed by
  Caelum, and `REPORTED` when taken from Sol's preflight receipt.

## Repository baseline

- Root: `C:\Users\atlas\Documents\Trader`
- Branch: `codex/lane-iii-perpetual-corrective-20260906`
- Reviewed HEAD: `1101980e21a1c74555c1eb141ab5362cdfe84e54`
- Commit: `Fix BeezConsole DEC26 preflight binding`
- Parent: `951faa509a2faf069e2d78304a2fe3dabe459916`
- Tree: `2b9e0a048dbd02bfcfa6e65fc86dd95196b3367f`
- `git diff --check HEAD^ HEAD`: PASS.
- At review start the only reported worktree status was the pre-existing
  untracked `docs/administration/caelum/audits/` tree. No user or Sol file was
  changed by this review.

The earlier 2026-09-20 10:47 MDT Caelum audit's F01 described a future
two-selector hazard at HEAD `998e229`. Current HEAD supersedes that specific
finding for this launcher path by enforcing selector equality before binding.

## Four-issue decision record

### 1. Historical terminal projection from a prior checkout — PASS

**OBSERVED evidence**

- The sealed operation
  `N:\Beelzebub\runtime\profile-switch\operations\profile-switch-614bd791f63e409d81f85779c2f2100f`
  has an intact manifest for commit `123e8d86...`, runtime root
  `N:\Beelzebub\runtime`, prior project root
  `N:\PongFHB\20260916T225247Z\implementation`, the current supported Python
  executable, `paper_only=true`, `live_capital=DENIED`, and a terminal
  `BLOCKED_SAFE` state. Manifest and state SHA-256 values were recorded as
  `B82EFB95...` and `FE5FD2B0...` during this review.
- `beez_console.py:329-378` relaxes `project_root` equality only when
  `require_current_git_sha` is false. `beez_console.py:470-489` selects that
  mode only for `EXPLICIT_MAINTENANCE`, rechecks requested operation identity,
  and still refuses every nonterminal stage.
- Remembered established launches and target adoption keep the default
  current-checkout requirement (`beez_console.py:418-458,500-518`). Historical
  manifest fallback still requires canonical integrity, known profiles, a
  syntactically valid prior Git SHA and policy/risk hashes, `paper_only=true`,
  and `live_capital=DENIED` (`beez_console.py:381-415`).

**Decision.** The change is narrow enough for the stated checkout-upgrade
inspection. It does not make an old project root current launch authority and
does not authorize a competing or active handoff.

**Release condition.** None for continued qualification. Any nonterminal state,
broken digest, selector mismatch, unexpected runtime root/Python, or remembered
launch using a prior checkout remains a HOLD.

### 2. Old ledger reuse refusal and fresh ledger — PASS

**OBSERVED evidence**

- `src/l3g_paper/ledger.py:1722-1783` binds an existing ledger to policy hash,
  risk hash, entry profile/version, canonical contract and topology version. A
  mismatch closes the connection and raises
  `PAPER_PROFILE_LEDGER_EPOCH_MISMATCH`; there is no reuse fallback.
- The old run's latest full verifier receipt at
  `N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260917T022503Z-b77a9f3c85f3\audit\ledger-verification-latest.json`
  passed chain integrity through sequence 293 under its historical epoch. That
  integrity result does not make the epoch reusable under a changed policy or
  topology.
- The fresh run at
  `N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11`
  has a distinct epoch and ledger identity. Its full verification receipt
  completed at `2026-09-20T21:19:50.982793Z` with `PASS`, `quick_check=ok`,
  chain/checkpoint valid and sequence 3. A later incremental receipt completed
  at `2026-09-20T21:24:53.497422Z` with `PASS` through sequence 6 and no errors
  or storage warnings.
- Sol **REPORTED** that the attempted old-ledger start produced the exact
  `PAPER_PROFILE_LEDGER_EPOCH_MISMATCH` refusal before creating the fresh run.

**Decision.** Refusing the old ledger was correct and must not be bypassed. The
fresh ledger plus fresh epoch is the accepted preflight path.

**Release condition.** The old ledger is permanently excluded from this
current-policy run. Continued qualification must retain the fresh path/epoch
and a current PASS verifier receipt. This condition is presently satisfied by
the cited receipts; it does not prove market-hour qualification or arming.

### 3. Launcher/backend DEC26 hash disagreement — PASS (implemented and tested)

**OBSERVED evidence**

- Current source resolves both topology selectors, rejects mixed versions with
  `PONG_PAPER_CONTRACT_TOPOLOGY_MISMATCH`, and derives launcher policy/risk
  hashes through `paper_policy_for_topology` and `paper_risk_for_topology`
  (`beez_console.py:289-322`). The backend independently does the same before
  constructing its runtime binding (`src/copytrade/control_center.py:1656-1672,
  1870-1890`).
- Independent pure-source resolution produced:
  - unadjusted policy: `09a3675aeea804904989d8ea3fe58de33b428842b014267fd312ac1625d7bcca`
  - unadjusted risk: `3f28fee84c478e8e2de13b6ed03e155bdaebd84b63a1d7040c0816dd4b933f7c`
  - DEC26 policy: `333f3c07bc6c7340e67b9238532eb1d9538c4e9b22644f6ecd23d4d49c8773e0`
  - DEC26 risk: `404251db8a372641f3a25d7edbb921cf5da6a4277bc3a6fd2fab5a2768269962`
- Five focused independent launcher tests passed in two commands: the DEC26
  topology hash binding, mixed-selector refusal, terminal prior-checkout
  acceptance, active-handoff refusal, and exact requested-binding requirement.
- Sol **REPORTED** 80 focused tests plus 8 collection/loss tests passing. Caelum
  did not rerun that larger suite or a competing benchmark.

**Decision.** HEAD `1101980` fixes the exact two-field refusal shown by the
user. This review accepts the implementation and focused tests. Caelum did not
query the live 8090 runtime, so it does not independently claim the repaired
commit is the currently loaded process.

**Release condition.** Sol's runtime receipt must show both selectors at DEC26,
the DEC26-adjusted hashes above, and exact service/ledger/audit/epoch identity.
Any unadjusted hash or mixed selector is a HOLD on using that listener. No
additional code HOLD is issued for qualification.

### 4. SEP26-labeled unconfigured bias sentinel under DEC26 — PASS for qualification; bounded defect

**OBSERVED evidence**

- `default_unconfigured_bias()` constructs a historical SEP26 frame
  (`src/l3g_paper/daily_bias.py:949-980`). A pure in-memory check confirmed
  `instrument=MNQ SEP26`, `configured=false`, `direction=UNAVAILABLE`, and
  eligibility `DAILY_BIAS_UNCONFIGURED`.
- Eligibility returns on `configured=false` before any direction can become
  entry support (`src/l3g_paper/daily_bias.py:449-466`).
- The host initially exposes that sentinel in status
  (`src/l3g_paper/pong_production.py:371-372,1955-1962`), which is the concrete
  observability defect.
- A runtime bridge cannot be created while the certificate is absent:
  `_maybe_create_bridge` requires both a certificate and native-input readiness,
  and native-input readiness itself requires the certificate
  (`pong_production.py:1461-1472,1521-1527`). Entry admission also rechecks all
  blockers (`pong_production.py:1082-1115`).
- Missing certificate produces `PONG_AUTHENTIC_HOUR_NOT_QUALIFIED`; activation
  cannot become a candidate while that blocker exists
  (`pong_production.py:1565-1580,1618-1665`).
- Both fresh certification and certificate reload call `_bias_from_inputs` with
  `instrument=self.topology.native_instrument`, then validate the reconstructed
  result hash before adoption (`pong_production.py:1308-1364,1366-1437`). A pure
  in-memory DEC26 reconstruction produced `instrument=MNQ DEC26` and
  `configured=true`.

**Decision.** The startup SEP26 label is not execution-capable and is replaced
only through the authenticated, topology-bound DEC26 certificate path. It is
therefore not a mixed-contract release blocker for collecting or qualifying
the current hour. It is a real status-label defect because an operator could
mistake a placeholder payload for contract-coherence evidence.

**Release conditions.** Keep activation held until the accepted status has a
fresh `qualified_hour_certificate`, matching result hash, unexpired
`daily_bias`, `daily_bias.instrument=MNQ DEC26`, and all other authority blockers
cleared. That activation hold is already encoded. Separately, before treating
pre-certificate status as a contract-clean display, the integration owner
should make the unconfigured sentinel topology-specific (or label it explicitly
contract-neutral) and add a DEC26 pre-certificate status test. That follow-up is
not required to continue current qualification.

## Current receipt boundary

The newest existing collection receipt inspected at
`2026-09-20T21:30:45.0554739Z` was `COLLECTING` with transport `CONNECTING`,
zero enqueued/written/durable records, and the required 16:00-17:00 MDT interval
still `PENDING`. It reported `OBSERVE_ONLY`, paper execution `DISARMED`, live
authority `DISARMED`, live capital `DENIED`, topology
`PONG_CONTRACT_TOPOLOGY_DEC26_V1`, native `MNQ DEC26`, canonical `MNQZ6`, and no
queue overflow/rejection/duplicate counts. This is a file receipt, not a Caelum
runtime query, and it is not a qualifying-hour pass.

Caelum did not independently observe account identity, position, working
orders, arming state, loaded binary, or current 8090 binding. Those remain Sol's
runtime-owner evidence responsibility.

## Administrative HOLD ledger

| Affected action | Status | Evidence / consequence | Release condition | Owner |
| --- | --- | --- | --- | --- |
| Reuse 2026-09-17 ledger for current policy | **HOLD** | Source rejects changed policy/topology identity; old chain integrity is historical only | None; use the fresh 2026-09-20 path/epoch | Sol (satisfied operationally by fresh run) |
| Continue fresh disarmed DEC26 collection/qualification | **PASS** | Four assigned issues have safe source paths; fresh ledger verification passes | Preserve exact fresh binding and loss/durability gates | Sol |
| Activate entries | **HOLD** | No accepted fresh-hour certificate in inspected receipt; current collection still pending | Fresh DEC26 certificate, unexpired DEC26 bias, exact runtime/account/flat/order identity, all blockers clear, and separate authorized operational start | Sol |
| Treat pre-certificate daily-bias status as contract-coherence proof | **HOLD** | Placeholder says SEP26 despite DEC26 topology | Topology-specific/neutral sentinel display, or wait for accepted DEC26 certificate status | Integration owner / Sol |

## Verification performed by Caelum

- Read `AGENTS.md`, `.codex/agents/beelzebub_admin.toml`,
  `docs/administration/caelum/CHARTER.md`, and `CONCURRENCY.md` before review.
- Inspected commit `1101980`, exact diff, relevant source predicates, relevant
  tests, historical manifest/state fields, fresh and old verifier receipts, and
  the latest fresh collection receipt.
- Ran five focused unit tests only; all passed. Ran no benchmark, replay,
  application, runtime, process, account, ledger-write, or network action.
- Verified the commit diff with `git diff --check HEAD^ HEAD`.

## Next concrete action

Sol should continue the disarmed listener-first preflight and preserve one
exact runtime-binding receipt plus the fresh-hour collector/certificate receipt.
If the certificate is accepted, confirm the published daily-bias instrument is
`MNQ DEC26` before considering the separate operational-start decision. The
topology-specific placeholder cleanup can be scheduled as a small follow-up;
it should not interrupt the current qualifying hour.
