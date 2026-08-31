# Codex context handoff

**Snapshot:** 2026-08-30 (America/Denver)
**Repository:** `achilles611/Trader`
**Handoff branch:** `codex/l3g-ledger-epoch2-recovery`

This is the small, Git-tracked source of truth for Codex work context in this
repository. It indexes every currently listed Codex task whose working
directory is `C:\Users\atlas\Documents\Trader`, plus the one archived task
from that directory. Task titles describe their original scope; verify the
working tree and tests before treating an older task as complete.

## Current direction

The current `l3h` branch has an isolated, fail-closed live-capital authority
boundary without changing the sealed L3G Sim101 paper capability. It now has a
dedicated HMAC/replay-safe loopback protocol, native AddOn source, write-ahead
lifecycle, reconciliation supervisor, bootstrap/install/parity/kill tooling,
and detailed disabled dashboard gates. No local signed capability, installed
L3H AddOn, compiled DLL, runtime hello, or Sim101 matrix is present. The
truthful terminal state is `BLOCKED_CAPABILITY_MISSING`; no L3H order path is
enabled or authorized. The exact next action is visible AddOn installation and
compile, followed by the disarmed Sim101 mechanical matrix.

## Task index

| State | Task | Brief context |
| --- | --- | --- |
| Active | Track Codex context windows | This handoff: publish a durable context index for other agents. |
| Idle | Freeze f4 and build observer | Commission Anvil v1.8.1 in Trader-f4, then build a public-only Hyperliquid observer. |
| Idle | Build f4 counterfactual lab | Integrate the xGodMode architecture as a safe counterfactual execution subsystem. |
| Not loaded | Show sim101 and LucidFlex balances | Add paper-trading balance display to BeezConsole. |
| Not loaded | Fix L3G ledger throughput | Recover PaperLedger durable-writer throughput while preserving the historical hash chain. |
| Not loaded | Create Obsidian flow view | Create an Obsidian view for phases, lanes, flowcharts, and dependencies. |
| Not loaded | Fix observer starvation hotfix | Repair a stale market observer during Lane III ledger rehearsal. |
| Not loaded | Fix Lane III ledger-tail trust | Diagnose the ledger-tail trust model while preserving safety protections. |
| Not loaded | Verify Phase E handoff block | Validate the preregistered Phase E acquisition block and readiness state. |
| Not loaded | Fix commissioning ledger liveness | Repair the ledger freshness gate, deploy a hotfix, and resume validated Sim101 commissioning. |
| Not loaded | Verify Beelzebub maintenance bringup | Rebuild the AddOn, migrate audit proof, restart BeezConsole, and verify bindings and ledger benchmark. |
| Not loaded | Harden ledger verification runtime | Harden the Lane III verifier, WAL, runtime binding, BeezConsole, and ledger provenance. |
| Not loaded | Implement local ledger verifier | Add deterministic local ledger verification with fast and forensic modes. |
| Not loaded | Complete paper commissioning closure | Complete the Sim101 Lane III commissioning lifecycle, ledger exit audit, and flat-state verification. |
| Not loaded | Count working hours used | Calculate total working hours used. |
| Not loaded | Commission NY_AFTER paper session | Implement session-local NY_AFTER commissioning with BeezConsole, NinjaTrader Sim101, and Lane III ledger work. |
| Not loaded | Reconfigure Beelzebub storage | Recover C: space and commission 500 GB NVMe as hot storage. |
| Archived | Codex Handoff — Freeze Phase D and Begin Phase E Mission… | Earlier Phase D-to-E handoff context; retained for historical reference. |

## Working-tree caution

At the time of this snapshot, the branch has uncommitted work in the following
areas: BeezConsole, the control-center UI, the NinjaTrader read-only AddOn,
copy-trade control/commissioning code, related tests, and two launcher scripts.
Those changes predate this handoff and are intentionally not included in its
commit. Do not discard, amend, or assume ownership of them without first
inspecting `git status` and the diff.

## One-step agent handoff

Give another agent this instruction:

> Fetch `origin/codex/l3g-ledger-epoch2-recovery`, read
> `docs/CODEX_CONTEXT_HANDOFF.md`, inspect `git status` and the relevant diff,
> then continue only the task you are assigned. Preserve existing uncommitted
> work and the paper-only / no-external-writes safety boundary.

To read the exact published snapshot without checking out the branch:

```powershell
git fetch origin codex/l3g-ledger-epoch2-recovery
git show FETCH_HEAD:docs/CODEX_CONTEXT_HANDOFF.md
```

## Update convention

Before a material handoff, update this file with the new task, current focus,
verification status, and any working-tree cautions; commit it separately and
push it to the handoff branch. This keeps context reviewable without mixing it
with implementation changes.
