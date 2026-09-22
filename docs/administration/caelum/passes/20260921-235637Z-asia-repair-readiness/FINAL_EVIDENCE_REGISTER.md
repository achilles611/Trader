# Final evidence register — Asia repair readiness

Review timestamp: `2026-09-22T01:06:14Z`

This register distinguishes repository implementation, sealed offline evidence, and state that can only be established during a later live preflight. Hashes are SHA-256 unless explicitly identified as Git object IDs.

## Repository identity

| Item | Identity | Review note |
|---|---|---|
| Final evidence HEAD | Git commit `c886f505dca2973d83bac7c85ba8f3fd50a19c82` | Current HEAD at independent review start. |
| Final evidence tree | Git tree `ec4bcbc9cb82d94508c2ff96f573ad7804eca6d1` | Includes the fail-closed runbook. |
| Integrated repair commit | Git commit `b4a46c2d73030f008e305a687069456c5d1bda0d` | Ancestor of final evidence HEAD. |
| Integrated repair tree | Git tree `c3e5ca334e63f8332883a5f31ac9013785be905a` | Frozen repair-code tree. |
| Initial pass baseline | Git commit `04a1f22013a00cf595f41424f77f93e65cf62918` | Starting point supplied to the pass. |
| Runbook | `docs/operations/beelzebub-asia-qualification-next-pass-20260922.md` | `7161E8A6FE85AE436A6656C9B122ED78B93AF4D6C0CB8BE789751C28BA6EFC11` |

Repository checks found no repair-code changes between `b4a46c2...` and `c886f505...`; the latter adds the operational runbook. Unrelated dirty and untracked user/agent files were neither changed nor staged by this review.

## Sealed external evidence

| Artifact | Path | SHA-256 | What it establishes | What it does not establish |
|---|---|---|---|---|
| Predeclaration | `N:\Beelzebub\asia-repair-readiness\integrated-20260922T004021Z\PREDECLARATION.md` | `6AC1CD47BEB659D437CFBA640A2F0A6F176BB0C57994506552243D9A0F0DD27A` | Frozen matched baseline/candidate comparison design and identities. | Natural-feed qualification or policy approval. |
| Coordinated comparison | `N:\Beelzebub\asia-repair-readiness\integrated-20260922T004021Z\COORDINATED_COMPARISON.md` | `7CB00EE18CB25406844F9C17EF6B6E77405C34F57A26F8E4784080519D30D2BA` | Exact 10,000-event integrity and localized timing; records `NOT_QUALIFIED`. | Service equilibrium, a numeric acceptance threshold, or an immutable resource limit. |
| Baseline result | `N:\Beelzebub\asia-repair-readiness\integrated-20260922T004021Z\baseline-protected\result.json` | `653A092DFE4D6A6AADE9F34E8910E5D5CFF8F6A7239302D38AE3ADFA626029DC` | Baseline matched synthetic result. | Candidate qualification. |
| Candidate result | `N:\Beelzebub\asia-repair-readiness\integrated-20260922T004021Z\candidate-protected\result.json` | `6D8F76A4CF9A1684A656BDD13623535A9AB1F9D253895D66208E83DC642D6F32` | Candidate exact-integrity result and residual lag/age. | Repair closure. |
| Healthy/protected/recovered bridge result | `N:\Beelzebub\asia-repair-readiness\integrated-20260922T004021Z\healthy-protected\asia-periodic-healthy-flat.json` | `4DC61C4F3F61E2ED312C3AD620C6C8C802BAF2E1791150D4B448A441E885ECAF` | Offline healthy `R=2.0`, protection exclusion, recovery, zero native dispatch. | Approved threshold or live bridge/native behavior. |
| Focused JUnit | `N:\Beelzebub\asia-repair-readiness\integrated-20260922T004021Z\focused-tests.junit.xml` | `6E25536857F8F269E62B6BC57EE06A02E696062E991FE152B268ECFF81A42BD2` | 97 cases (`68` tests plus `29` subtests), zero failures/errors/skips. | Native installation, feed health, or account state. |
| Native lifecycle evidence | `N:\Beelzebub\asia-repair-readiness\native-temp\NATIVE_LIFECYCLE_REPAIR_EVIDENCE.md` | `C681348F1651A95E625B0980D50B36B45FA5FFE6077E57FED57A30A25ADD7C5F` | Extracted source harness, callback ordering, tests, Roslyn source compile. | Installed, loaded, or NinjaTrader runtime verification. |
| Receiver repair evidence | `N:\Beelzebub\asia-repair-receiver\RECEIVER_COLLECTOR_REPAIR_EVIDENCE.md` | `FC24BAB61A115C3BBE6CF53414FFD741638C6BB48DD4601B02B7EB967586EB16` | Bounded receiver/sender telemetry implementation and focused tests. | Resolved service deficit. |
| Capacity path trace | `N:\Beelzebub\receiver-periodic-corrective-evidence\CAPACITY_PATH_TRACE.md` | `6363BE5959CC91F2B1B924FC56E2B1958C7B4BB1689000F1C941E48D4A40C6F4` | Ledger is already rooted on `N:`; no migration is applicable; durable lock remains. | Authority to clear the latch without current preflight. |

## Critical current-source identities

| Source | Review identity |
|---|---|
| `ninjatrader/NinjaScript/AddOns/BeelzebubReadOnlyAddOn.cs` | File SHA-256 `FFBB655D60A392E20DBEEA9DD8F8DB1ECBD4E68A1CD56B9C4F5A258F11E619E5`; embedded producer fingerprint `17995c2795e16bf52bedd9644a865261e3a20473f1e2bb7b75b4947bc558a525`. |
| `src/l3f_provider/ninjatrader_observation.py` | File SHA-256 `3AF093B0E429F46DE2EDC624DEB353EEBD0B8029519951837FBA30CB3C5EB86F`; expected fingerprint matches the embedded native value. |
| `src/l3g_paper/pong_periodic_telemetry.py` | File SHA-256 `CFBCFC3D16EC5CD10D21B95614A5A7F3D9F436A40869F162A2D4B2E3E6ECE602`. |

The native evidence receipt predates the final parent synchronization of the embedded producer fingerprint. Therefore its isolated file hash is not asserted as the final working-file hash. The synchronized final source and expectation were independently inspected at the frozen repair commit.

## Comparison facts retained verbatim as numeric evidence

| Measure | Baseline | Candidate | Administrative interpretation |
|---|---:|---:|---|
| Completed/durable/ordered events | 10,000 | 10,000 | Exact synthetic integrity is verified offline. |
| Producer-overflow delta | 0 | 0 | Narrow loss accounting is clean. |
| Transport-failure delta | 0 | 0 | Narrow loss accounting is clean. |
| Completion rate | 288.3907/s | 288.6646/s | Approximately 0.095% improvement is immaterial. |
| End processing lag | 24.676 s | 24.643 s | Open-ingress backlog remains. |
| Maximum collector-local age | 11.094 s | 11.469 s | Candidate age is worse and exceeds the unchanged 1,000 ms freshness bound. |

Candidate stage measurements include approximately `257.020 ms` collector enqueue, `6,327.363 ms` paper fanout, `25,147.743 ms` collector encode/hash, `5,930.661 ms` status persistence, and `20,103.946 ms` sender socket writes. These stages overlap; they must not be summed or used to claim a single causal bottleneck without further controlled evidence.

## Runtime facts intentionally not claimed

At review close, the following remain current-state unknowns rather than omissions to be inferred from offline evidence:

- installed and loaded NinjaTrader assembly/source identity;
- actual native teardown under blocked socket I/O;
- current listener/storage binding and distinct-producer state;
- current Sim101 account, positions, orders, reconciliation, and arming state;
- current source continuity, canonical depth, freshness, producer-overflow, and transport-failure state;
- supported capacity-latch recovery and post-recovery full verification;
- the fixed natural ratio window; and
- any Gates B–D, paper, arming, or order authority.

These are classified `LIVE PREFLIGHT PENDING`; none was silently promoted from historical or synthetic proof.
