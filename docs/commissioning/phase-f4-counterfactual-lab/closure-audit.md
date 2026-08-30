# f4.1.1 closure audit

## Baseline and scope

- Isolated worktree: `C:\Users\atlas\Documents\Trader-f4`
- Branch: `codex/phase-f4-anvil-semantic-restoration`
- Starting commit: `868693b2b060abd2c476bb575314dec2105a816a`
- The main `C:\Users\atlas\Documents\Trader` checkout was not altered.
- Changes are confined to the F4 laboratory, focused F4 tests, and F4
  commissioning documentation. No F5 or Hyperliquid work is present.
- The preserved failed runs `2382988a-c1aa-4346-8e02-2569c44b4a50` and
  `6d514557-82a2-4e76-8894-87267ee2f3cb` remain untouched.

## Defect and measured provider behavior

The failed manifests were inspected first. They prove that genuine Anvil
`v1.8.1` returned different nested full-state hashes after a successful
`evm_revert`, but the old evidence contract intentionally retained neither raw
dump. The original bytes therefore cannot be reconstructed from those
manifests. This diagnostic-provenance gap is recorded rather than hidden.

The exact preserved scenario was then reproduced against the official pinned
binary without a fixed genesis timestamp. Diagnostic run
`b8fe9b7c-069f-46a5-bdfd-509170469ca9` preserved both raw dumps. Its complete
structural diff contains one changed JSON pointer:

| JSON pointer | Before | After | Classification | Rule | Unknown |
| --- | --- | --- | --- | --- | --- |
| `/block/timestamp` | `0x1` | `0x6a948f66` | `PROVIDER_SERIALIZATION` | `ANVIL_V1_8_1_GENESIS_BLOCK_ENV_REANCHOR_V1` | no |

No account, block-history, transaction, or historical-state path changed.
The canonical header, complete account projection, transaction pool, and
declared balance witness restored exactly. The semantic before and after hash
was
`5e4659857293fa36e9848ce8eef1a6224ea5ff2ae3048588dd05a80112273215`.
The raw hashes were respectively
`743f7b09abd034265b76a7363d8df270bf451cb6704afa15aa87c3645eebf1f6`
and
`cf705aa8ff16ded33a502049b9b1e4d36eeb38a1abe498ba6b55249e7743ea22`.
The accepted reason was `RESTORED_SEMANTICALLY_RAW_DUMP_DIFFERED`, with one
classified difference and zero unknown differences.

Pinned Foundry source confirms the cause: `anvil_dumpState` serializes the
in-memory `BlockEnv`; snapshot revert reconstructs that environment from the
unchanged canonical block header. The exact rule accepts only the initial
`BlockEnv.timestamp = 1` to unchanged canonical-genesis timestamp transition
at block zero, with equal semantic states, equal canonical heads, and exact
provider commit identity. Every other path or condition remains fail-closed.
No recursive metadata, history, or snapshot ignore rule exists.

## Correction

Genuine Anvil v1.8.1 commissioning exposed that raw `anvil_dumpState` byte
equality was not a valid standalone restoration oracle. F4 now preserves exact
raw dump hashes and companion dumps as tamper-evident provider evidence and
uses strict `ANVIL_EXECUTION_STATE_V1` semantic commitments over the pinned
provider identity, all accounts and storage, the canonical head, and the
observable execution environment. Transaction-pool state and mutation probes
are verified separately.

Every commissioned mutation has an explicit before, mutated, and restored
witness. A mutation must demonstrably change its witness. Restoration requires
`evm_revert = true`, equal semantic commitments, exact witness and canonical
head restoration, empty and restored pending/queued pools, complete structural
classification, and zero unknown differences. Unknown schema, observation,
structural, witness, semantic, txpool, or revert failures retain bounded reason
codes and quarantine the process. `anvil_loadState` is not used as a recovery
fallback. Impersonation is excluded because it has no independent readable
post-cleanup witness in the pinned provider.

This correction separates provider serialization identity from verified
execution-state identity. It does not ignore metadata.

## Pinned Anvil identity

- Version: `anvil Version: 1.8.1`
- Foundry tag: `v1.8.1`
- Foundry commit: `982849d3140c01fd3b72905759581a132df7aa98`
- Provenance: `https://github.com/foundry-rs/foundry/releases/tag/v1.8.1`
- Official Windows archive SHA-256:
  `02d98fc2c573793960ee06b7f642487d483fe30572f7e248804c207334a418d8`
- Extracted `anvil.exe` SHA-256:
  `c6e29da1b010fe00bac6c0dc5c29484bd641deb5a84050aea10d13e9dc4fe26f`
- Commissioning binary:
  `C:\Users\atlas\AppData\Local\Temp\Trader-f4-foundry-v1.8.1\bin\anvil.exe`
- Bind: dynamic `127.0.0.1` loopback port only
- Forked: `false`
- Fixed commissioning genesis timestamp: `1700000000`

## Real-Anvil regression and commissioning

The genuine-Anvil suite ran all 12 tests with zero skips. Positive coverage
proved native-balance, code, storage, and time/block restoration, persisted
evidence validation, and the real raw-mismatch/semantic-match case. Negative
coverage injected residual balance, code, storage, txpool, and unknown dump
drift and forced `evm_revert` failure. Every negative case produced the exact
bounded quarantine reason and verified process termination and port release.

Two fresh deterministic commissioning processes ran the same scenario:

| Run ID | Manifest SHA-256 | Scenario | Semantic before/after | Raw before/after | Result |
| --- | --- | --- | --- | --- | --- |
| `df51e218-0017-4beb-b019-172e07b51aa1` | `b97d291421f138054bb0576ba345dc6645d1cccd93d9ee48dd1cb2c81fcc0535` | `1e17d73771b1c828a190555c2946b9dc8ee1a73267e32333d111f73699be4efa` | `1e5412d12ea009357aba30a741f7784c9a2852813a34a742096a76e0b99b98a1` | `b4265a26d030f894ad09822ca8f7c10c7a710bdde0bc79fea492b830de4862ff` | `RESTORED_EXACTLY` |
| `0feb3af5-01da-430f-911f-9834add713d0` | `e45aef3f9e5e1d729bfe83a7e3c5ff21323e4eac528bb706b98909a4823007f7` | `1e17d73771b1c828a190555c2946b9dc8ee1a73267e32333d111f73699be4efa` | `1e5412d12ea009357aba30a741f7784c9a2852813a34a742096a76e0b99b98a1` | `b4265a26d030f894ad09822ca8f7c10c7a710bdde0bc79fea492b830de4862ff` | `RESTORED_EXACTLY` |

Both used `ANVIL_EXECUTION_STATE_V1`, produced the same mutation behavior,
semantic baseline and restored state, raw baseline and restored dump,
canonical head, empty txpool, classification rule set, scenario hash,
normalized result, and restoration verdict. Both have zero classified and zero
unknown differences. All six companion artifacts in each run validate against
the manifest. Both child processes terminated and their ports released.

## Protected regression gates

- F4 targeted: 50/50, including 12/12 genuine-Anvil, zero skips
- Frozen F.0-F.3: 56/56
- Phase D: 51/51
- Phase E: 105/105
- Full Python discovery: 432/432
- Python compilation: pass
- `pip check`: pass, no broken requirements
- UI production build: pass
- UI tests: 15/15
- `npm audit --audit-level=high`: pass, zero vulnerabilities
- Evidence integrity and secret validation: pass for all three new runs
- Source secret scan: pass, zero recognized secret values
- Frozen-path audit: pass, zero changed paths
- Import-boundary audit: pass, zero forbidden F4 imports
- Network-write audit: pass; the sole F4 transport is exact-loopback guarded
- Order-submission audit: pass, zero production order-call sites
- Remaining Anvil process count: zero

## Authority and safety accounting

- Remote RPC contacts: 0
- Production network writes: 0
- Testnet network writes: 0
- Testnet orders: 0
- Mainnet orders: 0
- Real wallet credentials: none
- Chain fork: false
- Anvil bind: loopback only
- Local Anvil mutations: fresh disposable process only, reverted and verified

No authority, frozen-path, network-write, credential, or order-submission
boundary changed.

## Closure status

All f4.1.1 implementation, evidence, genuine-provider, negative-control,
determinism, regression, integrity, cleanup, and safety gates pass.

`READY_FROZEN`
