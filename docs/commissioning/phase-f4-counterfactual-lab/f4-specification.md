# Phase f4 — Counterfactual Execution Laboratory

## Status and constitutional boundary

`f4` is a Lane II side-domain with one allowed capability:

```text
COUNTERFACTUAL_SIMULATION / COUNTERFACTUAL_ONLY
```

It is not a strategy, signal, prediction, scientific-evaluation, execution,
trading, or live-capital authority.  It imports neither `src.phase_e`, the
frozen Trader V0 artifact, `phase_d_bridge`, nor the Hyperliquid testnet
adapter.  Its evidence types are independent immutable contracts and cannot
satisfy F.0/F.2 exact-type admission.

No production or testnet network is mutated.  An upstream fork source, when
configured outside a scenario, is HTTPS read-only source material; only a
local Anvil process can receive privileged state mutations.

## Scenario format

`CounterfactualScenario` is an immutable canonical-hashed document with:

- schema, scenario identity/version, deterministic seed, backend, target
  domain/chain, pinned fork block where relevant, and initial fingerprint;
- an ordered allowlisted mutation manifest and allowlisted assertions;
- an explicit hard timeout and `COUNTERFACTUAL_ONLY` provenance;
- optional parent scenario hash and mutation delta for deterministic branches.

Scenario documents are JSON only.  They reject callable/module/command/RPC/
URL fields, secret-like fields, opaque objects, unrecognized keys, and
unknown verbs before a backend starts.

## Backends

### Venue model

`VenueModelBackend` has no network capability.  It can represent balances,
positions, foreign positions/orders, marks, metadata, partial/duplicate/
out-of-order fills, cancellation races, time, stale truth, account mismatch,
rate limits, transport failure, and ambiguous submission.  Hostile or
ambiguous state latches `RECONCILIATION_REQUIRED`: unknown always means
reconcile, never guess.

### EVM Anvil

`AnvilBackend` starts a fresh Foundry Anvil process per scenario, pinned to
`127.0.0.1` and a dynamic port.  It expects Foundry/Anvil `v1.8.1`; a newer
toolchain is reported but not silently treated as commissioned.  Scenarios
expose only these named capabilities:

```text
snapshot, revert, set_native_balance, set_contract_code, set_storage_slot,
impersonate_account, stop_impersonation, advance_timestamp, mine_block,
mine_blocks, dump_state, load_state
```

The adapter maps those verbs internally to Foundry's documented local RPC
surface (`evm_snapshot`, `evm_revert`, `anvil_setBalance`, `anvil_setCode`,
`anvil_setStorageAt`, impersonation, mining, and state-management methods).
There is no scenario-level `rpc(method, params)` API.  Addresses, code,
storage keys/words, uint bounds, timestamps, block counts, chain identity,
fork pinning, and loopback binding are validated.

`dump_state` is process-local only.  Its large raw content is neither written
to Git nor included in evidence.  `load_state` can only name a state dumped
earlier in the same process.

## Isolation and cleanup

The coordinator takes a backend *factory*, not a backend instance.  One run
therefore receives one mutable universe.  Every run snapshots first and uses
`finally` to revert and fingerprint the restored state.  A failed revert or
failed restoration fingerprint kills and quarantines the process.  All
backends are closed/discarded at end of run, including successful runs.

## Evidence and artifacts

Evidence records only bounded hashes, verbs, state differences, assertion
names, toolchain identity, fork chain/block, cleanup state, run state, and
non-secret diagnostics.  It never includes provider payloads, private keys,
seed phrases, Rabby data, wallet secrets, auth headers, or raw state dumps.
Run manifests live under the project-relative logical root:

```text
runtime/lane_ii_lab/<run-id>/run-manifest.json
```

The manifest embeds the exact scenario for replay but remains
`COUNTERFACTUAL_ONLY` engineering evidence, not an alpha/effectiveness claim.

## CLI

```text
python -m src.lane_ii.lab.cli validate <scenario.json>
python -m src.lane_ii.lab.cli run <scenario.json>
python -m src.lane_ii.lab.cli replay <run-manifest.json>
python -m src.lane_ii.lab.cli doctor
```

`doctor` reports the authority domain, Anvil availability/version, loopback
binding capability, project-relative artifact root, supported backends,
no-secret state, and whether real-Anvil smoke commissioning is available.
