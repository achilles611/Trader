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
`127.0.0.1` and a dynamic port.  The commissioned backend is exact Foundry /
Anvil `v1.8.1`, commit `982849d3140c01fd3b72905759581a132df7aa98`.
The official Windows archive SHA-256 is
`02d98fc2c573793960ee06b7f642487d483fe30572f7e248804c207334a418d8`;
the extracted `anvil.exe` SHA-256 is
`c6e29da1b010fe00bac6c0dc5c29484bd641deb5a84050aea10d13e9dc4fe26f`.
Another binary, release, commit, fork, or remote RPC fails closed.  The local
genesis timestamp is fixed at `1700000000` so independent clean processes have
the same canonical baseline.  Scenarios expose only mutations with an
independent restoration witness:

```text
set_native_balance, set_nonce, set_contract_code, set_storage_slot,
advance_timestamp, mine_block, mine_blocks
```

The adapter maps those verbs internally to Foundry's documented local RPC
surface.  Snapshot, revert, and dump are coordinator-only lifecycle
operations.  `anvil_loadState` is not a recovery path.  Impersonation is not
commissioned because pinned Anvil does not expose an independent readable
post-cleanup witness for the active impersonation set.  There is no
scenario-level `rpc(method, params)` API.  Addresses, code, storage keys and
words, integer bounds, timestamps, block counts, chain identity, and loopback
binding are validated.

## Restoration fingerprints

F4 keeps two separate commitments.

`raw_provider_dump_sha256` is SHA-256 over the exact UTF-8 text returned by
`anvil_dumpState`.  The exact before and after provider dumps are retained as
runtime evidence outside Git.  Their hashes and equality verdict remain in
the run manifest.

`ANVIL_EXECUTION_STATE_V1` is SHA-256 over canonical JSON containing:

- exact client release/commit, Ethereum execution profile, hard fork, chain
  ID, and explicit `forked = false`;
- every decoded account address, nonce, balance, bytecode, storage slot, and
  storage value without treating missing data as empty data;
- the complete supported canonical latest-header projection, including hash,
  parent hash, state root, timestamp, gas, base fee, beneficiary, prevrandao,
  and blob fields; and
- observable automine, interval-mining, gas-price, coinbase, and canonical
  time state.

The decoder requires the exact supported v1.8.1 dump, block, header, account,
and storage shapes.  Malformed data, missing required fields, new fields, a
different version, fork metadata, nonempty retained transactions, or
unreadable observations fail closed.  The transaction pool is checked
separately with `txpool_status`; both pending and queued must restore to zero.

Each mutation records an exact before, mutated, and restored witness using
balance, nonce, code, storage, or canonical-head observations.  A mutation
that does not change its witness is rejected.  A missing or non-restored
witness quarantines the process even when semantic hashes match.

## Structural classification

Every decoded raw-dump difference receives a JSON-pointer classification in
`raw_dump_structural_diff.json`.  Account, chain, transaction, witness, and
unknown differences quarantine.  No rule matches names or recursively
ignores `metadata`, `history`, or `snapshot` fields.

The only tolerated v1 rule is
`ANVIL_V1_8_1_GENESIS_BLOCK_ENV_REANCHOR_V1`.  It applies only to
`/block/timestamp` when the pre-revert value is the Anvil genesis sentinel
`0x1`, the post-revert value exactly equals the unchanged canonical genesis
header timestamp, both semantic states and canonical heads are equal, the
best block remains zero, and both provider identities are the pinned commit.
Pinned source shows that `anvil_dumpState` serializes the internal `BlockEnv`
and `evm_revert` re-anchors that environment from the canonical block header.
This is a version-bound separation of provider serialization identity from
verified execution-state identity; it is not an instruction to ignore
metadata.

## Isolation and cleanup

The coordinator takes a backend *factory*, not a backend instance.  One run
therefore receives one mutable universe.  It captures and validates the raw
dump, semantic state, canonical head, transaction pool, and declared mutation
witnesses before snapshot.  In `finally` it requires `evm_revert = true`,
semantic equality, exact witness and head restoration, an empty restored
txpool, complete structural classification, and zero unknown differences.
Failure kills and quarantines the process.  Success still terminates and
discards it, then verifies child exit and loopback-port release.

## Evidence and artifacts

Evidence records bounded hashes, verbs, state differences, assertion names,
toolchain identity, fork chain/block, cleanup state, run state, and non-secret
diagnostics.  Companion runtime artifacts contain the exact validated raw
dumps, semantic projections, structural classifications, and witness
manifest.  They never contain private keys, seed phrases, Rabby data, wallet
secrets, or auth headers.
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
