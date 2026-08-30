# CODEX HANDOFF — Lane II `f4.1.1`
## Semantic Restoration Fingerprint, Real-Anvil Regression, and Final Freeze

**Project:** Beelzebub  
**Lane:** Lane II  
**Phase:** `f4.1.1`  
**Disposition at entry:** `PARTIALLY_READY`  
**Purpose:** Correct one real-Anvil commissioning defect without reopening `f4`

---

# 0. Current facts

The previous real-Anvil commissioning pass was valid and must be preserved as evidence.

Verified:

- Genuine Anvil `v1.8.1`
- Official Foundry release provenance
- SHA-256 validation
- Two separate clean Anvil processes
- Equivalent normalized scenario results across both runs
- Both runs failed closed with:

```text
REVERT_FAILED_PROCESS_QUARANTINED
```

- `evm_revert` restored the externally observed mutation state
- The full raw `anvil_dumpState` fingerprint changed after revert
- Evidence integrity passed
- Secret exclusion passed
- Child termination passed
- Temporary backend cleanup passed
- Loopback-port release passed
- 392 Python tests discovered
- Real-Anvil F4 smoke executed
- F4 targeted suite: `10/10`
- UI build passed
- UI tests: `15/15`
- `pip check` passed
- npm audit passed
- Python compilation passed
- Secret scan passed
- Frozen-path diff passed
- Import-boundary audit passed
- Network-write audit passed
- Order-submission audit passed
- No tracked source diff
- No Anvil process remains
- Temporary Foundry download was removed

Starting implementation commit remains:

```text
868693b2b060abd2c476bb575314dec2105a816a
```

This is not a failed architecture. It is a commissioning-discovered defect in the restoration-verification oracle.

---

# 1. Worktree and branch

Use only:

```text
C:\Users\atlas\Documents\Trader-f4
```

Do not operate in:

```text
C:\Users\atlas\Documents\Trader
```

That checkout contains unrelated Lane III work and must remain untouched.

Required starting commit:

```text
868693b2b060abd2c476bb575314dec2105a816a
```

Create:

```text
codex/phase-f4-anvil-semantic-restoration
```

Before changes, report:

- Current branch
- Current HEAD
- Worktree status
- Relationship to `868693b2b060abd2c476bb575314dec2105a816a`
- Existing commissioning evidence paths
- Frozen-path diff status

Do not delete or overwrite the two prior quarantined run manifests.

---

# 2. Mission

Replace the incorrect assumption:

> Raw `anvil_dumpState` bytes must be identical before snapshot and after revert.

with the stricter and more accurate rule:

> Revert is accepted only when a versioned canonical execution-state commitment and every declared mutation witness are identical before and after restoration; raw provider serialization remains preserved as diagnostic evidence.

Then:

1. Add focused unit tests.
2. Add a real-Anvil coordinator regression.
3. Add negative real-Anvil drift tests.
4. Repeat the commissioning scenario from two clean processes.
5. Rerun every protected regression and audit.
6. Freeze `f4` only if every gate passes.

---

# 3. Important source-level correction

Do not begin by blindly deleting fields named “snapshot,” “history,” or “metadata.”

For pinned Anvil `v1.8.1`, first structurally decode the actual `anvil_dumpState` payload and produce a path-level pre/post diff.

The pinned Foundry `SerializableState` shape includes, depending on build/network:

```text
block
accounts
best_block_number
blocks
transactions
historical_states
network-specific optional fields
```

The Anvil snapshot registry is separately exposed through Anvil metadata, not necessarily as a top-level `anvil_dumpState` field.

Therefore:

- Identify the exact changed paths in the two retained commissioning artifacts.
- Classify each path.
- Do not assume every difference is harmless.
- Do not create an open-ended recursive ignore rule.
- Unknown or unclassified changed paths remain fail-closed.

---

# 4. Two fingerprints, two purposes

Implement two distinct hashes.

## 4.1 Raw provider fingerprint

```text
raw_provider_dump_sha256
```

Definition:

```text
SHA-256(exact raw bytes returned by anvil_dumpState)
```

Purpose:

- Tamper-evident evidence
- Provider-version diagnostics
- Reproduction of serialization differences
- Upstream bug analysis

It is retained before mutation and after revert.

A raw mismatch is recorded but is not, by itself, proof of execution-state drift.

Do not remove this fingerprint.

## 4.2 Semantic execution-state fingerprint

Introduce:

```text
ANVIL_EXECUTION_STATE_V1
```

and:

```text
semantic_state_sha256
```

Definition:

```text
SHA-256(
    canonical_json(
        versioned execution-state projection
    )
)
```

This becomes one required component of the restoration verdict.

The projection must be:

- Versioned
- Strictly schema-validated
- Deterministically ordered
- Hex-normalized
- Explicit about included and excluded fields
- Bound to the exact Anvil version family
- Rejected when an unknown schema or field is encountered

---

# 5. `ANVIL_EXECUTION_STATE_V1` projection

The semantic projection should contain the following categories.

## 5.1 Provider and chain identity

Include:

```text
fingerprint_schema
backend_type
client_version
client_commit_if_available
chain_id
network_profile_if_available
forked = false
```

Commissioning must fail if:

- The backend is not genuine Anvil
- The Anvil version is not the approved pinned version
- A fork is active
- A remote upstream is configured
- Chain identity changes during the run

Do not include volatile process identity, port, PID, temporary path, or Anvil instance UUID in the semantic equality hash.

Keep those in evidence.

## 5.2 Canonical current account state

From the decoded dump, include the complete canonical account map available to the local Anvil process.

For each address:

```text
address
nonce
balance
code
storage
```

Canonicalization:

- Lowercase addresses
- Fixed-width or documented canonical hex
- No leading-sign ambiguity
- Numeric values normalized to one representation
- Code normalized to lowercase `0x` hex
- Storage slots sorted lexicographically
- Storage values normalized
- Accounts sorted lexicographically
- Empty versus absent account is not normalized away unless proven equivalent and explicitly specified
- Zero storage versus absent storage is not normalized away unless proven equivalent and explicitly specified

The complete account-state projection is the primary guard against real balance, nonce, code, or storage drift.

## 5.3 Current canonical chain head

Include exact observable head identity:

```text
latest_block_number
latest_block_hash
latest_parent_hash
latest_state_root
latest_timestamp
latest_gas_limit
latest_base_fee_if_supported
latest_beneficiary
latest_prevrandao_if_supported
latest_blob_fields_if_supported
```

Use exact public RPC results from the pinned local Anvil process.

If a field is unsupported, encode an explicit unsupported marker rather than silently omitting it.

For scenarios that do not mine, these fields should return to baseline.

For scenarios that mine or advance time, revert must restore the baseline head and environment as declared by the scenario contract.

## 5.4 Current execution environment

Where the pinned Anvil RPC exposes the values, include:

```text
automine
interval_mining
block_timestamp_interval
current_call_timestamp_or_equivalent
gas_price_policy
chain_id
coinbase
```

Only include values that can be read deterministically and verified before and after.

Do not invent a value when no getter exists.

For a write-only capability, use an explicit mutation witness and cleanup proof instead.

## 5.5 Transaction pool

Include:

```text
pending_count
queued_count
canonical_txpool_projection
```

The laboratory should normally require an empty transaction pool before and after a scenario.

Any residual pending or queued transaction is state drift and must quarantine the process.

## 5.6 Impersonation witness

For each address impersonated by the scenario:

- Record that the capability was started.
- Require a successful stop call in `finally`.
- Compare the sorted account list or another pinned readable witness where valid.
- Require no unexpected additional account introduced through impersonation.
- Disallow auto-impersonation in this commissioning pass unless its state can be read and verified.

If impersonation state cannot be proved restored, quarantine.

---

# 6. Fields not automatically included in semantic equality

These fields may be provider serialization or retained-history material rather than current execution state:

```text
historical_states
noncanonical stored block history
noncanonical stored transaction history
snapshot registry IDs
snapshot registry bookkeeping
process instance ID
RPC request IDs
temporary paths
timestamps of the Beelzebub run itself
PIDs
ports
```

However, none may be excluded merely because its name looks internal.

A field may be excluded from `ANVIL_EXECUTION_STATE_V1` only when all are true:

1. Its exact JSON path is documented.
2. Its behavior was observed in genuine Anvil `v1.8.1`.
3. Current canonical account state is unchanged.
4. Current canonical head is unchanged.
5. The transaction pool is unchanged and empty.
6. Every mutation witness is restored.
7. The field cannot influence the next scenario under the current fresh-process-per-run policy.
8. Its exclusion has a focused test.
9. It remains present in raw evidence.
10. An unknown new field does not inherit the exclusion.

Do not exclude the entire `blocks`, `transactions`, or `historical_states` subtree until the actual retained artifacts have been diffed and the classification is justified.

---

# 7. Mutation-surface witnesses

The semantic fingerprint is necessary but not sufficient.

Every allowlisted mutation must declare exact precondition, mutated-state, and restored-state probes.

Create a registry approximately equivalent to:

```text
MutationWitnessSpec
    mutation_verb
    target_identity
    read_before
    expected_mutated
    read_after_revert
    equality_rule
    missingness_rule
```

## 7.1 Native balance

Probe:

```text
eth_getBalance(target, latest)
```

Require:

```text
before != mutated
after_revert == before
```

## 7.2 Nonce

Probe:

```text
eth_getTransactionCount(target, latest)
```

Require exact restoration.

## 7.3 Contract code

Probe:

```text
eth_getCode(target, latest)
```

Require byte-for-byte restoration.

## 7.4 Storage slot

Probe:

```text
eth_getStorageAt(target, slot, latest)
```

Require exact 32-byte restoration for every touched slot.

## 7.5 Time

Probe all readable time/head fields used by the scenario.

Require exact restoration according to the scenario’s declared baseline.

## 7.6 Block advancement

Probe:

```text
eth_blockNumber
eth_getBlockByNumber(latest, false)
```

Require exact canonical-head restoration.

## 7.7 Impersonation

Require successful stop plus the best available readable state witness.

No implicit cleanup.

## 7.8 Loaded state

`anvil_loadState` must not be used as a substitute for failed snapshot restoration in this pass.

It appends/overwrites state and is not an acceptable hidden fallback for proving `evm_revert`.

## 7.9 Unknown mutation

A mutation with no witness specification is rejected before execution.

---

# 8. Structural diff classifier

Implement a strict diff artifact:

```text
raw_dump_structural_diff.json
```

Each entry:

```text
json_pointer
before_type
after_type
before_hash_or_bounded_value
after_hash_or_bounded_value
classification
classification_rule_id
included_in_semantic_fingerprint
reason
```

Allowed classifications:

```text
EXECUTION_STATE
CANONICAL_CHAIN_STATE
TRANSACTION_POOL_STATE
DECLARED_MUTATION_WITNESS
PROVIDER_SERIALIZATION
PROVIDER_RETAINED_HISTORY
SNAPSHOT_LIFECYCLE_METADATA
VOLATILE_PROCESS_EVIDENCE
UNKNOWN
```

Rules:

- `EXECUTION_STATE`, `CANONICAL_CHAIN_STATE`, `TRANSACTION_POOL_STATE`, or `DECLARED_MUTATION_WITNESS` mismatch means quarantine.
- `UNKNOWN` means quarantine.
- Provider-only differences may be tolerated only under a pinned, exact-path classification rule.
- Never classify by substring alone.
- Never ignore all future children of a JSON subtree without schema validation.
- Record the raw hashes even when a difference is tolerated.

---

# 9. Restoration algorithm

Implement the coordinator lifecycle approximately as:

```python
raw_before = backend.dump_state_raw()
decoded_before = decode_and_validate_anvil_dump(raw_before)

semantic_before = build_execution_state_v1(
    decoded_dump=decoded_before,
    rpc_observations=collect_global_restoration_observations(),
)

snapshot_id = backend.snapshot()
witnesses_before = collect_mutation_witnesses(scenario)

try:
    apply_mutations()
    witnesses_mutated = collect_mutation_witnesses(scenario)
    assert_declared_mutations_observed()
    execute_experiment()
    capture_experiment_evidence()
finally:
    revert_rpc_result = backend.revert(snapshot_id)

    raw_after = backend.dump_state_raw()
    decoded_after = decode_and_validate_anvil_dump(raw_after)

    semantic_after = build_execution_state_v1(
        decoded_dump=decoded_after,
        rpc_observations=collect_global_restoration_observations(),
    )

    witnesses_after = collect_mutation_witnesses(scenario)
    structural_diff = classify_dump_diff(decoded_before, decoded_after)

    restoration = evaluate_restoration(
        revert_rpc_result,
        semantic_before,
        semantic_after,
        witnesses_before,
        witnesses_after,
        structural_diff,
    )

    persist_all_evidence()

    if not restoration.accepted:
        quarantine_and_kill_process()
```

Restoration succeeds only when:

```text
evm_revert returned true
AND semantic_before == semantic_after
AND every mutation witness restored
AND txpool restored/empty
AND no unknown structural difference exists
AND all provider-only differences match pinned classifications
```

A raw dump mismatch is permitted only when all the above pass.

---

# 10. Result taxonomy

Do not overload every failure as a generic revert failure.

Add bounded internal reason codes such as:

```text
REVERT_RPC_REJECTED_PROCESS_QUARANTINED
SEMANTIC_STATE_DRIFT_PROCESS_QUARANTINED
MUTATION_WITNESS_DRIFT_PROCESS_QUARANTINED
TXPOOL_DRIFT_PROCESS_QUARANTINED
UNKNOWN_DUMP_DIFFERENCE_PROCESS_QUARANTINED
UNSUPPORTED_ANVIL_DUMP_SCHEMA_PROCESS_QUARANTINED
RESTORATION_OBSERVATION_FAILED_PROCESS_QUARANTINED
RESTORED_SEMANTICALLY_RAW_DUMP_DIFFERED
RESTORED_EXACTLY
```

Preserve external compatibility if an existing public status contract requires:

```text
REVERT_FAILED_PROCESS_QUARANTINED
```

In that case, add the bounded reason as a separate field rather than silently changing a frozen public enum.

---

# 11. Evidence schema additions

Persist:

```text
raw_dump_before_sha256
raw_dump_after_sha256
raw_dump_equal
semantic_fingerprint_schema
semantic_before_sha256
semantic_after_sha256
semantic_equal
mutation_witness_manifest
mutation_witness_before
mutation_witness_mutated
mutation_witness_after
mutation_witnesses_restored
canonical_head_before
canonical_head_after
txpool_before
txpool_after
structural_diff_artifact
structural_diff_hash
classified_difference_count
unknown_difference_count
revert_rpc_result
restoration_verdict
restoration_reason_code
anvil_version
anvil_release_commit
backend_process_identity
process_terminated
port_released
```

Do not persist secrets.

Keep large raw dumps bounded and governed by existing artifact policy.

---

# 12. Required focused unit tests

At minimum:

1. Canonical JSON ordering does not affect semantic hash.
2. Address ordering does not affect semantic hash.
3. Storage-key ordering does not affect semantic hash.
4. Hex case and approved numeric representation normalize deterministically.
5. Balance change changes semantic hash.
6. Nonce change changes semantic hash.
7. Code change changes semantic hash.
8. Storage change changes semantic hash.
9. Account addition changes semantic hash.
10. Account removal changes semantic hash.
11. Canonical-head change changes semantic hash.
12. Block timestamp change changes semantic hash.
13. Txpool residue changes semantic hash or fails restoration.
14. Exact pinned provider-only metadata difference can be classified.
15. Unknown difference fails closed.
16. Unsupported dump schema fails closed.
17. Malformed dump fails closed.
18. Missing required account field fails closed.
19. Missing required RPC observation fails closed.
20. Raw mismatch plus semantic/witness equality can pass with the explicit restored-semantically verdict.
21. Raw equality plus mutation-witness mismatch still fails.
22. Semantic equality plus unknown structural difference still fails.
23. Classification rules are exact-path and version-bound.
24. A new Anvil version cannot silently reuse the old projection.
25. Secret redaction remains intact.

---

# 13. Required real-Anvil coordinator tests

Use genuine Anvil `v1.8.1`, isolated process, loopback-only, no fork.

## 13.1 Positive balance restoration

```text
capture baseline
snapshot
set balance
prove balance changed
revert
prove balance restored
prove semantic fingerprint restored
record raw dump behavior
accept only under classified differences
terminate process
prove port release
```

## 13.2 Positive code restoration

Set nonempty local code at a laboratory address, prove change, revert, prove byte-for-byte restoration, and pass semantic verification.

## 13.3 Positive storage restoration

Set a declared slot, prove change, revert, prove exact restoration, and pass semantic verification.

## 13.4 Positive time/block restoration

Exercise the approved time or block mutation recipe, revert, and prove current canonical head/environment returns to baseline.

## 13.5 Negative residual balance drift

After nominal revert, deliberately introduce a second balance drift inside the test harness before restoration verification.

Expected:

```text
SEMANTIC_STATE_DRIFT_PROCESS_QUARANTINED
```

## 13.6 Negative residual code drift

Expected quarantine.

## 13.7 Negative residual storage drift

Expected quarantine.

## 13.8 Negative unknown dump-field drift

Inject or fixture an unclassified structural difference.

Expected:

```text
UNKNOWN_DUMP_DIFFERENCE_PROCESS_QUARANTINED
```

## 13.9 Revert false/error

Expected:

```text
REVERT_RPC_REJECTED_PROCESS_QUARANTINED
```

Do not permit semantic equality to override a false or failed `evm_revert`.

---

# 14. Recommissioning procedure

After focused tests pass:

1. Reacquire genuine official Anvil `v1.8.1`.
2. Verify the approved SHA-256.
3. Record executable path and release provenance.
4. Run the approved commissioning scenario from a clean process.
5. Persist all new evidence.
6. Terminate process and prove port release.
7. Repeat from a second clean process.
8. Compare normalized outcomes.
9. Compare semantic restoration artifacts.
10. Verify both runs pass.
11. Remove temporary Foundry download if that remains the approved practice.
12. Confirm no Anvil process remains.

Required two-run equality:

```text
same scenario hash
same semantic fingerprint schema
same baseline semantic fingerprint
same mutated witness semantics
same restored semantic fingerprint
same restoration verdict
same normalized outcome
same classified-difference rule set
zero unknown differences
```

Volatile process fields may differ and remain outside normalized equality.

---

# 15. Protected regressions and audits

Rerun:

- F4 targeted suite with genuine-Anvil tests enabled
- Zero F4 skips
- Frozen F.0–F.3 regressions
- Phase D regressions
- Phase E regressions
- Full Python test discovery
- UI build
- UI tests
- Python compilation
- `pip check`
- npm audit
- Secret scan
- Frozen-path diff
- Import-boundary audit
- Network-write audit
- Order-submission audit
- Artifact-integrity validation

Required unchanged facts:

```text
production/testnet network writes = 0
testnet orders = 0
mainnet orders = 0
credentials = none
remote RPC = none
fork = false
```

---

# 16. Allowed changes

Allowed:

- A new strict semantic-fingerprint implementation inside the F4 laboratory boundary
- A strict dump decoder/canonicalizer
- A structural-diff classifier
- Mutation-witness extensions
- Focused tests
- Evidence-schema additions
- Commissioning documentation

Not allowed:

- Broad F4 redesign
- Weakening quarantine behavior
- Removing raw dump evidence
- Treating `evm_revert == true` as sufficient
- A generic recursive metadata stripper
- Ignoring all history fields without proof
- `anvil_loadState` fallback
- Process reuse as part of this correction
- F.0–F.3 implementation changes
- Phase D changes
- Phase E changes
- Trader V0 changes
- Lane III changes
- Hyperliquid funding/access work
- Any live or testnet order
- Any remote chain contact

---

# 17. Frozen-path protection

Show zero unintended diff for at least:

```text
src/lane_ii/boundary.py
src/lane_ii/trader_v0.py
src/copytrade/hyperliquid_testnet.py
src/phase_d/**
src/phase_e/**
docs/commissioning/phase-f0-lane-ii-boundary/**
docs/commissioning/phase-f1-trader-v0/**
docs/commissioning/phase-e5-prospective-experiment/**
docs/commissioning/phase-e6-prospective-acquisition/**
src/lane_iii/**
```

Only F4-local files, focused tests, and F4 commissioning records should change unless an existing additive shared contract is demonstrably required.

---

# 18. Final freeze gates

Declare:

```text
READY_FROZEN
```

only when all are true:

1. Exact source diff from the two prior dumps was classified.
2. No broad or heuristic ignore rule was introduced.
3. `ANVIL_EXECUTION_STATE_V1` is documented and version-bound.
4. Raw dump hashes remain persisted.
5. Semantic state hashes restore exactly.
6. Every declared mutation witness restores exactly.
7. Current canonical head restores exactly.
8. Txpool is restored and empty.
9. No unknown structural difference remains.
10. Genuine Anvil v1.8.1 positive tests pass.
11. Genuine Anvil negative drift tests quarantine correctly.
12. Two clean commissioning runs pass.
13. Normalized outcomes match.
14. Evidence validates.
15. Child processes terminate.
16. Ports release.
17. F4 suite has zero skips.
18. Protected regressions pass.
19. Compilation and dependency checks pass.
20. UI build/tests pass.
21. Secret scan passes.
22. Frozen-path diff is zero.
23. Import-boundary audit passes.
24. Network-write audit reports zero remote writes.
25. Testnet orders remain zero.
26. Mainnet orders remain zero.
27. Final worktree is clean.
28. Branch is pushed.
29. Closure audit records the defect and correction.
30. No downstream F5 work was mixed into the branch.

If any gate fails:

```text
PARTIALLY_READY
```

and retain fail-closed quarantine behavior.

---

# 19. Closure language

The closure audit should state approximately:

> Genuine Anvil v1.8.1 commissioning revealed that raw `anvil_dumpState`
> byte equality was not a valid standalone restoration oracle. F4 retained
> the raw dump hashes as tamper-evident provider evidence and introduced a
> strict, versioned execution-state fingerprint plus mutation-specific
> restoration witnesses. Unknown differences still quarantine the process.
> Genuine-Anvil positive restoration and negative residual-drift tests passed,
> two clean commissioning runs matched, and no authority, network, secret,
> frozen-path, or order-submission boundary changed.

Do not describe the correction as “ignoring the dump mismatch.”

Describe it as:

```text
separating provider serialization identity from verified execution-state identity
```

---

# 20. Final report format

Return:

```text
STATUS:
PHASE:
BRANCH:
STARTING COMMIT:
FINAL COMMIT:
ANVIL VERSION:
ANVIL RELEASE COMMIT:
ANVIL SHA-256:
ANVIL PATH/PROVENANCE:

PRIOR RAW-DUMP DIFFERENCE:
CHANGED JSON PATHS:
CLASSIFIED DIFFERENCES:
UNKNOWN DIFFERENCES:

SEMANTIC FINGERPRINT SCHEMA:
SEMANTIC BASELINE HASH:
SEMANTIC RESTORED HASH:
SEMANTIC EQUALITY:

MUTATION WITNESSES:
MUTATION WITNESS RESTORATION:

REAL-ANVIL POSITIVE TESTS:
REAL-ANVIL NEGATIVE TESTS:
F4 TARGETED TESTS:
FULL PYTHON TESTS:
UI BUILD:
UI TESTS:
PIP CHECK:
NPM AUDIT:
COMPILATION:

EVIDENCE LOCATIONS:
EVIDENCE HASHES:
TWO-RUN DETERMINISM:
PROCESS SHUTDOWN:
PORT RELEASE:

FROZEN-PATH DIFF:
IMPORT-BOUNDARY AUDIT:
SECRET SCAN:
REMOTE NETWORK WRITES:
TESTNET ORDERS:
MAINNET ORDERS:

WORKTREE STATUS:
PUSH STATUS:
REMAINING BLOCKERS:
NEXT ALLOWED PHASE:
```

If every gate passes:

```text
NEXT ALLOWED PHASE: f5.0 architecture and third-party adoption registry
```

Otherwise:

```text
NEXT ALLOWED PHASE: none; remain in f4.1.1
```
