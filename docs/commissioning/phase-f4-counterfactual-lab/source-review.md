# f4 source review — xGodMode clean-room replacement

## Reviewed material

The conceptual review covered the public `xGodMode/godmode` ecosystem,
including its GodMode/Ganache approach to forked local state, account
impersonation, contract-code replacement, storage replacement, and
protocol-specific presets.  The historical project described a modified
Ganache CLI, Truffle workflows, and recipes for Maker, Compound, and Uniswap
V2.  It was used as design provenance only; no xGodMode source, package, or
contract artifact is imported into this repository.

Useful primary/background references reviewed:

- [xGodMode / godmode](https://github.com/xGodMode/godmode)
- [xGodMode / contract-library](https://github.com/xGodMode/contract-library)
- [Foundry Anvil documentation](https://www.getfoundry.sh/anvil/index.html)
- [Foundry installation and binary-provenance guidance](https://www.getfoundry.sh/getting-started/installation)

## Concepts adopted

- Disposable local EVM state may model balances, code, storage, ownership,
  time, and account behavior that a production process must never possess.
- A local fork can provide deterministic, pinned source material for adverse
  integration experiments.
- Named, versioned recipes make repeatable hostile conditions easier to
  review than ad-hoc test bodies.
- Counterfactual branching is useful for engineering comparison, not an
  empirical edge claim.

## Concepts rejected

- `@xgm/godmode`, `godmode-ganache-cli`, `godmode-ganache-core`, Truffle,
  Node 12, and Web3 1.x are not dependencies.
- No arbitrary protocol plugin, arbitrary RPC method, imported Python module,
  shell command, URL, callback, or opaque object may appear in a scenario.
- The historical Maker/Compound/Uniswap V2 artifacts are not revived merely
  for feature parity.  A future protocol recipe must name and test its storage
  layout explicitly.
- An Internet-exposed local mutation endpoint is prohibited.  `f4` binds only
  `127.0.0.1` on an ephemeral port and terminates every node after one run.

## Critical cleanup correction

The GodMode pattern effectively permitted a `read → replace → execute →
restore` sequence without a structurally guaranteed restoration path after an
experiment error.  `f4` instead uses:

```text
snapshot
try:
    mutate
    execute experiment
    capture bounded evidence
finally:
    revert snapshot
    verify restoration
```

Revert failure kills and quarantines the whole Anvil process.  The process is
not reusable, and no address-level lock is treated as adequate isolation.

## Clean-room statement

`src/lane_ii/lab/` is new Python code designed for this repository's frozen
Lane II authority boundary.  It is a clean-room reimplementation of useful
ideas, not a port, wrapper, or runtime dependency on GodMode or Ganache.
