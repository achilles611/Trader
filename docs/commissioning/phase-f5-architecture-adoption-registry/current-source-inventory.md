# Current source inventory

The generated `governance/generated/source-ownership.json` is the authoritative inventory. It assigns the exact checkout to 14 logical components: copy-trading, Phase E, Lane II F0/F1/F2/F4, Lane II package contract, UI, CI, project contracts, scripts, launchers, tests, and F5 governance. F4 laboratory files are marked frozen. Generated reports, documentation, binary assets, caches, runtime data, and build output are explicitly excluded.

Static component edges are in `component-graph.json`; full per-file imports are in `dependency-graph.json`. The check requires 100 percent ownership, zero gaps, zero overlaps, zero unresolved edges, and no unreviewed cross-component cycle.
