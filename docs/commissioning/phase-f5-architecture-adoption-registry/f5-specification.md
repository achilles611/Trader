# F5.0 architecture and adoption registry

F5 is an offline governance boundary. It describes and audits existing code, authority, dependencies, storage, toolchains, and evidence; it grants none of them. Registry validation does not import trading modules, load secrets, install packages, or contact a network endpoint.

The component and adoption schemas reject unknown fields. Registry YAML uses a safe duplicate-key rejecting loader and refuses aliases. Canonical JSON is UTF-8, NFC-normalized, key-sorted, compact, and LF-terminated. Run identifiers, timestamps, paths, ports, hostnames, and process IDs are excluded from normalized evidence comparisons.

Source ownership covers each included Python, TypeScript, JavaScript, PowerShell, batch, YAML, JSON, configuration, lock, and manifest file exactly once. AST parsing builds static import graphs without executing project code. F4 protected paths are sealed against the F4.1.1 commit before every audit.

Adoptions require exact lock or artifact identity, immutable provenance, license evidence, authority and network classifications, upgrade/rollback plans, and ownership. Upgrade classes are `DOCUMENTATION_ONLY`, `PATCH_REQUALIFICATION`, `SEMANTIC_RECOMMISSIONING`, `AUTHORITY_REVIEW`, and `PHASE_UNFREEZE_REQUIRED`. Any Anvil identity, semantic schema, mutation-set, process-boundary, or fork behavior change is semantic recommissioning at minimum.

Storage policy reserves 20 percent free capacity, bounds every active writer, retains immutable commissioning evidence, and keeps commissioned toolchains. CI runs only static offline validation; it neither requires `N:`, credentials, nor a real Anvil executable.
