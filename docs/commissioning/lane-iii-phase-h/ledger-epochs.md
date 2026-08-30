# Ledger epochs

L3H writes a new canonical event ledger; it never migrates, truncates, rewrites,
or re-labels the historical L3G paper ledger. Each L3H commissioning epoch is
bound in the signed capability and chain events use optimistic stream versions
plus a previous-record hash. A new epoch is required after a completed canary.
