# Lane III paper ledger lifecycle (design only)

This hotfix does not rotate the active ledger.

```text
HOT ACTIVE EPOCH -> Full verify -> seal terminal hash + manifest -> cold archive -> new hot epoch anchored to predecessor
```

Review rotation at 40 GiB main DB or 30 days, whichever comes first, but perform it only at a no-position session boundary after Full verification. The manifest needs the ledger UUID, explicit epoch, terminal sequence/hash, Full artifact ID, predecessor hash, archive location, and SHA-256 cold-storage checksum. Recovery restores to a new path, validates the archive checksum, runs Full, and records the predecessor anchor before writes begin. Retention analysis for `OBSERVATION`, `EVIDENCE`, and `DECISION` is driven by the read-only storage profile; this design deletes nothing.
