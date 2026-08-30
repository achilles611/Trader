# Preimplementation audit

- Starting commit: `f70c62af1e4f4eadc86d4eb3e8d99c2e33aa431c`.
- F5 worktree: `C:\Users\atlas\Documents\Trader-f5`; starting tree was clean.
- Dependency contracts: `requirements.txt`, `requirements.lock`, `requirements-build.txt`, UI `package.json`, and UI `package-lock.json`.
- Mutable CI references at start: `actions/checkout@v6`, `actions/setup-python@v6`, and `actions/setup-node@v6`.
- Historical F4 executable: `C:\Users\atlas\AppData\Local\Temp\Trader-f4-foundry-v1.8.1\bin\anvil.exe`.
- Protected surface: all `src/lane_ii/lab/**`, F4 tests, F4 commissioning documents, and F4.1.1 commit-delta files.

The design selects strict committed registry records, static analysis, canonical reporting, an `N:`-preferred durable archive/toolchain cache, and no runtime imports from governance.
