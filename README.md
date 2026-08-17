# Beelzebub

Beelzebub is a paper-only, short-horizon scientific copy-trading research
system. Public wallet activity is treated as sensor evidence, never as trading
authority. It studies versioned features, hypotheses, experiments, indicators,
and models before emitting only shadow/simulation decisions.

## Safety boundary

- No private keys, signer secrets, exchange write routes, transfers, or
  mainnet-capital authority exist in the supported runtime.
- Raw wallet activity cannot directly create a position. It must pass the
  scientific decision gate with validated evidence, positive net edge,
  independent risk sizing, and a maximum 600-second holding limit.
- The former ETH/Coinbase bot and its live-order dependency were permanently
  removed in Phase D.6.

## Windows setup

```powershell
cd E:\Beelzebub
.\.venv\Scripts\python.exe -m pip install -r requirements.lock
.\.venv\Scripts\python.exe main.py copy-storage-status
.\.venv\Scripts\python.exe main.py copy-control-center --with-watcher
```

Active state is stored on `E:\Beelzebub\runtime\hot`; archival state is on
`D:\BeelzebubData`. The Control Center is local and read-only with respect to
external venues.

## Documentation

- [Copy-trading architecture](docs/COPYTRADING.md)
- [Scientific Alpha Engine](docs/SCIENTIFIC_ALPHA_ENGINE.md)
- [Windows storage topology](docs/STORAGE_TOPOLOGY_WINDOWS.md)
- [Control Center](docs/COPYTRADING_CONTROL_CENTER.md)

Phase D.6 adds the durable automated-science worker and is documented in
`docs/PHASE_D6_AUTOMATED_SCIENCE.md`.
