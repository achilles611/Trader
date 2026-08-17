# Windows storage topology

Beelzebub uses an explicit hot/cold boundary:

| Role | Location | Contains |
| --- | --- | --- |
| Hot | `E:\Beelzebub\runtime\hot` | active SQLite/WAL, reconciliation, runtime state, current features/models, bounded spool |
| Cold | `D:\BeelzebubData` | archives, logs, exports, Obsidian, historical source cache, experiments, graveyard, snapshots, backups |
| Legacy | `C:\Users\atlas\Documents\Trader` | retained source copy until post-migration review |

Set these environment values for the interactive account or the service wrapper before starting the application:

```powershell
$env:BEELZEBUB_HOME = 'E:\Beelzebub'
$env:BEELZEBUB_HOT_ROOT = 'E:\Beelzebub\runtime\hot'
$env:BEELZEBUB_COLD_ROOT = 'D:\BeelzebubData'
```

The code does not hard-code these paths. Without overrides it uses relative `runtime/hot` and `runtime/cold` roots so Linux CI and temporary-directory tests remain portable.

`copytrade.sqlite3` and its WAL must remain hot. D: is never opened by the observation → feature → confidence → decision → risk/reconciliation path. The cold archive worker only copies completed local spool files and writes a checksum/record-count manifest. Missing D: reports `DEGRADED_ARCHIVAL`; the active engine and all safety gates continue unchanged while the bounded E: spool accumulates archival work. Spool byte and age limits prevent unbounded E: growth.

To move a legacy database safely:

```powershell
cd E:\Beelzebub
.\.venv\Scripts\python.exe main.py copy-storage-migrate `
  --source C:\Users\atlas\Documents\Trader\artifacts\copytrade.sqlite3
```

The command uses SQLite backup snapshots, verifies integrity, refuses to overwrite a newer destination, records checksums/provenance, and never deletes the source. Archive flushing is deliberately separate:

```powershell
.\.venv\Scripts\python.exe main.py copy-archive-flush
```

Run that from a scheduled archive/report worker, not the trading process’s decision loop.
