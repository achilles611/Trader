# Preimplementation defect register

| ID | Severity | Status | Evidence | Disposition |
| --- | --- | --- | --- | --- |
| BASELINE-TEST-DISCOVERY-001 | P1 | fixed | Seven L3G tests used package-relative imports, which failed under the mandated discovery command. | Converted to absolute `tests.l3g_helpers` imports; 102 focused tests pass. |
| BASELINE-TZDATA-001 | P1 | environment-fixed | The host `python` lacked `tzdata`, while the locked project virtual environment includes it. | Run commissioning validation with `.venv312\\Scripts\\python.exe`; CI already installs `requirements.lock`. |
| L3H-EVENT-STORE-001 | P1 | fixed | Windows test cleanup exposed retained SQLite handles. | Closed every event-store connection; L3H regression suite passes. |
| L3G-LOAD-BASELINE-001 | P2 | not-reproduced | One load assertion failed while duplicate full-suite runners overlapped. | Single isolated reproduction passed; retain in final suite. |
