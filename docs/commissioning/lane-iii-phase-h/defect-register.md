# L3H defect register

| ID | Severity | State | Evidence | Resolution |
| --- | --- | --- | --- | --- |
| L3H-CLASS-002 | P1 | fixed | `Sim101 Evaluation` could be classified as evaluation from its suffix, discarding simulation evidence. | `classify_account` treats concurrent Sim101/evaluation signals as `UNKNOWN`; regression test added. |
| L3H-INSTALL-001 | P1 gate | blocked external | No installed L3H source, compiled DLL, runtime hello, or port 48137 listener was observed. | Run visible NT8 deployment/compile and Sim101 proof; software cannot fabricate it. |
| L3H-PROTECT-001 | P1 gate | blocked external | Native stop lifecycle source exists but no installed partial/disconnect/stop-reject proof exists. | Complete the installed Sim101 protection matrix before activation can enable. |
