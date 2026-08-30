# F5 architecture map

This static map distinguishes dependency flow from authority and evidence policy; it grants no authority.

```mermaid
flowchart LR
  copytrade --> project_contracts
  f4_lab --> lane_ii_core
  governance --> project_contracts
  lane_ii_core --> project_contracts
  lane_ii_f1 --> lane_ii_f0
  lane_ii_f2 --> copytrade
  lane_ii_f2 --> lane_ii_f0
  lane_ii_f2 --> lane_ii_f1
  phase_e --> project_contracts
  project_tests --> copytrade
  project_tests --> f4_lab
  project_tests --> governance
  project_tests --> lane_ii_f0
  project_tests --> lane_ii_f1
  project_tests --> lane_ii_f2
  project_tests --> phase_e
```

- Dependency flow: static import edges above.
- Data/control/authority flow: constrained by each component record and frozen manifests.
- Evidence flow: immutable F4 packages and F5 commissioning evidence are external archival artifacts.
