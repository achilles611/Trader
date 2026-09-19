# Project agent instructions

## Caelum administrator specialist

A reusable project specialist named `beelzebub_admin` is defined in `.codex/agents/beelzebub_admin.toml`. When Joseph requests Caelum, the administrator, or administrative oversight alongside other specialists, spawn it as one concurrent specialist if supported. Give it the current objective, authorized scope, assigned writable paths, acceptance criteria and locations of current evidence. Read its charter at `docs/administration/caelum/CHARTER.md` and concurrency protocol at `docs/administration/caelum/CONCURRENCY.md`.

Keep the parent as the final integrator and assign one runtime operator. Route administrative findings and correction requests to the relevant owners; resolve a concrete HOLD before the affected integration or launch. The role does not authorize live trading or override platform permissions. If the named role cannot be selected, explicitly use a generic subagent instructed to read those same files and report the fallback.
