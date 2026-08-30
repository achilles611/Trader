"""AST-only dependency graph construction; it never imports project code."""

from __future__ import annotations

import ast
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

from .canonical import canonical_hash
from .errors import GovernanceError


def _module_for_path(path: str) -> str | None:
    if not path.endswith(".py") or not path.startswith(("src/", "tests/")):
        return None
    value = path[:-3].replace("/", ".")
    return value[:-9] if value.endswith(".__init__") else value


def _relative_module(module: str, level: int, target: str | None) -> str | None:
    parts = module.split(".")[:-1]
    if level > len(parts):
        return None
    base = parts[: len(parts) - level + 1]
    if target:
        base.extend(target.split("."))
    return ".".join(base)


def _scan_python(path: Path, module: str) -> tuple[list[str], list[str]]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=path.name)
    except SyntaxError as exc:
        raise GovernanceError("REGISTRY_SCHEMA_INVALID", f"syntax:{path.name}") from exc
    imports: list[str] = []
    dynamic: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            target = _relative_module(module, node.level, node.module) if node.level else node.module
            if target:
                imports.append(target)
        elif isinstance(node, ast.Call):
            name = ""
            if isinstance(node.func, ast.Name):
                name = node.func.id
            elif isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name):
                name = f"{node.func.value.id}.{node.func.attr}"
            if name in {"__import__", "eval", "exec", "importlib.import_module"}:
                if not node.args or not isinstance(node.args[0], ast.Constant):
                    dynamic.append(name)
    return sorted(set(imports)), sorted(set(dynamic))


_TS_IMPORT = re.compile(r"(?:import|export)\s+(?:[^'\"]+?\s+from\s+)?['\"]([^'\"]+)['\"]|require\(\s*['\"]([^'\"]+)['\"]\s*\)|import\(\s*['\"]([^'\"]+)['\"]\s*\)")


def _scan_ts(path: Path) -> tuple[list[str], list[str]]:
    text = path.read_text(encoding="utf-8")
    matches = [next(item for item in match.groups() if item is not None) for match in _TS_IMPORT.finditer(text)]
    computed = ["computed-import"] if re.search(r"(?:require|import)\(\s*[^'\"\s]", text) else []
    return sorted(set(matches)), computed


def build_dependency_graph(root: Path, ownership: dict[str, Any], components: list[dict[str, Any]], *, enforce: bool = True) -> tuple[dict[str, Any], dict[str, Any]]:
    owner_by_path = {item["path"]: item["owning_component"] for item in ownership["files"]}
    module_to_owner: dict[str, str] = {}
    for path, owner in owner_by_path.items():
        module = _module_for_path(path)
        if module:
            module_to_owner[module] = owner
    edges: set[tuple[str, str, str, str]] = set()
    external: set[str] = set()
    dynamic: list[dict[str, str]] = []
    for relative, owner in sorted(owner_by_path.items()):
        full = root / relative
        if relative.endswith(".py"):
            module = _module_for_path(relative)
            if not module:
                continue
            imports, dynamic_values = _scan_python(full, module)
            for value in dynamic_values:
                dynamic.append({"path": relative, "mechanism": value})
            for imported in imports:
                resolved = next((name for name in sorted(module_to_owner, key=len, reverse=True) if imported == name or imported.startswith(name + ".")), None)
                if resolved:
                    target = module_to_owner[resolved]
                    if target != owner:
                        edges.add((owner, target, relative, imported))
                elif not imported.startswith(("src", "tests")):
                    external.add(imported.split(".", 1)[0])
                else:
                    raise GovernanceError("UNRESOLVED_INTERNAL_EDGE", imported)
        elif relative.endswith((".ts", ".tsx", ".js", ".mjs")):
            imports, dynamic_values = _scan_ts(full)
            for value in dynamic_values:
                dynamic.append({"path": relative, "mechanism": value})
            for imported in imports:
                if imported.startswith("."):
                    # Relative UI edges stay within a single registered component.
                    continue
                external.add(imported.split("/", 1)[0] if not imported.startswith("@") else "/".join(imported.split("/")[:2]))
    if dynamic and enforce:
        raise GovernanceError("DYNAMIC_IMPORT_UNDECLARED", dynamic[0]["path"])
    declared = {item["component_id"]: set(item["internal_dependencies"]) for item in components}
    if enforce:
        for source, target, _, _ in edges:
            if target not in declared[source]:
                raise GovernanceError("UNDECLARED_IMPORT_EDGE", f"{source}->{target}")
    component_edges = sorted({(source, target) for source, target, _, _ in edges})
    if enforce:
        _assert_acyclic(component_edges)
    dependency = {
        "schema": "BEELZEBUB_DEPENDENCY_GRAPH_V1",
        "internal_import_edges": [{"from": a, "to": b, "path": p, "module": m} for a, b, p, m in sorted(edges)],
        "external_import_roots": sorted(external),
        "dynamic_loading": dynamic,
    }
    dependency["canonical_sha256"] = canonical_hash(dependency)
    graph = {
        "schema": "BEELZEBUB_COMPONENT_GRAPH_V1",
        "components": sorted(item["component_id"] for item in components),
        "edges": [{"from": a, "to": b, "kind": "static-import"} for a, b in component_edges],
        "unreviewed_cross_component_cycles": [],
    }
    graph["canonical_sha256"] = canonical_hash(graph)
    return graph, dependency


def _assert_acyclic(edges: list[tuple[str, str]]) -> None:
    graph: dict[str, set[str]] = defaultdict(set)
    for source, target in edges:
        graph[source].add(target)
    active: set[str] = set()
    finished: set[str] = set()

    def visit(value: str) -> None:
        if value in active:
            raise GovernanceError("UNRESOLVED_INTERNAL_EDGE", "cross-component-cycle")
        if value in finished:
            return
        active.add(value)
        for target in graph[value]:
            visit(target)
        active.remove(value)
        finished.add(value)

    for item in sorted(graph):
        visit(item)


def architecture_map(graph: dict[str, Any]) -> str:
    rows = ["# F5 architecture map", "", "This static map distinguishes dependency flow from authority and evidence policy; it grants no authority.", "", "```mermaid", "flowchart LR"]
    rows.extend(f"  {edge['from'].replace('-', '_')} --> {edge['to'].replace('-', '_')}" for edge in graph["edges"])
    rows.extend(["```", "", "- Dependency flow: static import edges above.", "- Data/control/authority flow: constrained by each component record and frozen manifests.", "- Evidence flow: immutable F4 packages and F5 commissioning evidence are external archival artifacts.", ""])
    return "\n".join(rows)
