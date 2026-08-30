"""Safe YAML parsing with duplicate-key and alias rejection."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from .errors import GovernanceError


class StrictSafeLoader(yaml.SafeLoader):
    pass


def _mapping(loader: StrictSafeLoader, node: yaml.MappingNode, deep: bool = False) -> dict[Any, Any]:
    pairs: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in pairs:
            raise GovernanceError("DUPLICATE_YAML_KEY", str(key))
        pairs[key] = loader.construct_object(value_node, deep=deep)
    return pairs


StrictSafeLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _mapping)


def load_yaml(path: Path) -> Any:
    raw = path.read_text(encoding="utf-8")
    try:
        for event in yaml.parse(raw, Loader=StrictSafeLoader):
            if isinstance(event, yaml.events.AliasEvent) or getattr(event, "anchor", None):
                raise GovernanceError("REGISTRY_SCHEMA_INVALID", "YAML anchors and aliases are forbidden")
        return yaml.load(raw, Loader=StrictSafeLoader)
    except GovernanceError:
        raise
    except yaml.YAMLError as exc:
        raise GovernanceError("REGISTRY_SCHEMA_INVALID", path.as_posix()) from exc
