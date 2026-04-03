from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator


class AnalysisSchemaError(RuntimeError):
    pass


def load_analysis_schema(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if "schema" not in payload:
        raise AnalysisSchemaError("Analysis schema file must contain a top-level 'schema' object.")
    return payload


def validate_analysis_payload(payload: dict[str, Any], schema_document: dict[str, Any]) -> None:
    validator = Draft202012Validator(schema_document["schema"])
    errors = sorted(validator.iter_errors(payload), key=lambda item: list(item.path))
    if errors:
        first = errors[0]
        path = ".".join(str(part) for part in first.path)
        raise AnalysisSchemaError(f"Analysis payload failed schema validation at {path or '<root>'}: {first.message}")


def build_response_format(schema_document: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "json_schema",
        "name": schema_document.get("name", "trader_swarm_analysis"),
        "strict": bool(schema_document.get("strict", True)),
        "schema": schema_document["schema"],
    }
