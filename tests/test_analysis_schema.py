from __future__ import annotations

import unittest
from pathlib import Path

from src.analysis.schema import load_analysis_schema, validate_analysis_payload


class AnalysisSchemaTests(unittest.TestCase):
    def test_valid_payload_passes(self) -> None:
        root = Path(__file__).resolve().parents[1]
        schema_document = load_analysis_schema(root / "config" / "analysis_schema.json")
        payload = {
            "cycle_verdict": "hold",
            "summary": "No action.",
            "global_findings": ["Signals were mixed."],
            "bot_findings": [
                {
                    "bot_id": "tr1",
                    "diagnosis": ["Flat performance."],
                    "recommended_parameter_changes": {},
                    "confidence": 0.5,
                }
            ],
            "cross_bot_patterns": ["Low conviction across swarm."],
            "risk_flags": [],
            "next_experiments": [
                {
                    "priority": 1,
                    "scope": "config",
                    "description": "Keep parameters stable for another cycle.",
                }
            ],
            "patch_requests": [],
        }
        validate_analysis_payload(payload, schema_document)


if __name__ == "__main__":
    unittest.main()
