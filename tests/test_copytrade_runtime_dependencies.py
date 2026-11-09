from __future__ import annotations

import unittest
from pathlib import Path


class CopytradeRuntimeDependencyTests(unittest.TestCase):
    def test_documented_s3_lz4_discovery_dependencies_are_declared(self) -> None:
        root = Path(__file__).resolve().parents[1]
        requirements = (root / "requirements.txt").read_text(encoding="utf-8").lower()
        declared = {line.split("=", 1)[0].split(">", 1)[0].split("<", 1)[0].strip() for line in requirements.splitlines() if line.strip() and not line.startswith("#")}
        discovery_source = (root / "src" / "copytrade" / "discovery.py").read_text(encoding="utf-8")
        self.assertIn("import boto3", discovery_source)
        self.assertIn("import lz4.frame", discovery_source)
        self.assertTrue({"boto3", "lz4"}.issubset(declared))


if __name__ == "__main__":
    unittest.main()
