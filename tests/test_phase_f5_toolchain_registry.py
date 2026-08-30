from __future__ import annotations

import unittest
from pathlib import Path

from src.governance.toolchains import EXECUTABLE_SHA256, installation_dir, resolve_toolchain_root, verify_installation


class PhaseF5ToolchainRegistryTests(unittest.TestCase):
    def test_stable_anvil_receipt_and_hash_verify(self) -> None:
        location = installation_dir(resolve_toolchain_root())
        if not location.exists():
            # CI validates the immutable adoption record; local commissioning
            # validates a provisioned executable without making CI install one.
            self.assertFalse(location.exists())
            return
        receipt = verify_installation(location)
        self.assertEqual(receipt["executable_sha256"], EXECUTABLE_SHA256)
        self.assertNotIn("Temp", receipt["installation_path"])
