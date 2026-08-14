from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from src.copytrade.config import CopyTradeConfig
from src.copytrade.control_center import create_control_center_app


class ControlCenterLauncherTests(unittest.TestCase):
    def test_websocket_endpoint_is_registered_as_a_websocket(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = CopyTradeConfig()
            config = replace(base, artifacts=replace(base.artifacts, database_path=Path(directory) / "copytrade.sqlite3"))
            app = create_control_center_app(config)
        route = next(item for item in app.routes if item.path == "/ws")
        self.assertEqual(route.dependant.websocket_param_name, "websocket")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
