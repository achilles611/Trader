from __future__ import annotations

import inspect
import unittest

from src.copytrade.control_center import create_control_center_app
from src.copytrade.config import CopyTradeConfig


class PaperControlCenterTests(unittest.TestCase):
    def test_only_closed_parameterless_paper_post_controls_exist(self) -> None:
        app = create_control_center_app(CopyTradeConfig())
        routes = {route.path: route for route in app.routes if hasattr(route, "methods")}
        expected = {
            "/api/lane-iii/paper/arm", "/api/lane-iii/paper/pause",
            "/api/lane-iii/paper/resume", "/api/lane-iii/paper/flatten-and-disarm",
        }
        self.assertTrue(expected.issubset(routes))
        self.assertFalse(any("order" in path and path.startswith("/api/lane-iii/paper") for path in routes))
        for path in expected:
            self.assertEqual(set(routes[path].methods), {"POST"})
            self.assertEqual(inspect.signature(routes[path].endpoint).parameters, {})
            self.assertNotIn("{", path)


if __name__ == "__main__":
    unittest.main()
