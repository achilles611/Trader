"""Production-style HTTP routing checks for the built BeezConsole surface.

This deliberately uses a real Uvicorn listener and the built frontend files;
it does not replace either the SPA fallback or FastAPI routing with mocks.
The app lifespan is disabled so no NinjaTrader listener, verifier, or paper
runtime starts while the routing contract is exercised.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
import json
import os
from pathlib import Path
import socket
from tempfile import TemporaryDirectory
import threading
import time
import unittest
from urllib.error import HTTPError, URLError
from urllib.request import urlopen
from unittest.mock import patch

import uvicorn

from src.copytrade.config import CopyTradeConfig
from src.copytrade.control_center import create_control_center_app


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
FRONTEND_DIST = REPOSITORY_ROOT / "control-center-ui" / "dist"


def _response(url: str) -> tuple[int, str, bytes]:
    try:
        with urlopen(url, timeout=3) as response:  # noqa: S310 - loopback test server only
            return response.status, response.headers.get_content_type(), response.read()
    except HTTPError as error:
        return error.code, error.headers.get_content_type(), error.read()


@contextmanager
def _combined_server(app: object):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, lifespan="off", access_log=False, log_level="error"))
    thread = threading.Thread(target=server.run, name="test-control-center-http", daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + 5
        while True:
            try:
                _response(f"{base_url}/")
                break
            except URLError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.02)
        yield base_url
    finally:
        server.should_exit = True
        thread.join(timeout=5)
        if thread.is_alive():
            raise RuntimeError("temporary combined control-center server did not stop")


class ProductionControlCenterRoutingTests(unittest.TestCase):
    def _app(self, root: Path) -> object:
        defaults = CopyTradeConfig()
        config = replace(defaults, artifacts=replace(defaults.artifacts, database_path=root / "copytrade.sqlite3"))
        with patch.dict(os.environ, {
            "BEELZEBUB_L3G_PAPER_LEDGER": str(root / "lane_iii_paper.sqlite3"),
            "BEELZEBUB_LEDGER_AUDIT_ROOT": str(root / "audit"),
        }, clear=False):
            return create_control_center_app(config)

    def test_built_frontend_and_slim_status_use_real_combined_http_routing(self) -> None:
        self.assertTrue((FRONTEND_DIST / "index.html").is_file(), "Run `npm run build` in control-center-ui before this production routing test.")
        with TemporaryDirectory() as directory:
            with _combined_server(self._app(Path(directory))) as base_url:
                root_status, root_type, root_body = _response(f"{base_url}/")
                slim_status, slim_type, slim_body = _response(f"{base_url}/api/lane-iii/paper/slim-status")
                missing_status, missing_type, missing_body = _response(f"{base_url}/api/not-a-real-endpoint")

        self.assertEqual((root_status, root_type), (200, "text/html"))
        self.assertIn(b'<div id="root">', root_body)
        self.assertEqual((slim_status, slim_type), (200, "application/json"))
        self.assertEqual(json.loads(slim_body)["schema"], "lane-iii-phase-g-slim-status-v1")
        self.assertEqual((missing_status, missing_type), (404, "application/json"))
        missing = json.loads(missing_body)
        self.assertEqual(missing["code"], "API_ENDPOINT_NOT_FOUND")
        self.assertEqual(missing["path"], "/api/not-a-real-endpoint")

    def test_non_post_slim_command_requests_are_json_and_never_execute(self) -> None:
        """GET is intentionally used so command endpoints are not invoked."""
        self.assertTrue((FRONTEND_DIST / "index.html").is_file(), "Run `npm run build` in control-center-ui before this production routing test.")
        paths = (
            "/api/lane-iii/paper/ledger-verification",
            "/api/lane-iii/paper/commissioning-start",
            "/api/lane-iii/paper/flatten-and-disarm",
        )
        with TemporaryDirectory() as directory:
            with _combined_server(self._app(Path(directory))) as base_url:
                responses = {path: _response(f"{base_url}{path}") for path in paths}

        for path, (status, content_type, body) in responses.items():
            with self.subTest(path=path):
                self.assertIn(status, {200, 404})
                self.assertEqual(content_type, "application/json")
                self.assertNotIn(b"<!doctype", body.lower())


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
