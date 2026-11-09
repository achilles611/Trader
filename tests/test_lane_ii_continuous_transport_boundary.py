from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from scripts.lane_ii_continuous.transport import NetworkBoundary


class TransportSubprocessBoundaryTests(unittest.TestCase):
    def test_actual_bound_helper_start_and_override_denials_without_network(self):
        # The audit hook is permanent, so install it only in this isolated
        # subprocess. Spaces in the helper path exercise Windows quoting.
        with tempfile.TemporaryDirectory(prefix="lane ii bound helper ") as tmp:
            helper = Path(tmp) / "dummy public helper.py"
            helper.write_text("import json, sys\nassert sys.argv[1:] == ['net']\nprint(json.dumps({'bound_helper': True}))\n")
            code = r'''
import json, os, pathlib, subprocess, sys
sys.path.insert(0, sys.argv[1])
from scripts.lane_ii_continuous.transport import install_boundary
helper = str(pathlib.Path(sys.argv[2]).resolve())
def deny_all_network(event, args):
    if event.startswith("socket."):
        raise AssertionError("FIXTURE_NETWORK_FORBIDDEN:" + event)
sys.addaudithook(deny_all_network)
boundary = install_boundary("worker", helper)
command = [sys.executable, "-I", "-S", "-B", helper, "net"]
result = subprocess.run(command, capture_output=True, text=True, timeout=5, check=True)
assert json.loads(result.stdout) == {"bound_helper": True}
cases = [
    ("arguments", command + ["extra"], {}),
    ("helper", command[:-2] + [helper + ".unbound", "net"], {}),
    ("interpreter", [sys.executable + ".unbound"] + command[1:], {}),
    ("cwd", command, {"cwd": str(pathlib.Path(helper).parent)}),
    ("env", command, {"env": {"BOUNDARY_FIXTURE": "ONLY"}}),
    ("executable", command, {"executable": sys.executable if os.name == "nt" else sys.executable + ".unbound"}),
    ("shell", command, {"shell": True}),
]
denied = []
for label, argv, options in cases:
    try:
        subprocess.run(argv, capture_output=True, text=True, timeout=5, **options)
    except PermissionError as exc:
        assert str(exc) == "PUBLIC_OBSERVATION_BOUNDARY:UNBOUND_CHILD", (label, str(exc))
        denied.append(label)
    else:
        raise AssertionError("UNBOUND_CHILD_STARTED:" + label)
print(json.dumps({"allowed": result.returncode == 0, "denied": denied, "boundary": boundary.status()}))
'''
            result = subprocess.run(
                [sys.executable, "-I", "-S", "-B", "-c", code, str(Path(__file__).resolve().parents[1]), str(helper)],
                capture_output=True, text=True, timeout=20,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            observed = json.loads(result.stdout)
            self.assertTrue(observed["allowed"])
            self.assertEqual(observed["denied"], ["arguments", "helper", "interpreter", "cwd", "env", "executable", "shell"])
            self.assertEqual(observed["boundary"]["owned_http_helper_starts"], 1)
            self.assertEqual(observed["boundary"]["denied_network_actions"], 7)
            for field in ("public_get_requests", "public_socket_connects", "model_api_calls", "authenticated_account_calls", "real_orders"):
                self.assertEqual(observed["boundary"][field], 0, field)

    def test_windows_requires_exact_serialization_and_no_overrides(self):
        boundary = NetworkBoundary("worker", Path("fixture helper.py"))
        expected = [sys.executable, "-I", "-S", "-B", boundary.helper_path, "net"]
        command = subprocess.list2cmdline(expected)
        with patch("scripts.lane_ii_continuous.transport.os.name", "nt"):
            boundary.audit("subprocess.Popen", (None, command, None, None))
            for args in ((None, expected, None, None), (None, command + " ", None, None),
                         (sys.executable, command, None, None), (None, command, ".", None),
                         (None, command, None, {}), (None, command)):
                with self.assertRaisesRegex(PermissionError, "UNBOUND_CHILD"):
                    boundary.audit("subprocess.Popen", args)
        self.assertEqual(boundary.status()["owned_http_helper_starts"], 1)

    def test_posix_requires_exact_argument_sequence_and_bound_executable(self):
        boundary = NetworkBoundary("worker", Path("fixture helper.py"))
        expected = [sys.executable, "-I", "-S", "-B", boundary.helper_path, "net"]
        with patch("scripts.lane_ii_continuous.transport.os.name", "posix"):
            boundary.audit("subprocess.Popen", (sys.executable, expected, None, None))
            for args in ((sys.executable, subprocess.list2cmdline(expected), None, None),
                         (None, expected, None, None), ("other", expected, None, None),
                         (sys.executable, expected + ["extra"], None, None),
                         (sys.executable, expected, ".", None), (sys.executable, expected, None, {})):
                with self.assertRaisesRegex(PermissionError, "UNBOUND_CHILD"):
                    boundary.audit("subprocess.Popen", args)
        self.assertEqual(boundary.status()["owned_http_helper_starts"], 1)


if __name__ == "__main__":
    unittest.main()
