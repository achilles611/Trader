from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import beez_console


class BeezConsoleTests(unittest.TestCase):
    def test_project_root_uses_packaged_executable_directory(self) -> None:
        root = beez_console.project_root(frozen=True, executable=r"C:\\Trader\\BeezConsole.exe")
        self.assertEqual(root, Path(r"C:\\Trader"))

    def test_brave_path_prefers_environment_override(self) -> None:
        override = Path(r"C:\\Tools\\brave.exe")
        found = beez_console.brave_path(
            environment={"BRAVE_PATH": str(override)}, finder=lambda _: None, exists=lambda path: path == override
        )
        self.assertEqual(found, override)

    def test_port_detection_handles_socket_failure(self) -> None:
        with patch("beez_console.socket.create_connection", side_effect=OSError):
            self.assertFalse(beez_console.port_is_open())

    def test_project_root_validation_reports_missing_main(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".venv" / "Scripts").mkdir(parents=True)
            (root / ".venv" / "Scripts" / "python.exe").touch()
            with self.assertRaisesRegex(RuntimeError, "main.py"):
                beez_console.validate_project_root(root)

    def test_project_root_validation_reports_missing_venv(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "main.py").touch()
            with self.assertRaisesRegex(RuntimeError, ".venv"):
                beez_console.validate_project_root(root)

    def test_running_server_opens_brave_without_duplicate_backend(self) -> None:
        root = Path(r"C:\\Trader")
        brave = Path(r"C:\\Tools\\brave.exe")
        with (
            patch("beez_console.project_root", return_value=root),
            patch("beez_console.validate_project_root"),
            patch("beez_console.port_is_open", return_value=True),
            patch("beez_console.server_is_responding", return_value=True),
            patch("beez_console.start_server") as start_server,
            patch("beez_console.brave_path", return_value=brave),
            patch("beez_console.open_brave") as open_brave,
        ):
            self.assertEqual(beez_console.main(), 0)
        start_server.assert_not_called()
        open_brave.assert_called_once_with(brave, root)

    def test_wait_reports_exited_backend(self) -> None:
        process = Mock()
        process.poll.return_value = 1
        with patch("beez_console.server_is_responding", return_value=False):
            with self.assertRaisesRegex(RuntimeError, "stopped"):
                beez_console.wait_for_server(process, timeout_seconds=0.1)

    def test_missing_project_root_reports_a_gui_startup_error(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with (
                patch("beez_console.project_root", return_value=Path(directory)),
                patch("beez_console.show_error") as show_error,
            ):
                self.assertEqual(beez_console.main(), 1)
        show_error.assert_called_once()
        self.assertIn("main.py", show_error.call_args.args[0])


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
