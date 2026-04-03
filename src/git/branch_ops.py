from __future__ import annotations

import subprocess
from pathlib import Path


class BranchOperationError(RuntimeError):
    pass


def _git(root_dir: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=root_dir,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise BranchOperationError(result.stderr.strip() or result.stdout.strip() or f"git {' '.join(args)} failed")
    return result.stdout.strip()


def create_experiment_branch(root_dir: Path, branch_name: str, *, base_ref: str) -> str:
    _git(root_dir, "checkout", "-B", branch_name, base_ref)
    return branch_name


def current_branch(root_dir: Path) -> str:
    return _git(root_dir, "branch", "--show-current")


def capture_diff(root_dir: Path, output_path: Path, *, base_ref: str) -> None:
    diff = _git(root_dir, "diff", "--binary", base_ref)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(diff, encoding="utf-8")
