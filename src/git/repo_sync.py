from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path


class RepoSyncError(RuntimeError):
    pass


class DirtyWorktreeError(RepoSyncError):
    def __init__(self, dirty_files: list[str]) -> None:
        self.dirty_files = dirty_files
        super().__init__(
            "Working tree is dirty; aborting live orchestration cycle. "
            f"Dirty files: {', '.join(dirty_files)}. "
            "Remediation: commit the changes, stash them intentionally, or reset them explicitly."
        )


@dataclass(frozen=True)
class RepoState:
    branch: str
    git_sha: str
    is_dirty: bool
    dirty_files: list[str]


def _git(root_dir: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=root_dir,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RepoSyncError(result.stderr.strip() or result.stdout.strip() or f"git {' '.join(args)} failed")
    return result.stdout.strip()


def get_repo_state(root_dir: Path) -> RepoState:
    branch = _git(root_dir, "branch", "--show-current")
    git_sha = _git(root_dir, "rev-parse", "HEAD")
    status = _git(root_dir, "status", "--porcelain")
    dirty_files = [line.strip() for line in status.splitlines() if line.strip()]
    return RepoState(branch=branch, git_sha=git_sha, is_dirty=bool(dirty_files), dirty_files=dirty_files)


def resolve_ref_sha(root_dir: Path, ref: str) -> str | None:
    try:
        return _git(root_dir, "rev-parse", "--verify", ref)
    except RepoSyncError:
        return None


def ensure_clean_worktree(root_dir: Path) -> RepoState:
    state = get_repo_state(root_dir)
    if state.is_dirty:
        raise DirtyWorktreeError(state.dirty_files)
    return state


def sync_to_production(root_dir: Path, *, remote: str, branch: str) -> RepoState:
    ensure_clean_worktree(root_dir)
    _git(root_dir, "fetch", "--prune", remote)
    _git(root_dir, "checkout", branch)
    _git(root_dir, "reset", "--hard", f"{remote}/{branch}")
    _git(root_dir, "submodule", "update", "--init", "--recursive")
    return get_repo_state(root_dir)
