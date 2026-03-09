from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
import os
from typing import Any

from ...api.branches import delete_branch, get_branch, list_branches
from ...api.workspaces import get_workspace
from ..branch_store import BranchInfo, list_cached_branches, remove_branch
from ..config import load_config_async


@dataclass(frozen=True, slots=True)
class BranchCommandOptions:
    cwd: str | None = None


@dataclass(frozen=True, slots=True)
class BranchListResult:
    success: bool
    branches: list[dict[str, Any]]
    error: str | None = None


@dataclass(frozen=True, slots=True)
class BranchStatusResult:
    success: bool
    branch: dict[str, Any] | None = None
    error: str | None = None


@dataclass(frozen=True, slots=True)
class BranchDeleteResult:
    success: bool
    deleted: bool = False
    error: str | None = None


def run_branch_list(options: BranchCommandOptions | dict[str, Any] | None = None) -> BranchListResult:
    normalized = options if isinstance(options, BranchCommandOptions) else BranchCommandOptions(**(options or {}))
    try:
        config = load_config_async(normalized.cwd or os.getcwd())
        branches = list_branches({"base_url": config["base_url"], "token": config["token"]})
        return BranchListResult(success=True, branches=[asdict(branch) for branch in branches])
    except Exception as error:
        return BranchListResult(success=False, branches=[], error=str(error))


def run_branch_status(branch_name: str | None = None, options: BranchCommandOptions | dict[str, Any] | None = None) -> BranchStatusResult:
    normalized = options if isinstance(options, BranchCommandOptions) else BranchCommandOptions(**(options or {}))
    try:
        config = load_config_async(normalized.cwd or os.getcwd())
        target = branch_name or config.get("tinybird_branch")
        if not target:
            return BranchStatusResult(success=False, error="No branch name provided and no tinybird_branch detected")
        branch = get_branch({"base_url": config["base_url"], "token": config["token"]}, target)
        return BranchStatusResult(success=True, branch=asdict(branch))
    except Exception as error:
        return BranchStatusResult(success=False, error=str(error))


def run_branch_delete(branch_name: str, options: BranchCommandOptions | dict[str, Any] | None = None) -> BranchDeleteResult:
    normalized = options if isinstance(options, BranchCommandOptions) else BranchCommandOptions(**(options or {}))
    try:
        config = load_config_async(normalized.cwd or os.getcwd())
        delete_branch({"base_url": config["base_url"], "token": config["token"]}, branch_name)

        try:
            workspace = get_workspace({"base_url": config["base_url"], "token": config["token"]})
            remove_branch(workspace.id, branch_name)
        except Exception:
            pass

        return BranchDeleteResult(success=True, deleted=True)
    except Exception as error:
        return BranchDeleteResult(success=False, deleted=False, error=str(error))


def run_branch_list_cached(options: BranchCommandOptions | dict[str, Any] | None = None) -> BranchListResult:
    normalized = options if isinstance(options, BranchCommandOptions) else BranchCommandOptions(**(options or {}))
    try:
        config = load_config_async(normalized.cwd or os.getcwd())
        workspace = get_workspace({"base_url": config["base_url"], "token": config["token"]})
        cached = list_cached_branches(workspace.id)
        return BranchListResult(
            success=True,
            branches=[{"name": name, **asdict(info)} for name, info in cached.items()],
        )
    except Exception as error:
        return BranchListResult(success=False, branches=[], error=str(error))


__all__ = [
    "BranchCommandOptions",
    "BranchListResult",
    "BranchStatusResult",
    "BranchDeleteResult",
    "run_branch_list",
    "run_branch_status",
    "run_branch_delete",
    "run_branch_list_cached",
]
