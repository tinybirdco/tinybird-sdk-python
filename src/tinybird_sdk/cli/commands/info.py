from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any

from ...api.branches import list_branches
from ...api.local import is_local_running
from ...api.resources import list_datasources, list_pipes
from ...api.workspaces import get_workspace
from ..config import config_exists, find_existing_config_path, load_config_async
from ..git import get_current_git_branch


@dataclass(frozen=True, slots=True)
class InfoCommandOptions:
    cwd: str | None = None
    json: bool = False


@dataclass(frozen=True, slots=True)
class InfoCommandResult:
    success: bool
    cloud: dict[str, Any] | None = None
    local: dict[str, Any] | None = None
    branch: dict[str, Any] | None = None
    project: dict[str, Any] | None = None
    branches: list[dict[str, Any]] | None = None
    error: str | None = None


def run_info(options: InfoCommandOptions | dict[str, Any] | None = None) -> InfoCommandResult:
    normalized = options if isinstance(options, InfoCommandOptions) else InfoCommandOptions(**(options or {}))
    cwd = normalized.cwd or os.getcwd()

    if not config_exists(cwd):
        return InfoCommandResult(success=False, error="No Tinybird config found in current directory tree")

    try:
        config = load_config_async(cwd)
        workspace = get_workspace({"base_url": config["base_url"], "token": config["token"]})
        branches = list_branches({"base_url": config["base_url"], "token": config["token"]})

        datasource_names = list_datasources({"base_url": config["base_url"], "token": config["token"]})
        pipe_names = list_pipes({"base_url": config["base_url"], "token": config["token"]})

        project_info = {
            "cwd": config["cwd"],
            "config_path": config["config_path"],
            "include": config["include"],
            "resources": {
                "datasources": len(datasource_names),
                "pipes": len(pipe_names),
            },
        }

        branch_info = {
            "git_branch": config.get("git_branch") or get_current_git_branch(),
            "tinybird_branch": config.get("tinybird_branch"),
            "is_main_branch": config.get("is_main_branch"),
            "dev_mode": config.get("dev_mode"),
        }

        cloud_info = {
            "base_url": config["base_url"],
            "workspace": {
                "id": workspace.id,
                "name": workspace.name,
                "scope": workspace.scope,
                "user_email": workspace.user_email,
            },
        }

        local_info = {
            "running": is_local_running(),
            "base_url": "http://localhost:7181",
        }

        return InfoCommandResult(
            success=True,
            cloud=cloud_info,
            local=local_info,
            branch=branch_info,
            project=project_info,
            branches=[asdict(branch) for branch in branches],
        )
    except Exception as error:
        return InfoCommandResult(success=False, error=str(error))


__all__ = ["InfoCommandOptions", "InfoCommandResult", "run_info"]
