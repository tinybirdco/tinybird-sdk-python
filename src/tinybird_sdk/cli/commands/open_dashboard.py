from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any, Literal
import webbrowser

from ...api.dashboard import get_branch_dashboard_url, get_dashboard_url, get_local_dashboard_url
from ...api.workspaces import get_workspace
from ..config import load_config_async


Environment = Literal["cloud", "local", "branch"]


@dataclass(frozen=True, slots=True)
class OpenDashboardCommandOptions:
    cwd: str | None = None
    environment: Environment | None = None


@dataclass(frozen=True, slots=True)
class OpenDashboardCommandResult:
    success: bool
    environment: Environment | None = None
    url: str | None = None
    browser_opened: bool = False
    error: str | None = None


def run_open_dashboard(options: OpenDashboardCommandOptions | dict[str, Any] | None = None) -> OpenDashboardCommandResult:
    normalized = options if isinstance(options, OpenDashboardCommandOptions) else OpenDashboardCommandOptions(**(options or {}))

    try:
        config = load_config_async(normalized.cwd or os.getcwd())
        workspace = get_workspace({"base_url": config["base_url"], "token": config["token"]})

        env = normalized.environment or config.get("dev_mode") or "cloud"
        if env == "local":
            workspace_name = config.get("tinybird_branch") or workspace.name
            url = get_local_dashboard_url(workspace_name)
        elif env == "branch":
            branch_name = config.get("tinybird_branch")
            if not branch_name:
                return OpenDashboardCommandResult(success=False, error="No tinybird branch available", environment="branch")
            url = get_branch_dashboard_url(config["base_url"], workspace.name, branch_name)
            if not url:
                return OpenDashboardCommandResult(success=False, error="Could not derive branch dashboard URL", environment="branch")
        else:
            url = get_dashboard_url(config["base_url"], workspace.name)
            if not url:
                return OpenDashboardCommandResult(success=False, error="Could not derive dashboard URL", environment="cloud")

        opened = webbrowser.open(url)
        return OpenDashboardCommandResult(
            success=True,
            environment=env if env in {"cloud", "local", "branch"} else "cloud",
            url=url,
            browser_opened=bool(opened),
        )
    except Exception as error:
        return OpenDashboardCommandResult(success=False, error=str(error))


__all__ = ["Environment", "OpenDashboardCommandOptions", "OpenDashboardCommandResult", "run_open_dashboard"]
