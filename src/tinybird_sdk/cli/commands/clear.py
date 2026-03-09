from __future__ import annotations

from dataclasses import dataclass
import os
import time
from typing import Any

from ...api.branches import clear_branch
from ...api.local import clear_local_workspace, get_local_tokens, get_local_workspace_name
from ..config import load_config_async


@dataclass(frozen=True, slots=True)
class ClearCommandOptions:
    cwd: str | None = None
    dev_mode_override: str | None = None


@dataclass(frozen=True, slots=True)
class ClearResult:
    success: bool
    duration_ms: int
    workspace: str | None = None
    branch: str | None = None
    error: str | None = None


def run_clear(options: ClearCommandOptions | dict[str, Any] | None = None) -> ClearResult:
    start = int(time.time() * 1000)
    normalized = options if isinstance(options, ClearCommandOptions) else ClearCommandOptions(**(options or {}))

    try:
        config = load_config_async(normalized.cwd or os.getcwd())
    except Exception as error:
        return ClearResult(success=False, error=str(error), duration_ms=int(time.time() * 1000) - start)

    dev_mode = normalized.dev_mode_override or config.get("dev_mode", "branch")

    try:
        if dev_mode == "local":
            tokens = get_local_tokens()
            workspace_name = get_local_workspace_name(config.get("tinybird_branch"), config["cwd"])
            clear_local_workspace(tokens, workspace_name)
            return ClearResult(
                success=True,
                workspace=workspace_name,
                duration_ms=int(time.time() * 1000) - start,
            )

        if not config.get("tinybird_branch"):
            return ClearResult(
                success=False,
                error="No tinybird branch detected. Switch to a feature branch first.",
                duration_ms=int(time.time() * 1000) - start,
            )

        clear_branch({"base_url": config["base_url"], "token": config["token"]}, config["tinybird_branch"])
        return ClearResult(
            success=True,
            branch=config["tinybird_branch"],
            duration_ms=int(time.time() * 1000) - start,
        )
    except Exception as error:
        return ClearResult(success=False, error=str(error), duration_ms=int(time.time() * 1000) - start)


__all__ = ["ClearCommandOptions", "ClearResult", "run_clear"]
