from __future__ import annotations

from dataclasses import dataclass
import os
import signal
import time
from typing import Any

from ...generator.loader import watch_schema
from ..config import load_config_async
from ..output import output
from .build import run_build


@dataclass(frozen=True, slots=True)
class LoginInfo:
    workspace_name: str | None = None
    user_email: str | None = None


@dataclass(frozen=True, slots=True)
class DevCommandOptions:
    cwd: str | None = None
    once: bool = False
    dev_mode_override: str | None = None


@dataclass(frozen=True, slots=True)
class BranchReadyInfo:
    git_branch: str | None = None
    tinybird_branch: str | None = None
    dashboard_url: str | None = None
    is_local: bool = False


@dataclass(frozen=True, slots=True)
class DevController:
    stop: Any


def run_dev(options: DevCommandOptions | dict[str, Any] | None = None) -> dict[str, Any]:
    normalized = options if isinstance(options, DevCommandOptions) else DevCommandOptions(**(options or {}))
    cwd = normalized.cwd or os.getcwd()

    config = load_config_async(cwd)

    first = run_build({"cwd": cwd, "dev_mode_override": normalized.dev_mode_override})
    if not first.success:
        return {
            "success": False,
            "error": first.error,
            "duration_ms": first.duration_ms,
        }

    if normalized.once:
        return {
            "success": True,
            "build": first,
            "duration_ms": first.duration_ms,
        }

    output.success("Watching for changes. Press Ctrl+C to stop.")

    last_result: dict[str, Any] = {"build": first}

    def on_change() -> None:
        output.highlight("Change detected, rebuilding...")
        result = run_build({"cwd": cwd, "dev_mode_override": normalized.dev_mode_override})
        last_result["build"] = result
        if result.success:
            output.show_build_success(result.duration_ms, is_rebuild=True)
        else:
            output.show_build_failure(is_rebuild=True)
            if result.error:
                output.error(result.error)

    controller = watch_schema({"include_paths": config["include"], "cwd": config["cwd"]}, on_change)

    try:
        while True:
            time.sleep(0.2)
    except KeyboardInterrupt:
        controller.stop()

    return {
        "success": True,
        "build": last_result.get("build"),
    }


__all__ = [
    "LoginInfo",
    "DevCommandOptions",
    "BranchReadyInfo",
    "DevController",
    "run_dev",
]
