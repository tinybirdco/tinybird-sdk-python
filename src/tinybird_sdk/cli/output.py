from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import os
from typing import Any


class _Colors:
    reset = "\x1b[0m"
    bold = "\x1b[1m"
    red = "\x1b[91m"
    green = "\x1b[92m"
    yellow = "\x1b[38;5;208m"
    blue = "\x1b[94m"
    gray = "\x1b[90m"


_NO_COLOR = os.getenv("NO_COLOR") is not None or not os.isatty(1)


def _colorize(text: str, color: str) -> str:
    if _NO_COLOR:
        return text
    value = getattr(_Colors, color)
    return f"{value}{text}{_Colors.reset}"


def success(message: str) -> None:
    print(_colorize(message, "green"))


def error(message: str) -> None:
    print(_colorize(message, "red"))


def warning(message: str) -> None:
    print(_colorize(message, "yellow"))


def info(message: str) -> None:
    print(message)


def highlight(message: str) -> None:
    print(_colorize(message, "blue"))


def gray(message: str) -> None:
    print(_colorize(message, "gray"))


def bold(message: str) -> None:
    print(_colorize(message, "bold"))


def format_time() -> str:
    return datetime.now().strftime("%H:%M:%S")


def format_duration(ms: int) -> str:
    if ms < 1000:
        return f"{ms}ms"
    return f"{ms / 1000:.1f}s"


def show_resource_change(path: str, status: str) -> None:
    print(f"✓ {path} {status}")


@dataclass(frozen=True, slots=True)
class ResourceChange:
    status: str
    name: str
    type: str


def show_changes_table(changes: list[ResourceChange]) -> None:
    if not changes:
        gray("* No changes to be deployed")
        return
    info("\n* Changes to be deployed:")
    for change in changes:
        print(f"- {change.status:8} {change.name:24} {change.type}")


def show_resource_warning(level: str, resource: str, message: str) -> None:
    warning(f"△ {level}: {resource}: {message}")


def show_build_errors(errors: list[dict[str, str]]) -> None:
    for entry in errors:
        if entry.get("filename"):
            error(entry["filename"])
        for line in entry.get("error", "").split("\n"):
            error(f"  {line}")
        print()


def show_build_success(duration_ms: int, is_rebuild: bool = False) -> None:
    success(f"\n✓ {'Rebuild' if is_rebuild else 'Build'} completed in {format_duration(duration_ms)}")


def show_build_failure(is_rebuild: bool = False) -> None:
    error(f"\n✗ {'Rebuild' if is_rebuild else 'Build'} failed")


def show_no_changes() -> None:
    warning("△ Not deploying. No changes.")


def show_waiting_for_deployment() -> None:
    info("» Waiting for deployment to be ready...")


def show_deployment_ready() -> None:
    success("✓ Deployment is ready")


def show_deployment_live(deployment_id: str) -> None:
    success(f"✓ Deployment #{deployment_id} is live!")


def show_validating_deployment() -> None:
    info("» Validating deployment...")


def show_deploy_success(duration_ms: int) -> None:
    success(f"\n✓ Deploy completed in {format_duration(duration_ms)}")


def show_deploy_failure() -> None:
    error("\n✗ Deploy failed")


@dataclass(frozen=True, slots=True)
class BranchDisplayInfo:
    mode: str
    git_branch: str | None = None
    tinybird_branch: str | None = None
    created: bool | None = None
    branch_id: str | None = None
    workspace_name: str | None = None


def show_branch_info(info_data: BranchDisplayInfo) -> None:
    info(f"Mode: {info_data.mode}")
    if info_data.git_branch:
        info(f"Git branch: {info_data.git_branch}")
    if info_data.tinybird_branch:
        info(f"Tinybird branch: {info_data.tinybird_branch}")


def show_info(data: dict[str, Any]) -> None:
    print(json_dumps(data))


def json_dumps(value: Any) -> str:
    import json

    return json.dumps(value, indent=2, default=str)


class _Output:
    success = staticmethod(success)
    error = staticmethod(error)
    warning = staticmethod(warning)
    info = staticmethod(info)
    highlight = staticmethod(highlight)
    gray = staticmethod(gray)
    bold = staticmethod(bold)
    format_time = staticmethod(format_time)
    format_duration = staticmethod(format_duration)
    show_resource_change = staticmethod(show_resource_change)
    show_changes_table = staticmethod(show_changes_table)
    show_resource_warning = staticmethod(show_resource_warning)
    show_build_errors = staticmethod(show_build_errors)
    show_build_success = staticmethod(show_build_success)
    show_build_failure = staticmethod(show_build_failure)
    show_no_changes = staticmethod(show_no_changes)
    show_waiting_for_deployment = staticmethod(show_waiting_for_deployment)
    show_deployment_ready = staticmethod(show_deployment_ready)
    show_deployment_live = staticmethod(show_deployment_live)
    show_validating_deployment = staticmethod(show_validating_deployment)
    show_deploy_success = staticmethod(show_deploy_success)
    show_deploy_failure = staticmethod(show_deploy_failure)
    show_branch_info = staticmethod(show_branch_info)
    show_info = staticmethod(show_info)


output = _Output()


__all__ = [
    "output",
    "ResourceChange",
    "BranchDisplayInfo",
    "success",
    "error",
    "warning",
    "info",
    "highlight",
    "gray",
    "bold",
]
