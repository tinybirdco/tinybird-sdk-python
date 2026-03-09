from __future__ import annotations

import json
from pathlib import Path
from typing import Literal


PackageManager = Literal["pnpm", "yarn", "bun", "npm"]
TINYBIRD_SDK_PACKAGE = "@tinybirdco/sdk"


def _detect_package_manager_from_lockfile(directory: Path) -> PackageManager | None:
    if (directory / "pnpm-lock.yaml").exists():
        return "pnpm"
    if (directory / "yarn.lock").exists():
        return "yarn"
    if (directory / "bun.lockb").exists():
        return "bun"
    if (directory / "package-lock.json").exists():
        return "npm"
    return None


def _detect_package_manager_from_workspace(directory: Path) -> PackageManager | None:
    if (directory / "pnpm-workspace.yaml").exists() or (directory / "pnpm-workspace.yml").exists():
        return "pnpm"
    return None


def _detect_package_manager_from_package_json(directory: Path) -> PackageManager | None:
    package_json_path = directory / "package.json"
    if not package_json_path.exists():
        return None
    try:
        package_json = json.loads(package_json_path.read_text(encoding="utf-8"))
    except Exception:
        return None

    package_manager = package_json.get("package_manager")
    if not isinstance(package_manager, str):
        return None
    if package_manager.startswith("pnpm"):
        return "pnpm"
    if package_manager.startswith("yarn"):
        return "yarn"
    if package_manager.startswith("bun"):
        return "bun"
    if package_manager.startswith("npm"):
        return "npm"
    return None


def _search_dirs(start: Path) -> list[Path]:
    dirs: list[Path] = []
    current = start.resolve()
    while True:
        dirs.append(current)
        if current.parent == current:
            break
        current = current.parent
    return dirs


def _find_nearest_package_json(start: Path) -> Path | None:
    for directory in _search_dirs(start):
        package_json_path = directory / "package.json"
        if package_json_path.exists():
            return package_json_path
    return None


def get_package_manager_run_cmd(package_manager: PackageManager) -> str:
    if package_manager == "pnpm":
        return "pnpm run"
    if package_manager == "yarn":
        return "yarn"
    if package_manager == "bun":
        return "bun run"
    return "npm run"


def get_package_manager_install_cmd(package_manager: PackageManager) -> str:
    if package_manager == "pnpm":
        return "pnpm install"
    if package_manager == "yarn":
        return "yarn install"
    if package_manager == "bun":
        return "bun install"
    return "npm install"


def get_package_manager_add_cmd(package_manager: PackageManager) -> str:
    if package_manager == "pnpm":
        return "pnpm add"
    if package_manager == "yarn":
        return "yarn add"
    if package_manager == "bun":
        return "bun add"
    return "npm install"


def detect_package_manager(cwd: str | None = None) -> PackageManager:
    start = Path(cwd or ".").resolve()
    for directory in _search_dirs(start):
        from_lockfile = _detect_package_manager_from_lockfile(directory)
        if from_lockfile:
            return from_lockfile

        from_workspace = _detect_package_manager_from_workspace(directory)
        if from_workspace:
            return from_workspace

        from_package_json = _detect_package_manager_from_package_json(directory)
        if from_package_json:
            return from_package_json

    return "npm"


def detect_package_manager_install_cmd(cwd: str | None = None) -> str:
    return get_package_manager_install_cmd(detect_package_manager(cwd))


def has_tinybird_sdk_dependency(cwd: str | None = None) -> bool:
    package_json_path = _find_nearest_package_json(Path(cwd or ".").resolve())
    if not package_json_path:
        return False

    try:
        package_json = json.loads(package_json_path.read_text(encoding="utf-8"))
    except Exception:
        return False

    for field in ("dependencies", "dev_dependencies", "peer_dependencies", "optional_dependencies"):
        deps = package_json.get(field)
        if isinstance(deps, dict) and TINYBIRD_SDK_PACKAGE in deps:
            return True
    return False


def detect_package_manager_run_cmd(cwd: str | None = None) -> str:
    return get_package_manager_run_cmd(detect_package_manager(cwd))


__all__ = [
    "PackageManager",
    "get_package_manager_run_cmd",
    "get_package_manager_install_cmd",
    "get_package_manager_add_cmd",
    "detect_package_manager",
    "detect_package_manager_install_cmd",
    "has_tinybird_sdk_dependency",
    "detect_package_manager_run_cmd",
]
