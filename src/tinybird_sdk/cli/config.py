from __future__ import annotations

import json
import os
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .config_loader import load_config_file
from .config_types import DevMode, TinybirdConfig
from .git import get_current_git_branch, get_tinybird_branch_name, is_main_branch

DEFAULT_BASE_URL = "https://api.tinybird.co"
LOCAL_BASE_URL = "http://localhost:7181"
CONFIG_FILES = [
    "tinybird.config.py",
    "tinybird_config.py",
    "tinybird.config.json",
    "tinybird.json",
]
DEFAULT_CONFIG_FILE = "tinybird.config.json"
TINYBIRD_FILE = "lib/tinybird.py"


@dataclass(frozen=True, slots=True)
class ResolvedConfig:
    include: list[str]
    token: str
    base_url: str
    config_path: str
    cwd: str
    git_branch: str | None
    tinybird_branch: str | None
    is_main_branch: bool
    dev_mode: DevMode


def load_env_files(directory: str) -> None:
    def _load(path: Path) -> None:
        if not path.exists() or not path.is_file():
            return
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export ") :].strip()
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)

    base = Path(directory)
    _load(base / ".env.local")
    _load(base / ".env")


def has_src_folder(cwd: str) -> bool:
    src = Path(cwd) / "src"
    return src.exists() and src.is_dir()


def get_tinybird_dir(cwd: str) -> str:
    base = Path(cwd)
    return str((base / "src" / "lib") if has_src_folder(cwd) else (base / "lib"))


def get_relative_tinybird_dir(cwd: str) -> str:
    return f"src/{TINYBIRD_FILE}" if has_src_folder(cwd) else TINYBIRD_FILE


def get_datasources_path(cwd: str) -> str:
    return str(Path(get_tinybird_dir(cwd)) / "datasources.py")


def get_pipes_path(cwd: str) -> str:
    return str(Path(get_tinybird_dir(cwd)) / "pipes.py")


def get_client_path(cwd: str) -> str:
    return str(Path(get_tinybird_dir(cwd)) / "client.py")


def _interpolate_env_vars(value: str) -> str:
    pattern = re.compile(r"\$\{([^}]+)\}")

    def replacer(match: re.Match[str]) -> str:
        variable = match.group(1)
        env = os.getenv(variable)
        if env is None:
            raise ValueError(f"Environment variable {variable} is not set")
        return env

    return pattern.sub(replacer, value)


def find_config_file(start_dir: str) -> dict[str, str] | None:
    current = Path(start_dir).resolve()

    while True:
        for filename in CONFIG_FILES:
            candidate = current / filename
            if candidate.exists():
                return {"path": str(candidate), "type": filename}

        parent = current.parent
        if parent == current:
            return None
        current = parent


def _resolve_config(config: TinybirdConfig, config_path: str) -> ResolvedConfig:
    if not config.include and not config.schema:
        raise ValueError(
            f"Missing 'include' field in {config_path}. Add an array of files to scan for datasources and pipes."
        )
    if not config.token:
        raise ValueError(f"Missing 'token' field in {config_path}")

    include = config.include or [config.schema]  # type: ignore[list-item]

    config_dir = str(Path(config_path).parent)
    load_env_files(config_dir)

    token = _interpolate_env_vars(config.token)
    base_url = _interpolate_env_vars(config.base_url) if config.base_url else DEFAULT_BASE_URL

    return ResolvedConfig(
        include=include,
        token=token,
        base_url=base_url,
        config_path=config_path,
        cwd=config_dir,
        git_branch=get_current_git_branch(),
        tinybird_branch=get_tinybird_branch_name(),
        is_main_branch=is_main_branch(),
        dev_mode=config.dev_mode or "branch",
    )


def load_config(cwd: str | None = None) -> dict[str, Any]:
    cwd = cwd or os.getcwd()
    config_result = find_config_file(cwd)

    if not config_result:
        raise ValueError(
            "Could not find config file. Run 'tinybird init' to create one. "
            f"Searched for: {', '.join(CONFIG_FILES)}"
        )

    config_path = config_result["path"]
    config_type = config_result["type"]

    if config_type.endswith(".py"):
        raise ValueError(
            f"Config file {config_path} is a Python file. Use load_config_async() for .py files."
        )

    with open(config_path, "r", encoding="utf-8") as fp:
        data = json.load(fp)

    resolved = _resolve_config(TinybirdConfig(**data), config_path)
    return asdict(resolved)


def load_config_async(cwd: str | None = None) -> dict[str, Any]:
    cwd = cwd or os.getcwd()
    config_result = find_config_file(cwd)

    if not config_result:
        raise ValueError(
            "Could not find config file. Run 'tinybird init' to create one. "
            f"Searched for: {', '.join(CONFIG_FILES)}"
        )

    loaded = load_config_file(config_result["path"], cwd=cwd)
    resolved = _resolve_config(TinybirdConfig(**loaded.config), loaded.filepath)
    return asdict(resolved)


def config_exists(cwd: str | None = None) -> bool:
    return find_config_file(cwd or os.getcwd()) is not None


def get_config_path(cwd: str | None = None) -> str:
    return str(Path(cwd or os.getcwd()) / DEFAULT_CONFIG_FILE)


def find_existing_config_path(cwd: str | None = None) -> str | None:
    base = Path(cwd or os.getcwd())
    for filename in CONFIG_FILES:
        candidate = base / filename
        if candidate.exists():
            return str(candidate)
    return None


def get_existing_or_new_config_path(cwd: str | None = None) -> str:
    return find_existing_config_path(cwd) or get_config_path(cwd)


def update_config(config_path: str, updates: dict[str, Any]) -> None:
    path = Path(config_path)
    if not path.exists():
        raise ValueError(f"Config not found at {config_path}")
    if path.suffix != ".json":
        raise ValueError(f"Cannot update {config_path}. Only JSON config files can be updated programmatically.")

    with open(path, "r", encoding="utf-8") as fp:
        current = json.load(fp)

    merged = {**current, **updates}

    with open(path, "w", encoding="utf-8") as fp:
        json.dump(merged, fp, indent=2)
        fp.write("\n")


def has_valid_token(cwd: str | None = None) -> bool:
    try:
        config_result = find_config_file(cwd or os.getcwd())
        if not config_result:
            return False

        if config_result["path"].endswith(".py"):
            config = load_config_async(cwd)
            return bool(config.get("token"))

        with open(config_result["path"], "r", encoding="utf-8") as fp:
            data = json.load(fp)

        token = data.get("token")
        if not token:
            return False

        load_env_files(str(Path(config_result["path"]).parent))
        if "${" in token:
            _interpolate_env_vars(token)
        return True
    except Exception:
        return False
