from __future__ import annotations

import json
import importlib.util
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class LoadedConfig:
    config: dict[str, Any]
    filepath: str


def _resolve_python_config(filepath: Path) -> dict[str, Any]:
    """Load a Python config file and extract the config dict."""
    module_name = f"_tinybird_config_{filepath.stem}"

    spec = importlib.util.spec_from_file_location(module_name, filepath)
    if spec is None or spec.loader is None:
        raise ValueError(f"Could not load Python config from {filepath}")

    module = importlib.util.module_from_spec(spec)

    # Temporarily add to sys.modules for relative imports
    old_module = sys.modules.get(module_name)
    sys.modules[module_name] = module

    try:
        spec.loader.exec_module(module)
    finally:
        # Restore previous state
        if old_module is None:
            sys.modules.pop(module_name, None)
        else:
            sys.modules[module_name] = old_module

    # Look for config in various places
    config: dict[str, Any] | None = None

    # 1. Look for 'config' attribute
    if hasattr(module, "config"):
        config = getattr(module, "config")
    # 2. Look for 'CONFIG' attribute
    elif hasattr(module, "CONFIG"):
        config = getattr(module, "CONFIG")
    # 3. Look for default export pattern
    elif hasattr(module, "default"):
        config = getattr(module, "default")
    # 4. Look for get_config() function
    elif hasattr(module, "get_config"):
        get_config = getattr(module, "get_config")
        if callable(get_config):
            config = get_config()

    if config is None:
        raise ValueError(
            f"Python config file {filepath} must export a 'config' dict, "
            "'CONFIG' dict, 'default' dict, or 'get_config()' function"
        )

    if not isinstance(config, dict):
        raise ValueError(f"Config in {filepath} must be a dict, got {type(config).__name__}")

    return config


def load_config_file(config_path: str, cwd: str | None = None) -> LoadedConfig:
    """Load a config file (supports .json and .py)."""
    base = Path(cwd or ".").resolve()
    filepath = Path(config_path)
    if not filepath.is_absolute():
        filepath = (base / filepath).resolve()

    if not filepath.exists():
        raise ValueError(f"Config file not found: {filepath}")

    ext = filepath.suffix.lower()

    if ext == ".json":
        raw = filepath.read_text(encoding="utf-8")
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            raise ValueError(f"Config in {filepath} must be a JSON object")
        return LoadedConfig(config=parsed, filepath=str(filepath))

    if ext == ".py":
        return LoadedConfig(config=_resolve_python_config(filepath), filepath=str(filepath))

    raise ValueError(f'Unsupported config extension "{ext}". Use .json or .py')


__all__ = ["LoadedConfig", "load_config_file"]
