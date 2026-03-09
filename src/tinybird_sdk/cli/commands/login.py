from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from ..auth import browser_login
from ..config import find_existing_config_path, update_config
from ..env import save_tinybird_base_url, save_tinybird_token


@dataclass(frozen=True, slots=True)
class RunLoginOptions:
    cwd: str | None = None
    api_host: str | None = None
    persist: bool = True


@dataclass(frozen=True, slots=True)
class LoginResult:
    success: bool
    token: str | None = None
    base_url: str | None = None
    workspace_name: str | None = None
    user_email: str | None = None
    error: str | None = None


def run_login(options: RunLoginOptions | dict[str, Any] | None = None) -> LoginResult:
    normalized = options if isinstance(options, RunLoginOptions) else RunLoginOptions(**(options or {}))
    result = browser_login({"api_host": normalized.api_host} if normalized.api_host else {})
    if not result.success:
        return LoginResult(success=False, error=result.error)

    if normalized.persist and result.token:
        cwd = normalized.cwd or "."
        save_tinybird_token(cwd, result.token)
        if result.base_url:
            save_tinybird_base_url(cwd, result.base_url)

        config_path = find_existing_config_path(cwd)
        if config_path and config_path.endswith(".json"):
            updates: dict[str, Any] = {"token": "${TINYBIRD_TOKEN}"}
            if result.base_url:
                updates["base_url"] = result.base_url
            update_config(config_path, updates)

    return LoginResult(
        success=True,
        token=result.token,
        base_url=result.base_url,
        workspace_name=result.workspace_name,
        user_email=result.user_email,
    )


__all__ = ["RunLoginOptions", "LoginResult", "run_login"]
