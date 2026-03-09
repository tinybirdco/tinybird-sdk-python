from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .fetcher import tinybird_fetch


@dataclass(frozen=True, slots=True)
class WorkspaceApiConfig:
    base_url: str
    token: str


@dataclass(frozen=True, slots=True)
class TinybirdWorkspace:
    id: str
    name: str
    user_id: str
    user_email: str
    scope: str
    main: str | None = None


class WorkspaceApiError(Exception):
    def __init__(self, message: str, status: int, body: Any = None):
        super().__init__(message)
        self.status = status
        self.body = body


def get_workspace(config: WorkspaceApiConfig | dict[str, Any]) -> TinybirdWorkspace:
    normalized = config if isinstance(config, WorkspaceApiConfig) else WorkspaceApiConfig(**config)
    response = tinybird_fetch(
        f"{normalized.base_url.rstrip('/')}/v1/workspace",
        method="GET",
        headers={"Authorization": f"Bearer {normalized.token}"},
    )
    if not response.ok:
        raise WorkspaceApiError(
            f"Failed to get workspace: {response.status_code}",
            response.status_code,
            response.text,
        )
    return TinybirdWorkspace(**response.json())
