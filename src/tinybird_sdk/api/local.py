from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

from ..cli.config import LOCAL_BASE_URL
from .fetcher import tinybird_fetch


@dataclass(frozen=True, slots=True)
class LocalTokens:
    user_token: str
    admin_token: str
    workspace_admin_token: str


@dataclass(frozen=True, slots=True)
class LocalWorkspace:
    id: str
    name: str
    token: str


class LocalNotRunningError(Exception):
    pass


class LocalApiError(Exception):
    def __init__(self, message: str, status: int | None = None, body: Any = None):
        super().__init__(message)
        self.status = status
        self.body = body


def is_local_running() -> bool:
    try:
        response = tinybird_fetch(f"{LOCAL_BASE_URL}/tokens", method="GET", timeout=5)
        return response.ok
    except Exception:
        return False


def get_local_tokens() -> LocalTokens:
    try:
        response = tinybird_fetch(f"{LOCAL_BASE_URL}/tokens", method="GET", timeout=5)
        if not response.ok:
            raise LocalApiError(f"Failed to get local tokens: {response.status_code}", response.status_code, response.text)
        data = response.json()
        if not data.get("user_token") or not data.get("admin_token") or not data.get("workspace_admin_token"):
            raise LocalApiError("Invalid tokens response from local Tinybird - missing required fields")
        return LocalTokens(**data)
    except LocalApiError:
        raise
    except Exception as error:
        raise LocalNotRunningError(
            "Tinybird local is not running. Start it with:\n"
            "docker run -d -p 7181:7181 --name tinybird-local tinybirdco/tinybird-local:latest"
        ) from error


def list_local_workspaces(admin_token: str) -> dict[str, Any]:
    query = urlencode({"with_organization": "true", "token": admin_token})
    response = tinybird_fetch(f"{LOCAL_BASE_URL}/v1/user/workspaces?{query}", method="GET")
    if not response.ok:
        raise LocalApiError(f"Failed to list local workspaces: {response.status_code}", response.status_code, response.text)
    data = response.json()
    return {
        "workspaces": [LocalWorkspace(**workspace) for workspace in data.get("workspaces", [])],
        "organization_id": data.get("organization_id"),
    }


def create_local_workspace(user_token: str, workspace_name: str, organization_id: str | None = None) -> LocalWorkspace:
    body = {"name": workspace_name}
    if organization_id:
        body["assign_to_organization_id"] = organization_id

    response = tinybird_fetch(
        f"{LOCAL_BASE_URL}/v1/workspaces",
        method="POST",
        headers={
            "Authorization": f"Bearer {user_token}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        body=urlencode(body),
    )
    if not response.ok:
        raise LocalApiError(
            f"Failed to create local workspace: {response.status_code}",
            response.status_code,
            response.text,
        )
    return LocalWorkspace(**response.json())


def get_or_create_local_workspace(tokens: LocalTokens, workspace_name: str) -> dict[str, Any]:
    listed = list_local_workspaces(tokens.admin_token)
    workspaces: list[LocalWorkspace] = listed["workspaces"]

    existing = next((workspace for workspace in workspaces if workspace.name == workspace_name), None)
    if existing:
        return {"workspace": existing, "was_created": False}

    create_local_workspace(tokens.user_token, workspace_name, listed.get("organization_id"))

    refreshed = list_local_workspaces(tokens.admin_token)["workspaces"]
    created = next((workspace for workspace in refreshed if workspace.name == workspace_name), None)
    if not created:
        raise LocalApiError(f"Created workspace '{workspace_name}' but could not find it in workspace list")

    return {"workspace": created, "was_created": True}


def get_local_workspace_name(tinybird_branch: str | None, cwd: str) -> str:
    if tinybird_branch:
        return tinybird_branch

    digest = hashlib.sha256(cwd.encode("utf-8")).hexdigest()
    return f"Build_{digest[:16]}"


def delete_local_workspace(user_token: str, workspace_id: str) -> None:
    response = tinybird_fetch(
        f"{LOCAL_BASE_URL}/v1/workspaces/{workspace_id}?hard_delete_confirmation=yes",
        method="DELETE",
        headers={"Authorization": f"Bearer {user_token}"},
    )
    if not response.ok:
        raise LocalApiError(
            f"Failed to delete local workspace: {response.status_code}",
            response.status_code,
            response.text,
        )


def clear_local_workspace(tokens: LocalTokens, workspace_name: str) -> LocalWorkspace:
    listed = list_local_workspaces(tokens.admin_token)
    workspaces: list[LocalWorkspace] = listed["workspaces"]

    current = next((workspace for workspace in workspaces if workspace.name == workspace_name), None)
    if not current:
        raise LocalApiError(f"Workspace '{workspace_name}' not found")

    delete_local_workspace(tokens.user_token, current.id)
    create_local_workspace(tokens.user_token, workspace_name, listed.get("organization_id"))

    refreshed = list_local_workspaces(tokens.admin_token)["workspaces"]
    recreated = next((workspace for workspace in refreshed if workspace.name == workspace_name), None)
    if not recreated:
        raise LocalApiError(f"Workspace '{workspace_name}' was not recreated properly. Please try again.")

    return recreated
