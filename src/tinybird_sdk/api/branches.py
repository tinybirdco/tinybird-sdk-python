from __future__ import annotations

import time
from dataclasses import asdict
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

from .fetcher import tinybird_fetch


@dataclass(frozen=True, slots=True)
class BranchApiConfig:
    base_url: str
    token: str


@dataclass(frozen=True, slots=True)
class TinybirdBranch:
    id: str
    name: str
    created_at: str
    token: str | None = None


class BranchApiError(Exception):
    def __init__(self, message: str, status: int, body: Any = None):
        super().__init__(message)
        self.status = status
        self.body = body


def _headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _poll_job(config: BranchApiConfig, job_id: str, max_attempts: int = 120, interval_ms: int = 1000) -> None:
    for _ in range(max_attempts):
        response = tinybird_fetch(
            f"{config.base_url.rstrip('/')}/v0/jobs/{job_id}",
            method="GET",
            headers=_headers(config.token),
        )
        if not response.ok:
            raise BranchApiError(
                f"Failed to poll job '{job_id}': {response.status_code}",
                response.status_code,
                response.text,
            )

        body = response.json()
        status = body.get("status")
        if status == "done":
            return
        if status == "error":
            raise BranchApiError(
                f"Job '{job_id}' failed: {body.get('error', 'Unknown error')}",
                500,
                body,
            )

        time.sleep(interval_ms / 1000)

    raise BranchApiError(f"Job '{job_id}' timed out after {max_attempts} attempts", 408)


def create_branch(config: BranchApiConfig | dict[str, Any], name: str) -> TinybirdBranch:
    normalized = config if isinstance(config, BranchApiConfig) else BranchApiConfig(**config)
    url = f"{normalized.base_url.rstrip('/')}/v1/environments?{urlencode({'name': name})}"
    response = tinybird_fetch(url, method="POST", headers=_headers(normalized.token))

    if not response.ok:
        body = response.text
        if response.status_code == 403:
            message = (
                f"Permission denied creating branch '{name}'. "
                "Make sure TINYBIRD_TOKEN is a workspace admin token (not a branch token). "
                f"API response: {body}"
            )
        elif response.status_code == 409:
            message = f"Branch '{name}' already exists."
        else:
            message = f"Failed to create branch '{name}': {response.status_code}. API response: {body}"
        raise BranchApiError(message, response.status_code, body)

    job = response.json().get("job", {})
    job_id = job.get("id")
    if not job_id:
        raise BranchApiError("Unexpected response from branch creation: no job ID returned", 500, response.json())

    _poll_job(normalized, job_id)
    return get_branch(normalized, name)


def list_branches(config: BranchApiConfig | dict[str, Any]) -> list[TinybirdBranch]:
    normalized = config if isinstance(config, BranchApiConfig) else BranchApiConfig(**config)
    response = tinybird_fetch(
        f"{normalized.base_url.rstrip('/')}/v1/environments",
        method="GET",
        headers=_headers(normalized.token),
    )
    if not response.ok:
        raise BranchApiError(
            f"Failed to list branches: {response.status_code}",
            response.status_code,
            response.text,
        )

    items = response.json().get("environments", [])
    return [TinybirdBranch(**item) for item in items]


def get_branch(config: BranchApiConfig | dict[str, Any], name: str) -> TinybirdBranch:
    normalized = config if isinstance(config, BranchApiConfig) else BranchApiConfig(**config)
    url = f"{normalized.base_url.rstrip('/')}/v0/environments/{name}?with_token=true"
    response = tinybird_fetch(url, method="GET", headers=_headers(normalized.token))
    if not response.ok:
        raise BranchApiError(
            f"Failed to get branch '{name}': {response.status_code}",
            response.status_code,
            response.text,
        )
    return TinybirdBranch(**response.json())


def delete_branch(config: BranchApiConfig | dict[str, Any], name: str) -> None:
    normalized = config if isinstance(config, BranchApiConfig) else BranchApiConfig(**config)
    branch = get_branch(normalized, name)
    response = tinybird_fetch(
        f"{normalized.base_url.rstrip('/')}/v0/environments/{branch.id}",
        method="DELETE",
        headers=_headers(normalized.token),
    )
    if not response.ok:
        raise BranchApiError(
            f"Failed to delete branch '{name}': {response.status_code}",
            response.status_code,
            response.text,
        )


def branch_exists(config: BranchApiConfig | dict[str, Any], name: str) -> bool:
    branches = list_branches(config)
    return any(branch.name == name for branch in branches)


def get_or_create_branch(config: BranchApiConfig | dict[str, Any], name: str) -> dict[str, Any]:
    normalized = config if isinstance(config, BranchApiConfig) else BranchApiConfig(**config)
    try:
        branch = get_branch(normalized, name)
        return {**asdict(branch), "was_created": False}
    except BranchApiError as error:
        if error.status == 404:
            branch = create_branch(normalized, name)
            return {**asdict(branch), "was_created": True}
        raise


def clear_branch(config: BranchApiConfig | dict[str, Any], name: str) -> TinybirdBranch:
    normalized = config if isinstance(config, BranchApiConfig) else BranchApiConfig(**config)
    delete_branch(normalized, name)
    return create_branch(normalized, name)
