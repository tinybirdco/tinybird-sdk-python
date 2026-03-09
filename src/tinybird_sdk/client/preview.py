from __future__ import annotations

import os
import sys
from typing import Any

from .._http import normalize_base_url
from ..api.fetcher import tinybird_fetch

_cached_branch_token: str | None = None
_cached_branch_name: str | None = None


def is_preview_environment() -> bool:
    return bool(
        os.getenv("VERCEL_ENV") == "preview"
        or os.getenv("GITHUB_HEAD_REF")
        or os.getenv("CI_MERGE_REQUEST_SOURCE_BRANCH_NAME")
        or (os.getenv("CI") and os.getenv("TINYBIRD_PREVIEW_MODE") == "true")
    )


def get_preview_branch_name() -> str | None:
    if os.getenv("TINYBIRD_BRANCH_NAME"):
        return os.environ["TINYBIRD_BRANCH_NAME"]
    if os.getenv("VERCEL_GIT_COMMIT_REF"):
        return os.environ["VERCEL_GIT_COMMIT_REF"]
    if os.getenv("GITHUB_HEAD_REF"):
        return os.environ["GITHUB_HEAD_REF"]
    if os.getenv("GITHUB_REF_NAME"):
        return os.environ["GITHUB_REF_NAME"]
    if os.getenv("CI_MERGE_REQUEST_SOURCE_BRANCH_NAME"):
        return os.environ["CI_MERGE_REQUEST_SOURCE_BRANCH_NAME"]
    if os.getenv("CI_COMMIT_BRANCH"):
        return os.environ["CI_COMMIT_BRANCH"]
    if os.getenv("CIRCLE_BRANCH"):
        return os.environ["CIRCLE_BRANCH"]
    if os.getenv("BUILD_SOURCEBRANCHNAME"):
        return os.environ["BUILD_SOURCEBRANCHNAME"]
    if os.getenv("BITBUCKET_BRANCH"):
        return os.environ["BITBUCKET_BRANCH"]
    return None


def _sanitize_branch_name(branch_name: str) -> str:
    import re

    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", branch_name)
    sanitized = re.sub(r"_+", "_", sanitized)
    return sanitized.strip("_")


def _fetch_branch_token(base_url: str, workspace_token: str, branch_name: str) -> str | None:
    sanitized = _sanitize_branch_name(branch_name)
    preview_branch_name = f"tmp_ci_{sanitized}"
    url = f"{normalize_base_url(base_url)}/v0/environments/{preview_branch_name}?with_token=true"

    try:
        response = tinybird_fetch(
            url,
            method="GET",
            headers={"Authorization": f"Bearer {workspace_token}"},
        )
        if not response.ok:
            return None
        return response.json().get("token")
    except Exception:
        return None


def resolve_token(options: dict[str, Any] | None = None) -> str:
    options = options or {}

    branch_token = os.getenv("TINYBIRD_BRANCH_TOKEN")
    if branch_token:
        return branch_token

    configured_token = options.get("token") or os.getenv("TINYBIRD_TOKEN")
    if not configured_token:
        raise ValueError(
            "TINYBIRD_TOKEN is not configured. Set it in your environment or pass it to Tinybird(...) or create_client()."
        )

    if is_preview_environment():
        branch_name = get_preview_branch_name()
        if branch_name:
            global _cached_branch_token, _cached_branch_name

            if _cached_branch_token and _cached_branch_name == branch_name:
                return _cached_branch_token

            base_url = options.get("base_url") or os.getenv("TINYBIRD_URL") or "https://api.tinybird.co"
            branch_token = _fetch_branch_token(base_url, configured_token, branch_name)
            if branch_token:
                _cached_branch_token = branch_token
                _cached_branch_name = branch_name
                return branch_token

            expected = f"tmp_ci_{_sanitize_branch_name(branch_name)}"
            print(
                f"[tinybird] Preview branch '{expected}' not found. Run 'tinybird preview' to create it. Falling back to workspace token.",
                file=sys.stderr,
            )

    return configured_token


def clear_token_cache() -> None:
    global _cached_branch_token, _cached_branch_name
    _cached_branch_token = None
    _cached_branch_name = None
