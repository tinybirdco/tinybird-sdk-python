from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .._http import create_multipart_body
from .build import BuildConfig
from .fetcher import tinybird_fetch

if TYPE_CHECKING:
    from ..generator.index import GeneratedResources


@dataclass(frozen=True, slots=True)
class DeploymentFeedback:
    resource: str
    level: str
    message: str


@dataclass(frozen=True, slots=True)
class Deployment:
    id: str
    status: str
    live: bool | None = None
    created_at: str | None = None
    updated_at: str | None = None


def deploy_to_main(
    config: BuildConfig | dict[str, Any],
    resources: GeneratedResources,
    options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized = config if isinstance(config, BuildConfig) else BuildConfig(**config)
    options = options or {}
    debug = options.get("debug", False)
    poll_interval = options.get("poll_interval_ms", 1000) / 1000.0
    max_poll_attempts = options.get("max_poll_attempts", 120)

    files: list[tuple[str, str, bytes, str | None]] = []
    for ds in resources.datasources:
        files.append(("data_project://", f"{ds.name}.datasource", ds.content.encode(), "text/plain"))
    for pipe in resources.pipes:
        files.append(("data_project://", f"{pipe.name}.pipe", pipe.content.encode(), "text/plain"))

    content_type, body = create_multipart_body(files=files)

    base_url = normalized.base_url.rstrip("/")
    deploy_url = f"{base_url}/v1/deploy"

    query: list[str] = []
    if options.get("check"):
        query.append("check=true")
    if options.get("allow_destructive_operations"):
        query.append("allow_destructive_operations=true")
    if query:
        deploy_url = f"{deploy_url}?{'&'.join(query)}"

    response = tinybird_fetch(
        deploy_url,
        method="POST",
        headers={
            "Authorization": f"Bearer {normalized.token}",
            "Content-Type": content_type,
        },
        body=body,
    )

    try:
        parsed = json.loads(response.text or "{}")
    except json.JSONDecodeError as error:
        raise ValueError(
            f"Failed to parse response from Tinybird API: {response.status_code} {response.text}"
        ) from error

    def _format_errors() -> str:
        feedback = (parsed.get("deployment") or {}).get("feedback") or []
        errors = [item for item in feedback if item.get("level") == "ERROR"]
        if errors:
            return "\n".join(
                f"{(item.get('resource', '').split(' ')[-1] or item.get('resource'))}: {item.get('message')}"
                for item in errors
            )
        api_errors = parsed.get("errors") or []
        if api_errors:
            return "\n".join(
                f"[{item.get('filename')}] {item.get('error')}" if item.get("filename") else str(item.get("error"))
                for item in api_errors
            )
        if parsed.get("error"):
            return str(parsed["error"])
        return f"HTTP {response.status_code}: {response.text}"

    if not response.ok:
        return {
            "success": False,
            "result": "failed",
            "error": _format_errors(),
            "datasource_count": len(resources.datasources),
            "pipe_count": len(resources.pipes),
            "connection_count": len(resources.connections),
        }

    if options.get("check"):
        if parsed.get("result") == "failed":
            return {
                "success": False,
                "result": "failed",
                "error": _format_errors(),
                "datasource_count": len(resources.datasources),
                "pipe_count": len(resources.pipes),
                "connection_count": len(resources.connections),
            }

        return {
            "success": True,
            "result": parsed.get("result", "success"),
            "datasource_count": len(resources.datasources),
            "pipe_count": len(resources.pipes),
            "connection_count": len(resources.connections),
        }

    if parsed.get("result") == "no_changes":
        return {
            "success": True,
            "result": "no_changes",
            "datasource_count": len(resources.datasources),
            "pipe_count": len(resources.pipes),
            "connection_count": len(resources.connections),
            "pipes": {"changed": [], "created": [], "deleted": []},
            "datasources": {"changed": [], "created": [], "deleted": []},
        }

    if parsed.get("result") == "failed" or not parsed.get("deployment"):
        return {
            "success": False,
            "result": "failed",
            "error": _format_errors(),
            "datasource_count": len(resources.datasources),
            "pipe_count": len(resources.pipes),
            "connection_count": len(resources.connections),
        }

    deployment = parsed["deployment"]
    deployment_id = deployment["id"]

    for _ in range(max_poll_attempts):
        if deployment.get("status") == "data_ready":
            break

        time.sleep(poll_interval)

        status_response = tinybird_fetch(
            f"{base_url}/v1/deployments/{deployment_id}",
            method="GET",
            headers={"Authorization": f"Bearer {normalized.token}"},
        )
        if not status_response.ok:
            return {
                "success": False,
                "result": "failed",
                "error": f"Failed to check deployment status: {status_response.status_code}",
                "datasource_count": len(resources.datasources),
                "pipe_count": len(resources.pipes),
                "connection_count": len(resources.connections),
                "build_id": deployment_id,
            }

        deployment = status_response.json().get("deployment", deployment)
        if debug:
            print(f"[debug] deployment status: {deployment.get('status')}")

        if deployment.get("status") in {"failed", "error"}:
            return {
                "success": False,
                "result": "failed",
                "error": f"Deployment failed with status: {deployment.get('status')}",
                "datasource_count": len(resources.datasources),
                "pipe_count": len(resources.pipes),
                "connection_count": len(resources.connections),
                "build_id": deployment_id,
            }

    if deployment.get("status") != "data_ready":
        return {
            "success": False,
            "result": "failed",
            "error": f"Deployment timed out after {max_poll_attempts} attempts. Last status: {deployment.get('status')}",
            "datasource_count": len(resources.datasources),
            "pipe_count": len(resources.pipes),
            "connection_count": len(resources.connections),
            "build_id": deployment_id,
        }

    set_live_response = tinybird_fetch(
        f"{base_url}/v1/deployments/{deployment_id}/set-live",
        method="POST",
        headers={"Authorization": f"Bearer {normalized.token}"},
    )

    if not set_live_response.ok:
        return {
            "success": False,
            "result": "failed",
            "error": f"Failed to set deployment as live: {set_live_response.status_code} {set_live_response.text}",
            "datasource_count": len(resources.datasources),
            "pipe_count": len(resources.pipes),
            "connection_count": len(resources.connections),
            "build_id": deployment_id,
        }

    return {
        "success": True,
        "result": "success",
        "datasource_count": len(resources.datasources),
        "pipe_count": len(resources.pipes),
        "connection_count": len(resources.connections),
        "build_id": deployment_id,
        "pipes": {
            "changed": deployment.get("changed_pipe_names", []),
            "created": deployment.get("new_pipe_names", []),
            "deleted": deployment.get("deleted_pipe_names", []),
        },
        "datasources": {
            "changed": deployment.get("changed_datasource_names", []),
            "created": deployment.get("new_datasource_names", []),
            "deleted": deployment.get("deleted_datasource_names", []),
        },
    }
