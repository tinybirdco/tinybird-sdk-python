from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .._http import create_multipart_body
from .fetcher import tinybird_fetch

if TYPE_CHECKING:
    from ..generator.index import GeneratedResources


@dataclass(frozen=True, slots=True)
class BuildConfig:
    base_url: str
    token: str


class BuildError(dict):
    pass


def build_to_tinybird(
    config: BuildConfig | dict[str, Any],
    resources: GeneratedResources,
    options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized = config if isinstance(config, BuildConfig) else BuildConfig(**config)
    debug = (options or {}).get("debug", False)

    files: list[tuple[str, str, bytes, str | None]] = []
    for ds in resources.datasources:
        files.append(("data_project://", f"{ds.name}.datasource", ds.content.encode("utf-8"), "text/plain"))
    for pipe in resources.pipes:
        files.append(("data_project://", f"{pipe.name}.pipe", pipe.content.encode("utf-8"), "text/plain"))
    for conn in resources.connections:
        files.append(("data_project://", f"{conn.name}.connection", conn.content.encode("utf-8"), "text/plain"))

    content_type, body = create_multipart_body(files=files)
    url = f"{normalized.base_url.rstrip('/')}/v1/build"

    response = tinybird_fetch(
        url,
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
            f"Failed to parse response from Tinybird API: {response.status_code}. Body: {response.text}"
        ) from error

    def _format_errors() -> str:
        if parsed.get("errors"):
            messages = []
            for item in parsed["errors"]:
                prefix = f"[{item.get('filename')}] " if item.get("filename") else ""
                messages.append(f"{prefix}{item.get('error')}")
            return "\n".join(messages)
        return parsed.get("error") or f"HTTP {response.status_code}"

    if not response.ok:
        return {
            "success": False,
            "result": "failed",
            "error": _format_errors(),
            "errors": parsed.get("errors"),
            "datasource_count": len(resources.datasources),
            "pipe_count": len(resources.pipes),
            "connection_count": len(resources.connections),
        }

    if parsed.get("result") == "failed":
        return {
            "success": False,
            "result": "failed",
            "error": _format_errors(),
            "errors": parsed.get("errors"),
            "datasource_count": len(resources.datasources),
            "pipe_count": len(resources.pipes),
            "connection_count": len(resources.connections),
        }

    if debug:
        print(f"[debug] /v1/build response: {parsed}")

    build = parsed.get("build", {})
    return {
        "success": True,
        "result": parsed.get("result", "success"),
        "datasource_count": len(resources.datasources),
        "pipe_count": len(resources.pipes),
        "connection_count": len(resources.connections),
        "build_id": build.get("id"),
        "pipes": {
            "changed": build.get("changed_pipe_names", []),
            "created": build.get("new_pipe_names", []),
            "deleted": build.get("deleted_pipe_names", []),
        },
        "datasources": {
            "changed": build.get("changed_datasource_names", []),
            "created": build.get("new_datasource_names", []),
            "deleted": build.get("deleted_datasource_names", []),
        },
        "changed_pipe_names": build.get("changed_pipe_names", []),
        "new_pipe_names": build.get("new_pipe_names", []),
    }


def validate_build_config(config: dict[str, Any]) -> None:
    if not config.get("base_url"):
        raise ValueError("Missing base_url in configuration")
    if not config.get("token"):
        raise ValueError("Missing token in configuration")
