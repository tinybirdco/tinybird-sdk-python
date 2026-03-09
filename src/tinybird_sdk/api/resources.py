from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal
from urllib.parse import quote

from .fetcher import tinybird_fetch
from .workspaces import WorkspaceApiConfig


class ResourceApiError(Exception):
    def __init__(self, message: str, status: int, endpoint: str, body: Any = None):
        super().__init__(message)
        self.status = status
        self.endpoint = endpoint
        self.body = body


@dataclass(frozen=True, slots=True)
class DatasourceColumn:
    name: str
    type: str
    jsonpath: str | None = None
    default: str | None = None
    codec: str | None = None


@dataclass(frozen=True, slots=True)
class DatasourceEngine:
    type: str
    sorting_key: str | None = None
    partition_key: str | None = None
    primary_key: str | None = None
    ttl: str | None = None
    ver: str | None = None
    sign: str | None = None
    version: str | None = None
    summing_columns: str | None = None


@dataclass(frozen=True, slots=True)
class DatasourceInfo:
    name: str
    columns: list[DatasourceColumn]
    engine: DatasourceEngine
    description: str | None = None
    forward_query: str | None = None


@dataclass(frozen=True, slots=True)
class PipeNode:
    name: str
    sql: str
    description: str | None = None


@dataclass(frozen=True, slots=True)
class PipeParam:
    name: str
    type: str
    required: bool
    default: str | int | None = None
    description: str | None = None


PipeType = Literal["endpoint", "materialized", "copy", "pipe"]
ResourceFileType = Literal["datasource", "pipe", "connection"]


@dataclass(frozen=True, slots=True)
class PipeInfo:
    name: str
    nodes: list[PipeNode]
    params: list[PipeParam]
    type: PipeType
    output_columns: list[DatasourceColumn]
    description: str | None = None
    endpoint: dict[str, Any] | None = None
    materialized: dict[str, Any] | None = None
    copy: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class ResourceFile:
    name: str
    type: ResourceFileType
    filename: str
    content: str


def _headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _handle_response(response, endpoint: str) -> Any:
    if response.status_code == 401:
        raise ResourceApiError("Invalid or expired token", 401, endpoint)
    if response.status_code == 403:
        raise ResourceApiError("Insufficient permissions to access resources", 403, endpoint)
    if response.status_code == 404:
        raise ResourceApiError("Resource not found", 404, endpoint)
    if not response.ok:
        raise ResourceApiError(
            f"API request failed: {response.status_code}",
            response.status_code,
            endpoint,
            response.text,
        )
    return response.json()


def _handle_text_response(response, endpoint: str) -> str:
    if response.status_code == 401:
        raise ResourceApiError("Invalid or expired token", 401, endpoint)
    if response.status_code == 403:
        raise ResourceApiError("Insufficient permissions to access resources", 403, endpoint)
    if response.status_code == 404:
        raise ResourceApiError("Resource not found", 404, endpoint)
    if not response.ok:
        raise ResourceApiError(
            f"API request failed: {response.status_code}",
            response.status_code,
            endpoint,
            response.text,
        )
    return response.text


def _parse_engine_type(engine_string: str | None) -> str:
    if not engine_string:
        return "MergeTree"
    return engine_string.split("(", 1)[0]


def _extract_names(payload: dict[str, Any], keys: list[str]) -> list[str]:
    for key in keys:
        value = payload.get(key)
        if not isinstance(value, list):
            continue

        names: list[str] = []
        for item in value:
            if isinstance(item, str):
                names.append(item)
                continue
            if isinstance(item, dict) and isinstance(item.get("name"), str):
                names.append(item["name"])
        return names

    return []


def _fetch_text_from_any_endpoint(config: WorkspaceApiConfig, endpoints: list[str]) -> str:
    last_not_found: ResourceApiError | None = None
    for endpoint in endpoints:
        response = tinybird_fetch(
            f"{config.base_url.rstrip('/')}{endpoint}",
            method="GET",
            headers=_headers(config.token),
        )
        if response.status_code == 404:
            last_not_found = ResourceApiError("Resource not found", 404, endpoint)
            continue
        return _handle_text_response(response, endpoint)

    raise last_not_found or ResourceApiError("Resource not found", 404, endpoints[0] if endpoints else "unknown")


def list_datasources(config: WorkspaceApiConfig | dict[str, Any]) -> list[str]:
    normalized = config if isinstance(config, WorkspaceApiConfig) else WorkspaceApiConfig(**config)
    response = tinybird_fetch(
        f"{normalized.base_url.rstrip('/')}/v0/datasources",
        method="GET",
        headers=_headers(normalized.token),
    )
    payload = _handle_response(response, "/v0/datasources")
    return _extract_names(payload, ["datasources"])


def get_datasource(config: WorkspaceApiConfig | dict[str, Any], name: str) -> DatasourceInfo:
    normalized = config if isinstance(config, WorkspaceApiConfig) else WorkspaceApiConfig(**config)
    endpoint = f"/v0/datasources/{name}"
    response = tinybird_fetch(
        f"{normalized.base_url.rstrip('/')}{endpoint}",
        method="GET",
        headers=_headers(normalized.token),
    )
    payload = _handle_response(response, endpoint)

    raw_columns = (payload.get("schema") or {}).get("columns") or payload.get("columns") or []
    engine = payload.get("engine") or {}

    return DatasourceInfo(
        name=payload["name"],
        description=payload.get("description"),
        columns=[
            DatasourceColumn(
                name=column["name"],
                type=column["type"],
                jsonpath=column.get("jsonpath"),
                default=column.get("default_value"),
                codec=column.get("codec"),
            )
            for column in raw_columns
        ],
        engine=DatasourceEngine(
            type=_parse_engine_type(engine.get("engine")),
            sorting_key=engine.get("sorting_key") or engine.get("engine_sorting_key") or payload.get("sorting_key"),
            partition_key=engine.get("partition_key") or engine.get("engine_partition_key") or payload.get("partition_key"),
            primary_key=engine.get("primary_key") or engine.get("engine_primary_key") or payload.get("primary_key"),
            ttl=payload.get("ttl"),
            ver=engine.get("engine_ver"),
            sign=engine.get("engine_sign"),
            version=engine.get("engine_version"),
            summing_columns=engine.get("engine_summing_columns"),
        ),
        forward_query=payload.get("forward_query"),
    )


def list_pipes(config: WorkspaceApiConfig | dict[str, Any]) -> list[str]:
    normalized = config if isinstance(config, WorkspaceApiConfig) else WorkspaceApiConfig(**config)
    response = tinybird_fetch(
        f"{normalized.base_url.rstrip('/')}/v0/pipes",
        method="GET",
        headers=_headers(normalized.token),
    )
    payload = _handle_response(response, "/v0/pipes")
    return _extract_names(payload, ["pipes"])


def list_pipes_v1(config: WorkspaceApiConfig | dict[str, Any]) -> list[str]:
    normalized = config if isinstance(config, WorkspaceApiConfig) else WorkspaceApiConfig(**config)
    endpoint = "/v1/pipes"
    response = tinybird_fetch(
        f"{normalized.base_url.rstrip('/')}{endpoint}",
        method="GET",
        headers=_headers(normalized.token),
    )
    if response.status_code == 404:
        return list_pipes(normalized)
    payload = _handle_response(response, endpoint)
    return _extract_names(payload, ["pipes", "data"])


def get_datasource_file(config: WorkspaceApiConfig | dict[str, Any], name: str) -> str:
    normalized = config if isinstance(config, WorkspaceApiConfig) else WorkspaceApiConfig(**config)
    encoded = quote(name, safe="")
    return _fetch_text_from_any_endpoint(
        normalized,
        [
            f"/v0/datasources/{encoded}.datasource",
            f"/v0/datasources/{encoded}?format=datasource",
        ],
    )


def get_pipe_file(config: WorkspaceApiConfig | dict[str, Any], name: str) -> str:
    normalized = config if isinstance(config, WorkspaceApiConfig) else WorkspaceApiConfig(**config)
    encoded = quote(name, safe="")
    return _fetch_text_from_any_endpoint(
        normalized,
        [
            f"/v1/pipes/{encoded}.pipe",
            f"/v0/pipes/{encoded}.pipe",
            f"/v1/pipes/{encoded}?format=pipe",
            f"/v0/pipes/{encoded}?format=pipe",
        ],
    )


def list_connectors(config: WorkspaceApiConfig | dict[str, Any]) -> list[str]:
    normalized = config if isinstance(config, WorkspaceApiConfig) else WorkspaceApiConfig(**config)
    endpoint = "/v0/connectors"
    response = tinybird_fetch(
        f"{normalized.base_url.rstrip('/')}{endpoint}",
        method="GET",
        headers=_headers(normalized.token),
    )
    if response.status_code == 404:
        return []
    payload = _handle_response(response, endpoint)
    return _extract_names(payload, ["connectors", "connections"])


def get_connector_file(config: WorkspaceApiConfig | dict[str, Any], name: str) -> str:
    normalized = config if isinstance(config, WorkspaceApiConfig) else WorkspaceApiConfig(**config)
    encoded = quote(name, safe="")
    return _fetch_text_from_any_endpoint(
        normalized,
        [
            f"/v0/connectors/{encoded}.connection",
            f"/v0/connectors/{encoded}?format=connection",
        ],
    )


def get_pipe(config: WorkspaceApiConfig | dict[str, Any], name: str) -> PipeInfo:
    normalized = config if isinstance(config, WorkspaceApiConfig) else WorkspaceApiConfig(**config)
    endpoint = f"/v0/pipes/{name}"
    response = tinybird_fetch(
        f"{normalized.base_url.rstrip('/')}{endpoint}",
        method="GET",
        headers=_headers(normalized.token),
    )
    payload = _handle_response(response, endpoint)

    pipe_type: PipeType = "pipe"
    if payload.get("endpoint"):
        pipe_type = "endpoint"
    elif payload.get("materialized_datasource"):
        pipe_type = "materialized"
    elif payload.get("copy_target_datasource"):
        pipe_type = "copy"

    nodes = [
        PipeNode(name=node["name"], sql=node["sql"], description=node.get("description"))
        for node in payload.get("nodes", [])
    ]

    params: list[PipeParam] = []
    seen: set[str] = set()
    for node in payload.get("nodes", []):
        for param in node.get("params", []):
            if param["name"] in seen:
                continue
            seen.add(param["name"])
            params.append(
                PipeParam(
                    name=param["name"],
                    type=param["type"],
                    default=param.get("default"),
                    required=param.get("required", True),
                    description=param.get("description"),
                )
            )

    output_columns: list[DatasourceColumn] = []
    if payload.get("nodes"):
        for column in payload["nodes"][-1].get("columns", []):
            output_columns.append(DatasourceColumn(name=column["name"], type=column["type"]))

    return PipeInfo(
        name=payload["name"],
        description=payload.get("description"),
        nodes=nodes,
        params=params,
        type=pipe_type,
        endpoint={"enabled": True} if pipe_type == "endpoint" else None,
        materialized={"datasource": payload["materialized_datasource"]}
        if payload.get("materialized_datasource")
        else None,
        copy={
            "target_datasource": payload["copy_target_datasource"],
            "copy_schedule": payload.get("copy_schedule"),
            "copy_mode": payload.get("copy_mode"),
        }
        if payload.get("copy_target_datasource")
        else None,
        output_columns=output_columns,
    )


def fetch_all_resources(config: WorkspaceApiConfig | dict[str, Any]) -> dict[str, Any]:
    normalized = config if isinstance(config, WorkspaceApiConfig) else WorkspaceApiConfig(**config)
    datasource_names = list_datasources(normalized)
    pipe_names = list_pipes(normalized)
    return {
        "datasources": [get_datasource(normalized, name) for name in datasource_names],
        "pipes": [get_pipe(normalized, name) for name in pipe_names],
    }


def pull_all_resource_files(config: WorkspaceApiConfig | dict[str, Any]) -> dict[str, list[ResourceFile]]:
    normalized = config if isinstance(config, WorkspaceApiConfig) else WorkspaceApiConfig(**config)
    datasource_names = list_datasources(normalized)
    pipe_names = list_pipes_v1(normalized)
    connector_names = list_connectors(normalized)

    datasources = [
        ResourceFile(
            name=name,
            type="datasource",
            filename=f"{name}.datasource",
            content=get_datasource_file(normalized, name),
        )
        for name in datasource_names
    ]
    pipes = [
        ResourceFile(
            name=name,
            type="pipe",
            filename=f"{name}.pipe",
            content=get_pipe_file(normalized, name),
        )
        for name in pipe_names
    ]
    connections = [
        ResourceFile(
            name=name,
            type="connection",
            filename=f"{name}.connection",
            content=get_connector_file(normalized, name),
        )
        for name in connector_names
    ]

    return {
        "datasources": datasources,
        "pipes": pipes,
        "connections": connections,
    }


def has_resources(config: WorkspaceApiConfig | dict[str, Any]) -> bool:
    normalized = config if isinstance(config, WorkspaceApiConfig) else WorkspaceApiConfig(**config)
    return len(list_datasources(normalized)) > 0 or len(list_pipes(normalized)) > 0
