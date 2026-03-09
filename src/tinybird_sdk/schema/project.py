from __future__ import annotations

from dataclasses import dataclass, field
import os
from typing import TYPE_CHECKING, Any, Callable

from .datasource import DatasourceDefinition
from .pipe import PipeDefinition, get_endpoint_config
from .connection import ConnectionDefinition

if TYPE_CHECKING:
    from ..client.base import TinybirdClient
    from ..client.types import QueryResult


DatasourcesDefinition = dict[str, DatasourceDefinition]
PipesDefinition = dict[str, PipeDefinition]
ConnectionsDefinition = dict[str, ConnectionDefinition]


@dataclass(frozen=True, slots=True)
class ProjectConfig:
    datasources: DatasourcesDefinition = field(default_factory=dict)
    pipes: PipesDefinition = field(default_factory=dict)
    connections: ConnectionsDefinition = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TinybirdClientConfig:
    datasources: DatasourcesDefinition
    pipes: PipesDefinition
    base_url: str | None = None
    token: str | None = None
    config_dir: str | None = None
    dev_mode: bool | None = None


class _DatasourceAccessor:
    def __init__(self, get_client: Callable[[], TinybirdClient], datasource_name: str):
        self._get_client = get_client
        self._datasource_name = datasource_name

    def ingest(self, event: dict[str, Any]) -> dict[str, Any]:
        return self._get_client().datasources.ingest(self._datasource_name, event)

    def append(self, options: dict[str, Any]) -> dict[str, Any]:
        return self._get_client().datasources.append(self._datasource_name, options)

    def replace(self, options: dict[str, Any]) -> dict[str, Any]:
        return self._get_client().datasources.replace(self._datasource_name, options)

    def delete(self, options: dict[str, Any]) -> dict[str, Any]:
        return self._get_client().datasources.delete(self._datasource_name, options)

    def truncate(self, options: dict[str, Any] | None = None) -> dict[str, Any]:
        return self._get_client().datasources.truncate(self._datasource_name, options or {})


class _PipeAccessor:
    def __init__(self, get_client: Callable[[], TinybirdClient], pipe_key: str, pipe_name: str, endpoint_enabled: bool):
        self._get_client = get_client
        self._pipe_key = pipe_key
        self._pipe_name = pipe_name
        self._endpoint_enabled = endpoint_enabled

    def query(self, params: dict[str, Any] | None = None) -> QueryResult:
        if not self._endpoint_enabled:
            raise ValueError(
                f'Pipe "{self._pipe_key}" is not exposed as an endpoint. Set "endpoint: true" in the pipe definition to enable querying.'
            )
        return self._get_client().query(self._pipe_name, params or {})


class Tinybird:
    _RESERVED_NAMES = {"tokens", "datasources", "sql", "client"}

    def __init__(self, config: dict[str, Any] | TinybirdClientConfig):
        if isinstance(config, TinybirdClientConfig):
            normalized = config
        else:
            normalized = TinybirdClientConfig(
                datasources=config["datasources"],
                pipes=config["pipes"],
                base_url=config.get("base_url"),
                token=config.get("token"),
                config_dir=config.get("config_dir"),
                dev_mode=config.get("dev_mode"),
            )

        datasources = normalized.datasources
        pipes = normalized.pipes

        self.__options = {
            "base_url": normalized.base_url,
            "token": normalized.token,
            "config_dir": normalized.config_dir,
            "dev_mode": normalized.dev_mode,
        }
        self.__client: TinybirdClient | None = None

        for name in datasources:
            if name in self._RESERVED_NAMES:
                raise ValueError(
                    f'Name conflict: "{name}" is reserved by the client API. Rename this datasource to expose it as a top-level client property.'
                )

        for name in pipes:
            if name in datasources:
                raise ValueError(
                    f'Name conflict: "{name}" is defined as both datasource and pipe. Rename one of them to expose both as top-level client properties.'
                )
            if name in self._RESERVED_NAMES:
                raise ValueError(
                    f'Name conflict: "{name}" is reserved by the client API. Rename this pipe to expose it as a top-level client property.'
                )

        get_client = self.__get_client
        for key, datasource in datasources.items():
            setattr(self, key, _DatasourceAccessor(get_client, datasource._name))

        for key, pipe in pipes.items():
            endpoint_config = get_endpoint_config(pipe)
            setattr(
                self,
                key,
                _PipeAccessor(
                    get_client,
                    key,
                    pipe._name,
                    endpoint_enabled=endpoint_config is not None,
                ),
            )

    def __get_client(self) -> TinybirdClient:
        if self.__client is None:
            from ..client.base import create_client
            from ..client.preview import resolve_token

            base_url = self.__options["base_url"] or "https://api.tinybird.co"
            resolved_token = resolve_token({"base_url": base_url, "token": self.__options["token"]})
            dev_mode = (
                self.__options["dev_mode"]
                if self.__options["dev_mode"] is not None
                else os.getenv("NODE_ENV") == "development"
            )
            self.__client = create_client(
                {
                    "base_url": base_url,
                    "token": resolved_token,
                    "dev_mode": dev_mode,
                    "config_dir": self.__options["config_dir"],
                }
            )
        return self.__client

    @property
    def tokens(self):
        if self.__client is None:
            raise ValueError(
                "Client not initialized. Call a query or ingest method first, or access client asynchronously."
            )
        return self.__client.tokens

    @property
    def datasources(self):
        if self.__client is None:
            raise ValueError(
                "Client not initialized. Call a query or ingest method first, or access client asynchronously."
            )
        return self.__client.datasources

    @property
    def client(self) -> TinybirdClient:
        if self.__client is None:
            raise ValueError(
                "Client not initialized. Call a query or ingest method first, or access client asynchronously."
            )
        return self.__client

    def sql(self, sql_query: str, options: dict[str, Any] | None = None) -> QueryResult:
        return self.__get_client().sql(sql_query, options or {})


@dataclass(frozen=True, slots=True)
class ProjectDefinition:
    datasources: DatasourcesDefinition
    pipes: PipesDefinition
    connections: ConnectionsDefinition
    tinybird: Tinybird
    _type: str = "project"


def define_project(config: dict[str, Any] | ProjectConfig) -> ProjectDefinition:
    if isinstance(config, ProjectConfig):
        datasources = config.datasources
        pipes = config.pipes
        connections = config.connections
    else:
        datasources = config.get("datasources", {}) or {}
        pipes = config.get("pipes", {}) or {}
        connections = config.get("connections", {}) or {}

    tinybird = Tinybird({"datasources": datasources, "pipes": pipes})

    return ProjectDefinition(
        datasources=datasources,
        pipes=pipes,
        connections=connections,
        tinybird=tinybird,
    )


def is_project_definition(value: Any) -> bool:
    return isinstance(value, ProjectDefinition)


def get_datasource_names(project: ProjectDefinition) -> list[str]:
    return list(project.datasources.keys())


def get_pipe_names(project: ProjectDefinition) -> list[str]:
    return list(project.pipes.keys())


def get_datasource(project: ProjectDefinition, name: str) -> DatasourceDefinition:
    return project.datasources[name]


def get_pipe(project: ProjectDefinition, name: str) -> PipeDefinition:
    return project.pipes[name]
