from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..schema.project import ConnectionsDefinition, DatasourcesDefinition, PipesDefinition, ProjectDefinition
from .client import GenerateClientOptions, GeneratedClient, generate_client_file
from .connection import GeneratedConnection, generate_all_connections
from .datasource import GeneratedDatasource, generate_all_datasources
from .loader import (
    LoadEntitiesOptions,
    LoadedEntities,
    LoadedSchema,
    LoaderOptions,
    entities_to_project,
    load_entities,
    load_schema,
)
from .pipe import GeneratedPipe, generate_all_pipes


@dataclass(frozen=True, slots=True)
class GeneratedResources:
    datasources: list[GeneratedDatasource]
    pipes: list[GeneratedPipe]
    connections: list[GeneratedConnection]


@dataclass(frozen=True, slots=True)
class BuildResult:
    resources: GeneratedResources
    project: ProjectDefinition
    schema_path: str
    schema_dir: str
    stats: dict[str, int]


@dataclass(frozen=True, slots=True)
class BuildOptions:
    schema_path: str
    cwd: str | None = None


def generate_resources(project: ProjectDefinition) -> GeneratedResources:
    return GeneratedResources(
        datasources=generate_all_datasources(project.datasources),
        pipes=generate_all_pipes(project.pipes),
        connections=generate_all_connections(project.connections),
    )


def build(options: BuildOptions | dict[str, Any]) -> BuildResult:
    normalized = options if isinstance(options, BuildOptions) else BuildOptions(**options)
    loaded: LoadedSchema = load_schema(
        LoaderOptions(schema_path=normalized.schema_path, cwd=normalized.cwd)
    )
    resources = generate_resources(loaded.project)

    return BuildResult(
        resources=resources,
        project=loaded.project,
        schema_path=loaded.schema_path,
        schema_dir=loaded.schema_dir,
        stats={
            "datasource_count": len(resources.datasources),
            "pipe_count": len(resources.pipes),
            "connection_count": len(resources.connections),
        },
    )


@dataclass(frozen=True, slots=True)
class BuildFromIncludeOptions:
    include_paths: list[str]
    cwd: str | None = None


@dataclass(frozen=True, slots=True)
class BuildFromIncludeResult:
    resources: GeneratedResources
    entities: LoadedEntities
    stats: dict[str, int]


def generate_resources_from_entities(
    datasources: DatasourcesDefinition,
    pipes: PipesDefinition,
    connections: ConnectionsDefinition | None = None,
) -> GeneratedResources:
    return GeneratedResources(
        datasources=generate_all_datasources(datasources),
        pipes=generate_all_pipes(pipes),
        connections=generate_all_connections(connections or {}),
    )


def build_from_include(options: BuildFromIncludeOptions | dict[str, Any]) -> BuildFromIncludeResult:
    normalized = options if isinstance(options, BuildFromIncludeOptions) else BuildFromIncludeOptions(**options)

    entities = load_entities(
        LoadEntitiesOptions(include_paths=normalized.include_paths, cwd=normalized.cwd)
    )

    project_parts = entities_to_project(entities)
    resources = generate_resources_from_entities(
        project_parts["datasources"],
        project_parts["pipes"],
        project_parts["connections"],
    )

    for raw in entities.raw_datasources:
        resources.datasources.append(GeneratedDatasource(name=raw.name, content=raw.content))

    for raw in entities.raw_pipes:
        resources.pipes.append(GeneratedPipe(name=raw.name, content=raw.content))

    return BuildFromIncludeResult(
        resources=resources,
        entities=entities,
        stats={
            "datasource_count": len(resources.datasources),
            "pipe_count": len(resources.pipes),
            "connection_count": len(resources.connections),
        },
    )


__all__ = [
    "GeneratedResources",
    "BuildResult",
    "BuildOptions",
    "generate_resources",
    "build",
    "BuildFromIncludeOptions",
    "BuildFromIncludeResult",
    "generate_resources_from_entities",
    "build_from_include",
    "load_schema",
    "load_entities",
    "entities_to_project",
    "LoaderOptions",
    "LoadedSchema",
    "LoadedEntities",
    "LoadEntitiesOptions",
    "generate_client_file",
    "GenerateClientOptions",
    "GeneratedClient",
]
