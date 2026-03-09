from __future__ import annotations

from dataclasses import dataclass
import importlib.util
from pathlib import Path
import time
from types import ModuleType
from typing import Any, Callable

from ..schema.connection import ConnectionDefinition, is_connection_definition
from ..schema.datasource import DatasourceDefinition, is_datasource_definition
from ..schema.pipe import PipeDefinition, is_pipe_definition
from ..schema.project import (
    ConnectionsDefinition,
    DatasourcesDefinition,
    PipesDefinition,
    ProjectDefinition,
    is_project_definition,
)
from .include_paths import get_include_watch_directories, resolve_include_files


@dataclass(frozen=True, slots=True)
class LoadedSchema:
    project: ProjectDefinition
    schema_path: str
    schema_dir: str


@dataclass(frozen=True, slots=True)
class LoaderOptions:
    schema_path: str
    cwd: str | None = None


@dataclass(frozen=True, slots=True)
class EntityInfo:
    export_name: str
    source_file: str


@dataclass(frozen=True, slots=True)
class RawDatafile:
    name: str
    content: str
    source_file: str


@dataclass(frozen=True, slots=True)
class LoadEntitiesOptions:
    include_paths: list[str]
    cwd: str | None = None


@dataclass(frozen=True, slots=True)
class LoadedEntities:
    datasources: dict[str, dict[str, Any]]
    pipes: dict[str, dict[str, Any]]
    connections: dict[str, dict[str, Any]]
    raw_datasources: list[RawDatafile]
    raw_pipes: list[RawDatafile]
    source_files: list[str]


@dataclass(frozen=True, slots=True)
class WatchOptions:
    include_paths: list[str]
    cwd: str | None = None
    interval_ms: int = 500


@dataclass(frozen=True, slots=True)
class WatchController:
    stop: Callable[[], None]


def _load_python_module(file_path: Path) -> ModuleType:
    module_name = f"tinybird_user_schema_{int(time.time() * 1000)}_{file_path.stem}"
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if not spec or not spec.loader:
        raise ValueError(f"Unable to load module from {file_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_schema(options: LoaderOptions | dict[str, Any]) -> LoadedSchema:
    normalized = options if isinstance(options, LoaderOptions) else LoaderOptions(**options)
    cwd = Path(normalized.cwd or ".").resolve()
    schema_path = Path(normalized.schema_path)
    if not schema_path.is_absolute():
        schema_path = cwd / schema_path
    schema_path = schema_path.resolve()

    if not schema_path.exists():
        raise ValueError(f"Schema file not found: {schema_path}")
    if schema_path.suffix != ".py":
        raise ValueError(f"Schema must be a Python file: {schema_path}")

    module = _load_python_module(schema_path)

    project: ProjectDefinition | None = None
    default_export = getattr(module, "default", None)
    if default_export and is_project_definition(default_export):
        project = default_export
    elif is_project_definition(getattr(module, "project", None)):
        project = getattr(module, "project")
    else:
        for value in module.__dict__.values():
            if is_project_definition(value):
                project = value
                break

    if project is None:
        raise ValueError(
            f"No ProjectDefinition found in {schema_path}. Export `project` or a value created with define_project()."
        )

    return LoadedSchema(project=project, schema_path=str(schema_path), schema_dir=str(schema_path.parent))


def _is_raw_datafile(source_path: str) -> bool:
    return source_path.endswith(".datasource") or source_path.endswith(".pipe") or source_path.endswith(".connection")


def load_entities(options: LoadEntitiesOptions | dict[str, Any]) -> LoadedEntities:
    normalized = options if isinstance(options, LoadEntitiesOptions) else LoadEntitiesOptions(**options)
    cwd = Path(normalized.cwd or ".").resolve()
    include_files = resolve_include_files(normalized.include_paths, str(cwd))

    datasources: dict[str, dict[str, Any]] = {}
    pipes: dict[str, dict[str, Any]] = {}
    connections: dict[str, dict[str, Any]] = {}
    raw_datasources: list[RawDatafile] = []
    raw_pipes: list[RawDatafile] = []
    source_files: list[str] = []

    for include_file in include_files:
        source_files.append(include_file.source_path)

        if _is_raw_datafile(include_file.source_path):
            p = Path(include_file.absolute_path)
            content = p.read_text(encoding="utf-8")
            name = p.stem
            raw = RawDatafile(name=name, content=content, source_file=include_file.source_path)
            if include_file.source_path.endswith(".datasource"):
                raw_datasources.append(raw)
            elif include_file.source_path.endswith(".pipe"):
                raw_pipes.append(raw)
            continue

        absolute = Path(include_file.absolute_path)
        if absolute.suffix != ".py":
            continue

        module = _load_python_module(absolute)
        for export_name, value in module.__dict__.items():
            if export_name.startswith("_"):
                continue
            info = EntityInfo(export_name=export_name, source_file=include_file.source_path)
            if is_datasource_definition(value):
                datasources[export_name] = {"definition": value, "info": info}
            elif is_pipe_definition(value):
                pipes[export_name] = {"definition": value, "info": info}
            elif is_connection_definition(value):
                connections[export_name] = {"definition": value, "info": info}

    return LoadedEntities(
        datasources=datasources,
        pipes=pipes,
        connections=connections,
        raw_datasources=raw_datasources,
        raw_pipes=raw_pipes,
        source_files=source_files,
    )


def entities_to_project(entities: LoadedEntities) -> dict[str, Any]:
    datasources: DatasourcesDefinition = {}
    pipes: PipesDefinition = {}
    connections: ConnectionsDefinition = {}

    for name, payload in entities.datasources.items():
        datasources[name] = payload["definition"]
    for name, payload in entities.pipes.items():
        pipes[name] = payload["definition"]
    for name, payload in entities.connections.items():
        connections[name] = payload["definition"]

    return {
        "datasources": datasources,
        "pipes": pipes,
        "connections": connections,
    }


def watch_schema(options: WatchOptions | dict[str, Any], callback: Callable[[], None]) -> WatchController:
    normalized = options if isinstance(options, WatchOptions) else WatchOptions(**options)
    cwd = Path(normalized.cwd or ".").resolve()
    interval = max(normalized.interval_ms, 100) / 1000.0
    watch_dirs = [Path(p) for p in get_include_watch_directories(normalized.include_paths, str(cwd))]
    stop_flag = {"stop": False}

    mtimes: dict[str, float] = {}

    def snapshot() -> dict[str, float]:
        result: dict[str, float] = {}
        for watch_dir in watch_dirs:
            if not watch_dir.exists() or not watch_dir.is_dir():
                continue
            for child in watch_dir.rglob("*"):
                if child.is_file():
                    try:
                        result[str(child.resolve())] = child.stat().st_mtime
                    except OSError:
                        continue
        return result

    mtimes.update(snapshot())

    import threading

    def loop() -> None:
        while not stop_flag["stop"]:
            time.sleep(interval)
            current = snapshot()
            if current != mtimes:
                mtimes.clear()
                mtimes.update(current)
                callback()

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()

    def stop() -> None:
        stop_flag["stop"] = True
        thread.join(timeout=2)

    return WatchController(stop=stop)


__all__ = [
    "LoadedSchema",
    "LoaderOptions",
    "EntityInfo",
    "RawDatafile",
    "LoadEntitiesOptions",
    "LoadedEntities",
    "WatchOptions",
    "WatchController",
    "load_schema",
    "load_entities",
    "entities_to_project",
    "watch_schema",
]
