from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from .discovery import discover_resource_files
from .emit_ts import emit_migration_file_content, validate_resource_for_emission
from .parse import parse_resource_file
from .parser_utils import MigrationParseError
from .types import MigrationError, MigrationResult, ParsedResource, ResourceFile


@dataclass(frozen=True, slots=True)
class MigrateOptions:
    patterns: list[str]
    cwd: str | None = None
    out: str | None = None
    strict: bool = True
    dry_run: bool = False
    force: bool = False


def _to_migration_error(resource: ResourceFile, error: Exception) -> MigrationError:
    return MigrationError(
        file_path=resource.file_path,
        resource_name=resource.name,
        resource_kind=resource.kind,
        message=str(error),
    )


def _sort_resources_for_output(resources: list[ParsedResource]) -> list[ParsedResource]:
    order = {"connection": 0, "datasource": 1, "pipe": 2}
    return sorted(resources, key=lambda item: (order[item.kind], item.name))


def run_migrate(options: MigrateOptions | dict[str, Any]) -> MigrationResult:
    normalized = options if isinstance(options, MigrateOptions) else MigrateOptions(**options)
    cwd = Path(normalized.cwd or ".").resolve()
    output_path = Path(normalized.out or "tinybird.migration.py")
    if not output_path.is_absolute():
        output_path = cwd / output_path

    errors: list[MigrationError] = []

    if not normalized.patterns:
        return MigrationResult(
            success=False,
            output_path=str(output_path),
            migrated=[],
            errors=[
                MigrationError(
                    file_path=".",
                    resource_name="patterns",
                    resource_kind="datasource",
                    message="At least one file, directory, or glob pattern is required.",
                )
            ],
            dry_run=normalized.dry_run,
        )

    discovered = discover_resource_files(normalized.patterns, str(cwd))
    errors.extend(discovered.errors)

    parsed_resources: list[ParsedResource] = []
    for resource in discovered.resources:
        try:
            parsed_resources.append(parse_resource_file(resource))
        except MigrationParseError as error:
            errors.append(
                MigrationError(
                    file_path=error.file_path,
                    resource_name=error.resource_name,
                    resource_kind=error.resource_kind,
                    message=str(error),
                )
            )
        except Exception as error:  # pragma: no cover - defensive
            errors.append(_to_migration_error(resource, error))

    parsed_connections = [r for r in parsed_resources if r.kind == "connection"]
    parsed_datasources = [r for r in parsed_resources if r.kind == "datasource"]
    parsed_pipes = [r for r in parsed_resources if r.kind == "pipe"]

    migrated: list[ParsedResource] = []
    migrated_connection_names: set[str] = set()
    migrated_datasource_names: set[str] = set()
    parsed_connection_type_by_name = {connection.name: connection.connection_type for connection in parsed_connections}

    for connection in parsed_connections:
        try:
            validate_resource_for_emission(connection)
            migrated.append(connection)
            migrated_connection_names.add(connection.name)
        except Exception as error:
            errors.append(
                MigrationError(
                    file_path=connection.file_path,
                    resource_name=connection.name,
                    resource_kind=connection.kind,
                    message=str(error),
                )
            )

    for datasource in parsed_datasources:
        referenced_connection_name = (
            datasource.kafka.connection_name
            if datasource.kafka
            else datasource.s3.connection_name
            if datasource.s3
            else datasource.gcs.connection_name
            if datasource.gcs
            else None
        )

        if referenced_connection_name and referenced_connection_name not in migrated_connection_names:
            errors.append(
                MigrationError(
                    file_path=datasource.file_path,
                    resource_name=datasource.name,
                    resource_kind=datasource.kind,
                    message=(
                        f'Datasource references missing/unmigrated connection '
                        f'"{referenced_connection_name}".'
                    ),
                )
            )
            continue

        if datasource.kafka:
            kafka_connection_type = parsed_connection_type_by_name.get(datasource.kafka.connection_name)
            if kafka_connection_type != "kafka":
                errors.append(
                    MigrationError(
                        file_path=datasource.file_path,
                        resource_name=datasource.name,
                        resource_kind=datasource.kind,
                        message=(
                            f'Datasource kafka ingestion requires a kafka connection, found '
                            f'"{kafka_connection_type or "(none)"}".'
                        ),
                    )
                )
                continue

        import_config = datasource.s3 or datasource.gcs
        normalized_datasource = datasource
        if import_config:
            import_connection_type = parsed_connection_type_by_name.get(import_config.connection_name)
            if import_connection_type not in {"s3", "gcs"}:
                errors.append(
                    MigrationError(
                        file_path=datasource.file_path,
                        resource_name=datasource.name,
                        resource_kind=datasource.kind,
                        message=(
                            "Datasource import directives require an s3 or gcs connection, found "
                            f'"{import_connection_type or "(none)"}".'
                        ),
                    )
                )
                continue

            if import_connection_type == "gcs":
                normalized_datasource = replace(
                    datasource,
                    gcs=replace(import_config),
                    s3=None,
                )
            else:
                normalized_datasource = replace(
                    datasource,
                    s3=replace(import_config),
                    gcs=None,
                )

        try:
            validate_resource_for_emission(normalized_datasource)
            migrated.append(normalized_datasource)
            migrated_datasource_names.add(normalized_datasource.name)
        except Exception as error:
            errors.append(
                MigrationError(
                    file_path=normalized_datasource.file_path,
                    resource_name=normalized_datasource.name,
                    resource_kind=normalized_datasource.kind,
                    message=str(error),
                )
            )

    for pipe in parsed_pipes:
        if pipe.type == "sink":
            sink_connection_name = pipe.sink.connection_name if pipe.sink else None
            if not sink_connection_name or sink_connection_name not in migrated_connection_names:
                errors.append(
                    MigrationError(
                        file_path=pipe.file_path,
                        resource_name=pipe.name,
                        resource_kind=pipe.kind,
                        message=(
                            f'Sink pipe references missing/unmigrated connection '
                            f'"{sink_connection_name or "(none)"}".'
                        ),
                    )
                )
                continue

            sink_connection_type = parsed_connection_type_by_name.get(sink_connection_name)
            if not sink_connection_type:
                errors.append(
                    MigrationError(
                        file_path=pipe.file_path,
                        resource_name=pipe.name,
                        resource_kind=pipe.kind,
                        message=f'Sink pipe connection "{sink_connection_name}" could not be resolved.',
                    )
                )
                continue

            if pipe.sink and sink_connection_type != pipe.sink.service:
                errors.append(
                    MigrationError(
                        file_path=pipe.file_path,
                        resource_name=pipe.name,
                        resource_kind=pipe.kind,
                        message=(
                            f'Sink pipe service "{pipe.sink.service}" is incompatible with connection '
                            f'"{sink_connection_name}" type "{sink_connection_type}".'
                        ),
                    )
                )
                continue

        if pipe.type == "materialized" and (
            not pipe.materialized_datasource or pipe.materialized_datasource not in migrated_datasource_names
        ):
            errors.append(
                MigrationError(
                    file_path=pipe.file_path,
                    resource_name=pipe.name,
                    resource_kind=pipe.kind,
                    message=(
                        f'Materialized pipe references missing/unmigrated datasource '
                        f'"{pipe.materialized_datasource or '(none)'}".'
                    ),
                )
            )
            continue

        if pipe.type == "copy" and (
            not pipe.copy_target_datasource or pipe.copy_target_datasource not in migrated_datasource_names
        ):
            errors.append(
                MigrationError(
                    file_path=pipe.file_path,
                    resource_name=pipe.name,
                    resource_kind=pipe.kind,
                    message=(
                        f'Copy pipe references missing/unmigrated datasource '
                        f'"{pipe.copy_target_datasource or '(none)'}".'
                    ),
                )
            )
            continue

        try:
            validate_resource_for_emission(pipe)
            migrated.append(pipe)
        except Exception as error:
            errors.append(
                MigrationError(
                    file_path=pipe.file_path,
                    resource_name=pipe.name,
                    resource_kind=pipe.kind,
                    message=str(error),
                )
            )

    sorted_migrated = _sort_resources_for_output(migrated)
    output_content: str | None = None

    if sorted_migrated:
        try:
            output_content = emit_migration_file_content(sorted_migrated)
        except Exception as error:
            errors.append(
                MigrationError(
                    file_path=".",
                    resource_name="output",
                    resource_kind="datasource",
                    message=f"Failed to emit migration output: {error}",
                )
            )

    if not normalized.dry_run and output_content:
        if output_path.exists() and not normalized.force:
            errors.append(
                MigrationError(
                    file_path=str(output_path.relative_to(cwd)),
                    resource_name=output_path.name,
                    resource_kind="datasource",
                    message=f"Output file already exists: {output_path}. Use --force to overwrite.",
                )
            )
        else:
            output_path.write_text(output_content, encoding="utf-8")

    success = (len(errors) == 0) if normalized.strict else True
    return MigrationResult(
        success=success,
        output_path=str(output_path),
        migrated=sorted_migrated,
        errors=errors,
        dry_run=normalized.dry_run,
        output_content=output_content,
    )
