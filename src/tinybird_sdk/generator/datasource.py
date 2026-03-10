from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from ..schema.datasource import (
    ColumnDefinition,
    DatasourceDefinition,
    SchemaDefinition,
    get_column_json_path,
    get_column_type,
)
from ..schema.engines import EngineConfig, get_engine_clause
from ..schema.types import TypeValidator


@dataclass(frozen=True, slots=True)
class GeneratedDatasource:
    name: str
    content: str


def _escape_sql_string(value: str) -> str:
    return value.replace("'", "\\'")


def _format_default_value(value: Any, tinybird_type: str) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, str):
        return f"'{_escape_sql_string(value)}'"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, datetime):
        if tinybird_type.startswith("Date") and "Time" not in tinybird_type:
            return f"'{value.date().isoformat()}'"
        return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"
    if isinstance(value, date):
        return f"'{value.isoformat()}'"
    if isinstance(value, (list, dict)):
        return json.dumps(value)
    return f"'{_escape_sql_string(str(value))}'"


def _generate_column_line(column_name: str, column: TypeValidator | ColumnDefinition, include_json_paths: bool) -> str:
    validator = get_column_type(column)
    json_path = get_column_json_path(column)
    tinybird_type = validator._tinybirdType

    parts = [f"    {column_name} {tinybird_type}"]

    if include_json_paths:
        effective_json_path = json_path if isinstance(json_path, str) and json_path else f"$.{column_name}"
        parts.append(f"`json:{effective_json_path}`")

    if validator._modifiers.has_default:
        if isinstance(validator._modifiers.default_expression, str):
            parts.append(f"DEFAULT {validator._modifiers.default_expression}")
        else:
            parts.append(f"DEFAULT {_format_default_value(validator._modifiers.default_value, tinybird_type)}")

    if validator._modifiers.codec:
        parts.append(f"CODEC({validator._modifiers.codec})")

    return " ".join(parts)


def _generate_schema(schema: SchemaDefinition, include_json_paths: bool) -> str:
    lines = ["SCHEMA >"]
    names = list(schema.keys())

    for index, name in enumerate(names):
        suffix = "," if index < len(names) - 1 else ""
        lines.append(_generate_column_line(name, schema[name], include_json_paths) + suffix)

    return "\n".join(lines)


def _generate_engine_config(engine: EngineConfig | None) -> str:
    if engine is None:
        return 'ENGINE "MergeTree"'
    return get_engine_clause(engine)


def _generate_kafka_config(kafka: Any) -> str:
    lines = [
        f"KAFKA_CONNECTION_NAME {kafka.connection._name}",
        f"KAFKA_TOPIC {kafka.topic}",
    ]
    if kafka.group_id:
        lines.append(f"KAFKA_GROUP_ID {kafka.group_id}")
    if kafka.auto_offset_reset:
        lines.append(f"KAFKA_AUTO_OFFSET_RESET {kafka.auto_offset_reset}")
    if kafka.store_raw_value is not None:
        lines.append(f"KAFKA_STORE_RAW_VALUE {'True' if kafka.store_raw_value else 'False'}")
    return "\n".join(lines)


def _generate_import_config(import_config: Any) -> str:
    lines = [
        f"IMPORT_CONNECTION_NAME {import_config.connection._name}",
        f"IMPORT_BUCKET_URI {import_config.bucket_uri}",
    ]
    if import_config.schedule:
        lines.append(f"IMPORT_SCHEDULE {import_config.schedule}")
    if import_config.from_timestamp:
        lines.append(f"IMPORT_FROM_TIMESTAMP {import_config.from_timestamp}")
    return "\n".join(lines)


def _generate_forward_query(forward_query: str | None) -> str | None:
    if not forward_query or not forward_query.strip():
        return None
    lines = ["FORWARD_QUERY >"]
    lines.extend(f"    {line}" for line in forward_query.strip().splitlines())
    return "\n".join(lines)


def _generate_shared_with(shared_with: tuple[str, ...]) -> str | None:
    if not shared_with:
        return None
    lines = ["SHARED_WITH >"]
    for index, workspace in enumerate(shared_with):
        suffix = "," if index < len(shared_with) - 1 else ""
        lines.append(f"    {workspace}{suffix}")
    return "\n".join(lines)


def _generate_tokens(tokens: tuple[Any, ...]) -> list[str]:
    lines: list[str] = []
    for token in tokens:
        if hasattr(token, "token"):
            lines.append(f"TOKEN {token.token._name} {token.scope}")
        else:
            for permission in token.permissions:
                lines.append(f"TOKEN {token.name} {permission}")
    return lines


def _generate_indexes(indexes: tuple[Any, ...]) -> str | None:
    if not indexes:
        return None

    lines = ["INDEXES >"]
    for index in indexes:
        lines.append(
            f"    {index.name} {index.expr} TYPE {index.type} GRANULARITY {index.granularity}"
        )
    return "\n".join(lines)


def generate_datasource(datasource: DatasourceDefinition) -> GeneratedDatasource:
    parts: list[str] = []

    if datasource.options.description:
        parts.extend([f"DESCRIPTION >\n    {datasource.options.description}", ""])

    include_json_paths = datasource.options.json_paths is not False
    parts.append(_generate_schema(datasource._schema, include_json_paths))
    parts.append("")
    parts.append(_generate_engine_config(datasource.options.engine))

    indexes = _generate_indexes(datasource.options.indexes)
    if indexes:
        parts.extend(["", indexes])

    if datasource.options.kafka:
        parts.extend(["", _generate_kafka_config(datasource.options.kafka)])

    if datasource.options.s3:
        parts.extend(["", _generate_import_config(datasource.options.s3)])

    if datasource.options.gcs:
        parts.extend(["", _generate_import_config(datasource.options.gcs)])

    forward_query = _generate_forward_query(datasource.options.forward_query)
    if forward_query:
        parts.extend(["", forward_query])

    token_lines = _generate_tokens(datasource.options.tokens)
    if token_lines:
        parts.extend(["", "\n".join(token_lines)])

    shared_with = _generate_shared_with(datasource.options.shared_with)
    if shared_with:
        parts.extend(["", shared_with])

    return GeneratedDatasource(name=datasource._name, content="\n".join(parts))


def generate_all_datasources(datasources: dict[str, DatasourceDefinition]) -> list[GeneratedDatasource]:
    return [generate_datasource(datasource) for datasource in datasources.values()]
