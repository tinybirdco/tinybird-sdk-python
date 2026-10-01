from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..codegen.type_mapper import clickhouse_type_to_validator
from ..codegen.utils import to_pascal_case, to_snake_case


@dataclass(frozen=True, slots=True)
class AnalyzedColumn:
    name: str
    clickhouse_type: str
    present_pct: float | None = None


@dataclass(frozen=True, slots=True)
class AnalyzedSchema:
    columns: tuple[AnalyzedColumn, ...]
    format: str | None = None
    dialect: dict[str, Any] = field(default_factory=dict)


def parse_analyze_response(response: dict[str, Any]) -> AnalyzedSchema:
    """Turn a raw `/v0/analyze` API response into an `AnalyzedSchema`.

    Tolerates the two shapes the Analyze API is known to return: a
    `analysis.columns` list of `{name, recommended_type}` entries, or a
    `analysis.schema`/`schema` ClickHouse DDL-like string (e.g.
    `"col1 Int64, col2 Nullable(String)"`) as a fallback.
    """
    analysis = response.get("analysis") or {}
    raw_columns = analysis.get("columns")
    columns: list[AnalyzedColumn] = []

    if isinstance(raw_columns, list) and raw_columns:
        for col in raw_columns:
            if not isinstance(col, dict):
                continue
            name = col.get("name")
            ch_type = col.get("recommended_type") or col.get("type")
            if not name or not ch_type:
                continue
            columns.append(
                AnalyzedColumn(
                    name=str(name),
                    clickhouse_type=str(ch_type),
                    present_pct=col.get("present_pct"),
                )
            )
    else:
        schema_str = analysis.get("schema") or response.get("schema")
        if isinstance(schema_str, str) and schema_str.strip():
            columns.extend(_parse_schema_ddl(schema_str))

    if not columns:
        raise ValueError(
            "Could not find column information in the analyze response. Expected "
            "'analysis.columns' (a list of {'name', 'recommended_type'} entries) or an "
            "'analysis.schema'/'schema' ClickHouse DDL string."
        )

    dialect = analysis.get("dialect") or response.get("dialect") or {}
    detected_format = analysis.get("format") or response.get("format")

    return AnalyzedSchema(
        columns=tuple(columns),
        format=detected_format if isinstance(detected_format, str) else None,
        dialect=dict(dialect) if isinstance(dialect, dict) else {},
    )


def _parse_schema_ddl(schema: str) -> list[AnalyzedColumn]:
    columns: list[AnalyzedColumn] = []
    for part in schema.split(","):
        part = part.strip()
        if not part:
            continue
        name, _, ch_type = part.partition(" ")
        name = name.strip("`")
        ch_type = ch_type.strip()
        if name and ch_type:
            columns.append(AnalyzedColumn(name=name, clickhouse_type=ch_type))
    return columns


def generate_schema_code(analyzed: AnalyzedSchema, name: str) -> str:
    """Generate `define_datasource(...)` source code from an analyzed schema.

    Mirrors `codegen.generate_datasource_code`'s output shape so analyzed and
    reverse-codegen'd datasources read the same way.
    """
    var_name = to_snake_case(name)
    type_name = to_pascal_case(name)
    lines: list[str] = [f"{var_name} = define_datasource({name!r}, {{", "    'schema': {"]
    for column in analyzed.columns:
        lines.append(
            f"        {column.name!r}: {clickhouse_type_to_validator(column.clickhouse_type)},"
        )
    lines.append("    },")
    lines.append("})")
    lines.append("")
    lines.append(f"{type_name}Row = dict")
    return "\n".join(lines)
