from __future__ import annotations

from typing import Any

from ..schema.datasource import DatasourceDefinition, get_column_type
from ..schema.pipe import PipeDefinition


def infer_row_schema(datasource: DatasourceDefinition) -> dict[str, str]:
    result: dict[str, str] = {}
    for name, validator_or_column in datasource._schema.items():
        validator = get_column_type(validator_or_column)
        result[name] = validator._tinybirdType
    return result


def infer_params_schema(pipe: PipeDefinition) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for name, validator in pipe._params.items():
        result[name] = {
            "tinybird_type": validator._tinybirdType,
            "required": validator._required,
            "default": validator._default,
            "description": validator._description,
        }
    return result


def infer_output_schema(pipe: PipeDefinition) -> dict[str, str]:
    output = pipe._output or {}
    return {name: validator._tinybirdType for name, validator in output.items()}


def infer_materialized_target(pipe: PipeDefinition):
    if pipe.options.materialized:
        return pipe.options.materialized.datasource
    return None


def is_materialized_pipe(pipe: PipeDefinition) -> bool:
    return pipe.options.materialized is not None


__all__ = [
    "infer_row_schema",
    "infer_params_schema",
    "infer_output_schema",
    "infer_materialized_target",
    "is_materialized_pipe",
]
