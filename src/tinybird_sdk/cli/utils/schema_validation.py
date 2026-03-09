from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ...client.base import TinybirdClient
from ...schema.pipe import PipeDefinition
from ...schema.project import PipesDefinition, ProjectDefinition


@dataclass(frozen=True, slots=True)
class SchemaValidationOptions:
    pipe_names: list[str]
    base_url: str
    token: str
    project: ProjectDefinition | None = None
    entities: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class ValidationIssue:
    pipe_name: str
    type: str
    message: str


@dataclass(frozen=True, slots=True)
class SchemaValidationResult:
    valid: bool
    issues: list[ValidationIssue]
    pipes_validated: list[str]
    pipes_skipped: list[str]


def validate_pipe_schemas(options: SchemaValidationOptions | dict[str, Any]) -> SchemaValidationResult:
    normalized = options if isinstance(options, SchemaValidationOptions) else SchemaValidationOptions(**options)

    client = TinybirdClient({"base_url": normalized.base_url, "token": normalized.token})

    result = SchemaValidationResult(valid=True, issues=[], pipes_validated=[], pipes_skipped=[])

    pipes: PipesDefinition = {}
    if normalized.entities is not None:
        entity_pipes = normalized.entities.get("pipes", {})
        for name, payload in entity_pipes.items():
            pipes[name] = payload["definition"]
    elif normalized.project is not None:
        pipes = normalized.project.pipes

    issues = list(result.issues)
    validated = list(result.pipes_validated)
    skipped = list(result.pipes_skipped)
    valid = True

    for pipe_name in normalized.pipe_names:
        pipe = next((item for item in pipes.values() if item._name == pipe_name), None)
        if not pipe:
            continue

        if _has_required_params(pipe):
            skipped.append(pipe_name)
            continue

        if not pipe._output:
            skipped.append(pipe_name)
            continue

        params = _build_default_params(pipe)

        try:
            response = client.query(pipe_name, params)
            validation = _validate_output_schema(response.get("meta", []), pipe._output)
            if not validation["valid"]:
                valid = False

            for missing in validation["missing_columns"]:
                issues.append(
                    ValidationIssue(
                        pipe_name=pipe_name,
                        type="error",
                        message=f"Missing column '{missing['name']}' (expected: {missing['expected_type']})",
                    )
                )
            for mismatch in validation["type_mismatches"]:
                issues.append(
                    ValidationIssue(
                        pipe_name=pipe_name,
                        type="error",
                        message=(
                            f"Type mismatch '{mismatch['name']}': expected {mismatch['expected_type']}, "
                            f"got {mismatch['actual_type']}"
                        ),
                    )
                )
            for extra in validation["extra_columns"]:
                issues.append(
                    ValidationIssue(
                        pipe_name=pipe_name,
                        type="warning",
                        message=f"Extra column '{extra['name']}' ({extra['actual_type']}) not in output schema",
                    )
                )

            validated.append(pipe_name)
        except Exception:
            skipped.append(pipe_name)

    return SchemaValidationResult(valid=valid, issues=issues, pipes_validated=validated, pipes_skipped=skipped)


def _has_required_params(pipe: PipeDefinition) -> bool:
    if not pipe._params:
        return False
    for param in pipe._params.values():
        if param._required and param._default is None:
            return True
    return False


def _build_default_params(pipe: PipeDefinition) -> dict[str, Any]:
    params: dict[str, Any] = {}
    if not pipe._params:
        return params
    for name, param in pipe._params.items():
        if param._default is not None:
            params[name] = param._default
    return params


def _normalize_type(type_name: str) -> str:
    normalized = type_name
    normalized = normalized.replace("LowCardinality(Nullable(", "")
    normalized = normalized.replace("Nullable(", "")
    normalized = normalized.replace("LowCardinality(", "")
    if normalized.endswith(")"):
        normalized = normalized[:-1]
    normalized = normalized.replace("DateTime('UTC')", "DateTime")
    return normalized


def _types_are_compatible(actual: str, expected: str) -> bool:
    return _normalize_type(actual) == _normalize_type(expected)


def _validate_output_schema(response_meta: list[dict[str, str]], output_schema: dict[str, Any]) -> dict[str, Any]:
    result = {
        "valid": True,
        "missing_columns": [],
        "extra_columns": [],
        "type_mismatches": [],
    }

    response_columns = {col["name"]: col["type"] for col in response_meta}

    for name, validator in output_schema.items():
        expected_type = validator._tinybirdType
        actual_type = response_columns.get(name)
        if not actual_type:
            result["missing_columns"].append({"name": name, "expected_type": expected_type})
            result["valid"] = False
        elif not _types_are_compatible(actual_type, expected_type):
            result["type_mismatches"].append(
                {"name": name, "expected_type": expected_type, "actual_type": actual_type}
            )
            result["valid"] = False
        response_columns.pop(name, None)

    for name, actual_type in response_columns.items():
        result["extra_columns"].append({"name": name, "actual_type": actual_type})

    return result


__all__ = [
    "SchemaValidationOptions",
    "ValidationIssue",
    "SchemaValidationResult",
    "validate_pipe_schemas",
]
