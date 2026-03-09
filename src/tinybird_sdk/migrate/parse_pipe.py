from __future__ import annotations

import re
from dataclasses import replace

from .parser_utils import (
    MigrationParseError,
    is_blank,
    parse_directive_line,
    parse_quoted_value,
    read_directive_block,
    split_lines,
    split_top_level_comma,
)
from .types import PipeModel, PipeNodeModel, PipeParamModel, PipeTokenModel, ResourceFile, SinkKafkaModel, SinkModel, SinkS3Model


PIPE_DIRECTIVES = {
    "DESCRIPTION",
    "NODE",
    "SQL",
    "TYPE",
    "CACHE",
    "DATASOURCE",
    "DEPLOYMENT_METHOD",
    "TARGET_DATASOURCE",
    "COPY_SCHEDULE",
    "COPY_MODE",
    "TOKEN",
}


def _is_pipe_directive_line(line: str) -> bool:
    if not line:
        return False
    directive = parse_directive_line(line)
    return directive["key"] in PIPE_DIRECTIVES


def _next_non_blank(lines: list[str], start_index: int) -> int:
    i = start_index
    while i < len(lines) and (is_blank(lines[i]) or lines[i].strip().startswith("#")):
        i += 1
    return i


def _infer_output_columns_from_sql(sql: str) -> list[str]:
    match = re.search(r"select\s+([\s\S]+?)\s+from\s", sql, flags=re.IGNORECASE | re.UNICODE)
    if not match:
        return ["result"]

    select_clause = match.group(1)
    expressions = split_top_level_comma(select_clause)
    columns: list[str] = []

    for expression in expressions:
        alias = re.search(r"\s+AS\s+`?([a-zA-Z_][a-zA-Z0-9_]*)`?\s*$", expression, flags=re.IGNORECASE)
        if alias:
            columns.append(alias.group(1))
            continue

        simple = re.search(r"(?:^|\.)`?([a-zA-Z_][a-zA-Z0-9_]*)`?\s*$", expression)
        if simple:
            columns.append(simple.group(1))

    return list(dict.fromkeys(columns or ["result"]))


def _map_template_function_to_param_type(func: str) -> str | None:
    aliases = {
        "string": "String",
        "uuid": "UUID",
        "int": "Int32",
        "integer": "Int32",
        "int8": "Int8",
        "int16": "Int16",
        "int32": "Int32",
        "int64": "Int64",
        "uint8": "UInt8",
        "uint16": "UInt16",
        "uint32": "UInt32",
        "uint64": "UInt64",
        "float32": "Float32",
        "float64": "Float64",
        "boolean": "Boolean",
        "bool": "Boolean",
        "date": "Date",
        "datetime": "DateTime",
        "datetime64": "DateTime64",
        "array": "Array",
        "column": "column",
        "json": "JSON",
    }
    lower = func.lower()

    mapped = aliases.get(lower)
    if mapped:
        return mapped

    if lower.startswith("datetime64"):
        return "DateTime64"
    if lower.startswith("datetime"):
        return "DateTime"

    return None


def _parse_param_default(raw_value: str) -> str | int | float | bool:
    trimmed = raw_value.strip()
    if re.fullmatch(r"-?\d+(\.\d+)?", trimmed):
        return float(trimmed) if "." in trimmed else int(trimmed)
    if re.fullmatch(r"(?i:true|false)", trimmed):
        return trimmed.lower() == "true"
    if (trimmed.startswith("'") and trimmed.endswith("'")) or (
        trimmed.startswith('"') and trimmed.endswith('"')
    ):
        return trimmed[1:-1]
    raise ValueError(f'Unsupported parameter default value: "{raw_value}"')


def _parse_keyword_argument(raw_arg: str) -> tuple[str, str] | None:
    equals_index = raw_arg.find("=")
    if equals_index <= 0:
        return None

    key = raw_arg[:equals_index].strip()
    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", key):
        return None

    value = raw_arg[equals_index + 1 :].strip()
    if not value:
        return None

    return key, value


def _parse_required_flag(raw_value: str) -> bool:
    normalized = raw_value.strip().lower()
    if normalized in {"true", "1"}:
        return True
    if normalized in {"false", "0"}:
        return False
    raise ValueError(f'Unsupported required value: "{raw_value}"')


def _parse_param_options(raw_args: list[str]) -> tuple[str | int | float | bool | None, bool | None, str | None]:
    default_value: str | int | float | bool | None = None
    required: bool | None = None
    description: str | None = None

    for raw_arg in raw_args:
        trimmed = raw_arg.strip()
        if not trimmed:
            continue

        keyword = _parse_keyword_argument(trimmed)
        if not keyword:
            default_value = _parse_param_default(trimmed)
            continue

        key_lower = keyword[0].lower()
        value = keyword[1]

        if key_lower == "default":
            default_value = _parse_param_default(value)
            continue

        if key_lower == "required":
            required = _parse_required_flag(value)
            continue

        if key_lower == "description":
            parsed_description = _parse_param_default(value)
            if not isinstance(parsed_description, str):
                raise ValueError(f'Unsupported description value: "{value}"')
            description = parsed_description

    return default_value, required, description


def _extract_template_function_calls(expression: str) -> list[dict[str, str | int]]:
    def _mask_parentheses_inside_quotes(value: str) -> str:
        output: list[str] = []
        in_single_quote = False
        in_double_quote = False

        for i, char in enumerate(value):
            prev = value[i - 1] if i > 0 else ""
            if char == "'" and not in_double_quote and prev != "\\":
                in_single_quote = not in_single_quote
                output.append(char)
                continue

            if char == '"' and not in_single_quote and prev != "\\":
                in_double_quote = not in_double_quote
                output.append(char)
                continue

            if (in_single_quote or in_double_quote) and (char == "(" or char == ")"):
                output.append(" ")
                continue

            output.append(char)

        return "".join(output)

    masked_expression = _mask_parentheses_inside_quotes(expression)
    call_regex = re.compile(r"([a-zA-Z_][a-zA-Z0-9_]*)\s*\(([^()]*)\)")
    calls: list[dict[str, str | int]] = []

    for match in call_regex.finditer(masked_expression):
        start = match.start()
        full_call = expression[start : start + len(match.group(0))]
        open_paren = full_call.find("(")
        close_paren = full_call.rfind(")")
        args_raw = full_call[open_paren + 1 : close_paren] if open_paren >= 0 and close_paren > open_paren else ""

        calls.append(
            {
                "function_name": match.group(1),
                "args_raw": args_raw,
                "full_call": full_call,
                "start": start,
                "end": start + len(full_call),
            }
        )

    return calls


def _should_parse_template_function_as_param(mapped_type: str) -> bool:
    return mapped_type != "Array"


def _normalize_sql_placeholders(sql: str) -> str:
    placeholder_regex = re.compile(r"\{\{\s*([^{}]+?)\s*\}\}")

    def _rewrite(match: re.Match[str]) -> str:
        full_match = match.group(0)
        expression = str(match.group(1))
        calls = _extract_template_function_calls(expression)
        if not calls:
            return full_match

        rewritten = ""
        cursor = 0
        changed = False

        for call in calls:
            start = int(call["start"])
            end = int(call["end"])
            function_name = str(call["function_name"])
            args_raw = str(call["args_raw"])
            full_call = str(call["full_call"])

            rewritten += expression[cursor:start]
            replacement = full_call

            normalized_function = function_name.lower()
            if normalized_function not in {"error", "custom_error"}:
                mapped_type = _map_template_function_to_param_type(function_name)
                if mapped_type and _should_parse_template_function_as_param(mapped_type):
                    args = split_top_level_comma(args_raw)
                    if args:
                        param_name = args[0].strip()
                        if re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", param_name):
                            replacement = f"{function_name}({param_name})"

            if replacement != full_call:
                changed = True

            rewritten += replacement
            cursor = end

        rewritten += expression[cursor:]

        if not changed:
            return full_match
        return "{{ " + rewritten.strip() + " }}"

    return placeholder_regex.sub(_rewrite, sql)


def _infer_params_from_sql(sql: str, file_path: str, resource_name: str) -> list[PipeParamModel]:
    regex = re.compile(r"\{\{\s*([^{}]+?)\s*\}\}")
    params: dict[str, PipeParamModel] = {}

    for match in regex.finditer(sql):
        expression = match.group(1)
        calls = _extract_template_function_calls(expression)

        for call in calls:
            template_function = str(call["function_name"])
            normalized_template_function = template_function.lower()
            if normalized_template_function in {"error", "custom_error"}:
                continue

            mapped_type = _map_template_function_to_param_type(template_function)
            if not mapped_type:
                raise MigrationParseError(
                    file_path,
                    "pipe",
                    resource_name,
                    f'Unsupported placeholder function in strict mode: "{template_function}"',
                )

            args = split_top_level_comma(str(call["args_raw"]))
            if len(args) == 0:
                raise MigrationParseError(
                    file_path,
                    "pipe",
                    resource_name,
                    f'Invalid template placeholder: "{call["full_call"]}"',
                )

            param_name = args[0].strip()
            is_identifier = re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", param_name) is not None
            if not is_identifier:
                if mapped_type == "column":
                    continue
                raise MigrationParseError(
                    file_path,
                    "pipe",
                    resource_name,
                    f'Unsupported parameter name in placeholder: "{{{{ {call["full_call"]} }}}}"',
                )

            default_value: str | int | float | bool | None = None
            required: bool | None = None
            description: str | None = None
            if len(args) > 1 and _should_parse_template_function_as_param(mapped_type):
                try:
                    default_value, required, description = _parse_param_options(args[1:])
                except Exception as error:
                    raise MigrationParseError(file_path, "pipe", resource_name, str(error)) from error

            existing = params.get(param_name)
            if existing:
                updated = existing
                if updated.type != mapped_type:
                    updated = replace(updated, type=mapped_type)

                if default_value is not None or updated.default_value is not None:
                    updated = replace(updated, default_value=default_value or updated.default_value)

                if description is not None or updated.description is not None:
                    updated = replace(updated, description=description or updated.description)

                optional_in_any_usage = (
                    updated.required is False
                    or required is False
                    or updated.default_value is not None
                    or default_value is not None
                )
                updated = replace(updated, required=not optional_in_any_usage)
                params[param_name] = updated
            else:
                is_required = required if required is not None else default_value is None
                params[param_name] = PipeParamModel(
                    name=param_name,
                    type=mapped_type,
                    required=is_required,
                    default_value=default_value,
                    description=description,
                )

    return sorted(params.values(), key=lambda item: item.name)


def _parse_token(file_path: str, resource_name: str, value: str) -> PipeTokenModel:
    trimmed = value.strip()
    quoted_match = re.fullmatch(r'^"([^"]+)"(?:\s+(READ))?$', trimmed)
    if quoted_match:
        return PipeTokenModel(name=quoted_match.group(1), scope="READ")

    parts = [part for part in re.split(r"\s+", trimmed) if part]
    if len(parts) == 0:
        raise MigrationParseError(file_path, "pipe", resource_name, "Invalid TOKEN line.")

    if len(parts) > 2:
        raise MigrationParseError(
            file_path,
            "pipe",
            resource_name,
            f'Unsupported TOKEN syntax in strict mode: "{value}"',
        )

    token_name = parts[0]
    if token_name.startswith('"') and token_name.endswith('"') and len(token_name) >= 2:
        token_name = token_name[1:-1]

    scope = parts[1] if len(parts) > 1 else "READ"
    if scope != "READ":
        raise MigrationParseError(
            file_path,
            "pipe",
            resource_name,
            f'Unsupported pipe token scope: "{scope}"',
        )

    return PipeTokenModel(name=token_name, scope="READ")


def _normalize_export_strategy(raw_value: str) -> str:
    normalized = parse_quoted_value(raw_value).lower()
    if normalized == "create_new":
        return "create_new"
    if normalized in {"replace", "truncate"}:
        return "replace"
    raise ValueError(f'Unsupported sink strategy in strict mode: "{raw_value}"')


def parse_pipe_file(resource: ResourceFile) -> PipeModel:
    lines = split_lines(resource.content)

    nodes: list[PipeNodeModel] = []
    raw_node_sqls: list[str] = []
    tokens: list[PipeTokenModel] = []
    description: str | None = None
    pipe_type: PipeModel.__annotations__["type"] = "pipe"  # type: ignore[assignment]
    cache_ttl: int | None = None
    materialized_datasource: str | None = None
    deployment_method: str | None = None
    copy_target_datasource: str | None = None
    copy_schedule: str | None = None
    copy_mode: str | None = None
    export_service: str | None = None
    export_connection_name: str | None = None
    export_topic: str | None = None
    export_bucket_uri: str | None = None
    export_file_template: str | None = None
    export_format: str | None = None
    export_schedule: str | None = None
    export_strategy: str | None = None
    export_compression: str | None = None

    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if not line or line.startswith("#"):
            i += 1
            continue

        if line == "DESCRIPTION >":
            block, next_index = read_directive_block(lines, i + 1, _is_pipe_directive_line)
            if description is None:
                description = "\n".join(block)
            elif nodes:
                last = nodes[-1]
                nodes[-1] = PipeNodeModel(name=last.name, sql=last.sql, description="\n".join(block))
            else:
                raise MigrationParseError(
                    resource.file_path,
                    "pipe",
                    resource.name,
                    "DESCRIPTION block is not attached to a node or pipe header.",
                )
            i = next_index
            continue

        if line.startswith("NODE "):
            node_name = line[len("NODE ") :].strip()
            if not node_name:
                raise MigrationParseError(resource.file_path, "pipe", resource.name, "NODE directive requires a name.")

            i += 1
            i = _next_non_blank(lines, i)

            node_description: str | None = None
            if i < len(lines) and lines[i].strip() == "DESCRIPTION >":
                desc_block, next_index = read_directive_block(lines, i + 1, _is_pipe_directive_line)
                node_description = "\n".join(desc_block)
                i = _next_non_blank(lines, next_index)

            if i >= len(lines) or lines[i].strip() != "SQL >":
                raise MigrationParseError(
                    resource.file_path,
                    "pipe",
                    resource.name,
                    f'Node "{node_name}" is missing SQL > block.',
                )

            sql_block, next_index = read_directive_block(lines, i + 1, _is_pipe_directive_line)
            if len(sql_block) == 0:
                raise MigrationParseError(
                    resource.file_path,
                    "pipe",
                    resource.name,
                    f'Node "{node_name}" has an empty SQL block.',
                )

            normalized_sql_lines = sql_block[1:] if sql_block and sql_block[0] == "%" else sql_block
            sql = "\n".join(normalized_sql_lines).strip()
            if not sql:
                raise MigrationParseError(
                    resource.file_path,
                    "pipe",
                    resource.name,
                    f'Node "{node_name}" has SQL marker '%' but no SQL body.',
                )

            raw_node_sqls.append(sql)
            nodes.append(PipeNodeModel(name=node_name, description=node_description, sql=_normalize_sql_placeholders(sql)))
            i = next_index
            continue

        directive = parse_directive_line(line)
        key = directive["key"]
        value = directive["value"]

        if key == "TYPE":
            normalized_type = parse_quoted_value(value).lower()
            if normalized_type == "endpoint":
                pipe_type = "endpoint"
            elif normalized_type == "materialized":
                pipe_type = "materialized"
            elif normalized_type == "copy":
                pipe_type = "copy"
            elif normalized_type == "sink":
                pipe_type = "sink"
            else:
                raise MigrationParseError(
                    resource.file_path,
                    "pipe",
                    resource.name,
                    f'Unsupported TYPE value in strict mode: "{parse_quoted_value(value)}"',
                )
        elif key == "CACHE":
            try:
                ttl = float(value)
            except ValueError as error:
                raise MigrationParseError(
                    resource.file_path,
                    "pipe",
                    resource.name,
                    f'Invalid CACHE value: "{value}"',
                ) from error

            if ttl < 0:
                raise MigrationParseError(
                    resource.file_path,
                    "pipe",
                    resource.name,
                    f'Invalid CACHE value: "{value}"',
                )
            cache_ttl = int(ttl) if ttl.is_integer() else ttl  # type: ignore[assignment]
        elif key == "DATASOURCE":
            materialized_datasource = value.strip()
        elif key == "DEPLOYMENT_METHOD":
            if value != "alter":
                raise MigrationParseError(
                    resource.file_path,
                    "pipe",
                    resource.name,
                    f'Unsupported DEPLOYMENT_METHOD: "{value}"',
                )
            deployment_method = "alter"
        elif key == "TARGET_DATASOURCE":
            copy_target_datasource = value.strip()
        elif key == "COPY_SCHEDULE":
            copy_schedule = value
        elif key == "COPY_MODE":
            if value not in {"append", "replace"}:
                raise MigrationParseError(
                    resource.file_path,
                    "pipe",
                    resource.name,
                    f'Unsupported COPY_MODE: "{value}"',
                )
            copy_mode = value
        elif key == "EXPORT_SERVICE":
            normalized = parse_quoted_value(value).lower()
            if normalized not in {"kafka", "s3"}:
                raise MigrationParseError(
                    resource.file_path,
                    "pipe",
                    resource.name,
                    f'Unsupported EXPORT_SERVICE in strict mode: "{value}"',
                )
            export_service = normalized
        elif key == "EXPORT_CONNECTION_NAME":
            export_connection_name = parse_quoted_value(value)
        elif key == "EXPORT_KAFKA_TOPIC":
            export_topic = parse_quoted_value(value)
        elif key == "EXPORT_BUCKET_URI":
            export_bucket_uri = parse_quoted_value(value)
        elif key == "EXPORT_FILE_TEMPLATE":
            export_file_template = parse_quoted_value(value)
        elif key == "EXPORT_FORMAT":
            export_format = parse_quoted_value(value)
        elif key == "EXPORT_SCHEDULE":
            export_schedule = parse_quoted_value(value)
        elif key == "EXPORT_STRATEGY":
            try:
                export_strategy = _normalize_export_strategy(value)
            except Exception:
                raise MigrationParseError(
                    resource.file_path,
                    "pipe",
                    resource.name,
                    f'Unsupported EXPORT_STRATEGY in strict mode: "{value}"',
                ) from None
        elif key == "EXPORT_WRITE_STRATEGY":
            try:
                export_strategy = _normalize_export_strategy(value)
            except Exception:
                raise MigrationParseError(
                    resource.file_path,
                    "pipe",
                    resource.name,
                    f'Unsupported EXPORT_WRITE_STRATEGY in strict mode: "{value}"',
                ) from None
        elif key == "EXPORT_COMPRESSION":
            normalized = parse_quoted_value(value).lower()
            if normalized not in {"none", "gzip", "snappy"}:
                raise MigrationParseError(
                    resource.file_path,
                    "pipe",
                    resource.name,
                    f'Unsupported EXPORT_COMPRESSION in strict mode: "{value}"',
                )
            export_compression = normalized
        elif key == "TOKEN":
            tokens.append(_parse_token(resource.file_path, resource.name, value))
        else:
            raise MigrationParseError(
                resource.file_path,
                "pipe",
                resource.name,
                f'Unsupported pipe directive in strict mode: "{line}"',
            )

        i += 1

    if not nodes:
        raise MigrationParseError(resource.file_path, "pipe", resource.name, "At least one NODE is required.")

    if pipe_type != "endpoint" and cache_ttl is not None:
        raise MigrationParseError(resource.file_path, "pipe", resource.name, "CACHE is only supported for TYPE endpoint.")

    if pipe_type == "materialized" and not materialized_datasource:
        raise MigrationParseError(
            resource.file_path,
            "pipe",
            resource.name,
            "DATASOURCE is required for TYPE MATERIALIZED.",
        )

    if pipe_type == "copy" and not copy_target_datasource:
        raise MigrationParseError(
            resource.file_path,
            "pipe",
            resource.name,
            "TARGET_DATASOURCE is required for TYPE COPY.",
        )

    has_sink_directives = any(
        value is not None
        for value in (
            export_service,
            export_connection_name,
            export_topic,
            export_bucket_uri,
            export_file_template,
            export_format,
            export_schedule,
            export_strategy,
            export_compression,
        )
    )
    if pipe_type != "sink" and has_sink_directives:
        raise MigrationParseError(
            resource.file_path,
            "pipe",
            resource.name,
            "EXPORT_* directives are only supported for TYPE sink.",
        )

    sink: SinkModel | None = None
    if pipe_type == "sink":
        if not export_connection_name:
            raise MigrationParseError(resource.file_path, "pipe", resource.name, "EXPORT_CONNECTION_NAME is required for TYPE sink.")

        has_kafka_directives = export_topic is not None
        has_s3_directives = any(
            value is not None for value in (export_bucket_uri, export_file_template, export_format, export_compression)
        )

        if has_kafka_directives and has_s3_directives:
            raise MigrationParseError(
                resource.file_path,
                "pipe",
                resource.name,
                "Sink pipe cannot mix Kafka and S3 export directives.",
            )

        inferred_service = export_service or ("kafka" if has_kafka_directives else "s3" if has_s3_directives else None)
        if not inferred_service:
            raise MigrationParseError(
                resource.file_path,
                "pipe",
                resource.name,
                "Sink pipe must define EXPORT_SERVICE or include service-specific export directives.",
            )

        if inferred_service == "kafka":
            if has_s3_directives:
                raise MigrationParseError(
                    resource.file_path,
                    "pipe",
                    resource.name,
                    "S3 export directives are not valid for Kafka sinks.",
                )
            if not export_topic:
                raise MigrationParseError(
                    resource.file_path,
                    "pipe",
                    resource.name,
                    "EXPORT_KAFKA_TOPIC is required for Kafka sinks.",
                )
            if not export_schedule:
                raise MigrationParseError(
                    resource.file_path,
                    "pipe",
                    resource.name,
                    "EXPORT_SCHEDULE is required for Kafka sinks.",
                )
            if export_strategy is not None:
                raise MigrationParseError(
                    resource.file_path,
                    "pipe",
                    resource.name,
                    "EXPORT_STRATEGY is only valid for S3 sinks.",
                )
            if export_compression is not None:
                raise MigrationParseError(
                    resource.file_path,
                    "pipe",
                    resource.name,
                    "EXPORT_COMPRESSION is only valid for S3 sinks.",
                )

            sink = SinkKafkaModel(
                service="kafka",
                connection_name=export_connection_name,
                topic=export_topic,
                schedule=export_schedule,
            )
        else:
            if has_kafka_directives:
                raise MigrationParseError(
                    resource.file_path,
                    "pipe",
                    resource.name,
                    "Kafka export directives are not valid for S3 sinks.",
                )
            if not export_bucket_uri or not export_file_template or not export_format or not export_schedule:
                raise MigrationParseError(
                    resource.file_path,
                    "pipe",
                    resource.name,
                    "S3 sinks require EXPORT_BUCKET_URI, EXPORT_FILE_TEMPLATE, EXPORT_FORMAT, and EXPORT_SCHEDULE.",
                )

            sink = SinkS3Model(
                service="s3",
                connection_name=export_connection_name,
                bucket_uri=export_bucket_uri,
                file_template=export_file_template,
                format=export_format,
                schedule=export_schedule,
                strategy=export_strategy,  # type: ignore[arg-type]
                compression=export_compression,  # type: ignore[arg-type]
            )

    params: list[PipeParamModel]
    if pipe_type in {"materialized", "copy"}:
        params = []
    else:
        params = _infer_params_from_sql("\n".join(raw_node_sqls), resource.file_path, resource.name)

    inferred_output_columns: list[str] = []
    if pipe_type == "endpoint":
        inferred_output_columns = _infer_output_columns_from_sql(nodes[-1].sql)

    return PipeModel(
        kind="pipe",
        name=resource.name,
        file_path=resource.file_path,
        description=description,
        type=pipe_type,
        nodes=nodes,
        cache_ttl=cache_ttl,
        materialized_datasource=materialized_datasource,
        deployment_method=deployment_method,  # type: ignore[arg-type]
        copy_target_datasource=copy_target_datasource,
        copy_schedule=copy_schedule,
        copy_mode=copy_mode,  # type: ignore[arg-type]
        sink=sink,
        tokens=tokens,
        params=params,
        inferred_output_columns=inferred_output_columns,
    )
