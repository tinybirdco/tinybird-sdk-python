from __future__ import annotations

import json
from typing import Any

from ..codegen.type_mapper import clickhouse_type_to_validator
from ..codegen.utils import to_snake_case
from .parser_utils import parse_literal_from_datafile
from .types import (
    DatasourceModel,
    GCSConnectionModel,
    KafkaConnectionModel,
    ParsedResource,
    PipeModel,
    S3ConnectionModel,
    SinkKafkaModel,
    SinkS3Model,
)


def _escape_string(value: str) -> str:
    return json.dumps(value)


def _normalized_base_type(type_name: str) -> str:
    current = type_name.strip()
    updated = True
    while updated:
        updated = False
        if current.startswith("Nullable(") and current.endswith(")"):
            current = current[len("Nullable(") : -1]
            updated = True
            continue
        if current.startswith("LowCardinality(") and current.endswith(")"):
            current = current[len("LowCardinality(") : -1]
            updated = True
            continue
    return current


def _is_boolean_type(type_name: str) -> bool:
    base = _normalized_base_type(type_name)
    return base in {"Bool", "Boolean"}


def _strict_column_type_to_validator(type_name: str) -> str:
    validator = clickhouse_type_to_validator(type_name)
    if "TODO: Unknown type" in validator:
        raise ValueError(f'Unsupported column type in strict mode: "{type_name}"')
    return validator


def _strict_param_base_validator(type_name: str) -> str:
    mapping = {
        "String": "p.string()",
        "UUID": "p.uuid()",
        "Int": "p.int32()",
        "Integer": "p.int32()",
        "Int8": "p.int8()",
        "Int16": "p.int16()",
        "Int32": "p.int32()",
        "Int64": "p.int64()",
        "UInt8": "p.uint8()",
        "UInt16": "p.uint16()",
        "UInt32": "p.uint32()",
        "UInt64": "p.uint64()",
        "Float32": "p.float32()",
        "Float64": "p.float64()",
        "Boolean": "p.boolean()",
        "Bool": "p.boolean()",
        "Date": "p.date()",
        "DateTime": "p.date_time()",
        "DateTime64": "p.date_time64()",
        "Array": "p.array(p.string())",
        "column": "p.column()",
        "JSON": "p.json()",
    }
    validator = mapping.get(type_name)
    if not validator:
        raise ValueError(f'Unsupported parameter type in strict mode: "{type_name}"')
    return validator


def _apply_param_optional(base_validator: str, required: bool, default_value: str | int | float | bool | None) -> str:
    if required and default_value is None:
        return base_validator

    if default_value is not None:
        return f"{base_validator}.optional({repr(default_value)})"
    return f"{base_validator}.optional()"


def _apply_param_description(validator: str, description: str | None) -> str:
    if description is None:
        return validator
    return f"{validator}.describe({_escape_string(description)})"


def _engine_function_name(type_name: str) -> str:
    mapping = {
        "MergeTree": "merge_tree",
        "ReplacingMergeTree": "replacing_merge_tree",
        "SummingMergeTree": "summing_merge_tree",
        "AggregatingMergeTree": "aggregating_merge_tree",
        "CollapsingMergeTree": "collapsing_merge_tree",
        "VersionedCollapsingMergeTree": "versioned_collapsing_merge_tree",
    }
    function_name = mapping.get(type_name)
    if not function_name:
        raise ValueError(f'Unsupported engine type in strict mode: "{type_name}"')
    return function_name


def _emit_engine_options(engine_model: Any) -> str:
    options: list[str] = []
    engine = engine_model

    if len(engine.sorting_key) == 1:
        options.append(f"'sorting_key': {_escape_string(engine.sorting_key[0])}")
    else:
        options.append(f"'sorting_key': [{', '.join(_escape_string(v) for v in engine.sorting_key)}]")

    if engine.partition_key:
        options.append(f"'partition_key': {_escape_string(engine.partition_key)}")
    if engine.primary_key:
        if len(engine.primary_key) == 1:
            options.append(f"'primary_key': {_escape_string(engine.primary_key[0])}")
        else:
            options.append(f"'primary_key': [{', '.join(_escape_string(v) for v in engine.primary_key)}]")
    if engine.ttl:
        options.append(f"'ttl': {_escape_string(engine.ttl)}")
    if engine.ver:
        options.append(f"'ver': {_escape_string(engine.ver)}")
    if engine.is_deleted:
        options.append(f"'is_deleted': {_escape_string(engine.is_deleted)}")
    if engine.sign:
        options.append(f"'sign': {_escape_string(engine.sign)}")
    if engine.version:
        options.append(f"'version': {_escape_string(engine.version)}")
    if engine.summing_columns:
        options.append(f"'columns': [{', '.join(_escape_string(v) for v in engine.summing_columns)}]")
    if engine.settings:
        settings_entries = []
        for key, value in engine.settings.items():
            settings_entries.append(f"{_escape_string(key)}: {repr(value)}")
        options.append(f"'settings': {{ {', '.join(settings_entries)} }}")

    return f"engine.{_engine_function_name(engine.type)}({{{', '.join(options)}}})"


def _emit_datasource(ds: DatasourceModel) -> str:
    variable_name = to_snake_case(ds.name)
    lines: list[str] = []
    has_json_path = any(column.json_path is not None for column in ds.columns)
    has_missing_json_path = any(column.json_path is None for column in ds.columns)

    if has_json_path and has_missing_json_path:
        raise ValueError(
            f'Datasource "{ds.name}" has mixed json path usage. This is not representable in strict mode.'
        )

    if ds.description:
        lines.append('"""')
        lines.extend(ds.description.split("\n"))
        lines.append('"""')

    lines.append(f"{variable_name} = define_datasource({_escape_string(ds.name)}, {{")
    if ds.description:
        lines.append(f"    'description': {_escape_string(ds.description)},")
    if not has_json_path:
        lines.append("    'json_paths': False,")

    lines.append("    'schema': {")
    for column in ds.columns:
        validator = _strict_column_type_to_validator(column.type)

        if column.default_expression is not None:
            try:
                parsed_default = parse_literal_from_datafile(column.default_expression)
                literal_value: Any = parsed_default
                if isinstance(parsed_default, (int, float)) and _is_boolean_type(column.type):
                    if parsed_default in {0, 1}:
                        literal_value = bool(parsed_default)
                    else:
                        raise ValueError(
                            f'Boolean default value must be 0 or 1 for column "{column.name}" in datasource "{ds.name}".'
                        )
                validator += f".default({repr(literal_value)})"
            except ValueError:
                validator += f".default_expr({_escape_string(column.default_expression)})"

        if column.codec:
            validator += f".codec({_escape_string(column.codec)})"

        if column.json_path:
            lines.append(
                f"        '{column.name}': column({validator}, {{'json_path': {_escape_string(column.json_path)}}}),"
            )
        else:
            lines.append(f"        '{column.name}': {validator},")
    lines.append("    },")
    if ds.engine:
        lines.append(f"    'engine': {_emit_engine_options(ds.engine)},")

    if ds.indexes:
        lines.append("    'indexes': [")
        for index in ds.indexes:
            lines.append(
                "        {'name': "
                + f"{_escape_string(index.name)}, 'expr': {_escape_string(index.expr)}, "
                + f"'type': {_escape_string(index.type)}, 'granularity': {index.granularity}"
                + "},"
            )
        lines.append("    ],")

    if ds.kafka:
        connection_var = to_snake_case(ds.kafka.connection_name)
        lines.append("    'kafka': {")
        lines.append(f"        'connection': {connection_var},")
        lines.append(f"        'topic': {_escape_string(ds.kafka.topic)},")
        if ds.kafka.group_id:
            lines.append(f"        'group_id': {_escape_string(ds.kafka.group_id)},")
        if ds.kafka.auto_offset_reset:
            lines.append(f"        'auto_offset_reset': {_escape_string(ds.kafka.auto_offset_reset)},")
        if ds.kafka.store_raw_value is not None:
            lines.append(f"        'store_raw_value': {ds.kafka.store_raw_value},")
        lines.append("    },")

    if ds.s3:
        connection_var = to_snake_case(ds.s3.connection_name)
        lines.append("    's3': {")
        lines.append(f"        'connection': {connection_var},")
        lines.append(f"        'bucket_uri': {_escape_string(ds.s3.bucket_uri)},")
        if ds.s3.schedule:
            lines.append(f"        'schedule': {_escape_string(ds.s3.schedule)},")
        if ds.s3.from_timestamp:
            lines.append(f"        'from_timestamp': {_escape_string(ds.s3.from_timestamp)},")
        lines.append("    },")

    if ds.gcs:
        connection_var = to_snake_case(ds.gcs.connection_name)
        lines.append("    'gcs': {")
        lines.append(f"        'connection': {connection_var},")
        lines.append(f"        'bucket_uri': {_escape_string(ds.gcs.bucket_uri)},")
        if ds.gcs.schedule:
            lines.append(f"        'schedule': {_escape_string(ds.gcs.schedule)},")
        if ds.gcs.from_timestamp:
            lines.append(f"        'from_timestamp': {_escape_string(ds.gcs.from_timestamp)},")
        lines.append("    },")

    if ds.forward_query:
        lines.append("    'forward_query': '''")
        lines.append(ds.forward_query)
        lines.append("''',")

    if ds.tokens:
        lines.append("    'tokens': [")
        for token in ds.tokens:
            lines.append(
                f"        {{'name': {_escape_string(token.name)}, 'permissions': [{_escape_string(token.scope)}]}},"
            )
        lines.append("    ],")

    if ds.shared_with:
        lines.append(f"    'shared_with': [{', '.join(_escape_string(v) for v in ds.shared_with)}],")

    lines.append("})")
    lines.append("")
    return "\n".join(lines)


def _emit_kafka_connection(connection: KafkaConnectionModel) -> str:
    variable_name = to_snake_case(connection.name)
    lines: list[str] = []
    lines.append(f"{variable_name} = define_kafka_connection({_escape_string(connection.name)}, {{")
    lines.append(f"    'bootstrap_servers': {_escape_string(connection.bootstrap_servers)},")
    if connection.security_protocol:
        lines.append(f"    'security_protocol': {_escape_string(connection.security_protocol)},")
    if connection.sasl_mechanism:
        lines.append(f"    'sasl_mechanism': {_escape_string(connection.sasl_mechanism)},")
    if connection.key:
        lines.append(f"    'key': {_escape_string(connection.key)},")
    if connection.secret:
        lines.append(f"    'secret': {_escape_string(connection.secret)},")
    if connection.schema_registry_url:
        lines.append(f"    'schema_registry_url': {_escape_string(connection.schema_registry_url)},")
    if connection.ssl_ca_pem:
        lines.append(f"    'ssl_ca_pem': {_escape_string(connection.ssl_ca_pem)},")
    lines.append("})")
    lines.append("")
    return "\n".join(lines)


def _emit_s3_connection(connection: S3ConnectionModel) -> str:
    variable_name = to_snake_case(connection.name)
    lines: list[str] = []
    lines.append(f"{variable_name} = define_s3_connection({_escape_string(connection.name)}, {{")
    lines.append(f"    'region': {_escape_string(connection.region)},")
    if connection.arn:
        lines.append(f"    'arn': {_escape_string(connection.arn)},")
    if connection.access_key:
        lines.append(f"    'access_key': {_escape_string(connection.access_key)},")
    if connection.secret:
        lines.append(f"    'secret': {_escape_string(connection.secret)},")
    lines.append("})")
    lines.append("")
    return "\n".join(lines)


def _emit_gcs_connection(connection: GCSConnectionModel) -> str:
    variable_name = to_snake_case(connection.name)
    lines: list[str] = []
    lines.append(f"{variable_name} = define_gcs_connection({_escape_string(connection.name)}, {{")
    lines.append(f"    'service_account_credentials_json': {_escape_string(connection.service_account_credentials_json)},")
    lines.append("})")
    lines.append("")
    return "\n".join(lines)


def _emit_connection(connection: KafkaConnectionModel | S3ConnectionModel | GCSConnectionModel) -> str:
    if isinstance(connection, S3ConnectionModel):
        return _emit_s3_connection(connection)
    if isinstance(connection, GCSConnectionModel):
        return _emit_gcs_connection(connection)
    return _emit_kafka_connection(connection)


def _emit_pipe(pipe: PipeModel) -> str:
    variable_name = to_snake_case(pipe.name)
    lines: list[str] = []
    endpoint_output_columns = pipe.inferred_output_columns or ["result"]

    if pipe.description:
        lines.append('"""')
        lines.extend(pipe.description.split("\n"))
        lines.append('"""')

    define_func = "define_pipe"
    if pipe.type == "materialized":
        define_func = "define_materialized_view"
    elif pipe.type == "copy":
        define_func = "define_copy_pipe"
    elif pipe.type == "sink":
        define_func = "define_sink_pipe"

    lines.append(f"{variable_name} = {define_func}({_escape_string(pipe.name)}, {{")

    if pipe.description:
        lines.append(f"    'description': {_escape_string(pipe.description)},")

    if pipe.type in {"pipe", "endpoint", "sink"} and pipe.params:
        lines.append("    'params': {")
        for param in pipe.params:
            base_validator = _strict_param_base_validator(param.type)
            validator = _apply_param_optional(base_validator, param.required, param.default_value)
            validator = _apply_param_description(validator, param.description)
            lines.append(f"        '{param.name}': {validator},")
        lines.append("    },")

    if pipe.type == "materialized":
        lines.append(f"    'datasource': {to_snake_case(pipe.materialized_datasource or '')},")
        if pipe.deployment_method:
            lines.append(f"    'deployment_method': {_escape_string(pipe.deployment_method)},")

    if pipe.type == "copy":
        lines.append(f"    'datasource': {to_snake_case(pipe.copy_target_datasource or '')},")
        if pipe.copy_mode:
            lines.append(f"    'copy_mode': {_escape_string(pipe.copy_mode)},")
        if pipe.copy_schedule:
            lines.append(f"    'copy_schedule': {_escape_string(pipe.copy_schedule)},")

    if pipe.type == "sink" and pipe.sink:
        lines.append("    'sink': {")
        lines.append(f"        'connection': {to_snake_case(pipe.sink.connection_name)},")
        if isinstance(pipe.sink, SinkKafkaModel):
            lines.append(f"        'topic': {_escape_string(pipe.sink.topic)},")
            lines.append(f"        'schedule': {_escape_string(pipe.sink.schedule)},")
        elif isinstance(pipe.sink, SinkS3Model):
            lines.append(f"        'bucket_uri': {_escape_string(pipe.sink.bucket_uri)},")
            lines.append(f"        'file_template': {_escape_string(pipe.sink.file_template)},")
            lines.append(f"        'schedule': {_escape_string(pipe.sink.schedule)},")
            lines.append(f"        'format': {_escape_string(pipe.sink.format)},")
            if pipe.sink.strategy:
                lines.append(f"        'strategy': {_escape_string(pipe.sink.strategy)},")
            if pipe.sink.compression:
                lines.append(f"        'compression': {_escape_string(pipe.sink.compression)},")
        lines.append("    },")

    lines.append("    'nodes': [")
    for node in pipe.nodes:
        lines.append("        node({")
        lines.append(f"            'name': {_escape_string(node.name)},")
        if node.description:
            lines.append(f"            'description': {_escape_string(node.description)},")
        lines.append("            'sql': '''")
        lines.append(node.sql)
        lines.append("''',")
        lines.append("        }),")
    lines.append("    ],")

    if pipe.type == "endpoint":
        if pipe.cache_ttl is not None:
            lines.append(f"    'endpoint': {{'enabled': True, 'cache': {{'enabled': True, 'ttl': {pipe.cache_ttl}}}}},")
        else:
            lines.append("    'endpoint': True,")
        lines.append("    'output': {")
        for column_name in endpoint_output_columns:
            lines.append(f"        '{column_name}': t.string(),")
        lines.append("    },")

    if pipe.tokens:
        lines.append("    'tokens': [")
        for token in pipe.tokens:
            lines.append(f"        {{'name': {_escape_string(token.name)}}},")
        lines.append("    ],")

    lines.append("})")
    lines.append("")
    return "\n".join(lines)


def emit_migration_file_content(resources: list[ParsedResource]) -> str:
    connections = [resource for resource in resources if resource.kind == "connection"]
    datasources = [resource for resource in resources if resource.kind == "datasource"]
    pipes = [resource for resource in resources if resource.kind == "pipe"]

    needs_column = any(column.json_path is not None for ds in datasources for column in ds.columns)
    needs_params = any(pipe.params for pipe in pipes)

    pipe_define_funcs = set()
    for pipe in pipes:
        if pipe.type == "materialized":
            pipe_define_funcs.add("define_materialized_view")
        elif pipe.type == "copy":
            pipe_define_funcs.add("define_copy_pipe")
        elif pipe.type == "sink":
            pipe_define_funcs.add("define_sink_pipe")
        else:
            pipe_define_funcs.add("define_pipe")

    imports = {
        "define_datasource",
        "node",
        "t",
    } | pipe_define_funcs

    for conn in connections:
        if isinstance(conn, KafkaConnectionModel):
            imports.add("define_kafka_connection")
        elif isinstance(conn, S3ConnectionModel):
            imports.add("define_s3_connection")
        elif isinstance(conn, GCSConnectionModel):
            imports.add("define_gcs_connection")
    if needs_column:
        imports.add("column")
    if needs_params:
        imports.add("p")
    if any(ds.engine is not None for ds in datasources):
        imports.add("engine")

    lines: list[str] = []
    lines.append('"""')
    lines.append("Generated by tinybird migrate.")
    lines.append("Review endpoint output schemas and defaults before production use.")
    lines.append('"""')
    lines.append("")
    lines.append(f"from tinybird_sdk import {', '.join(sorted(imports))}")
    lines.append("")

    if connections:
        lines.append("# Connections")
        lines.append("")
        for connection in connections:
            lines.append(_emit_connection(connection))

    if datasources:
        lines.append("# Datasources")
        lines.append("")
        for datasource in datasources:
            lines.append(_emit_datasource(datasource))

    if pipes:
        lines.append("# Pipes")
        lines.append("")
        for pipe in pipes:
            lines.append(_emit_pipe(pipe))

    return "\n".join(lines).rstrip() + "\n"


def validate_resource_for_emission(resource: ParsedResource) -> None:
    if resource.kind == "connection":
        _emit_connection(resource)
    elif resource.kind == "datasource":
        _emit_datasource(resource)
    else:
        _emit_pipe(resource)
