from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from .connection import KafkaConnectionDefinition, S3ConnectionDefinition
from .datasource import ColumnDefinition, DatasourceDefinition, SchemaDefinition, get_column_type
from .params import ParamValidator
from .token import TokenDefinition
from .types import TypeValidator, get_tinybird_type

SinkStrategy = str  # "create_new" | "replace"
SinkCompression = str  # "none" | "gzip" | "snappy"

NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

ParamsDefinition = dict[str, ParamValidator]
OutputDefinition = dict[str, TypeValidator]


@dataclass(frozen=True, slots=True)
class NodeDefinition:
    _name: str
    sql: str
    description: str | None = None
    _type: str = "node"


@dataclass(frozen=True, slots=True)
class EndpointCacheConfig:
    enabled: bool
    ttl: int | None = None


@dataclass(frozen=True, slots=True)
class EndpointConfig:
    enabled: bool
    cache: EndpointCacheConfig | None = None


@dataclass(frozen=True, slots=True)
class MaterializedConfig:
    datasource: DatasourceDefinition
    deployment_method: str | None = None


@dataclass(frozen=True, slots=True)
class CopyConfig:
    datasource: DatasourceDefinition
    copy_mode: str | None = None
    copy_schedule: str | None = None


@dataclass(frozen=True, slots=True)
class KafkaSinkConfig:
    connection: KafkaConnectionDefinition
    topic: str
    schedule: str


@dataclass(frozen=True, slots=True)
class S3SinkConfig:
    connection: S3ConnectionDefinition
    bucket_uri: str
    file_template: str
    format: str
    schedule: str
    strategy: SinkStrategy | None = None
    compression: SinkCompression | None = None


SinkConfig = KafkaSinkConfig | S3SinkConfig


@dataclass(frozen=True, slots=True)
class InlinePipeTokenConfig:
    name: str


@dataclass(frozen=True, slots=True)
class PipeTokenReference:
    token: TokenDefinition
    scope: str


PipeTokenConfig = InlinePipeTokenConfig | PipeTokenReference


@dataclass(frozen=True, slots=True)
class PipeOptions:
    nodes: tuple[NodeDefinition, ...]
    description: str | None = None
    params: ParamsDefinition = field(default_factory=dict)
    output: OutputDefinition | None = None
    endpoint: bool | EndpointConfig | None = None
    materialized: MaterializedConfig | None = None
    copy: CopyConfig | None = None
    sink: SinkConfig | None = None
    tokens: tuple[PipeTokenConfig, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class PipeDefinition:
    _name: str
    options: PipeOptions
    _type: str = "pipe"

    @property
    def _params(self) -> ParamsDefinition:
        return self.options.params

    @property
    def _output(self) -> OutputDefinition | None:
        return self.options.output


def node(options: dict[str, Any] | NodeDefinition) -> NodeDefinition:
    if isinstance(options, NodeDefinition):
        candidate = options
    else:
        candidate = NodeDefinition(
            _name=options["name"],
            sql=options["sql"],
            description=options.get("description"),
        )

    if not NAME_PATTERN.match(candidate._name):
        raise ValueError(
            f'Invalid node name: "{candidate._name}". Must start with a letter or underscore and contain only alphanumeric characters and underscores.'
        )
    return candidate


def _normalize_type_for_comparison(type_name: str) -> str:
    normalized = re.sub(r"^Nullable\((.+)\)$", r"\1", type_name)
    normalized = re.sub(r"^LowCardinality\((.+)\)$", r"\1", normalized)
    normalized = re.sub(r"^LowCardinality\(Nullable\((.+)\)\)$", r"\1", normalized)
    normalized = re.sub(r"^DateTime\('[^']+'\)$", "DateTime", normalized)
    normalized = re.sub(r"^DateTime64\((\d+),\s*'[^']+'\)$", r"DateTime64(\1)", normalized)
    return normalized


def _types_are_compatible(output_type: str, datasource_type: str) -> bool:
    normalized_output = _normalize_type_for_comparison(output_type)
    normalized_datasource = _normalize_type_for_comparison(datasource_type)

    if normalized_output == normalized_datasource:
        return True

    simple_agg = re.match(r"^SimpleAggregateFunction\([^,]+,\s*(.+)\)$", normalized_datasource)
    if simple_agg and normalized_output == simple_agg.group(1):
        return True

    agg = re.match(r"^AggregateFunction\([^,]+,\s*(.+)\)$", normalized_datasource)
    if agg and normalized_output == agg.group(1):
        return True

    return False


def _validate_materialized_schema(pipe_name: str, output: OutputDefinition, datasource: DatasourceDefinition) -> None:
    output_columns = list(output.keys())
    datasource_columns = list(datasource._schema.keys())

    missing = [col for col in datasource_columns if col not in output_columns]
    if missing:
        raise ValueError(
            f'Materialized view "{pipe_name}" output schema is missing columns from target datasource "{datasource._name}": {", ".join(missing)}'
        )

    extra = [col for col in output_columns if col not in datasource_columns]
    if extra:
        raise ValueError(
            f'Materialized view "{pipe_name}" output schema has columns not in target datasource "{datasource._name}": {", ".join(extra)}'
        )

    for column_name in output_columns:
        output_type = get_tinybird_type(output[column_name])
        datasource_type = get_tinybird_type(get_column_type(datasource._schema[column_name]))
        if not _types_are_compatible(output_type, datasource_type):
            raise ValueError(
                f'Materialized view "{pipe_name}" column "{column_name}" type mismatch: '
                f'output has "{output_type}" but target datasource "{datasource._name}" expects "{datasource_type}"'
            )


def define_pipe(name: str, options: dict[str, Any] | PipeOptions) -> PipeDefinition:
    if not NAME_PATTERN.match(name):
        raise ValueError(
            f'Invalid pipe name: "{name}". Must start with a letter or underscore and contain only alphanumeric characters and underscores.'
        )

    normalized = _normalize_pipe_options(options)

    if not normalized.nodes:
        raise ValueError(f'Pipe "{name}" must have at least one node.')

    if (normalized.endpoint or normalized.materialized) and (not normalized.output or len(normalized.output) == 0):
        raise ValueError(
            f'Pipe "{name}" must have an output schema defined when used as an endpoint or materialized view.'
        )

    type_count = len([x for x in (normalized.endpoint, normalized.materialized, normalized.copy, normalized.sink) if x])
    if type_count > 1:
        raise ValueError(
            f'Pipe "{name}" can only have one of: endpoint, materialized, copy, or sink configuration. A pipe must be at most one type.'
        )

    if normalized.materialized and normalized.output:
        _validate_materialized_schema(name, normalized.output, normalized.materialized.datasource)

    return PipeDefinition(_name=name, options=normalized)


def _normalize_pipe_options(options: dict[str, Any] | PipeOptions) -> PipeOptions:
    if isinstance(options, PipeOptions):
        return options

    normalized_nodes = tuple(node(item) for item in options.get("nodes", ()))
    params = options.get("params") or {}
    output = options.get("output")

    endpoint = options.get("endpoint")
    endpoint_config: bool | EndpointConfig | None
    if isinstance(endpoint, dict):
        cache = endpoint.get("cache")
        cache_config = EndpointCacheConfig(**cache) if isinstance(cache, dict) else cache
        endpoint_config = EndpointConfig(enabled=endpoint.get("enabled", True), cache=cache_config)
    else:
        endpoint_config = endpoint

    materialized_raw = options.get("materialized")
    materialized = (
        MaterializedConfig(**materialized_raw)
        if isinstance(materialized_raw, dict)
        else materialized_raw
    )

    copy_raw = options.get("copy")
    copy = CopyConfig(**copy_raw) if isinstance(copy_raw, dict) else copy_raw

    sink_raw = options.get("sink")
    sink = _normalize_sink_config(sink_raw) if isinstance(sink_raw, dict) else sink_raw

    tokens = tuple(_normalize_pipe_token(token) for token in options.get("tokens", ()) or ())

    return PipeOptions(
        description=options.get("description"),
        params=params,
        nodes=normalized_nodes,
        output=output,
        endpoint=endpoint_config,
        materialized=materialized,
        copy=copy,
        sink=sink,
        tokens=tokens,
    )


def _normalize_pipe_token(token: Any) -> PipeTokenConfig:
    if isinstance(token, (InlinePipeTokenConfig, PipeTokenReference)):
        return token
    if "token" in token:
        return PipeTokenReference(token=token["token"], scope=token["scope"])
    return InlinePipeTokenConfig(name=token["name"])


def _normalize_sink_config(raw: dict[str, Any]) -> SinkConfig:
    connection = raw["connection"]
    if isinstance(connection, KafkaConnectionDefinition):
        return KafkaSinkConfig(
            connection=connection,
            topic=raw["topic"],
            schedule=raw["schedule"],
        )
    if isinstance(connection, S3ConnectionDefinition):
        return S3SinkConfig(
            connection=connection,
            bucket_uri=raw["bucket_uri"],
            file_template=raw["file_template"],
            format=raw["format"],
            schedule=raw["schedule"],
            strategy=raw.get("strategy"),
            compression=raw.get("compression"),
        )
    raise ValueError(f"Sink connection must be a Kafka or S3 connection definition.")


def define_sink_pipe(name: str, options: dict[str, Any]) -> PipeDefinition:
    return define_pipe(
        name,
        {
            "description": options.get("description"),
            "params": options.get("params"),
            "nodes": options["nodes"],
            "sink": options["sink"],
            "tokens": options.get("tokens", ()),
        },
    )


def define_materialized_view(name: str, options: dict[str, Any]) -> PipeDefinition:
    datasource: DatasourceDefinition = options["datasource"]
    output: dict[str, TypeValidator] = {}
    for column_name, column in datasource._schema.items():
        output[column_name] = get_column_type(column)

    return define_pipe(
        name,
        {
            "description": options.get("description"),
            "nodes": options["nodes"],
            "output": output,
            "materialized": {
                "datasource": datasource,
                "deployment_method": options.get("deployment_method"),
            },
            "tokens": options.get("tokens", ()),
        },
    )


def define_endpoint(name: str, options: dict[str, Any]) -> PipeDefinition:
    endpoint = {"enabled": True}
    if options.get("cache"):
        endpoint["cache"] = options["cache"]

    return define_pipe(
        name,
        {
            "description": options.get("description"),
            "params": options.get("params"),
            "nodes": options["nodes"],
            "output": options["output"],
            "endpoint": endpoint,
            "tokens": options.get("tokens", ()),
        },
    )


def define_copy_pipe(name: str, options: dict[str, Any]) -> PipeDefinition:
    datasource: DatasourceDefinition = options["datasource"]
    output: dict[str, TypeValidator] = {}
    for column_name, column in datasource._schema.items():
        output[column_name] = get_column_type(column)

    return define_pipe(
        name,
        {
            "description": options.get("description"),
            "nodes": options["nodes"],
            "output": output,
            "copy": {
                "datasource": datasource,
                "copy_mode": options.get("copy_mode"),
                "copy_schedule": options.get("copy_schedule"),
            },
            "tokens": options.get("tokens", ()),
        },
    )


def is_pipe_definition(value: Any) -> bool:
    return isinstance(value, PipeDefinition)


def is_node_definition(value: Any) -> bool:
    return isinstance(value, NodeDefinition)


def get_endpoint_config(pipe: PipeDefinition) -> EndpointConfig | None:
    endpoint = pipe.options.endpoint
    if not endpoint:
        return None
    if isinstance(endpoint, bool):
        return EndpointConfig(enabled=True) if endpoint else None
    return endpoint if endpoint.enabled else None


def get_materialized_config(pipe: PipeDefinition) -> MaterializedConfig | None:
    return pipe.options.materialized


def get_copy_config(pipe: PipeDefinition) -> CopyConfig | None:
    return pipe.options.copy


def is_materialized_view(pipe: PipeDefinition) -> bool:
    return pipe.options.materialized is not None


def is_copy_pipe(pipe: PipeDefinition) -> bool:
    return pipe.options.copy is not None


def get_sink_config(pipe: PipeDefinition) -> SinkConfig | None:
    return pipe.options.sink


def is_sink_pipe(pipe: PipeDefinition) -> bool:
    return pipe.options.sink is not None


def get_node_names(pipe: PipeDefinition) -> list[str]:
    return [n._name for n in pipe.options.nodes]


def get_node(pipe: PipeDefinition, name: str) -> NodeDefinition | None:
    for n in pipe.options.nodes:
        if n._name == name:
            return n
    return None


def sql(*values: Any) -> str:
    parts: list[str] = []
    for value in values:
        if value is None:
            continue
        if isinstance(value, (str, int, float)):
            parts.append(str(value))
        elif hasattr(value, "_name"):
            parts.append(str(getattr(value, "_name")))
        else:
            parts.append(str(value))
    return "".join(parts)
