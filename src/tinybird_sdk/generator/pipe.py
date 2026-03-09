from __future__ import annotations

import re
from dataclasses import dataclass

from ..schema.pipe import (
    CopyConfig,
    EndpointConfig,
    KafkaSinkConfig,
    MaterializedConfig,
    PipeDefinition,
    S3SinkConfig,
    SinkConfig,
    get_copy_config,
    get_endpoint_config,
    get_materialized_config,
    get_sink_config,
)


@dataclass(frozen=True, slots=True)
class GeneratedPipe:
    name: str
    content: str


def _has_dynamic_parameters(sql: str) -> bool:
    return bool(re.search(r"\{\{[^}]+\}\}|\{%[^%]+%\}", sql))


def _generate_node(node) -> str:
    parts = [f"NODE {node._name}"]

    if node.description:
        parts.extend(["DESCRIPTION >", f"    {node.description}"])

    parts.append("SQL >")
    if _has_dynamic_parameters(node.sql):
        parts.append("    %")

    for line in node.sql.strip().splitlines():
        parts.append(f"    {line}")

    return "\n".join(parts)


def _generate_endpoint(endpoint: EndpointConfig) -> str:
    parts = ["TYPE endpoint"]
    if endpoint.cache and endpoint.cache.enabled:
        parts.append(f"CACHE {endpoint.cache.ttl if endpoint.cache.ttl is not None else 60}")
    return "\n".join(parts)


def _generate_materialized(config: MaterializedConfig) -> str:
    parts = ["TYPE MATERIALIZED", f"DATASOURCE {config.datasource._name}"]
    if config.deployment_method == "alter":
        parts.append("DEPLOYMENT_METHOD alter")
    return "\n".join(parts)


def _generate_copy(config: CopyConfig) -> str:
    parts = ["TYPE COPY", f"TARGET_DATASOURCE {config.datasource._name}"]
    if config.copy_schedule:
        parts.append(f"COPY_SCHEDULE {config.copy_schedule}")
    if config.copy_mode:
        parts.append(f"COPY_MODE {config.copy_mode}")
    return "\n".join(parts)


def _generate_sink(config: SinkConfig) -> str:
    parts = ["TYPE sink"]
    parts.append(f"EXPORT_CONNECTION_NAME {config.connection._name}")

    if isinstance(config, KafkaSinkConfig):
        parts.append(f"EXPORT_KAFKA_TOPIC {config.topic}")
        parts.append(f"EXPORT_SCHEDULE {config.schedule}")
    elif isinstance(config, S3SinkConfig):
        parts.append(f"EXPORT_BUCKET_URI {config.bucket_uri}")
        parts.append(f"EXPORT_FILE_TEMPLATE {config.file_template}")
        parts.append(f"EXPORT_SCHEDULE {config.schedule}")
        parts.append(f"EXPORT_FORMAT {config.format}")
        if config.strategy:
            parts.append(f"EXPORT_STRATEGY {config.strategy}")
        if config.compression:
            parts.append(f"EXPORT_COMPRESSION {config.compression}")

    return "\n".join(parts)


def _generate_tokens(tokens) -> list[str]:
    lines: list[str] = []
    for token in tokens:
        if hasattr(token, "token"):
            lines.append(f"TOKEN {token.token._name} {token.scope}")
        else:
            lines.append(f"TOKEN {token.name} READ")
    return lines


def generate_pipe(pipe: PipeDefinition) -> GeneratedPipe:
    parts: list[str] = []

    if pipe.options.description:
        parts.extend([f"DESCRIPTION >\n    {pipe.options.description}", ""])

    for index, node in enumerate(pipe.options.nodes):
        parts.append(_generate_node(node))
        if index < len(pipe.options.nodes) - 1:
            parts.append("")

    endpoint = get_endpoint_config(pipe)
    if endpoint:
        parts.extend(["", _generate_endpoint(endpoint)])

    materialized = get_materialized_config(pipe)
    if materialized:
        parts.extend(["", _generate_materialized(materialized)])

    copy = get_copy_config(pipe)
    if copy:
        parts.extend(["", _generate_copy(copy)])

    sink = get_sink_config(pipe)
    if sink:
        parts.extend(["", _generate_sink(sink)])

    token_lines = _generate_tokens(pipe.options.tokens)
    if token_lines:
        parts.extend(["", "\n".join(token_lines)])

    return GeneratedPipe(name=pipe._name, content="\n".join(parts))


def generate_all_pipes(pipes: dict[str, PipeDefinition]) -> list[GeneratedPipe]:
    return [generate_pipe(pipe) for pipe in pipes.values()]
