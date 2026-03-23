from __future__ import annotations

from dataclasses import dataclass

from ..schema.connection import (
    ConnectionDefinition,
    GCSConnectionDefinition,
    KafkaConnectionDefinition,
    S3ConnectionDefinition,
)


@dataclass(frozen=True, slots=True)
class GeneratedConnection:
    name: str
    content: str


def _generate_kafka_connection(connection: KafkaConnectionDefinition) -> str:
    options = connection.options
    parts = [
        "TYPE kafka",
        f"KAFKA_BOOTSTRAP_SERVERS {options.bootstrap_servers}",
    ]

    if options.security_protocol:
        parts.append(f"KAFKA_SECURITY_PROTOCOL {options.security_protocol}")
    if options.sasl_mechanism:
        parts.append(f"KAFKA_SASL_MECHANISM {options.sasl_mechanism}")
    if options.key:
        parts.append(f"KAFKA_KEY {options.key}")
    if options.secret:
        parts.append(f"KAFKA_SECRET {options.secret}")
    if options.schema_registry_url:
        parts.append(f"KAFKA_SCHEMA_REGISTRY_URL {options.schema_registry_url}")
    if options.ssl_ca_pem:
        if "\n" in options.ssl_ca_pem:
            indented = "\n".join(f"    {line}" for line in options.ssl_ca_pem.split("\n"))
            parts.append(f"KAFKA_SSL_CA_PEM >\n{indented}")
        else:
            parts.append(f"KAFKA_SSL_CA_PEM {options.ssl_ca_pem}")

    return "\n".join(parts)


def _generate_s3_connection(connection: S3ConnectionDefinition) -> str:
    options = connection.options
    parts = [
        "TYPE s3",
        f"S3_REGION {options.region}",
    ]

    if options.arn:
        parts.append(f"S3_ARN {options.arn}")
    if options.access_key:
        parts.append(f"S3_ACCESS_KEY {options.access_key}")
    if options.secret:
        parts.append(f"S3_SECRET {options.secret}")

    return "\n".join(parts)


def _generate_gcs_connection(connection: GCSConnectionDefinition) -> str:
    options = connection.options
    parts = [
        "TYPE gcs",
        f"GCS_SERVICE_ACCOUNT_CREDENTIALS_JSON {options.service_account_credentials_json}",
    ]

    return "\n".join(parts)


def generate_connection(connection: ConnectionDefinition) -> GeneratedConnection:
    if isinstance(connection, KafkaConnectionDefinition):
        return GeneratedConnection(name=connection._name, content=_generate_kafka_connection(connection))
    if isinstance(connection, S3ConnectionDefinition):
        return GeneratedConnection(name=connection._name, content=_generate_s3_connection(connection))
    if isinstance(connection, GCSConnectionDefinition):
        return GeneratedConnection(name=connection._name, content=_generate_gcs_connection(connection))
    raise ValueError(f"Unsupported connection type: {connection._connectionType}")


def generate_all_connections(connections: dict[str, ConnectionDefinition]) -> list[GeneratedConnection]:
    return [generate_connection(connection) for connection in connections.values()]
