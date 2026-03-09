from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

KafkaSecurityProtocol = str
KafkaSaslMechanism = str


def _validate_connection_name(name: str) -> None:
    if not NAME_PATTERN.match(name):
        raise ValueError(
            f'Invalid connection name: "{name}". Must start with a letter or underscore and contain only alphanumeric characters and underscores.'
        )


@dataclass(frozen=True, slots=True)
class KafkaConnectionOptions:
    bootstrap_servers: str
    security_protocol: KafkaSecurityProtocol | None = None
    sasl_mechanism: KafkaSaslMechanism | None = None
    key: str | None = None
    secret: str | None = None
    schema_registry_url: str | None = None
    ssl_ca_pem: str | None = None


@dataclass(frozen=True, slots=True)
class KafkaConnectionDefinition:
    _name: str
    options: KafkaConnectionOptions
    _type: str = "connection"
    _connectionType: str = "kafka"


@dataclass(frozen=True, slots=True)
class S3ConnectionOptions:
    region: str
    arn: str | None = None
    access_key: str | None = None
    secret: str | None = None


@dataclass(frozen=True, slots=True)
class S3ConnectionDefinition:
    _name: str
    options: S3ConnectionOptions
    _type: str = "connection"
    _connectionType: str = "s3"


@dataclass(frozen=True, slots=True)
class GCSConnectionOptions:
    service_account_credentials_json: str


@dataclass(frozen=True, slots=True)
class GCSConnectionDefinition:
    _name: str
    options: GCSConnectionOptions
    _type: str = "connection"
    _connectionType: str = "gcs"


ConnectionDefinition = KafkaConnectionDefinition | S3ConnectionDefinition | GCSConnectionDefinition


def define_kafka_connection(name: str, options: dict[str, Any] | KafkaConnectionOptions) -> KafkaConnectionDefinition:
    _validate_connection_name(name)
    normalized = options if isinstance(options, KafkaConnectionOptions) else KafkaConnectionOptions(**options)
    return KafkaConnectionDefinition(_name=name, options=normalized)


def define_s3_connection(name: str, options: dict[str, Any] | S3ConnectionOptions) -> S3ConnectionDefinition:
    _validate_connection_name(name)
    normalized = options if isinstance(options, S3ConnectionOptions) else S3ConnectionOptions(**options)

    if not normalized.arn and not (normalized.access_key and normalized.secret):
        raise ValueError("S3 connection requires either `arn` or both `access_key` and `secret`.")

    if (normalized.access_key and not normalized.secret) or (not normalized.access_key and normalized.secret):
        raise ValueError("S3 connection `access_key` and `secret` must be provided together.")

    return S3ConnectionDefinition(_name=name, options=normalized)


def define_gcs_connection(name: str, options: dict[str, Any] | GCSConnectionOptions) -> GCSConnectionDefinition:
    _validate_connection_name(name)
    normalized = options if isinstance(options, GCSConnectionOptions) else GCSConnectionOptions(**options)

    if not normalized.service_account_credentials_json.strip():
        raise ValueError("GCS connection `service_account_credentials_json` is required.")

    return GCSConnectionDefinition(_name=name, options=normalized)


def is_connection_definition(value: Any) -> bool:
    return isinstance(value, (KafkaConnectionDefinition, S3ConnectionDefinition, GCSConnectionDefinition))


def is_kafka_connection_definition(value: Any) -> bool:
    return isinstance(value, KafkaConnectionDefinition)


def is_s3_connection_definition(value: Any) -> bool:
    return isinstance(value, S3ConnectionDefinition)


def is_gcs_connection_definition(value: Any) -> bool:
    return isinstance(value, GCSConnectionDefinition)


def get_connection_type(connection: ConnectionDefinition) -> str:
    return connection._connectionType
