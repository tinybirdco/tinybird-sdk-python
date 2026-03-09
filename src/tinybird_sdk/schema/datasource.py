from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from .connection import GCSConnectionDefinition, KafkaConnectionDefinition, S3ConnectionDefinition
from .engines import EngineConfig
from .token import TokenDefinition
from .types import TypeValidator

NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


@dataclass(frozen=True, slots=True)
class ColumnDefinition:
    type: TypeValidator
    json_path: str | None = None


SchemaDefinition = dict[str, TypeValidator | ColumnDefinition]


@dataclass(frozen=True, slots=True)
class InlineTokenConfig:
    name: str
    permissions: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class DatasourceTokenReference:
    token: TokenDefinition
    scope: str


TokenConfig = InlineTokenConfig | DatasourceTokenReference


@dataclass(frozen=True, slots=True)
class KafkaConfig:
    connection: KafkaConnectionDefinition
    topic: str
    group_id: str | None = None
    auto_offset_reset: str | None = None
    store_raw_value: bool | None = None


@dataclass(frozen=True, slots=True)
class S3Config:
    connection: S3ConnectionDefinition
    bucket_uri: str
    schedule: str | None = None
    from_timestamp: str | None = None


@dataclass(frozen=True, slots=True)
class GCSConfig:
    connection: GCSConnectionDefinition
    bucket_uri: str
    schedule: str | None = None
    from_timestamp: str | None = None


@dataclass(frozen=True, slots=True)
class DatasourceIndex:
    name: str
    expr: str
    type: str
    granularity: int


@dataclass(frozen=True, slots=True)
class DatasourceOptions:
    schema: SchemaDefinition
    description: str | None = None
    engine: EngineConfig | None = None
    tokens: tuple[TokenConfig, ...] = field(default_factory=tuple)
    shared_with: tuple[str, ...] = field(default_factory=tuple)
    json_paths: bool = True
    forward_query: str | None = None
    indexes: tuple[DatasourceIndex, ...] = field(default_factory=tuple)
    kafka: KafkaConfig | None = None
    s3: S3Config | None = None
    gcs: GCSConfig | None = None


@dataclass(frozen=True, slots=True)
class DatasourceDefinition:
    _name: str
    options: DatasourceOptions
    _type: str = "datasource"

    @property
    def _schema(self) -> SchemaDefinition:
        return self.options.schema


def define_datasource(name: str, options: dict[str, Any] | DatasourceOptions) -> DatasourceDefinition:
    if not NAME_PATTERN.match(name):
        raise ValueError(
            f'Invalid datasource name: "{name}". Must start with a letter or underscore and contain only alphanumeric characters and underscores.'
        )

    if isinstance(options, DatasourceOptions):
        normalized = options
    else:
        tokens = tuple(_normalize_token_config(token) for token in options.get("tokens", ()) or ())
        shared_with = tuple(options.get("shared_with", ()) or ())
        indexes = tuple(
            index if isinstance(index, DatasourceIndex) else DatasourceIndex(**index)
            for index in (options.get("indexes", ()) or ())
        )
        kafka_cfg = options.get("kafka")
        kafka = KafkaConfig(**kafka_cfg) if isinstance(kafka_cfg, dict) else kafka_cfg
        s3_cfg = options.get("s3")
        s3 = S3Config(**s3_cfg) if isinstance(s3_cfg, dict) else s3_cfg
        gcs_cfg = options.get("gcs")
        gcs = GCSConfig(**gcs_cfg) if isinstance(gcs_cfg, dict) else gcs_cfg
        normalized = DatasourceOptions(
            description=options.get("description"),
            schema=options["schema"],
            engine=options.get("engine"),
            tokens=tokens,
            shared_with=shared_with,
            json_paths=options.get("json_paths", True),
            forward_query=options.get("forward_query"),
            indexes=indexes,
            kafka=kafka,
            s3=s3,
            gcs=gcs,
        )

    ingestion_count = sum(1 for x in [normalized.kafka, normalized.s3, normalized.gcs] if x is not None)
    if ingestion_count > 1:
        raise ValueError("Datasource can only define one ingestion option: `kafka`, `s3`, or `gcs`.")

    for index in normalized.indexes:
        if not index.name or any(char.isspace() for char in index.name):
            raise ValueError(
                f'Invalid datasource index name: "{index.name}". Index names must be non-empty and cannot contain whitespace.'
            )
        if not index.expr.strip():
            raise ValueError(f'Invalid datasource index "{index.name}": expr is required.')
        if not index.type.strip():
            raise ValueError(f'Invalid datasource index "{index.name}": type is required.')
        if isinstance(index.granularity, bool) or not isinstance(index.granularity, int) or index.granularity <= 0:
            raise ValueError(
                f'Invalid datasource index "{index.name}": granularity must be a positive integer.'
            )

    return DatasourceDefinition(_name=name, options=normalized)


def _normalize_token_config(token: Any) -> TokenConfig:
    if isinstance(token, (InlineTokenConfig, DatasourceTokenReference)):
        return token

    if "token" in token:
        return DatasourceTokenReference(token=token["token"], scope=token["scope"])

    permissions = token.get("permissions", ())
    return InlineTokenConfig(name=token["name"], permissions=tuple(permissions))


def is_datasource_definition(value: Any) -> bool:
    return isinstance(value, DatasourceDefinition)


def get_column_type(column: TypeValidator | ColumnDefinition) -> TypeValidator:
    if isinstance(column, ColumnDefinition):
        return column.type
    return column


def get_column_json_path(column: TypeValidator | ColumnDefinition) -> str | None:
    if isinstance(column, ColumnDefinition):
        return column.json_path
    return None


def get_column_names(schema: SchemaDefinition) -> list[str]:
    return list(schema.keys())


def column(type: TypeValidator, options: dict[str, Any] | None = None) -> ColumnDefinition:
    json_path = options.get("json_path") if options else None
    return ColumnDefinition(type=type, json_path=json_path)
