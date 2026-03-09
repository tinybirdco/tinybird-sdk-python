from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


ResourceKind = Literal["datasource", "pipe", "connection"]


@dataclass(frozen=True, slots=True)
class ResourceFile:
    kind: ResourceKind
    file_path: str
    absolute_path: str
    name: str
    content: str


@dataclass(frozen=True, slots=True)
class MigrationError:
    file_path: str
    resource_name: str
    resource_kind: ResourceKind
    message: str


@dataclass(frozen=True, slots=True)
class DatasourceColumnModel:
    name: str
    type: str
    json_path: str | None = None
    default_expression: str | None = None
    codec: str | None = None


@dataclass(frozen=True, slots=True)
class DatasourceEngineModel:
    type: str
    sorting_key: list[str]
    partition_key: str | None = None
    primary_key: list[str] | None = None
    ttl: str | None = None
    ver: str | None = None
    is_deleted: str | None = None
    sign: str | None = None
    version: str | None = None
    summing_columns: list[str] | None = None
    settings: dict[str, str | int | float | bool] | None = None


@dataclass(frozen=True, slots=True)
class DatasourceKafkaModel:
    connection_name: str
    topic: str
    group_id: str | None = None
    auto_offset_reset: Literal["earliest", "latest"] | None = None
    store_raw_value: bool | None = None


@dataclass(frozen=True, slots=True)
class DatasourceS3Model:
    connection_name: str
    bucket_uri: str
    schedule: str | None = None
    from_timestamp: str | None = None


@dataclass(frozen=True, slots=True)
class DatasourceGCSModel:
    connection_name: str
    bucket_uri: str
    schedule: str | None = None
    from_timestamp: str | None = None


@dataclass(frozen=True, slots=True)
class DatasourceTokenModel:
    name: str
    scope: Literal["READ", "APPEND"]


@dataclass(frozen=True, slots=True)
class DatasourceIndexModel:
    name: str
    expr: str
    type: str
    granularity: int


@dataclass(frozen=True, slots=True)
class DatasourceModel:
    kind: Literal["datasource"]
    name: str
    file_path: str
    columns: list[DatasourceColumnModel]
    engine: DatasourceEngineModel | None = None
    description: str | None = None
    indexes: list[DatasourceIndexModel] = field(default_factory=list)
    kafka: DatasourceKafkaModel | None = None
    s3: DatasourceS3Model | None = None
    gcs: DatasourceGCSModel | None = None
    forward_query: str | None = None
    tokens: list[DatasourceTokenModel] = field(default_factory=list)
    shared_with: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class PipeNodeModel:
    name: str
    sql: str
    description: str | None = None


@dataclass(frozen=True, slots=True)
class PipeTokenModel:
    name: str
    scope: Literal["READ"]


PipeTypeModel = Literal["pipe", "endpoint", "materialized", "copy", "sink"]


@dataclass(frozen=True, slots=True)
class PipeParamModel:
    name: str
    type: str
    required: bool
    default_value: str | int | float | bool | None = None
    description: str | None = None


@dataclass(frozen=True, slots=True)
class SinkKafkaModel:
    service: Literal["kafka"]
    connection_name: str
    topic: str
    schedule: str


@dataclass(frozen=True, slots=True)
class SinkS3Model:
    service: Literal["s3"]
    connection_name: str
    bucket_uri: str
    file_template: str
    format: str
    schedule: str
    strategy: Literal["create_new", "replace"] | None = None
    compression: Literal["none", "gzip", "snappy"] | None = None


SinkModel = SinkKafkaModel | SinkS3Model


@dataclass(frozen=True, slots=True)
class PipeModel:
    kind: Literal["pipe"]
    name: str
    file_path: str
    type: PipeTypeModel
    nodes: list[PipeNodeModel]
    description: str | None = None
    cache_ttl: int | None = None
    materialized_datasource: str | None = None
    deployment_method: Literal["alter"] | None = None
    copy_target_datasource: str | None = None
    copy_schedule: str | None = None
    copy_mode: Literal["append", "replace"] | None = None
    sink: SinkModel | None = None
    tokens: list[PipeTokenModel] = field(default_factory=list)
    params: list[PipeParamModel] = field(default_factory=list)
    inferred_output_columns: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class KafkaConnectionModel:
    kind: Literal["connection"]
    name: str
    file_path: str
    connection_type: Literal["kafka"]
    bootstrap_servers: str
    security_protocol: Literal["SASL_SSL", "PLAINTEXT", "SASL_PLAINTEXT"] | None = None
    sasl_mechanism: Literal["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER"] | None = None
    key: str | None = None
    secret: str | None = None
    schema_registry_url: str | None = None
    ssl_ca_pem: str | None = None


@dataclass(frozen=True, slots=True)
class S3ConnectionModel:
    kind: Literal["connection"]
    name: str
    file_path: str
    connection_type: Literal["s3"]
    region: str
    arn: str | None = None
    access_key: str | None = None
    secret: str | None = None


@dataclass(frozen=True, slots=True)
class GCSConnectionModel:
    kind: Literal["connection"]
    name: str
    file_path: str
    connection_type: Literal["gcs"]
    service_account_credentials_json: str


ConnectionModel = KafkaConnectionModel | S3ConnectionModel | GCSConnectionModel

ParsedResource = DatasourceModel | PipeModel | KafkaConnectionModel | S3ConnectionModel | GCSConnectionModel


@dataclass(frozen=True, slots=True)
class MigrationResult:
    success: bool
    output_path: str
    migrated: list[ParsedResource]
    errors: list[MigrationError]
    dry_run: bool
    output_content: str | None = None
