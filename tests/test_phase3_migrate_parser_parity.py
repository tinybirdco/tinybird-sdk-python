from __future__ import annotations

import pytest

from tinybird_sdk.migrate.parse_connection import parse_connection_file
from tinybird_sdk.migrate.parse_datasource import parse_datasource_file
from tinybird_sdk.migrate.parse_pipe import parse_pipe_file
from tinybird_sdk.migrate.parser_utils import MigrationParseError
from tinybird_sdk.migrate.types import ResourceFile


def _resource(kind: str, name: str, content: str) -> ResourceFile:
    extension = {
        "connection": "connection",
        "datasource": "datasource",
        "pipe": "pipe",
    }[kind]
    return ResourceFile(
        kind=kind,  # type: ignore[arg-type]
        name=name,
        file_path=f"{name}.{extension}",
        absolute_path=f"/tmp/{name}.{extension}",
        content=content,
    )


def test_parse_connection_supports_single_quoted_type_and_schema_registry() -> None:
    parsed = parse_connection_file(
        _resource(
            "connection",
            "broker",
            "\n".join(
                [
                    "TYPE 'kafka'",
                    "KAFKA_BOOTSTRAP_SERVERS localhost:9092",
                    "KAFKA_SCHEMA_REGISTRY_URL https://registry.example.com",
                ]
            ),
        )
    )

    assert parsed.connection_type == "kafka"
    assert parsed.bootstrap_servers == "localhost:9092"
    assert parsed.schema_registry_url == "https://registry.example.com"


def test_parse_datasource_supports_engine_is_deleted_and_kafka_store_raw_value() -> None:
    parsed = parse_datasource_file(
        _resource(
            "datasource",
            "events",
            "\n".join(
                [
                    "SCHEMA >",
                    "    id Int64",
                    'ENGINE "ReplacingMergeTree"',
                    'ENGINE_SORTING_KEY "id"',
                    'ENGINE_VER "version"',
                    'ENGINE_IS_DELETED "is_deleted"',
                    "KAFKA_CONNECTION_NAME stream",
                    "KAFKA_TOPIC events",
                    "KAFKA_STORE_RAW_VALUE True",
                ]
            ),
        )
    )

    assert parsed.engine is not None
    assert parsed.engine.is_deleted == "is_deleted"
    assert parsed.kafka is not None
    assert parsed.kafka.store_raw_value is True


def test_parse_datasource_supports_import_directives() -> None:
    parsed = parse_datasource_file(
        _resource(
            "datasource",
            "billing",
            "\n".join(
                [
                    "SCHEMA >",
                    "    id Int64",
                    "IMPORT_CONNECTION_NAME 'gcs_conn'",
                    "IMPORT_BUCKET_URI 'gs://my-bucket/path/*.ndjson'",
                    "IMPORT_SCHEDULE '@daily'",
                    "IMPORT_FROM_TIMESTAMP '2025-01-01 00:00:00'",
                ]
            ),
        )
    )

    assert parsed.s3 is not None
    assert parsed.s3.connection_name == "gcs_conn"
    assert parsed.s3.bucket_uri == "gs://my-bucket/path/*.ndjson"
    assert parsed.s3.schedule == "@daily"
    assert parsed.s3.from_timestamp == "2025-01-01 00:00:00"


def test_parse_pipe_supports_param_options_and_placeholder_normalization() -> None:
    parsed = parse_pipe_file(
        _resource(
            "pipe",
            "top_events",
            "\n".join(
                [
                    "TYPE endpoint",
                    "NODE endpoint",
                    "SQL >",
                    "    SELECT * FROM events",
                    "    WHERE user_id = {{ String(user_id, default='anon', required=false, description='User id') }}",
                    "      AND active = {{ Boolean(active, default=true) }}",
                    "      AND payload = {{ json(payload, description='Raw payload') }}",
                    "    ORDER BY {{ column(sort_col) }} DESC",
                ]
            ),
        )
    )

    assert [param.name for param in parsed.params] == ["active", "payload", "sort_col", "user_id"]

    active = next(param for param in parsed.params if param.name == "active")
    assert active.type == "Boolean"
    assert active.required is False
    assert active.default_value is True

    payload = next(param for param in parsed.params if param.name == "payload")
    assert payload.type == "JSON"
    assert payload.required is True
    assert payload.description == "Raw payload"

    sort_col = next(param for param in parsed.params if param.name == "sort_col")
    assert sort_col.type == "column"

    user_id = next(param for param in parsed.params if param.name == "user_id")
    assert user_id.required is False
    assert user_id.default_value == "anon"
    assert user_id.description == "User id"

    sql = parsed.nodes[0].sql
    assert "{{ String(user_id) }}" in sql
    assert "{{ Boolean(active) }}" in sql
    assert "{{ json(payload) }}" in sql


def test_parse_pipe_supports_export_write_strategy_alias() -> None:
    parsed = parse_pipe_file(
        _resource(
            "pipe",
            "s3_sink",
            "\n".join(
                [
                    "TYPE sink",
                    "EXPORT_CONNECTION_NAME archive",
                    "EXPORT_BUCKET_URI s3://bucket/path",
                    "EXPORT_FILE_TEMPLATE {date}.ndjson",
                    "EXPORT_FORMAT ndjson",
                    "EXPORT_SCHEDULE @hourly",
                    "EXPORT_WRITE_STRATEGY truncate",
                    "NODE export",
                    "SQL >",
                    "    SELECT id FROM events",
                ]
            ),
        )
    )

    assert parsed.sink is not None
    assert parsed.sink.service == "s3"
    assert parsed.sink.strategy == "replace"


def test_parse_pipe_rejects_export_directives_for_non_sink() -> None:
    with pytest.raises(MigrationParseError, match=r"EXPORT_\* directives are only supported for TYPE sink"):
        parse_pipe_file(
            _resource(
                "pipe",
                "bad_endpoint",
                "\n".join(
                    [
                        "TYPE endpoint",
                        "EXPORT_CONNECTION_NAME broker",
                        "NODE endpoint",
                        "SQL >",
                        "    SELECT 1",
                    ]
                ),
            )
        )


def test_parse_pipe_rejects_mixed_sink_directives() -> None:
    with pytest.raises(MigrationParseError, match="cannot mix Kafka and S3"):
        parse_pipe_file(
            _resource(
                "pipe",
                "bad_sink",
                "\n".join(
                    [
                        "TYPE sink",
                        "EXPORT_CONNECTION_NAME broker",
                        "EXPORT_KAFKA_TOPIC events",
                        "EXPORT_BUCKET_URI s3://bucket/path",
                        "EXPORT_SCHEDULE @hourly",
                        "NODE export",
                        "SQL >",
                        "    SELECT 1",
                    ]
                ),
            )
        )
