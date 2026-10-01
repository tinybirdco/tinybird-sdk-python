from __future__ import annotations

from pathlib import Path

import pytest

from tinybird_sdk.generator.pipe import generate_pipe
from tinybird_sdk.migrate.emit_ts import emit_migration_file_content
from tinybird_sdk.migrate.parse_pipe import parse_pipe_file
from tinybird_sdk.migrate.parser_utils import MigrationParseError
from tinybird_sdk.migrate.runner import run_migrate
from tinybird_sdk.migrate.types import ResourceFile, SinkGCSModel
from tinybird_sdk.schema.connection import define_gcs_connection
from tinybird_sdk.schema.pipe import GCSSinkConfig, define_sink_pipe, get_sink_config, node


def _resource(name: str, content: str) -> ResourceFile:
    return ResourceFile(
        kind="pipe",
        name=name,
        file_path=f"{name}.pipe",
        absolute_path=f"/tmp/{name}.pipe",
        content=content,
    )


def _gcs_connection():
    return define_gcs_connection(
        "landing_gcs", {"service_account_credentials_json": '{"project":"demo"}'}
    )


def test_define_sink_pipe_accepts_gcs_connection() -> None:
    connection = _gcs_connection()
    pipe = define_sink_pipe(
        "gcs_events_sink",
        {
            "sink": {
                "connection": connection,
                "bucket_uri": "gs://my-bucket/exports/",
                "file_template": "events_{date}",
                "format": "csv",
                "schedule": "@once",
                "strategy": "create_new",
                "compression": "gzip",
            },
            "nodes": [node({"name": "export", "sql": "SELECT 1"})],
        },
    )

    sink = get_sink_config(pipe)
    assert isinstance(sink, GCSSinkConfig)
    assert sink.connection is connection
    assert sink.bucket_uri == "gs://my-bucket/exports/"
    assert sink.strategy == "create_new"
    assert sink.compression == "gzip"


def test_generate_pipe_emits_export_service_gcs_hmac_for_gcs_sink() -> None:
    connection = _gcs_connection()
    pipe = define_sink_pipe(
        "gcs_events_sink",
        {
            "sink": {
                "connection": connection,
                "bucket_uri": "gs://my-bucket/exports/",
                "file_template": "events_{date}",
                "format": "csv",
                "schedule": "@once",
            },
            "nodes": [node({"name": "export", "sql": "SELECT 1"})],
        },
    )

    generated = generate_pipe(pipe).content

    assert "TYPE sink" in generated
    assert "EXPORT_SERVICE gcs_hmac" in generated
    assert f"EXPORT_CONNECTION_NAME {connection._name}" in generated
    assert "EXPORT_BUCKET_URI gs://my-bucket/exports/" in generated
    assert "EXPORT_FILE_TEMPLATE events_{date}" in generated
    assert "EXPORT_FORMAT csv" in generated
    assert "EXPORT_SCHEDULE @once" in generated


def test_parse_pipe_requires_explicit_export_service_for_gcs() -> None:
    parsed = parse_pipe_file(
        _resource(
            "gcs_sink",
            "\n".join(
                [
                    "TYPE sink",
                    "EXPORT_SERVICE gcs_hmac",
                    "EXPORT_CONNECTION_NAME landing_gcs",
                    "EXPORT_BUCKET_URI gs://bucket/path",
                    "EXPORT_FILE_TEMPLATE {date}.ndjson",
                    "EXPORT_FORMAT ndjson",
                    "EXPORT_SCHEDULE @hourly",
                    "EXPORT_COMPRESSION gzip",
                    "NODE export",
                    "SQL >",
                    "    SELECT id FROM events",
                ]
            ),
        )
    )

    assert parsed.sink is not None
    assert isinstance(parsed.sink, SinkGCSModel)
    assert parsed.sink.service == "gcs_hmac"
    assert parsed.sink.bucket_uri == "gs://bucket/path"
    assert parsed.sink.compression == "gzip"


def test_parse_pipe_without_export_service_defaults_blob_sink_to_s3() -> None:
    parsed = parse_pipe_file(
        _resource(
            "ambiguous_sink",
            "\n".join(
                [
                    "TYPE sink",
                    "EXPORT_CONNECTION_NAME archive",
                    "EXPORT_BUCKET_URI s3://bucket/path",
                    "EXPORT_FILE_TEMPLATE {date}.ndjson",
                    "EXPORT_FORMAT ndjson",
                    "EXPORT_SCHEDULE @hourly",
                    "NODE export",
                    "SQL >",
                    "    SELECT id FROM events",
                ]
            ),
        )
    )

    assert parsed.sink is not None
    assert parsed.sink.service == "s3"


def test_parse_pipe_rejects_unsupported_export_service() -> None:
    with pytest.raises(MigrationParseError, match="Unsupported EXPORT_SERVICE"):
        parse_pipe_file(
            _resource(
                "bad_sink",
                "\n".join(
                    [
                        "TYPE sink",
                        "EXPORT_SERVICE azure_blob",
                        "EXPORT_CONNECTION_NAME archive",
                        "EXPORT_BUCKET_URI blob://bucket/path",
                        "NODE export",
                        "SQL >",
                        "    SELECT 1",
                    ]
                ),
            )
        )


def test_run_migrate_accepts_gcs_hmac_sink_against_gcs_connection(tmp_path: Path) -> None:
    (tmp_path / "landing_gcs.connection").write_text(
        "\n".join(
            [
                "TYPE gcs",
                'GCS_SERVICE_ACCOUNT_CREDENTIALS_JSON \'{"project":"demo"}\'',
            ]
        ),
        encoding="utf-8",
    )
    (tmp_path / "gcs_sink.pipe").write_text(
        "\n".join(
            [
                "TYPE sink",
                "EXPORT_SERVICE gcs_hmac",
                "EXPORT_CONNECTION_NAME landing_gcs",
                "EXPORT_BUCKET_URI gs://bucket/path",
                "EXPORT_FILE_TEMPLATE {date}.ndjson",
                "EXPORT_FORMAT ndjson",
                "EXPORT_SCHEDULE @hourly",
                "NODE export",
                "SQL >",
                "    SELECT id FROM events",
            ]
        ),
        encoding="utf-8",
    )

    result = run_migrate(
        {
            "cwd": str(tmp_path),
            "patterns": ["*.connection", "*.pipe"],
            "dry_run": True,
        }
    )

    assert result.success is True, result.errors


def test_run_migrate_rejects_gcs_hmac_sink_against_s3_connection(tmp_path: Path) -> None:
    (tmp_path / "archive.connection").write_text(
        "\n".join(
            [
                "TYPE s3",
                "S3_REGION us-east-1",
                "S3_ARN arn:aws:iam::123456789012:role/demo",
            ]
        ),
        encoding="utf-8",
    )
    (tmp_path / "gcs_sink.pipe").write_text(
        "\n".join(
            [
                "TYPE sink",
                "EXPORT_SERVICE gcs_hmac",
                "EXPORT_CONNECTION_NAME archive",
                "EXPORT_BUCKET_URI gs://bucket/path",
                "EXPORT_FILE_TEMPLATE {date}.ndjson",
                "EXPORT_FORMAT ndjson",
                "EXPORT_SCHEDULE @hourly",
                "NODE export",
                "SQL >",
                "    SELECT id FROM events",
            ]
        ),
        encoding="utf-8",
    )

    result = run_migrate(
        {
            "cwd": str(tmp_path),
            "patterns": ["*.connection", "*.pipe"],
            "dry_run": True,
        }
    )

    assert result.success is False
    assert any("is incompatible with connection" in error.message for error in result.errors)


def test_emit_migration_emits_gcs_sink_fields() -> None:
    from tinybird_sdk.migrate.types import PipeModel, PipeNodeModel

    pipe = PipeModel(
        kind="pipe",
        name="gcs_sink",
        file_path="gcs_sink.pipe",
        type="sink",
        nodes=[PipeNodeModel(name="export", sql="SELECT 1")],
        sink=SinkGCSModel(
            service="gcs_hmac",
            connection_name="landing_gcs",
            bucket_uri="gs://bucket/path",
            file_template="{date}.ndjson",
            format="ndjson",
            schedule="@hourly",
            compression="gzip",
        ),
    )

    emitted = emit_migration_file_content([pipe])

    assert "'bucket_uri': \"gs://bucket/path\"" in emitted
    assert "'compression': \"gzip\"" in emitted
