from __future__ import annotations

from pathlib import Path

from tinybird_sdk.migrate.emit_ts import emit_migration_file_content
from tinybird_sdk.migrate.runner import run_migrate
from tinybird_sdk.migrate.types import (
    DatasourceColumnModel,
    DatasourceEngineModel,
    DatasourceIndexModel,
    DatasourceKafkaModel,
    DatasourceModel,
    KafkaConnectionModel,
    PipeModel,
    PipeNodeModel,
    PipeParamModel,
    SinkKafkaModel,
)


def test_emit_migration_includes_phase4_connection_and_datasource_fields() -> None:
    connection = KafkaConnectionModel(
        kind="connection",
        name="stream",
        file_path="stream.connection",
        connection_type="kafka",
        bootstrap_servers="localhost:9092",
        schema_registry_url="https://registry.example.com",
    )

    datasource = DatasourceModel(
        kind="datasource",
        name="events",
        file_path="events.datasource",
        columns=[DatasourceColumnModel(name="id", type="Int64")],
        engine=DatasourceEngineModel(
            type="ReplacingMergeTree",
            sorting_key=["id"],
            ver="version",
            is_deleted="is_deleted",
        ),
        indexes=[
            DatasourceIndexModel(
                name="idx_id",
                expr="id",
                type="minmax",
                granularity=64,
            )
        ],
        kafka=DatasourceKafkaModel(
            connection_name="stream",
            topic="events",
            store_raw_value=True,
        ),
    )

    emitted = emit_migration_file_content([connection, datasource])

    assert "'schema_registry_url': \"https://registry.example.com\"" in emitted
    assert "'is_deleted': \"is_deleted\"" in emitted
    assert "'indexes': [" in emitted
    assert "'store_raw_value': True" in emitted


def test_emit_migration_includes_param_descriptions_for_sink_pipes() -> None:
    pipe = PipeModel(
        kind="pipe",
        name="sink_pipe",
        file_path="sink_pipe.pipe",
        type="sink",
        nodes=[PipeNodeModel(name="export", sql="SELECT 1")],
        sink=SinkKafkaModel(
            service="kafka",
            connection_name="stream",
            topic="events",
            schedule="@once",
        ),
        params=[
            PipeParamModel(
                name="sort_col",
                type="column",
                required=True,
                description="Sort column",
            )
        ],
    )

    emitted = emit_migration_file_content([pipe])
    assert "'sort_col': p.column().describe(\"Sort column\")" in emitted


def test_run_migrate_maps_import_directives_to_gcs_when_connection_is_gcs(tmp_path: Path) -> None:
    (tmp_path / "gcs_conn.connection").write_text(
        "\n".join(
            [
                "TYPE gcs",
                "GCS_SERVICE_ACCOUNT_CREDENTIALS_JSON '{\"project\":\"demo\"}'",
            ]
        ),
        encoding="utf-8",
    )
    (tmp_path / "billing.datasource").write_text(
        "\n".join(
            [
                "SCHEMA >",
                "    id Int64",
                "IMPORT_CONNECTION_NAME gcs_conn",
                "IMPORT_BUCKET_URI gs://bucket/path/*.ndjson",
            ]
        ),
        encoding="utf-8",
    )

    result = run_migrate(
        {
            "cwd": str(tmp_path),
            "patterns": ["*.connection", "*.datasource"],
            "dry_run": True,
        }
    )

    assert result.success is True
    assert result.errors == []
    assert result.output_content is not None
    assert "'gcs': {" in result.output_content
    assert "'s3': {" not in result.output_content


def test_run_migrate_rejects_kafka_datasource_with_non_kafka_connection(tmp_path: Path) -> None:
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
    (tmp_path / "events.datasource").write_text(
        "\n".join(
            [
                "SCHEMA >",
                "    id Int64",
                "KAFKA_CONNECTION_NAME archive",
                "KAFKA_TOPIC events",
            ]
        ),
        encoding="utf-8",
    )

    result = run_migrate(
        {
            "cwd": str(tmp_path),
            "patterns": ["*.connection", "*.datasource"],
            "dry_run": True,
        }
    )

    assert result.success is False
    assert any("kafka ingestion requires a kafka connection" in error.message for error in result.errors)


def test_run_migrate_rejects_sink_connection_type_mismatch(tmp_path: Path) -> None:
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
    (tmp_path / "sink.pipe").write_text(
        "\n".join(
            [
                "TYPE sink",
                "EXPORT_SERVICE kafka",
                "EXPORT_CONNECTION_NAME archive",
                "EXPORT_KAFKA_TOPIC events",
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
