from __future__ import annotations

from tinybird_sdk import (
    define_datasource,
    define_gcs_connection,
    define_s3_connection,
    t,
)
from tinybird_sdk.generator.datasource import generate_datasource
from tinybird_sdk.migrate.emit_ts import emit_migration_file_content
from tinybird_sdk.migrate.parse_connection import parse_connection_file
from tinybird_sdk.migrate.parse_datasource import parse_datasource_file
from tinybird_sdk.migrate.types import ResourceFile


def test_s3_import_emits_import_format_when_set() -> None:
    s3 = define_s3_connection("landing_s3", {"region": "us-east-1", "arn": "arn:aws:iam::1:role/r"})
    datasource = define_datasource(
        "events",
        {
            "schema": {"id": t.string()},
            "s3": {
                "connection": s3,
                "bucket_uri": "s3://bucket/events/*.log",
                "import_format": "ndjson",
            },
        },
    )

    content = generate_datasource(datasource).content
    assert "IMPORT_BUCKET_URI s3://bucket/events/*.log" in content
    assert 'IMPORT_FORMAT "ndjson"' in content


def test_s3_import_omits_import_format_when_unset() -> None:
    s3 = define_s3_connection("landing_s3", {"region": "us-east-1", "arn": "arn:aws:iam::1:role/r"})
    datasource = define_datasource(
        "events",
        {
            "schema": {"id": t.string()},
            "s3": {"connection": s3, "bucket_uri": "s3://bucket/events/*.ndjson"},
        },
    )

    assert "IMPORT_FORMAT" not in generate_datasource(datasource).content


def test_gcs_import_emits_import_format_when_set() -> None:
    gcs = define_gcs_connection("landing_gcs", {"service_account_credentials_json": "{}"})
    datasource = define_datasource(
        "events_gcs",
        {
            "schema": {"id": t.string()},
            "gcs": {
                "connection": gcs,
                "bucket_uri": "gs://bucket/events/*.log",
                "import_format": "ndjson",
            },
        },
    )

    assert 'IMPORT_FORMAT "ndjson"' in generate_datasource(datasource).content


def test_parse_datasource_reads_import_format() -> None:
    resource = ResourceFile(
        kind="datasource",
        file_path="datasources/events.datasource",
        absolute_path="/x/datasources/events.datasource",
        name="events",
        content=(
            "SCHEMA >\n"
            "    `id` String `json:$.id`\n"
            "\n"
            "IMPORT_CONNECTION_NAME 'landing_s3'\n"
            "IMPORT_BUCKET_URI 's3://bucket/events/*.log'\n"
            'IMPORT_FORMAT "ndjson"\n'
        ),
    )

    model = parse_datasource_file(resource)
    assert model.s3 is not None
    assert model.s3.import_format == "ndjson"


def test_emit_ts_round_trip_for_s3_import_format() -> None:
    connection_resource = ResourceFile(
        kind="connection",
        file_path="connections/landing_s3.connection",
        absolute_path="/x/connections/landing_s3.connection",
        name="landing_s3",
        content='TYPE s3\nS3_REGION us-east-1\nS3_ARN "arn:aws:iam::1:role/r"\n',
    )
    datasource_resource = ResourceFile(
        kind="datasource",
        file_path="datasources/events.datasource",
        absolute_path="/x/datasources/events.datasource",
        name="events",
        content=(
            "SCHEMA >\n"
            "    `id` String `json:$.id`\n"
            "\n"
            "IMPORT_CONNECTION_NAME 'landing_s3'\n"
            "IMPORT_BUCKET_URI 's3://bucket/events/*.log'\n"
            'IMPORT_FORMAT "ndjson"\n'
        ),
    )

    connection = parse_connection_file(connection_resource)
    datasource = parse_datasource_file(datasource_resource)

    output = emit_migration_file_content([connection, datasource])
    assert "'import_format': \"ndjson\"" in output
