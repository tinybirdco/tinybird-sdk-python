from __future__ import annotations

from tinybird_sdk import (
    define_datasource,
    define_gcs_connection,
    define_s3_connection,
    t,
)
from tinybird_sdk.generator.datasource import generate_datasource


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
