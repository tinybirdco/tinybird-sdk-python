from __future__ import annotations

import pytest

import tinybird_sdk as sdk
from tinybird_sdk import (
    define_datasource,
    define_dynamodb_connection,
    define_s3_connection,
    is_connection_definition,
    is_dynamodb_connection_definition,
    t,
)
from tinybird_sdk.generator.connection import generate_connection
from tinybird_sdk.generator.datasource import generate_datasource
from tinybird_sdk.migrate.emit_ts import emit_migration_file_content
from tinybird_sdk.migrate.parse_connection import parse_connection_file
from tinybird_sdk.migrate.parse_datasource import parse_datasource_file
from tinybird_sdk.migrate.types import ResourceFile


def _connection() -> object:
    return define_dynamodb_connection(
        "events_dynamodb",
        {
            "region": "us-east-1",
            "arn": "arn:aws:iam::123456789012:role/tinybird-dynamodb-access",
        },
    )


def test_root_exports_include_dynamodb_symbols() -> None:
    assert hasattr(sdk, "define_dynamodb_connection")
    assert hasattr(sdk, "is_dynamodb_connection_definition")


def test_define_dynamodb_connection_metadata() -> None:
    connection = _connection()
    assert connection._connectionType == "dynamodb"
    assert connection._type == "connection"
    assert is_connection_definition(connection)
    assert is_dynamodb_connection_definition(connection)
    assert sdk.get_connection_type(connection) == "dynamodb"


def test_define_dynamodb_connection_requires_region_and_arn() -> None:
    with pytest.raises(ValueError, match="`region` is required"):
        define_dynamodb_connection("c", {"region": "  ", "arn": "arn:aws:iam::1:role/r"})
    with pytest.raises(ValueError, match="`arn` is required"):
        define_dynamodb_connection("c", {"region": "us-east-1", "arn": ""})


def test_define_dynamodb_connection_validates_name() -> None:
    with pytest.raises(ValueError, match="Invalid connection name"):
        define_dynamodb_connection("1bad", {"region": "us-east-1", "arn": "arn:aws:iam::1:role/r"})


def test_generate_dynamodb_connection() -> None:
    generated = generate_connection(_connection())
    assert generated.name == "events_dynamodb"
    assert generated.content == (
        "TYPE dynamodb\n"
        "DYNAMODB_ARN arn:aws:iam::123456789012:role/tinybird-dynamodb-access\n"
        "DYNAMODB_REGION us-east-1"
    )


def test_generate_datasource_with_dynamodb_import() -> None:
    datasource = define_datasource(
        "orders",
        {
            "schema": {
                "id": t.string(),
                "_record": t.string(),
            },
            "engine": sdk.engine.replacing_merge_tree(
                {"sorting_key": ["id"], "ver": "_timestamp", "is_deleted": "_is_deleted"}
            ),
            "dynamodb": {
                "connection": _connection(),
                "table_arn": "arn:aws:dynamodb:us-east-1:123456789012:table/orders",
                "export_bucket": "s3://my-export-bucket",
            },
        },
    )

    content = generate_datasource(datasource).content
    assert 'ENGINE "ReplacingMergeTree"' in content
    assert 'ENGINE_VER "_timestamp"' in content
    assert 'ENGINE_IS_DELETED "_is_deleted"' in content
    assert "IMPORT_CONNECTION_NAME events_dynamodb" in content
    assert "IMPORT_TABLE_ARN arn:aws:dynamodb:us-east-1:123456789012:table/orders" in content
    assert "IMPORT_EXPORT_BUCKET s3://my-export-bucket" in content


def test_datasource_rejects_multiple_ingestion_options() -> None:
    s3 = define_s3_connection("landing_s3", {"region": "us-east-1", "arn": "arn:aws:iam::1:role/r"})
    with pytest.raises(ValueError, match="one ingestion option"):
        define_datasource(
            "mixed",
            {
                "schema": {"id": t.string()},
                "s3": {"connection": s3, "bucket_uri": "s3://bucket"},
                "dynamodb": {
                    "connection": _connection(),
                    "table_arn": "arn:aws:dynamodb:us-east-1:1:table/t",
                    "export_bucket": "s3://export",
                },
            },
        )


def test_parse_dynamodb_connection_file() -> None:
    resource = ResourceFile(
        kind="connection",
        file_path="connections/events_dynamodb.connection",
        absolute_path="/x/connections/events_dynamodb.connection",
        name="events_dynamodb",
        content=(
            "TYPE dynamodb\n"
            'DYNAMODB_ARN "arn:aws:iam::123456789012:role/r"\n'
            "DYNAMODB_REGION us-east-1\n"
            "# a comment\n"
        ),
    )

    model = parse_connection_file(resource)
    assert model.connection_type == "dynamodb"
    assert model.arn == "arn:aws:iam::123456789012:role/r"
    assert model.region == "us-east-1"


def test_parse_dynamodb_connection_requires_arn_and_region() -> None:
    base = ResourceFile(
        kind="connection",
        file_path="c.connection",
        absolute_path="/x/c.connection",
        name="c",
        content="TYPE dynamodb\nDYNAMODB_REGION us-east-1\n",
    )
    with pytest.raises(Exception, match="DYNAMODB_ARN is required"):
        parse_connection_file(base)


def test_parse_datasource_with_dynamodb_import() -> None:
    resource = ResourceFile(
        kind="datasource",
        file_path="datasources/orders.datasource",
        absolute_path="/x/datasources/orders.datasource",
        name="orders",
        content=(
            "SCHEMA >\n"
            "    `id` String `json:$.Item.id`,\n"
            "    `_record` String `json:$.NewImage`\n"
            "\n"
            'ENGINE "ReplacingMergeTree"\n'
            "ENGINE_SORTING_KEY id\n"
            "\n"
            "IMPORT_CONNECTION_NAME 'events_dynamodb'\n"
            "IMPORT_TABLE_ARN 'arn:aws:dynamodb:us-east-1:123456789012:table/orders'\n"
            "IMPORT_EXPORT_BUCKET 's3://my-export-bucket'\n"
        ),
    )

    model = parse_datasource_file(resource)
    assert model.dynamodb is not None
    assert model.s3 is None
    assert model.dynamodb.connection_name == "events_dynamodb"
    assert model.dynamodb.table_arn == "arn:aws:dynamodb:us-east-1:123456789012:table/orders"
    assert model.dynamodb.export_bucket == "s3://my-export-bucket"


def test_emit_ts_round_trip_for_dynamodb() -> None:
    connection_resource = ResourceFile(
        kind="connection",
        file_path="connections/events_dynamodb.connection",
        absolute_path="/x/connections/events_dynamodb.connection",
        name="events_dynamodb",
        content=(
            "TYPE dynamodb\n"
            'DYNAMODB_ARN "arn:aws:iam::123456789012:role/r"\n'
            "DYNAMODB_REGION us-east-1\n"
        ),
    )
    datasource_resource = ResourceFile(
        kind="datasource",
        file_path="datasources/orders.datasource",
        absolute_path="/x/datasources/orders.datasource",
        name="orders",
        content=(
            "SCHEMA >\n"
            "    `id` String `json:$.Item.id`\n"
            "\n"
            'ENGINE "ReplacingMergeTree"\n'
            "ENGINE_SORTING_KEY id\n"
            "\n"
            "IMPORT_CONNECTION_NAME 'events_dynamodb'\n"
            "IMPORT_TABLE_ARN 'arn:aws:dynamodb:us-east-1:1:table/orders'\n"
            "IMPORT_EXPORT_BUCKET 's3://export'\n"
        ),
    )

    connection = parse_connection_file(connection_resource)
    datasource = parse_datasource_file(datasource_resource)

    output = emit_migration_file_content([connection, datasource])
    assert "define_dynamodb_connection" in output
    assert 'events_dynamodb = define_dynamodb_connection("events_dynamodb"' in output
    assert "'dynamodb': {" in output
    assert "'table_arn': \"arn:aws:dynamodb:us-east-1:1:table/orders\"" in output
    assert "'export_bucket': \"s3://export\"" in output
