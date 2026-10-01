from __future__ import annotations

import pytest

import tinybird_sdk as sdk
from tinybird_sdk import (
    define_gcs_connection,
    is_connection_definition,
    is_gcs_connection_definition,
)
from tinybird_sdk.generator.connection import generate_connection
from tinybird_sdk.migrate.emit_ts import emit_migration_file_content
from tinybird_sdk.migrate.parse_connection import parse_connection_file
from tinybird_sdk.migrate.types import ResourceFile


def _hmac_connection() -> object:
    return define_gcs_connection(
        "landing_gcs_hmac",
        {
            "hmac_access_id": "GOOG1EHMACACCESSID",
            "hmac_secret": "s3cr3t",
        },
    )


def test_define_gcs_connection_accepts_hmac_key_pair() -> None:
    connection = _hmac_connection()
    assert connection._connectionType == "gcs"
    assert connection._type == "connection"
    assert is_connection_definition(connection)
    assert is_gcs_connection_definition(connection)
    assert sdk.get_connection_type(connection) == "gcs"
    assert connection.options.hmac_access_id == "GOOG1EHMACACCESSID"
    assert connection.options.hmac_secret == "s3cr3t"
    assert connection.options.service_account_credentials_json is None


def test_define_gcs_connection_still_accepts_service_account_json() -> None:
    connection = define_gcs_connection("landing_gcs", {"service_account_credentials_json": "{}"})
    assert connection.options.service_account_credentials_json == "{}"
    assert connection.options.hmac_access_id is None
    assert connection.options.hmac_secret is None


def test_define_gcs_connection_requires_an_auth_method() -> None:
    with pytest.raises(ValueError, match="requires either"):
        define_gcs_connection("c", {})


def test_define_gcs_connection_rejects_mixing_auth_methods() -> None:
    with pytest.raises(ValueError, match="mutually exclusive"):
        define_gcs_connection(
            "c",
            {
                "service_account_credentials_json": "{}",
                "hmac_access_id": "id",
                "hmac_secret": "secret",
            },
        )


def test_define_gcs_connection_requires_hmac_pair_together() -> None:
    with pytest.raises(ValueError, match="must be provided together"):
        define_gcs_connection("c", {"hmac_access_id": "id"})
    with pytest.raises(ValueError, match="must be provided together"):
        define_gcs_connection("c", {"hmac_secret": "secret"})


def test_generate_gcs_connection_emits_hmac_directives() -> None:
    generated = generate_connection(_hmac_connection())
    assert generated.name == "landing_gcs_hmac"
    assert generated.content == (
        "TYPE gcs\nGCS_HMAC_ACCESS_ID GOOG1EHMACACCESSID\nGCS_HMAC_SECRET s3cr3t"
    )


def test_generate_gcs_connection_still_emits_service_account_json() -> None:
    connection = define_gcs_connection("landing_gcs", {"service_account_credentials_json": "{}"})
    generated = generate_connection(connection)
    assert generated.content == "TYPE gcs\nGCS_SERVICE_ACCOUNT_CREDENTIALS_JSON {}"


def test_parse_gcs_connection_file_with_hmac_directives() -> None:
    resource = ResourceFile(
        kind="connection",
        file_path="connections/landing_gcs_hmac.connection",
        absolute_path="/x/connections/landing_gcs_hmac.connection",
        name="landing_gcs_hmac",
        content=(
            "TYPE gcs\nGCS_HMAC_ACCESS_ID GOOG1EHMACACCESSID\nGCS_HMAC_SECRET s3cr3t\n# a comment\n"
        ),
    )

    model = parse_connection_file(resource)
    assert model.connection_type == "gcs"
    assert model.hmac_access_id == "GOOG1EHMACACCESSID"
    assert model.hmac_secret == "s3cr3t"
    assert model.service_account_credentials_json is None


def test_parse_gcs_connection_requires_an_auth_method() -> None:
    base = ResourceFile(
        kind="connection",
        file_path="c.connection",
        absolute_path="/x/c.connection",
        name="c",
        content="TYPE gcs\n",
    )
    with pytest.raises(Exception, match="require GCS_SERVICE_ACCOUNT_CREDENTIALS_JSON"):
        parse_connection_file(base)


def test_parse_gcs_connection_rejects_mixing_auth_methods() -> None:
    base = ResourceFile(
        kind="connection",
        file_path="c.connection",
        absolute_path="/x/c.connection",
        name="c",
        content=(
            "TYPE gcs\n"
            "GCS_SERVICE_ACCOUNT_CREDENTIALS_JSON {}\n"
            "GCS_HMAC_ACCESS_ID id\n"
            "GCS_HMAC_SECRET secret\n"
        ),
    )
    with pytest.raises(Exception, match="mutually exclusive"):
        parse_connection_file(base)


def test_parse_gcs_connection_requires_hmac_pair_together() -> None:
    base = ResourceFile(
        kind="connection",
        file_path="c.connection",
        absolute_path="/x/c.connection",
        name="c",
        content="TYPE gcs\nGCS_HMAC_ACCESS_ID id\n",
    )
    with pytest.raises(Exception, match="must be provided together"):
        parse_connection_file(base)


def test_emit_ts_round_trip_for_gcs_hmac() -> None:
    connection_resource = ResourceFile(
        kind="connection",
        file_path="connections/landing_gcs_hmac.connection",
        absolute_path="/x/connections/landing_gcs_hmac.connection",
        name="landing_gcs_hmac",
        content=("TYPE gcs\nGCS_HMAC_ACCESS_ID GOOG1EHMACACCESSID\nGCS_HMAC_SECRET s3cr3t\n"),
    )

    connection = parse_connection_file(connection_resource)
    output = emit_migration_file_content([connection])

    assert 'landing_gcs_hmac = define_gcs_connection("landing_gcs_hmac"' in output
    assert "'hmac_access_id': \"GOOG1EHMACACCESSID\"" in output
    assert "'hmac_secret': \"s3cr3t\"" in output
    assert "service_account_credentials_json" not in output
