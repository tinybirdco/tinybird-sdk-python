from __future__ import annotations

import pytest

from tinybird_sdk import define_datasource, define_kafka_connection, get_modifiers, t
from tinybird_sdk.generator.connection import generate_connection
from tinybird_sdk.generator.datasource import generate_datasource


def test_generate_connection_includes_kafka_schema_registry_url() -> None:
    kafka = define_kafka_connection(
        "broker",
        {
            "bootstrap_servers": "localhost:9092",
            "schema_registry_url": "https://registry.example.com",
        },
    )

    generated = generate_connection(kafka)
    assert "KAFKA_SCHEMA_REGISTRY_URL https://registry.example.com" in generated.content


def test_generate_datasource_includes_indexes_and_store_raw_value() -> None:
    kafka = define_kafka_connection("broker", {"bootstrap_servers": "localhost:9092"})
    datasource = define_datasource(
        "events",
        {
            "schema": {
                "id": t.int32(),
            },
            "indexes": [
                {
                    "name": "id_set",
                    "expr": "id",
                    "type": "set(100)",
                    "granularity": 1,
                }
            ],
            "kafka": {
                "connection": kafka,
                "topic": "events",
                "store_raw_value": True,
            },
        },
    )

    generated = generate_datasource(datasource)
    assert "INDEXES >" in generated.content
    assert "id_set id TYPE set(100) GRANULARITY 1" in generated.content
    assert "KAFKA_STORE_RAW_VALUE True" in generated.content


def test_define_datasource_validates_index_name_and_granularity() -> None:
    with pytest.raises(ValueError, match="Invalid datasource index name"):
        define_datasource(
            "events",
            {
                "schema": {"id": t.int32()},
                "indexes": [{"name": "bad name", "expr": "id", "type": "set(10)", "granularity": 1}],
            },
        )

    with pytest.raises(ValueError, match="granularity must be a positive integer"):
        define_datasource(
            "events",
            {
                "schema": {"id": t.int32()},
                "indexes": [{"name": "idx", "expr": "id", "type": "set(10)", "granularity": 0}],
            },
        )


def test_generate_datasource_ignores_non_string_json_path(monkeypatch: pytest.MonkeyPatch) -> None:
    datasource = define_datasource("events", {"schema": {"id": t.int32()}})

    monkeypatch.setattr(
        "tinybird_sdk.generator.datasource.get_column_json_path",
        lambda *_args, **_kwargs: (lambda _value: "$.bad"),
    )

    generated = generate_datasource(datasource)
    assert "`json:$.id`" in generated.content


def test_type_validator_default_expr_stores_expression() -> None:
    validator = t.uuid().default_expr("  generateUUIDv4()  ")
    modifiers = get_modifiers(validator)

    assert modifiers.has_default is True
    assert modifiers.default_expression == "generateUUIDv4()"
    assert modifiers.default_value is None


def test_generate_datasource_emits_unquoted_default_expression() -> None:
    datasource = define_datasource(
        "events",
        {
            "schema": {
                "id": t.uuid().default_expr("generateUUIDv4()"),
            }
        },
    )

    generated = generate_datasource(datasource)
    assert "id UUID `json:$.id` DEFAULT generateUUIDv4()" in generated.content


def test_type_validator_default_expr_rejects_empty_expression() -> None:
    with pytest.raises(ValueError, match="Default expression cannot be empty."):
        t.uuid().default_expr("   ")
