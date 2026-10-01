import pytest

from tinybird_sdk.infer import generate_schema_code, parse_analyze_response


def test_parse_analyze_response_from_columns_list() -> None:
    response = {
        "analysis": {
            "columns": [
                {"name": "id", "recommended_type": "Int64", "present_pct": 100.0},
                {"name": "name", "recommended_type": "Nullable(String)"},
            ],
            "format": "csv",
            "dialect": {"delimiter": ",", "new_line": "\\n"},
        }
    }

    analyzed = parse_analyze_response(response)

    assert [c.name for c in analyzed.columns] == ["id", "name"]
    assert analyzed.columns[0].clickhouse_type == "Int64"
    assert analyzed.columns[0].present_pct == 100.0
    assert analyzed.columns[1].clickhouse_type == "Nullable(String)"
    assert analyzed.format == "csv"
    assert analyzed.dialect == {"delimiter": ",", "new_line": "\\n"}


def test_parse_analyze_response_falls_back_to_schema_ddl_string() -> None:
    response = {"analysis": {"schema": "id Int64, name Nullable(String), ts DateTime"}}

    analyzed = parse_analyze_response(response)

    assert [c.name for c in analyzed.columns] == ["id", "name", "ts"]
    assert analyzed.columns[2].clickhouse_type == "DateTime"


def test_parse_analyze_response_raises_without_recognizable_columns() -> None:
    with pytest.raises(ValueError, match="Could not find column information"):
        parse_analyze_response({"analysis": {}})


def test_generate_schema_code_emits_t_validators() -> None:
    analyzed = parse_analyze_response({"analysis": {"schema": "id Int64, name Nullable(String)"}})

    code = generate_schema_code(analyzed, "events")

    assert "events = define_datasource('events', {" in code
    assert "'id': t.int64()," in code
    assert "'name': t.string().nullable()," in code
    assert "EventsRow = dict" in code
