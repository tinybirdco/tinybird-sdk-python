from __future__ import annotations

from pathlib import Path

from tinybird_sdk.api.resources import DatasourceColumn, DatasourceEngine, DatasourceInfo, PipeInfo, PipeNode, PipeParam
from tinybird_sdk.codegen.index import generate_client_file, generate_datasources_file, generate_pipes_file
from tinybird_sdk.codegen.type_mapper import clickhouse_type_to_validator, param_type_to_validator
from tinybird_sdk.codegen.utils import format_sql_for_template, generate_engine_code, to_camel_case


def _sample_resources() -> tuple[list[DatasourceInfo], list[PipeInfo]]:
    datasources = [
        DatasourceInfo(
            name="page_views",
            description="Page views",
            columns=[
                DatasourceColumn(name="id", type="Int32"),
                DatasourceColumn(name="url", type="String"),
            ],
            engine=DatasourceEngine(type="MergeTree", sorting_key="id"),
        )
    ]
    pipes = [
        PipeInfo(
            name="top_pages",
            description="Top pages",
            nodes=[PipeNode(name="n", sql="SELECT id, url FROM page_views")],
            params=[PipeParam(name="limit", type="Int32", required=False, default=10)],
            type="endpoint",
            output_columns=[
                DatasourceColumn(name="id", type="Int32"),
                DatasourceColumn(name="url", type="String"),
            ],
            endpoint={"enabled": True},
        )
    ]
    return datasources, pipes


def test_type_mapper_and_utils_parity_behaviors() -> None:
    assert clickhouse_type_to_validator("Nullable(LowCardinality(String))") == "t.string().low_cardinality().nullable()"
    assert param_type_to_validator("Int32", 10, required=False) == "p.int32().optional(10)"
    assert to_camel_case("class") == "_class"
    assert "engine.replacing_merge_tree" in generate_engine_code({"type": "ReplacingMergeTree", "sorting_key": "id", "ver": "v"})
    assert format_sql_for_template("{x}") == "{{x}}"


def test_codegen_golden_fixtures() -> None:
    datasources, pipes = _sample_resources()
    expected_base = Path("tests/fixtures/codegen/expected")

    expected_datasources = expected_base.joinpath("datasources.py").read_text(encoding="utf-8").rstrip() + "\n"
    expected_pipes = expected_base.joinpath("pipes.py").read_text(encoding="utf-8").rstrip() + "\n"
    expected_client = expected_base.joinpath("client.py").read_text(encoding="utf-8").rstrip() + "\n"

    assert generate_datasources_file(datasources) == expected_datasources
    assert generate_pipes_file(pipes, datasources) == expected_pipes
    assert generate_client_file(datasources, pipes) == expected_client
