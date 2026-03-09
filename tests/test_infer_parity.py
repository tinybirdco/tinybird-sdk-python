from tinybird_sdk import define_datasource, define_materialized_view, define_pipe, node, p, t
from tinybird_sdk.infer import (
    infer_materialized_target,
    infer_output_schema,
    infer_params_schema,
    infer_row_schema,
    is_materialized_pipe,
)


def test_infer_helpers() -> None:
    ds = define_datasource(
        "events",
        {"schema": {"id": t.int32(), "name": t.string()}, "engine": {"type": "MergeTree", "sorting_key": ["id"]}},
    )

    pipe = define_pipe(
        "top_events",
        {
            "params": {"limit": p.int32().optional(10)},
            "nodes": [node({"name": "n", "sql": "SELECT id, name FROM events LIMIT {{Int32(limit, 10)}}"})],
            "output": {"id": t.int32(), "name": t.string()},
            "endpoint": True,
        },
    )

    mv = define_materialized_view(
        "events_mv",
        {"datasource": ds, "nodes": [node({"name": "mv", "sql": "SELECT id, name FROM events"})]},
    )

    assert infer_row_schema(ds) == {"id": "Int32", "name": "String"}
    assert infer_params_schema(pipe)["limit"]["required"] is False
    assert infer_output_schema(pipe) == {"id": "Int32", "name": "String"}
    assert infer_materialized_target(mv) == ds
    assert is_materialized_pipe(mv) is True
