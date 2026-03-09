from tinybird_sdk import define_datasource, define_pipe, define_endpoint, define_materialized_view, node, t


def test_define_datasource_name_validation() -> None:
    try:
        define_datasource("bad-name", {"schema": {"id": t.string()}})
        assert False, "expected ValueError"
    except ValueError as error:
        assert "Invalid datasource name" in str(error)


def test_define_pipe_requires_nodes() -> None:
    try:
        define_pipe("my_pipe", {"nodes": [], "output": {"v": t.int32()}, "endpoint": True})
        assert False, "expected ValueError"
    except ValueError as error:
        assert "must have at least one node" in str(error)


def test_define_pipe_single_type_only() -> None:
    ds = define_datasource("sales", {"schema": {"day": t.date()}})
    try:
        define_pipe(
            "invalid_pipe",
            {
                "nodes": [node({"name": "n", "sql": "select 1"})],
                "output": {"day": t.date()},
                "endpoint": True,
                "materialized": {"datasource": ds},
            },
        )
        assert False, "expected ValueError"
    except ValueError as error:
        assert "can only have one of" in str(error)


def test_materialized_view_infers_output() -> None:
    ds = define_datasource(
        "sales_by_hour",
        {
            "schema": {
                "day": t.date(),
                "country": t.string().low_cardinality(),
                "total_sales": t.simple_aggregate_function("sum", t.uint64()),
            }
        },
    )

    mv = define_materialized_view(
        "sales_mv",
        {
            "datasource": ds,
            "nodes": [node({"name": "mv", "sql": "SELECT 1"})],
        },
    )

    assert mv._output is not None
    assert set(mv._output.keys()) == {"day", "country", "total_sales"}


def test_define_endpoint_helper() -> None:
    endpoint = define_endpoint(
        "top_pages",
        {
            "nodes": [node({"name": "endpoint", "sql": "SELECT 1"})],
            "output": {"count": t.int32()},
        },
    )
    assert endpoint._name == "top_pages"
