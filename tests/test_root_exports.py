import tinybird_sdk as sdk


def test_root_exports_include_parity_symbols() -> None:
    required = [
        "Tinybird",
        "column",
        "define_gcs_connection",
        "define_kafka_connection",
        "define_s3_connection",
        "define_sink_pipe",
        "define_token",
        "get_branch_dashboard_url",
        "get_column_json_path",
        "get_column_names",
        "get_column_type",
        "get_connection_type",
        "get_copy_config",
        "get_dashboard_url",
        "get_datasource",
        "get_datasource_names",
        "get_endpoint_config",
        "get_engine_clause",
        "get_local_dashboard_url",
        "get_materialized_config",
        "get_modifiers",
        "get_node",
        "get_node_names",
        "get_param_default",
        "get_param_description",
        "get_param_tinybird_type",
        "get_pipe",
        "get_pipe_names",
        "get_primary_key",
        "get_sorting_key",
        "get_tinybird_type",
        "is_connection_definition",
        "is_copy_pipe",
        "is_datasource_definition",
        "is_gcs_connection_definition",
        "is_kafka_connection_definition",
        "is_s3_connection_definition",
        "is_sink_pipe",
        "get_sink_config",
        "is_materialized_view",
        "is_node_definition",
        "is_param_required",
        "is_param_validator",
        "is_pipe_definition",
        "is_project_definition",
        "is_token_definition",
        "is_type_validator",
        "secret",
        "parse_api_url",
        "sql",
    ]

    missing = [name for name in required if not hasattr(sdk, name)]
    assert missing == []


def test_root_exports_exclude_legacy_aliases() -> None:
    assert not hasattr(sdk, "create_tinybird_client")
    assert not hasattr(sdk, "create_kafka_connection")
