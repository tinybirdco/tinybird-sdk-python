from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS = {
    # Schema core
    "t": ("tinybird_sdk.schema", "t"),
    "p": ("tinybird_sdk.schema", "p"),
    "engine": ("tinybird_sdk.schema", "engine"),
    "define_datasource": ("tinybird_sdk.schema", "define_datasource"),
    "define_pipe": ("tinybird_sdk.schema", "define_pipe"),
    "define_endpoint": ("tinybird_sdk.schema", "define_endpoint"),
    "define_materialized_view": ("tinybird_sdk.schema", "define_materialized_view"),
    "define_copy_pipe": ("tinybird_sdk.schema", "define_copy_pipe"),
    "define_sink_pipe": ("tinybird_sdk.schema", "define_sink_pipe"),
    "node": ("tinybird_sdk.schema", "node"),
    "sql": ("tinybird_sdk.schema", "sql"),
    "define_project": ("tinybird_sdk.schema", "define_project"),
    "Tinybird": ("tinybird_sdk.schema", "Tinybird"),
    # Schema helpers
    "column": ("tinybird_sdk.schema", "column"),
    "get_column_type": ("tinybird_sdk.schema", "get_column_type"),
    "get_column_json_path": ("tinybird_sdk.schema", "get_column_json_path"),
    "get_column_names": ("tinybird_sdk.schema", "get_column_names"),
    "define_kafka_connection": ("tinybird_sdk.schema", "define_kafka_connection"),
    "define_s3_connection": ("tinybird_sdk.schema", "define_s3_connection"),
    "define_gcs_connection": ("tinybird_sdk.schema", "define_gcs_connection"),
    "get_connection_type": ("tinybird_sdk.schema", "get_connection_type"),
    "is_connection_definition": ("tinybird_sdk.schema", "is_connection_definition"),
    "is_kafka_connection_definition": ("tinybird_sdk.schema", "is_kafka_connection_definition"),
    "is_s3_connection_definition": ("tinybird_sdk.schema", "is_s3_connection_definition"),
    "is_gcs_connection_definition": ("tinybird_sdk.schema", "is_gcs_connection_definition"),
    "secret": ("tinybird_sdk.schema", "secret"),
    "define_token": ("tinybird_sdk.schema", "define_token"),
    "is_token_definition": ("tinybird_sdk.schema", "is_token_definition"),
    "is_datasource_definition": ("tinybird_sdk.schema", "is_datasource_definition"),
    "is_pipe_definition": ("tinybird_sdk.schema", "is_pipe_definition"),
    "is_node_definition": ("tinybird_sdk.schema", "is_node_definition"),
    "is_project_definition": ("tinybird_sdk.schema", "is_project_definition"),
    "get_endpoint_config": ("tinybird_sdk.schema", "get_endpoint_config"),
    "get_materialized_config": ("tinybird_sdk.schema", "get_materialized_config"),
    "get_copy_config": ("tinybird_sdk.schema", "get_copy_config"),
    "is_materialized_view": ("tinybird_sdk.schema", "is_materialized_view"),
    "is_copy_pipe": ("tinybird_sdk.schema", "is_copy_pipe"),
    "is_sink_pipe": ("tinybird_sdk.schema", "is_sink_pipe"),
    "get_sink_config": ("tinybird_sdk.schema", "get_sink_config"),
    "get_node_names": ("tinybird_sdk.schema", "get_node_names"),
    "get_node": ("tinybird_sdk.schema", "get_node"),
    "get_datasource_names": ("tinybird_sdk.schema", "get_datasource_names"),
    "get_pipe_names": ("tinybird_sdk.schema", "get_pipe_names"),
    "get_datasource": ("tinybird_sdk.schema", "get_datasource"),
    "get_pipe": ("tinybird_sdk.schema", "get_pipe"),
    "is_type_validator": ("tinybird_sdk.schema", "is_type_validator"),
    "get_tinybird_type": ("tinybird_sdk.schema", "get_tinybird_type"),
    "get_modifiers": ("tinybird_sdk.schema", "get_modifiers"),
    "is_param_validator": ("tinybird_sdk.schema", "is_param_validator"),
    "get_param_tinybird_type": ("tinybird_sdk.schema", "get_param_tinybird_type"),
    "is_param_required": ("tinybird_sdk.schema", "is_param_required"),
    "get_param_default": ("tinybird_sdk.schema", "get_param_default"),
    "get_param_description": ("tinybird_sdk.schema", "get_param_description"),
    "get_engine_clause": ("tinybird_sdk.schema", "get_engine_clause"),
    "get_sorting_key": ("tinybird_sdk.schema", "get_sorting_key"),
    "get_primary_key": ("tinybird_sdk.schema", "get_primary_key"),
    # Client
    "TinybirdClient": ("tinybird_sdk.client", "TinybirdClient"),
    "create_client": ("tinybird_sdk.client", "create_client"),
    "TinybirdError": ("tinybird_sdk.client", "TinybirdError"),
    "is_preview_environment": ("tinybird_sdk.client", "is_preview_environment"),
    "get_preview_branch_name": ("tinybird_sdk.client", "get_preview_branch_name"),
    "resolve_token": ("tinybird_sdk.client", "resolve_token"),
    "clear_token_cache": ("tinybird_sdk.client", "clear_token_cache"),
    # API
    "TinybirdApi": ("tinybird_sdk.api.api", "TinybirdApi"),
    "create_tinybird_api": ("tinybird_sdk.api.api", "create_tinybird_api"),
    "create_tinybird_api_wrapper": ("tinybird_sdk.api.api", "create_tinybird_api_wrapper"),
    "TinybirdApiError": ("tinybird_sdk.api.api", "TinybirdApiError"),
    "create_jwt": ("tinybird_sdk.api.tokens", "create_jwt"),
    "TokenApiError": ("tinybird_sdk.api.tokens", "TokenApiError"),
    "parse_api_url": ("tinybird_sdk.api.dashboard", "parse_api_url"),
    "get_dashboard_url": ("tinybird_sdk.api.dashboard", "get_dashboard_url"),
    "get_branch_dashboard_url": ("tinybird_sdk.api.dashboard", "get_branch_dashboard_url"),
    "get_local_dashboard_url": ("tinybird_sdk.api.dashboard", "get_local_dashboard_url"),
}


__all__ = sorted(_EXPORTS.keys())


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if not target:
        raise AttributeError(f"module 'tinybird_sdk' has no attribute {name!r}")

    module_name, attr_name = target
    module = import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value
