from __future__ import annotations

from dataclasses import asdict, dataclass

from ..api.resources import DatasourceInfo, PipeInfo
from .type_mapper import clickhouse_type_to_validator, param_type_to_validator
from .utils import to_snake_case, to_pascal_case, escape_string, generate_engine_code


def generate_datasource_code(ds: DatasourceInfo) -> str:
    var_name = to_snake_case(ds.name)
    type_name = to_pascal_case(ds.name)
    lines: list[str] = []

    if ds.description:
        lines.extend(["\"\"\"", ds.description, "\"\"\""])

    lines.append(f"{var_name} = define_datasource({ds.name!r}, {{")
    if ds.description:
        lines.append(f"    'description': {ds.description!r},")

    has_jsonpath = any(col.jsonpath for col in ds.columns)
    if not has_jsonpath:
        lines.append("    'json_paths': False,")

    lines.append("    'schema': {")
    for column in ds.columns:
        lines.append(f"        {column.name!r}: {clickhouse_type_to_validator(column.type)},")
    lines.append("    },")

    lines.append(f"    'engine': {generate_engine_code(asdict(ds.engine))},")

    if ds.forward_query:
        lines.append(f"    'forward_query': {ds.forward_query!r},")

    lines.append("})")
    lines.append("")
    lines.append(f"{type_name}Row = dict")
    return "\n".join(lines)


def generate_pipe_code(pipe: PipeInfo) -> str:
    var_name = to_snake_case(pipe.name)
    lines: list[str] = []

    define_func = "define_pipe"
    if pipe.type == "endpoint":
        define_func = "define_endpoint"
    elif pipe.type == "materialized":
        define_func = "define_materialized_view"
    elif pipe.type == "copy":
        define_func = "define_copy_pipe"

    if pipe.description:
        lines.extend(["\"\"\"", pipe.description, "\"\"\""])

    lines.append(f"{var_name} = {define_func}({pipe.name!r}, {{")
    if pipe.description:
        lines.append(f"    'description': {pipe.description!r},")

    if pipe.type == "materialized" and pipe.materialized:
        lines.append(f"    'datasource': {to_snake_case(pipe.materialized['datasource'])},")
    elif pipe.type == "copy" and pipe.copy:
        lines.append(f"    'datasource': {to_snake_case(pipe.copy['target_datasource'])},")
        if pipe.copy.get("copy_schedule"):
            lines.append(f"    'copy_schedule': {pipe.copy['copy_schedule']!r},")
        if pipe.copy.get("copy_mode"):
            lines.append(f"    'copy_mode': {pipe.copy['copy_mode']!r},")

    if pipe.params and pipe.type not in {"materialized", "copy"}:
        lines.append("    'params': {")
        for param in pipe.params:
            validator = param_type_to_validator(param.type, param.default, param.required)
            if param.description:
                validator = f"{validator}.describe({param.description!r})"
            lines.append(f"        {param.name!r}: {validator},")
        lines.append("    },")

    lines.append("    'nodes': [")
    for node in pipe.nodes:
        lines.append("        node({")
        lines.append(f"            'name': {node.name!r},")
        lines.append(f"            'sql': {node.sql!r},")
        lines.append("        }),")
    lines.append("    ],")

    if pipe.type == "endpoint" and pipe.output_columns:
        lines.append("    'output': {")
        for column in pipe.output_columns:
            lines.append(f"        {column.name!r}: {clickhouse_type_to_validator(column.type)},")
        lines.append("    },")

    lines.append("})")
    return "\n".join(lines)


def generate_datasources_file(datasources: list[DatasourceInfo]) -> str:
    header = "from tinybird_sdk import define_datasource, t, engine\n\n"
    if not datasources:
        return header + "# No datasources found in workspace\n"
    return header + "\n\n".join(generate_datasource_code(ds) for ds in datasources) + "\n"


def generate_pipes_file(pipes: list[PipeInfo], datasources: list[DatasourceInfo]) -> str:
    imports = ["from tinybird_sdk import node, t"]

    if any(pipe.params and pipe.type not in {"materialized", "copy"} for pipe in pipes):
        imports[0] += ", p"

    if any(pipe.type == "endpoint" for pipe in pipes):
        imports[0] += ", define_endpoint"
    if any(pipe.type == "materialized" for pipe in pipes):
        imports[0] += ", define_materialized_view"
    if any(pipe.type == "copy" for pipe in pipes):
        imports[0] += ", define_copy_pipe"
    if any(pipe.type == "pipe" for pipe in pipes):
        imports[0] += ", define_pipe"

    lines = [imports[0]]

    referenced = set()
    datasource_names = {ds.name for ds in datasources}
    for pipe in pipes:
        if pipe.materialized and pipe.materialized.get("datasource") in datasource_names:
            referenced.add(pipe.materialized["datasource"])
        if pipe.copy and pipe.copy.get("target_datasource") in datasource_names:
            referenced.add(pipe.copy["target_datasource"])

    if referenced:
        lines.append(f"from .datasources import {', '.join(to_snake_case(name) for name in sorted(referenced))}")

    lines.append("")
    if not pipes:
        lines.append("# No pipes found in workspace")
        return "\n".join(lines) + "\n"

    lines.append("\n\n".join(generate_pipe_code(pipe) for pipe in pipes))
    return "\n".join(lines) + "\n"


def generate_client_file(datasources: list[DatasourceInfo], pipes: list[PipeInfo]) -> str:
    lines = [
        "from tinybird_sdk import Tinybird",
    ]

    if datasources:
        lines.append(f"from .datasources import {', '.join(to_snake_case(ds.name) for ds in datasources)}")

    endpoint_pipes = [pipe for pipe in pipes if pipe.type == "endpoint"]
    if endpoint_pipes:
        lines.append(f"from .pipes import {', '.join(to_snake_case(pipe.name) for pipe in endpoint_pipes)}")

    lines.extend(
        [
            "",
            "tinybird = Tinybird({",
            f"    'datasources': {{{', '.join(f'{to_snake_case(ds.name)!r}: {to_snake_case(ds.name)}' for ds in datasources)}}},",
            f"    'pipes': {{{', '.join(f'{to_snake_case(pipe.name)!r}: {to_snake_case(pipe.name)}' for pipe in endpoint_pipes)}}},",
            "})",
            "",
        ]
    )

    return "\n".join(lines)


@dataclass(frozen=True, slots=True)
class GeneratedFiles:
    datasources_content: str
    pipes_content: str
    client_content: str
    datasource_count: int
    pipe_count: int


def generate_all_files(datasources: list[DatasourceInfo], pipes: list[PipeInfo]) -> GeneratedFiles:
    return GeneratedFiles(
        datasources_content=generate_datasources_file(datasources),
        pipes_content=generate_pipes_file(pipes, datasources),
        client_content=generate_client_file(datasources, pipes),
        datasource_count=len(datasources),
        pipe_count=len(pipes),
    )


def generate_combined_file(datasources: list[DatasourceInfo], pipes: list[PipeInfo]) -> str:
    datasources_content = generate_datasources_file(datasources)
    pipes_content = generate_pipes_file(pipes, datasources)
    client_content = generate_client_file(datasources, pipes)

    return (
        "# Tinybird Definitions\n\n"
        + datasources_content
        + "\n"
        + pipes_content
        + "\n"
        + client_content
    )
