from __future__ import annotations

import re


_RESERVED = {
    "break",
    "case",
    "catch",
    "class",
    "const",
    "continue",
    "debugger",
    "default",
    "delete",
    "do",
    "else",
    "enum",
    "export",
    "extends",
    "false",
    "finally",
    "for",
    "function",
    "if",
    "import",
    "in",
    "instanceof",
    "new",
    "null",
    "return",
    "super",
    "switch",
    "this",
    "throw",
    "true",
    "try",
    "typeof",
    "undefined",
    "var",
    "void",
    "while",
    "with",
    "yield",
    "let",
    "static",
    "implements",
    "interface",
    "package",
    "private",
    "protected",
    "public",
    "await",
    "async",
}


def to_snake_case(value: str) -> str:
    """Convert a name to snake_case for Python variable names."""
    # Replace hyphens with underscores
    result = value.replace("-", "_")
    # Insert underscore before uppercase letters and convert to lowercase
    result = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", result).lower()
    # Clean up multiple underscores
    result = re.sub(r"_+", "_", result).strip("_")
    if result in _RESERVED or re.match(r"^\d", result):
        return f"_{result}"
    return result


def to_camel_case(value: str) -> str:
    result = re.sub(r"[-_](.)", lambda m: m.group(1).upper(), value)
    result = re.sub(r"^[A-Z]", lambda m: m.group(0).lower(), result)
    if result in _RESERVED or re.match(r"^\d", result):
        return f"_{result}"
    return result


def to_pascal_case(value: str) -> str:
    camel = to_camel_case(value)
    return camel[0].upper() + camel[1:] if camel else camel


def escape_string(value: str) -> str:
    return (
        value.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )


def parse_sorting_key(sorting_key: str | None = None) -> list[str]:
    if not sorting_key:
        return []
    return [part.strip() for part in sorting_key.split(",") if part.strip()]


def generate_engine_code(engine: dict[str, str | None]) -> str:
    sorting_key = parse_sorting_key(engine.get("sorting_key"))
    options: list[str] = []

    if len(sorting_key) == 1:
        options.append(f"'sorting_key': {sorting_key[0]!r}")
    elif sorting_key:
        options.append(f"'sorting_key': {[key for key in sorting_key]!r}")

    if engine.get("partition_key"):
        options.append(f"'partition_key': {engine['partition_key']!r}")

    if engine.get("primary_key") and engine.get("primary_key") != engine.get("sorting_key"):
        primary_key = parse_sorting_key(engine["primary_key"])
        if len(primary_key) == 1:
            options.append(f"'primary_key': {primary_key[0]!r}")
        elif primary_key:
            options.append(f"'primary_key': {primary_key!r}")

    if engine.get("ttl"):
        options.append(f"'ttl': {engine['ttl']!r}")

    if engine.get("type") == "ReplacingMergeTree" and engine.get("ver"):
        options.append(f"'ver': {engine['ver']!r}")

    if engine.get("type") in {"CollapsingMergeTree", "VersionedCollapsingMergeTree"} and engine.get("sign"):
        options.append(f"'sign': {engine['sign']!r}")

    if engine.get("type") == "VersionedCollapsingMergeTree" and engine.get("version"):
        options.append(f"'version': {engine['version']!r}")

    if engine.get("type") == "SummingMergeTree" and engine.get("summing_columns"):
        columns = parse_sorting_key(engine["summing_columns"])
        if columns:
            options.append(f"'columns': {columns!r}")

    mapping = {
        "MergeTree": "engine.merge_tree",
        "ReplacingMergeTree": "engine.replacing_merge_tree",
        "SummingMergeTree": "engine.summing_merge_tree",
        "AggregatingMergeTree": "engine.aggregating_merge_tree",
        "CollapsingMergeTree": "engine.collapsing_merge_tree",
        "VersionedCollapsingMergeTree": "engine.versioned_collapsing_merge_tree",
    }
    engine_function = mapping.get(engine.get("type") or "", "engine.merge_tree")

    options_dict = "{" + ", ".join(options) + "}" if options else "{'sorting_key': []}"
    return f"{engine_function}({options_dict})"


def indent(value: str, spaces: int) -> str:
    prefix = " " * spaces
    return "\n".join(prefix + line if line.strip() else line for line in value.splitlines())


def format_sql_for_template(sql: str) -> str:
    return sql.replace("```", "\\`\\`\\`").replace("{", "{{").replace("}", "}}")
