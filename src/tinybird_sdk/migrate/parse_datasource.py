from __future__ import annotations

import re

from .parser_utils import (
    MigrationParseError,
    is_blank,
    parse_directive_line,
    parse_quoted_value,
    split_comma_separated,
    split_lines,
    split_top_level_comma,
    strip_indent,
)
from .types import (
    DatasourceColumnModel,
    DatasourceEngineModel,
    DatasourceGCSModel,
    DatasourceIndexModel,
    DatasourceKafkaModel,
    DatasourceModel,
    DatasourceS3Model,
    DatasourceTokenModel,
    ResourceFile,
)


def _read_indented_block(lines: list[str], start_index: int) -> tuple[list[str], int]:
    collected: list[str] = []
    i = start_index

    while i < len(lines):
        line = lines[i] if i < len(lines) else ""
        if line.startswith("    "):
            collected.append(strip_indent(line))
            i += 1
            continue

        if is_blank(line):
            j = i + 1
            while j < len(lines) and is_blank(lines[j]):
                j += 1
            if j < len(lines) and lines[j].startswith("    "):
                collected.append("")
                i += 1
                continue

        break

    return collected, i


def _find_token_outside_contexts(input_value: str, token: str) -> int:
    depth = 0
    in_single = False
    in_double = False
    in_backtick = False

    for i in range(0, len(input_value) - len(token) + 1):
        char = input_value[i]
        prev = input_value[i - 1] if i > 0 else ""

        if char == "'" and not in_double and not in_backtick and prev != "\\":
            in_single = not in_single
        elif char == '"' and not in_single and not in_backtick and prev != "\\":
            in_double = not in_double
        elif char == "`" and not in_single and not in_double:
            in_backtick = not in_backtick
        elif not in_single and not in_double and not in_backtick:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1

        if not in_single and not in_double and not in_backtick and depth == 0:
            if input_value[i : i + len(token)] == token:
                return i

    return -1


def _normalize_column_name(value: str) -> str:
    trimmed = value.strip()
    if (trimmed.startswith("`") and trimmed.endswith("`")) or (trimmed.startswith('"') and trimmed.endswith('"')):
        return trimmed[1:-1]
    return trimmed


def _parse_column_line(file_path: str, resource_name: str, raw_line: str) -> DatasourceColumnModel:
    line = raw_line.strip().removesuffix(",")
    if not line:
        raise MigrationParseError(file_path, "datasource", resource_name, "Empty schema line.")

    first_space = re.search(r"\s", line)
    if not first_space:
        raise MigrationParseError(
            file_path,
            "datasource",
            resource_name,
            f'Invalid schema column definition: "{raw_line}"',
        )

    split_at = first_space.start()
    column_name = _normalize_column_name(line[:split_at].strip())
    rest = line[split_at + 1 :].strip()

    if not column_name:
        raise MigrationParseError(
            file_path,
            "datasource",
            resource_name,
            f'Invalid schema column name: "{raw_line}"',
        )

    codec_match = re.search(r"\s+CODEC\((.+)\)\s*$", rest)
    codec = codec_match.group(1).strip() if codec_match else None
    if codec_match:
        rest = rest[: codec_match.start()].strip()

    default_expression: str | None = None
    default_index = _find_token_outside_contexts(rest, " DEFAULT ")
    if default_index >= 0:
        default_expression = rest[default_index + len(" DEFAULT ") :].strip()
        rest = rest[:default_index].strip()

    json_path: str | None = None
    json_match = re.search(r"`json:([^`]+)`", rest)
    if json_match:
        json_path = json_match.group(1).strip()
        rest = re.sub(r"`json:[^`]+`", "", rest).strip()

    if not rest:
        raise MigrationParseError(
            file_path,
            "datasource",
            resource_name,
            f'Missing type in schema column: "{raw_line}"',
        )

    return DatasourceColumnModel(
        name=column_name,
        type=rest,
        json_path=json_path,
        default_expression=default_expression,
        codec=codec,
    )


def _parse_engine_settings(value: str) -> dict[str, str | int | float | bool]:
    raw = parse_quoted_value(value)
    parts = split_top_level_comma(raw)
    settings: dict[str, str | int | float | bool] = {}

    for part in parts:
        equal_index = part.find("=")
        if equal_index == -1:
            raise ValueError(f'Invalid ENGINE_SETTINGS part: "{part}"')
        key = part[:equal_index].strip()
        raw_value = part[equal_index + 1 :].strip()
        if not key:
            raise ValueError(f'Invalid ENGINE_SETTINGS key in "{part}"')

        if raw_value.startswith("'") and raw_value.endswith("'"):
            settings[key] = raw_value[1:-1].replace("\\'", "'")
        elif re.fullmatch(r"-?\d+(\.\d+)?", raw_value):
            settings[key] = float(raw_value) if "." in raw_value else int(raw_value)
        elif raw_value == "true":
            settings[key] = True
        elif raw_value == "false":
            settings[key] = False
        else:
            raise ValueError(f'Unsupported ENGINE_SETTINGS value: "{raw_value}"')

    return settings


def _parse_token(file_path: str, resource_name: str, value: str) -> DatasourceTokenModel:
    trimmed = value.strip()
    quoted_match = re.fullmatch(r'^"([^"]+)"\s+(READ|APPEND)$', trimmed)
    if quoted_match:
        return DatasourceTokenModel(name=quoted_match.group(1), scope=quoted_match.group(2))  # type: ignore[arg-type]

    parts = [part for part in re.split(r"\s+", trimmed) if part]
    if len(parts) < 2:
        raise MigrationParseError(
            file_path,
            "datasource",
            resource_name,
            f'Invalid TOKEN line: "{value}"',
        )

    if len(parts) > 2:
        raise MigrationParseError(
            file_path,
            "datasource",
            resource_name,
            f'Unsupported TOKEN syntax in strict mode: "{value}"',
        )

    raw_name, scope = parts
    name = raw_name[1:-1] if raw_name.startswith('"') and raw_name.endswith('"') and len(raw_name) >= 2 else raw_name
    if scope not in {"READ", "APPEND"}:
        raise MigrationParseError(
            file_path,
            "datasource",
            resource_name,
            f'Unsupported datasource token scope: "{scope}"',
        )

    return DatasourceTokenModel(name=name, scope=scope)  # type: ignore[arg-type]


def _parse_index_line(file_path: str, resource_name: str, raw_line: str) -> DatasourceIndexModel:
    line = raw_line.strip().removesuffix(",")
    if not line:
        raise MigrationParseError(file_path, "datasource", resource_name, "Empty INDEXES line.")

    match = re.fullmatch(r"(\S+)\s+(.+?)\s+TYPE\s+(.+?)\s+GRANULARITY\s+(\d+)", line, flags=re.IGNORECASE)
    if not match:
        raise MigrationParseError(
            file_path,
            "datasource",
            resource_name,
            f'Invalid INDEXES definition: "{raw_line}"',
        )

    granularity = int(match.group(4))
    if granularity <= 0:
        raise MigrationParseError(
            file_path,
            "datasource",
            resource_name,
            f'Invalid INDEXES GRANULARITY value: "{match.group(4)}"',
        )

    return DatasourceIndexModel(
        name=match.group(1),
        expr=match.group(2).strip(),
        type=match.group(3).strip(),
        granularity=granularity,
    )


def parse_datasource_file(resource: ResourceFile) -> DatasourceModel:
    lines = split_lines(resource.content)
    columns: list[DatasourceColumnModel] = []
    indexes: list[DatasourceIndexModel] = []
    tokens: list[DatasourceTokenModel] = []
    shared_with: list[str] = []
    description: str | None = None
    forward_query: str | None = None

    engine_type: str | None = None
    sorting_key: list[str] = []
    partition_key: str | None = None
    primary_key: list[str] | None = None
    ttl: str | None = None
    ver: str | None = None
    is_deleted: str | None = None
    sign: str | None = None
    version: str | None = None
    summing_columns: list[str] | None = None
    settings: dict[str, str | int | float | bool] | None = None

    kafka_connection_name: str | None = None
    kafka_topic: str | None = None
    kafka_group_id: str | None = None
    kafka_auto_offset_reset: str | None = None
    kafka_store_raw_value: bool | None = None

    import_connection_name: str | None = None
    import_bucket_uri: str | None = None
    import_schedule: str | None = None
    import_from_timestamp: str | None = None

    i = 0
    while i < len(lines):
        raw_line = lines[i]
        line = raw_line.strip()

        if not line or line.startswith("#"):
            i += 1
            continue

        if line == "DESCRIPTION >":
            block, next_index = _read_indented_block(lines, i + 1)
            if not block:
                raise MigrationParseError(resource.file_path, "datasource", resource.name, "DESCRIPTION block is empty.")
            description = "\n".join(block)
            i = next_index
            continue

        if line == "SCHEMA >":
            block, next_index = _read_indented_block(lines, i + 1)
            if not block:
                raise MigrationParseError(resource.file_path, "datasource", resource.name, "SCHEMA block is empty.")
            for schema_line in block:
                if is_blank(schema_line) or schema_line.strip().startswith("#"):
                    continue
                columns.append(_parse_column_line(resource.file_path, resource.name, schema_line))
            i = next_index
            continue

        if line == "INDEXES >":
            block, next_index = _read_indented_block(lines, i + 1)
            if not block:
                raise MigrationParseError(resource.file_path, "datasource", resource.name, "INDEXES block is empty.")
            for index_line in block:
                if is_blank(index_line) or index_line.strip().startswith("#"):
                    continue
                indexes.append(_parse_index_line(resource.file_path, resource.name, index_line))
            i = next_index
            continue

        if line == "FORWARD_QUERY >":
            block, next_index = _read_indented_block(lines, i + 1)
            if not block:
                raise MigrationParseError(resource.file_path, "datasource", resource.name, "FORWARD_QUERY block is empty.")
            forward_query = "\n".join(block)
            i = next_index
            continue

        if line == "SHARED_WITH >":
            block, next_index = _read_indented_block(lines, i + 1)
            for shared_line in block:
                normalized = shared_line.strip().removesuffix(",")
                if normalized:
                    shared_with.append(normalized)
            i = next_index
            continue

        directive = parse_directive_line(line)
        key = directive["key"]
        value = directive["value"]

        if key == "ENGINE":
            engine_type = parse_quoted_value(value)
        elif key == "ENGINE_SORTING_KEY":
            sorting_key = split_comma_separated(parse_quoted_value(value))
        elif key == "ENGINE_PARTITION_KEY":
            partition_key = parse_quoted_value(value)
        elif key == "ENGINE_PRIMARY_KEY":
            primary_key = split_comma_separated(parse_quoted_value(value))
        elif key == "ENGINE_TTL":
            ttl = parse_quoted_value(value)
        elif key == "ENGINE_VER":
            ver = parse_quoted_value(value)
        elif key == "ENGINE_IS_DELETED":
            is_deleted = parse_quoted_value(value)
        elif key == "ENGINE_SIGN":
            sign = parse_quoted_value(value)
        elif key == "ENGINE_VERSION":
            version = parse_quoted_value(value)
        elif key == "ENGINE_SUMMING_COLUMNS":
            summing_columns = split_comma_separated(parse_quoted_value(value))
        elif key == "ENGINE_SETTINGS":
            try:
                settings = _parse_engine_settings(value)
            except Exception as error:
                raise MigrationParseError(resource.file_path, "datasource", resource.name, str(error)) from error
        elif key == "KAFKA_CONNECTION_NAME":
            kafka_connection_name = value.strip()
        elif key == "KAFKA_TOPIC":
            kafka_topic = value.strip()
        elif key == "KAFKA_GROUP_ID":
            kafka_group_id = value.strip()
        elif key == "KAFKA_AUTO_OFFSET_RESET":
            if value not in {"earliest", "latest"}:
                raise MigrationParseError(
                    resource.file_path,
                    "datasource",
                    resource.name,
                    f'Invalid KAFKA_AUTO_OFFSET_RESET value: "{value}"',
                )
            kafka_auto_offset_reset = value
        elif key == "KAFKA_STORE_RAW_VALUE":
            normalized = value.lower()
            if normalized in {"true", "1"}:
                kafka_store_raw_value = True
            elif normalized in {"false", "0"}:
                kafka_store_raw_value = False
            else:
                raise MigrationParseError(
                    resource.file_path,
                    "datasource",
                    resource.name,
                    f'Invalid KAFKA_STORE_RAW_VALUE value: "{value}"',
                )
        elif key == "IMPORT_CONNECTION_NAME":
            import_connection_name = parse_quoted_value(value)
        elif key == "IMPORT_BUCKET_URI":
            import_bucket_uri = parse_quoted_value(value)
        elif key == "IMPORT_SCHEDULE":
            import_schedule = parse_quoted_value(value)
        elif key == "IMPORT_FROM_TIMESTAMP":
            import_from_timestamp = parse_quoted_value(value)
        elif key == "TOKEN":
            tokens.append(_parse_token(resource.file_path, resource.name, value))
        else:
            raise MigrationParseError(
                resource.file_path,
                "datasource",
                resource.name,
                f'Unsupported datasource directive in strict mode: "{line}"',
            )

        i += 1

    if not columns:
        raise MigrationParseError(resource.file_path, "datasource", resource.name, "SCHEMA block is required.")

    has_engine_directives = (
        len(sorting_key) > 0
        or partition_key is not None
        or (primary_key is not None and len(primary_key) > 0)
        or ttl is not None
        or ver is not None
        or is_deleted is not None
        or sign is not None
        or version is not None
        or (summing_columns is not None and len(summing_columns) > 0)
        or settings is not None
    )
    if not engine_type and has_engine_directives:
        engine_type = "MergeTree"

    if engine_type and not sorting_key:
        raise MigrationParseError(resource.file_path, "datasource", resource.name, "ENGINE_SORTING_KEY directive is required.")

    kafka: DatasourceKafkaModel | None = None
    if (
        kafka_connection_name
        or kafka_topic
        or kafka_group_id
        or kafka_auto_offset_reset
        or kafka_store_raw_value is not None
    ):
        if not kafka_connection_name or not kafka_topic:
            raise MigrationParseError(
                resource.file_path,
                "datasource",
                resource.name,
                "KAFKA_CONNECTION_NAME and KAFKA_TOPIC are required when Kafka directives are used.",
            )
        kafka = DatasourceKafkaModel(
            connection_name=kafka_connection_name,
            topic=kafka_topic,
            group_id=kafka_group_id,
            auto_offset_reset=kafka_auto_offset_reset,  # type: ignore[arg-type]
            store_raw_value=kafka_store_raw_value,
        )

    imported: DatasourceS3Model | None = None
    if import_connection_name or import_bucket_uri or import_schedule or import_from_timestamp:
        if not import_connection_name or not import_bucket_uri:
            raise MigrationParseError(
                resource.file_path,
                "datasource",
                resource.name,
                "IMPORT_CONNECTION_NAME and IMPORT_BUCKET_URI are required when import directives are used.",
            )
        imported = DatasourceS3Model(
            connection_name=import_connection_name,
            bucket_uri=import_bucket_uri,
            schedule=import_schedule,
            from_timestamp=import_from_timestamp,
        )

    if kafka and imported:
        raise MigrationParseError(
            resource.file_path,
            "datasource",
            resource.name,
            "Datasource cannot mix Kafka directives with import directives.",
        )

    return DatasourceModel(
        kind="datasource",
        name=resource.name,
        file_path=resource.file_path,
        description=description,
        columns=columns,
        engine=DatasourceEngineModel(
            type=engine_type,
            sorting_key=sorting_key,
            partition_key=partition_key,
            primary_key=primary_key,
            ttl=ttl,
            ver=ver,
            is_deleted=is_deleted,
            sign=sign,
            version=version,
            summing_columns=summing_columns,
            settings=settings,
        )
        if engine_type
        else None,
        indexes=indexes,
        kafka=kafka,
        s3=imported,
        gcs=None,
        forward_query=forward_query,
        tokens=tokens,
        shared_with=shared_with,
    )
