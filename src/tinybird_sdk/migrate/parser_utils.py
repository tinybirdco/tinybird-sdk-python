from __future__ import annotations

import json
from typing import Any

from .types import ResourceKind


class MigrationParseError(Exception):
    def __init__(self, file_path: str, resource_kind: ResourceKind, resource_name: str, message: str):
        super().__init__(message)
        self.file_path = file_path
        self.resource_kind = resource_kind
        self.resource_name = resource_name


def split_lines(content: str) -> list[str]:
    return content.replace("\r\n", "\n").split("\n")


def is_blank(line: str) -> bool:
    return len(line.strip()) == 0


def strip_indent(line: str) -> str:
    if line.startswith("    "):
        return line[4:]
    return line.lstrip()


def read_directive_block(
    lines: list[str],
    start_index: int,
    is_directive_line: Any,
) -> tuple[list[str], int]:
    collected: list[str] = []
    i = start_index

    while i < len(lines):
        line = lines[i].strip()
        if is_directive_line(line):
            break
        collected.append(line)
        i += 1

    first = 0
    while first < len(collected) and collected[first] == "":
        first += 1

    last = len(collected) - 1
    while last >= first and collected[last] == "":
        last -= 1

    normalized = collected[first : last + 1] if first <= last else []
    return normalized, i


def split_comma_separated(input: str) -> list[str]:
    return [part.strip() for part in input.split(",") if part.strip()]


def parse_quoted_value(input: str) -> str:
    trimmed = input.strip()
    if len(trimmed) >= 2 and (
        (trimmed.startswith('"') and trimmed.endswith('"'))
        or (trimmed.startswith("'") and trimmed.endswith("'"))
    ):
        return trimmed[1:-1]
    return trimmed


def parse_literal_from_datafile(value: str) -> str | int | float | bool | None | dict[str, Any] | list[Any]:
    trimmed = value.strip()

    if trimmed == "NULL":
        return None

    if trimmed.replace(".", "", 1).replace("-", "", 1).isdigit() and trimmed not in {"-", "."}:
        return float(trimmed) if "." in trimmed else int(trimmed)

    if trimmed == "1":
        return True
    if trimmed == "0":
        return False
    if trimmed.startswith("'") and trimmed.endswith("'"):
        return trimmed[1:-1].replace("\\'", "'")
    if (trimmed.startswith("{") and trimmed.endswith("}")) or (
        trimmed.startswith("[") and trimmed.endswith("]")
    ):
        parsed = json.loads(trimmed)
        if isinstance(parsed, (dict, list)):
            return parsed

    raise ValueError(f"Unsupported literal value: {value}")


def to_ts_literal(value: str | int | float | bool | None | dict[str, Any] | list[Any]) -> str:
    if value is None:
        return "None"
    if isinstance(value, (int, float, bool)):
        return repr(value)
    if isinstance(value, str):
        return json.dumps(value)
    return json.dumps(value)


def parse_directive_line(line: str) -> dict[str, str]:
    first_space = line.find(" ")
    if first_space == -1:
        return {"key": line.strip(), "value": ""}
    return {"key": line[:first_space].strip(), "value": line[first_space + 1 :].strip()}


def split_top_level_comma(input: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    depth = 0
    in_single = False
    in_double = False

    i = 0
    while i < len(input):
        char = input[i]
        prev = input[i - 1] if i > 0 else ""

        if char == "'" and not in_double and prev != "\\":
            in_single = not in_single
            current.append(char)
            i += 1
            continue

        if char == '"' and not in_single and prev != "\\":
            in_double = not in_double
            current.append(char)
            i += 1
            continue

        if not in_single and not in_double:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            elif char == "," and depth == 0:
                piece = "".join(current).strip()
                if piece:
                    parts.append(piece)
                current = []
                i += 1
                continue

        current.append(char)
        i += 1

    piece = "".join(current).strip()
    if piece:
        parts.append(piece)

    return parts
