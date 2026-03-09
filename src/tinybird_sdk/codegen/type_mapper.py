from __future__ import annotations

import re


def _parse_enum_values(enum_content: str) -> list[str]:
    return [match.group(1) for match in re.finditer(r"'([^']+)'\s*=\s*\d+", enum_content)]


def clickhouse_type_to_validator(ch_type: str) -> str:
    ch_type = ch_type.strip()

    nullable = re.match(r"^Nullable\((.+)\)$", ch_type)
    if nullable:
        return f"{clickhouse_type_to_validator(nullable.group(1))}.nullable()"

    low_card = re.match(r"^LowCardinality\((.+)\)$", ch_type)
    if low_card:
        return f"{clickhouse_type_to_validator(low_card.group(1))}.low_cardinality()"

    simple = {
        "String": "t.string()",
        "UUID": "t.uuid()",
        "Int8": "t.int8()",
        "Int16": "t.int16()",
        "Int32": "t.int32()",
        "Int64": "t.int64()",
        "Int128": "t.int128()",
        "Int256": "t.int256()",
        "UInt8": "t.uint8()",
        "UInt16": "t.uint16()",
        "UInt32": "t.uint32()",
        "UInt64": "t.uint64()",
        "UInt128": "t.uint128()",
        "UInt256": "t.uint256()",
        "Float32": "t.float32()",
        "Float64": "t.float64()",
        "Bool": "t.bool()",
        "Boolean": "t.bool()",
        "Date": "t.date()",
        "Date32": "t.date32()",
        "DateTime": "t.date_time()",
        "JSON": "t.json()",
        "Object": "t.json()",
        "IPv4": "t.ipv4()",
        "IPv6": "t.ipv6()",
    }
    if ch_type in simple:
        return simple[ch_type]

    dt_tz = re.match(r"^DateTime\('([^']+)'\)$", ch_type)
    if dt_tz:
        return f't.date_time("{dt_tz.group(1)}")'

    dt64 = re.match(r"^DateTime64\((\d+)(?:,\s*'([^']+)')?\)$", ch_type)
    if dt64:
        precision = dt64.group(1)
        tz = dt64.group(2)
        if tz:
            return f't.date_time64({precision}, "{tz}")'
        return f"t.date_time64({precision})"

    fixed = re.match(r"^FixedString\((\d+)\)$", ch_type)
    if fixed:
        return f"t.fixed_string({fixed.group(1)})"

    decimal = re.match(r"^Decimal\((\d+)(?:,\s*(\d+))?\)$", ch_type)
    if decimal:
        return f"t.decimal({decimal.group(1)}, {decimal.group(2) or '0'})"

    decimal_n = re.match(r"^Decimal(32|64|128|256)\((\d+)\)$", ch_type)
    if decimal_n:
        precision_map = {"32": 9, "64": 18, "128": 38, "256": 76}
        return f"t.decimal({precision_map[decimal_n.group(1)]}, {decimal_n.group(2)})"

    array = re.match(r"^Array\((.+)\)$", ch_type)
    if array:
        return f"t.array({clickhouse_type_to_validator(array.group(1))})"

    map_match = re.match(r"^Map\(([^,]+),\s*(.+)\)$", ch_type)
    if map_match:
        return f"t.map({clickhouse_type_to_validator(map_match.group(1))}, {clickhouse_type_to_validator(map_match.group(2))})"

    enum8 = re.match(r"^Enum8\((.+)\)$", ch_type)
    if enum8:
        values = _parse_enum_values(enum8.group(1))
        return f"t.enum8({', '.join(repr(v) for v in values)})" if values else "t.string()"

    enum16 = re.match(r"^Enum16\((.+)\)$", ch_type)
    if enum16:
        values = _parse_enum_values(enum16.group(1))
        return f"t.enum16({', '.join(repr(v) for v in values)})" if values else "t.string()"

    simple_agg = re.match(r"^SimpleAggregateFunction\((\w+),\s*(.+)\)$", ch_type)
    if simple_agg:
        return f't.simple_aggregate_function("{simple_agg.group(1)}", {clickhouse_type_to_validator(simple_agg.group(2))})'

    agg = re.match(r"^AggregateFunction\((\w+),\s*(.+)\)$", ch_type)
    if agg:
        return f't.aggregate_function("{agg.group(1)}", {clickhouse_type_to_validator(agg.group(2))})'

    if ch_type.startswith("Nested("):
        return "t.json()"

    return f"t.string()  # TODO: Unknown type: {ch_type}"


def param_type_to_validator(param_type: str, default_value: str | int | None = None, required: bool = True) -> str:
    param_type = param_type.strip()
    mapping = {
        "String": "p.string()",
        "UUID": "p.uuid()",
        "Int8": "p.int8()",
        "Int16": "p.int16()",
        "Int32": "p.int32()",
        "Int64": "p.int64()",
        "UInt8": "p.uint8()",
        "UInt16": "p.uint16()",
        "UInt32": "p.uint32()",
        "UInt64": "p.uint64()",
        "Float32": "p.float32()",
        "Float64": "p.float64()",
        "Boolean": "p.boolean()",
        "Bool": "p.boolean()",
        "Date": "p.date()",
        "DateTime": "p.date_time()",
        "DateTime64": "p.date_time64()",
    }

    validator = mapping.get(param_type)
    if not validator:
        if param_type.startswith("DateTime64"):
            validator = "p.date_time64()"
        elif param_type.startswith("DateTime"):
            validator = "p.date_time()"
        elif param_type.startswith("Array"):
            validator = "p.array(p.string())"
        else:
            validator = "p.string()"

    if not required or default_value is not None:
        if default_value is not None:
            validator = f"{validator}.optional({default_value!r})"
        else:
            validator = f"{validator}.optional()"

    return validator
