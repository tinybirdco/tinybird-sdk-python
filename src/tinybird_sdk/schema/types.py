from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date, datetime
from decimal import Decimal
from typing import Any


@dataclass(frozen=True, slots=True)
class TypeModifiers:
    nullable: bool = False
    low_cardinality: bool = False
    has_default: bool = False
    default_value: Any = None
    codec: str | None = None


@dataclass(frozen=True, slots=True)
class TypeValidator:
    _python_type: Any
    _tinybirdType: str
    _modifiers: TypeModifiers = field(default_factory=TypeModifiers)

    def nullable(self) -> "TypeValidator":
        tinybird_type = self._tinybirdType
        if self._modifiers.low_cardinality:
            base_type = tinybird_type.removeprefix("LowCardinality(").removesuffix(")")
            tinybird_type = f"LowCardinality(Nullable({base_type}))"
        else:
            tinybird_type = f"Nullable({tinybird_type})"

        return TypeValidator(
            _python_type=self._python_type,
            _tinybirdType=tinybird_type,
            _modifiers=replace(self._modifiers, nullable=True),
        )

    def low_cardinality(self) -> "TypeValidator":
        tinybird_type = self._tinybirdType
        if self._modifiers.nullable:
            base_type = tinybird_type.removeprefix("Nullable(").removesuffix(")")
            tinybird_type = f"LowCardinality(Nullable({base_type}))"
        else:
            tinybird_type = f"LowCardinality({tinybird_type})"

        return TypeValidator(
            _python_type=self._python_type,
            _tinybirdType=tinybird_type,
            _modifiers=replace(self._modifiers, low_cardinality=True),
        )

    def default(self, value: Any) -> "TypeValidator":
        return TypeValidator(
            _python_type=self._python_type,
            _tinybirdType=self._tinybirdType,
            _modifiers=replace(self._modifiers, has_default=True, default_value=value),
        )

    def codec(self, codec: str) -> "TypeValidator":
        return TypeValidator(
            _python_type=self._python_type,
            _tinybirdType=self._tinybirdType,
            _modifiers=replace(self._modifiers, codec=codec),
        )


class _TypeFactory:
    @staticmethod
    def _escape_enum_value(value: str) -> str:
        return value.replace("'", "\\'")

    def string(self) -> TypeValidator:
        return TypeValidator(str, "String")

    def fixed_string(self, length: int) -> TypeValidator:
        return TypeValidator(str, f"FixedString({length})")

    def uuid(self) -> TypeValidator:
        return TypeValidator(str, "UUID")

    def int8(self) -> TypeValidator:
        return TypeValidator(int, "Int8")

    def int16(self) -> TypeValidator:
        return TypeValidator(int, "Int16")

    def int32(self) -> TypeValidator:
        return TypeValidator(int, "Int32")

    def int64(self) -> TypeValidator:
        return TypeValidator(int, "Int64")

    def int128(self) -> TypeValidator:
        return TypeValidator(int, "Int128")

    def int256(self) -> TypeValidator:
        return TypeValidator(int, "Int256")

    def uint8(self) -> TypeValidator:
        return TypeValidator(int, "UInt8")

    def uint16(self) -> TypeValidator:
        return TypeValidator(int, "UInt16")

    def uint32(self) -> TypeValidator:
        return TypeValidator(int, "UInt32")

    def uint64(self) -> TypeValidator:
        return TypeValidator(int, "UInt64")

    def uint128(self) -> TypeValidator:
        return TypeValidator(int, "UInt128")

    def uint256(self) -> TypeValidator:
        return TypeValidator(int, "UInt256")

    def float32(self) -> TypeValidator:
        return TypeValidator(float, "Float32")

    def float64(self) -> TypeValidator:
        return TypeValidator(float, "Float64")

    def decimal(self, precision: int, scale: int) -> TypeValidator:
        return TypeValidator(Decimal, f"Decimal({precision}, {scale})")

    def bool(self) -> TypeValidator:
        return TypeValidator(bool, "Bool")

    def date(self) -> TypeValidator:
        return TypeValidator(date, "Date")

    def date32(self) -> TypeValidator:
        return TypeValidator(date, "Date32")

    def date_time(self, timezone: str | None = None) -> TypeValidator:
        if timezone:
            return TypeValidator(datetime, f"DateTime('{timezone}')")
        return TypeValidator(datetime, "DateTime")

    def date_time64(self, precision: int = 3, timezone: str | None = None) -> TypeValidator:
        if timezone:
            return TypeValidator(datetime, f"DateTime64({precision}, '{timezone}')")
        return TypeValidator(datetime, f"DateTime64({precision})")

    def array(self, element: TypeValidator) -> TypeValidator:
        return TypeValidator(list, f"Array({element._tinybirdType})")

    def tuple(self, *elements: TypeValidator) -> TypeValidator:
        tinybird_type = ", ".join(el._tinybirdType for el in elements)
        return TypeValidator(tuple, f"Tuple({tinybird_type})")

    def map(self, key_type: TypeValidator, value_type: TypeValidator) -> TypeValidator:
        return TypeValidator(dict, f"Map({key_type._tinybirdType}, {value_type._tinybirdType})")

    def json(self) -> TypeValidator:
        return TypeValidator(dict, "JSON")

    def enum8(self, *values: str) -> TypeValidator:
        mapping = ", ".join(f"'{self._escape_enum_value(v)}' = {idx + 1}" for idx, v in enumerate(values))
        return TypeValidator(str, f"Enum8({mapping})")

    def enum16(self, *values: str) -> TypeValidator:
        mapping = ", ".join(f"'{self._escape_enum_value(v)}' = {idx + 1}" for idx, v in enumerate(values))
        return TypeValidator(str, f"Enum16({mapping})")

    def ipv4(self) -> TypeValidator:
        return TypeValidator(str, "IPv4")

    def ipv6(self) -> TypeValidator:
        return TypeValidator(str, "IPv6")

    def simple_aggregate_function(self, func: str, validator: TypeValidator) -> TypeValidator:
        return TypeValidator(
            validator._python_type,
            f"SimpleAggregateFunction({func}, {validator._tinybirdType})",
        )

    def aggregate_function(self, func: str, validator: TypeValidator) -> TypeValidator:
        return TypeValidator(
            validator._python_type,
            f"AggregateFunction({func}, {validator._tinybirdType})",
        )


t = _TypeFactory()


def is_type_validator(value: Any) -> bool:
    return isinstance(value, TypeValidator)


def get_tinybird_type(validator: TypeValidator) -> str:
    return validator._tinybirdType


def get_modifiers(validator: TypeValidator) -> TypeModifiers:
    return validator._modifiers
