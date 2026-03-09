from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class ParamValidator:
    _python_type: Any
    _tinybirdType: str
    _required: bool = True
    _default: Any = None
    _description: str | None = None

    def optional(self, default_value: Any = None) -> "ParamValidator":
        return ParamValidator(
            _python_type=self._python_type,
            _tinybirdType=self._tinybirdType,
            _required=False,
            _default=default_value,
            _description=self._description,
        )

    def required(self) -> "ParamValidator":
        return ParamValidator(
            _python_type=self._python_type,
            _tinybirdType=self._tinybirdType,
            _required=True,
            _default=None,
            _description=self._description,
        )

    def describe(self, description: str) -> "ParamValidator":
        return ParamValidator(
            _python_type=self._python_type,
            _tinybirdType=self._tinybirdType,
            _required=self._required,
            _default=self._default,
            _description=description,
        )


class _ParamFactory:
    def string(self) -> ParamValidator:
        return ParamValidator(str, "String")

    def uuid(self) -> ParamValidator:
        return ParamValidator(str, "UUID")

    def int8(self) -> ParamValidator:
        return ParamValidator(int, "Int8")

    def int16(self) -> ParamValidator:
        return ParamValidator(int, "Int16")

    def int32(self) -> ParamValidator:
        return ParamValidator(int, "Int32")

    def int64(self) -> ParamValidator:
        return ParamValidator(int, "Int64")

    def uint8(self) -> ParamValidator:
        return ParamValidator(int, "UInt8")

    def uint16(self) -> ParamValidator:
        return ParamValidator(int, "UInt16")

    def uint32(self) -> ParamValidator:
        return ParamValidator(int, "UInt32")

    def uint64(self) -> ParamValidator:
        return ParamValidator(int, "UInt64")

    def float32(self) -> ParamValidator:
        return ParamValidator(float, "Float32")

    def float64(self) -> ParamValidator:
        return ParamValidator(float, "Float64")

    def boolean(self) -> ParamValidator:
        return ParamValidator(bool, "Boolean")

    def date(self) -> ParamValidator:
        return ParamValidator(str, "Date")

    def date_time(self) -> ParamValidator:
        return ParamValidator(str, "DateTime")

    def date_time64(self) -> ParamValidator:
        return ParamValidator(str, "DateTime64")

    def array(self, element: ParamValidator, separator: str | None = None) -> ParamValidator:  # noqa: ARG002
        return ParamValidator(list, "Array")

    def column(self) -> ParamValidator:
        return ParamValidator(str, "column")

    def json(self) -> ParamValidator:
        return ParamValidator(dict, "JSON")


p = _ParamFactory()


def is_param_validator(value: Any) -> bool:
    return isinstance(value, ParamValidator)


def get_param_tinybird_type(validator: ParamValidator) -> str:
    return validator._tinybirdType


def is_param_required(validator: ParamValidator) -> bool:
    return validator._required


def get_param_default(validator: ParamValidator) -> Any:
    return validator._default


def get_param_description(validator: ParamValidator) -> str | None:
    return validator._description
