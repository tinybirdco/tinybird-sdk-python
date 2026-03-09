from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

DatasourceTokenScope = str
PipeTokenScope = str


@dataclass(frozen=True, slots=True)
class TokenDefinition:
    _name: str
    _type: str = "token"


def define_token(name: str) -> TokenDefinition:
    if not NAME_PATTERN.match(name):
        raise ValueError(
            f'Invalid token name: "{name}". Must start with a letter or underscore and contain only alphanumeric characters and underscores.'
        )
    return TokenDefinition(_name=name)


def is_token_definition(value: Any) -> bool:
    return isinstance(value, TokenDefinition)
