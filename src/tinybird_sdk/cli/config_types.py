from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

DevMode = Literal["branch", "local"]


@dataclass(frozen=True, slots=True)
class TinybirdConfig:
    include: list[str] | None = None
    schema: str | None = None
    token: str | None = None
    base_url: str | None = None
    dev_mode: DevMode | None = None
