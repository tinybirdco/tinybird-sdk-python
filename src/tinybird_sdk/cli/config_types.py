from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Literal

DevMode = Literal["branch", "local"]
BranchDataMode = Literal["last_partition"]
BRANCH_DATA_MODE_VALUES: tuple[str, ...] = ("last_partition",)


class BranchDataModeEnum(StrEnum):
    LAST_PARTITION = "last_partition"


@dataclass(frozen=True, slots=True)
class TinybirdConfig:
    include: list[str] | None = None
    schema: str | None = None
    token: str | None = None
    base_url: str | None = None
    dev_mode: DevMode | None = None
    branch_data_mode: str | None = None
