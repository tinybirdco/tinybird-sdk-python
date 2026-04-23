from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Literal

DevMode = Literal["branch", "local"]
BranchDataOnCreate = Literal["last_partition", "all_partitions"]
BRANCH_DATA_ON_CREATE_VALUES: tuple[str, ...] = ("last_partition", "all_partitions")


class BranchDataOnCreateMode(StrEnum):
    LAST_PARTITION = "last_partition"
    ALL_PARTITIONS = "all_partitions"


@dataclass(frozen=True, slots=True)
class TinybirdConfig:
    include: list[str] | None = None
    schema: str | None = None
    token: str | None = None
    base_url: str | None = None
    dev_mode: DevMode | None = None
    branch_data_on_create: str | None = None
