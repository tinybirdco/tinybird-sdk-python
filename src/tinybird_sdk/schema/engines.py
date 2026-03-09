from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class EngineConfig:
    type: str
    sorting_key: str | list[str]
    partition_key: str | None = None
    primary_key: str | list[str] | None = None
    ttl: str | None = None
    settings: dict[str, Any] | None = None
    ver: str | None = None
    is_deleted: str | None = None
    columns: list[str] | None = None
    sign: str | None = None
    version: str | None = None


class _EngineFactory:
    def merge_tree(self, config: dict[str, Any]) -> EngineConfig:
        return EngineConfig(type="MergeTree", **config)

    def replacing_merge_tree(self, config: dict[str, Any]) -> EngineConfig:
        return EngineConfig(type="ReplacingMergeTree", **config)

    def summing_merge_tree(self, config: dict[str, Any]) -> EngineConfig:
        return EngineConfig(type="SummingMergeTree", **config)

    def aggregating_merge_tree(self, config: dict[str, Any]) -> EngineConfig:
        return EngineConfig(type="AggregatingMergeTree", **config)

    def collapsing_merge_tree(self, config: dict[str, Any]) -> EngineConfig:
        return EngineConfig(type="CollapsingMergeTree", **config)

    def versioned_collapsing_merge_tree(self, config: dict[str, Any]) -> EngineConfig:
        return EngineConfig(type="VersionedCollapsingMergeTree", **config)


engine = _EngineFactory()


def _normalize_key(key: str | list[str]) -> list[str]:
    return [key] if isinstance(key, str) else list(key)


def _escape_setting_value(value: str) -> str:
    return value.replace("'", "\\'")


def get_sorting_key(config: EngineConfig) -> list[str]:
    return _normalize_key(config.sorting_key)


def get_primary_key(config: EngineConfig) -> list[str]:
    if config.primary_key:
        return _normalize_key(config.primary_key)
    return get_sorting_key(config)


def get_engine_clause(config: EngineConfig) -> str:
    parts = [f'ENGINE "{config.type}"']

    if config.partition_key:
        parts.append(f'ENGINE_PARTITION_KEY "{config.partition_key}"')

    sorting = ", ".join(get_sorting_key(config))
    parts.append(f'ENGINE_SORTING_KEY "{sorting}"')

    if config.primary_key:
        primary = ", ".join(get_primary_key(config))
        parts.append(f'ENGINE_PRIMARY_KEY "{primary}"')

    if config.ttl:
        parts.append(f'ENGINE_TTL "{config.ttl}"')

    if config.type == "ReplacingMergeTree" and config.ver:
        parts.append(f'ENGINE_VER "{config.ver}"')

    if config.type in {"CollapsingMergeTree", "VersionedCollapsingMergeTree"} and config.sign:
        parts.append(f'ENGINE_SIGN "{config.sign}"')

    if config.type == "VersionedCollapsingMergeTree" and config.version:
        parts.append(f'ENGINE_VERSION "{config.version}"')

    if config.type == "SummingMergeTree" and config.columns:
        parts.append(f'ENGINE_SUMMING_COLUMNS "{", ".join(config.columns)}"')

    if config.settings:
        settings = []
        for key, value in config.settings.items():
            if isinstance(value, str):
                settings.append(f"{key}='{_escape_setting_value(value)}'")
            else:
                settings.append(f"{key}={value}")
        parts.append(f'ENGINE_SETTINGS "{", ".join(settings)}"')

    return "\n".join(parts)
