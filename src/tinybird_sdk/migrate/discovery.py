from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..generator.include_paths import resolve_include_files
from .types import MigrationError, ResourceFile, ResourceKind


_SUPPORTED_EXTENSIONS: dict[str, ResourceKind] = {
    ".datasource": "datasource",
    ".pipe": "pipe",
    ".connection": "connection",
}


def _normalize_path(file_path: str) -> str:
    return file_path.replace("\\", "/")


def _get_kind_from_path(file_path: str) -> ResourceKind | None:
    return _SUPPORTED_EXTENSIONS.get(Path(file_path).suffix.lower())


def _collect_directory_files(directory: Path) -> list[Path]:
    files: list[Path] = []
    for entry in sorted(directory.iterdir()):
        if entry.is_dir():
            if entry.name in {"node_modules", ".git"}:
                continue
            files.extend(_collect_directory_files(entry))
        elif entry.is_file():
            files.append(entry)
    return files


@dataclass(frozen=True, slots=True)
class DiscoverResourcesResult:
    resources: list[ResourceFile]
    errors: list[MigrationError]


def discover_resource_files(patterns: list[str], cwd: str) -> DiscoverResourcesResult:
    resources: list[ResourceFile] = []
    errors: list[MigrationError] = []
    seen: set[str] = set()
    base = Path(cwd).resolve()

    for pattern in patterns:
        absolute_pattern = Path(pattern)
        if not absolute_pattern.is_absolute():
            absolute_pattern = (base / absolute_pattern).resolve()

        if absolute_pattern.exists():
            if absolute_pattern.is_dir():
                for absolute_file_path in _collect_directory_files(absolute_pattern):
                    kind = _get_kind_from_path(str(absolute_file_path))
                    if not kind:
                        continue
                    key = _normalize_path(str(absolute_file_path.resolve()))
                    if key in seen:
                        continue
                    seen.add(key)
                    rel = _normalize_path(str(absolute_file_path.resolve().relative_to(base)))
                    resources.append(
                        ResourceFile(
                            kind=kind,
                            file_path=rel,
                            absolute_path=key,
                            name=absolute_file_path.stem,
                            content=absolute_file_path.read_text(encoding="utf-8"),
                        )
                    )
                continue

            kind = _get_kind_from_path(str(absolute_pattern))
            if not kind:
                errors.append(
                    MigrationError(
                        file_path=pattern,
                        resource_name=absolute_pattern.name,
                        resource_kind="datasource",
                        message=(
                            f"Unsupported file extension: {absolute_pattern.suffix or '(none)'}"
                            ". Use .datasource, .pipe, or .connection."
                        ),
                    )
                )
                continue

            key = _normalize_path(str(absolute_pattern.resolve()))
            if key in seen:
                continue
            seen.add(key)
            resources.append(
                ResourceFile(
                    kind=kind,
                    file_path=_normalize_path(str(absolute_pattern.resolve().relative_to(base))),
                    absolute_path=key,
                    name=absolute_pattern.stem,
                    content=absolute_pattern.read_text(encoding="utf-8"),
                )
            )
            continue

        try:
            matched = resolve_include_files([pattern], str(base))
            for entry in matched:
                kind = _get_kind_from_path(entry.absolute_path)
                if not kind:
                    continue
                key = _normalize_path(entry.absolute_path)
                if key in seen:
                    continue
                seen.add(key)
                absolute = Path(entry.absolute_path)
                resources.append(
                    ResourceFile(
                        kind=kind,
                        file_path=_normalize_path(entry.source_path),
                        absolute_path=key,
                        name=absolute.stem,
                        content=absolute.read_text(encoding="utf-8"),
                    )
                )
        except Exception as error:
            errors.append(
                MigrationError(
                    file_path=pattern,
                    resource_name=Path(pattern).name,
                    resource_kind="datasource",
                    message=str(error),
                )
            )

    resources.sort(key=lambda item: item.file_path)
    return DiscoverResourcesResult(resources=resources, errors=errors)


__all__ = ["DiscoverResourcesResult", "discover_resource_files"]
