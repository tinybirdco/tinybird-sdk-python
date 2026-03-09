from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import glob


@dataclass(frozen=True, slots=True)
class ResolvedIncludeFile:
    source_path: str
    absolute_path: str


_IGNORED_DIRECTORIES = {".git", "node_modules"}
_GLOB_CHARS = {"*", "?", "[", "]"}


def _has_glob(value: str) -> bool:
    return any(char in value for char in _GLOB_CHARS)


def _normalize(value: str) -> str:
    return value.replace("\\", "/")


def _is_ignored(path: Path) -> bool:
    return any(part in _IGNORED_DIRECTORIES for part in path.parts)


def resolve_include_files(include_paths: list[str], cwd: str) -> list[ResolvedIncludeFile]:
    base = Path(cwd).resolve()
    resolved: list[ResolvedIncludeFile] = []
    seen: set[str] = set()

    for include_path in include_paths:
        if _has_glob(include_path):
            absolute_pattern = include_path if Path(include_path).is_absolute() else str(base / include_path)
            matches = sorted(glob.glob(absolute_pattern, recursive=True))
            matches = [m for m in matches if Path(m).is_file() and not _is_ignored(Path(m))]
            if not matches:
                raise ValueError(f"Include pattern matched no files: {include_path}")

            for match in matches:
                key = _normalize(str(Path(match).resolve()))
                if key in seen:
                    continue
                seen.add(key)
                source = match if Path(include_path).is_absolute() else _normalize(str(Path(match).resolve().relative_to(base)))
                resolved.append(ResolvedIncludeFile(source_path=source, absolute_path=key))
            continue

        absolute = Path(include_path)
        if not absolute.is_absolute():
            absolute = base / include_path
        absolute = absolute.resolve()

        if not absolute.exists():
            raise ValueError(f"Include file not found: {absolute}")

        if absolute.is_dir():
            for child in sorted(absolute.rglob("*")):
                if not child.is_file() or _is_ignored(child):
                    continue
                key = _normalize(str(child.resolve()))
                if key in seen:
                    continue
                seen.add(key)
                source = _normalize(str(child.resolve().relative_to(base)))
                resolved.append(ResolvedIncludeFile(source_path=source, absolute_path=key))
            continue

        key = _normalize(str(absolute))
        if key in seen:
            continue
        seen.add(key)
        resolved.append(ResolvedIncludeFile(source_path=include_path, absolute_path=key))

    return resolved


def get_include_watch_directories(include_paths: list[str], cwd: str) -> list[str]:
    base = Path(cwd).resolve()
    watch_dirs: set[str] = set()

    for include_path in include_paths:
        if _has_glob(include_path):
            absolute_pattern = include_path if Path(include_path).is_absolute() else str(base / include_path)
            pattern = Path(absolute_pattern)
            anchor_parts: list[str] = []
            for part in pattern.parts:
                if _has_glob(part):
                    break
                anchor_parts.append(part)
            anchor = Path(*anchor_parts) if anchor_parts else base
            watch_dirs.add(_normalize(str(anchor.resolve())))
            continue

        absolute = Path(include_path)
        if not absolute.is_absolute():
            absolute = base / include_path
        absolute = absolute.resolve()
        watch_dirs.add(_normalize(str(absolute if absolute.is_dir() else absolute.parent)))

    return sorted(watch_dirs)


__all__ = ["ResolvedIncludeFile", "resolve_include_files", "get_include_watch_directories"]
