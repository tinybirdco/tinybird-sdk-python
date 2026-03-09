from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import time
from typing import Any, Literal

from ...api.resources import ResourceFile, pull_all_resource_files
from ..config import load_config_async


@dataclass(frozen=True, slots=True)
class PullCommandOptions:
    cwd: str | None = None
    output_dir: str = "."
    overwrite: bool = False


@dataclass(frozen=True, slots=True)
class PulledFileResult:
    name: str
    type: Literal["datasource", "pipe", "connection"]
    filename: str
    path: str
    relative_path: str
    status: Literal["created", "overwritten"]


@dataclass(frozen=True, slots=True)
class PullCommandResult:
    success: bool
    duration_ms: int
    output_dir: str | None = None
    files: list[PulledFileResult] | None = None
    stats: dict[str, int] | None = None
    error: str | None = None


def _flatten_resources(resources: dict[str, list[ResourceFile]]) -> list[ResourceFile]:
    return [
        *(resources.get("datasources") or []),
        *(resources.get("pipes") or []),
        *(resources.get("connections") or []),
    ]


def run_pull(options: PullCommandOptions | dict[str, Any] | None = None) -> PullCommandResult:
    start = int(time.time() * 1000)
    normalized = options if isinstance(options, PullCommandOptions) else PullCommandOptions(**(options or {}))
    cwd = Path(normalized.cwd or os.getcwd()).resolve()
    output_dir = Path(normalized.output_dir)
    if not output_dir.is_absolute():
        output_dir = cwd / output_dir

    try:
        config = load_config_async(str(cwd))
    except Exception as error:
        return PullCommandResult(success=False, error=str(error), duration_ms=int(time.time() * 1000) - start)

    try:
        pulled = pull_all_resource_files({"base_url": config["base_url"], "token": config["token"]})
    except Exception as error:
        return PullCommandResult(
            success=False,
            error=f"Pull failed: {error}",
            duration_ms=int(time.time() * 1000) - start,
        )

    all_files = sorted(_flatten_resources(pulled), key=lambda item: item.filename)

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        written: list[PulledFileResult] = []

        for file in all_files:
            absolute_path = output_dir / file.filename
            existed = absolute_path.exists()

            if existed and not normalized.overwrite:
                raise FileExistsError(str(absolute_path))

            absolute_path.write_text(file.content, encoding="utf-8")
            written.append(
                PulledFileResult(
                    name=file.name,
                    type=file.type,
                    filename=file.filename,
                    path=str(absolute_path),
                    relative_path=str(absolute_path.relative_to(cwd)),
                    status="overwritten" if existed else "created",
                )
            )

        return PullCommandResult(
            success=True,
            output_dir=str(output_dir),
            files=written,
            stats={
                "datasources": len(pulled.get("datasources") or []),
                "pipes": len(pulled.get("pipes") or []),
                "connections": len(pulled.get("connections") or []),
                "total": len(written),
            },
            duration_ms=int(time.time() * 1000) - start,
        )
    except FileExistsError as error:
        return PullCommandResult(
            success=False,
            error=f"File already exists: {error}. Use --force to overwrite existing files.",
            duration_ms=int(time.time() * 1000) - start,
        )
    except Exception as error:
        return PullCommandResult(
            success=False,
            error=f"Failed to write files: {error}",
            duration_ms=int(time.time() * 1000) - start,
        )


__all__ = ["PullCommandOptions", "PulledFileResult", "PullCommandResult", "run_pull"]
