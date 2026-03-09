from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import time
from typing import Any, Literal

from ...generator.index import BuildFromIncludeResult, build_from_include
from ..config import load_config_async


GeneratedResourceType = Literal["datasource", "pipe", "connection"]


@dataclass(frozen=True, slots=True)
class GeneratedResourceArtifact:
    type: GeneratedResourceType
    name: str
    relative_path: str
    content: str


@dataclass(frozen=True, slots=True)
class GenerateCommandOptions:
    cwd: str | None = None
    output_dir: str | None = None


@dataclass(frozen=True, slots=True)
class GenerateCommandResult:
    success: bool
    duration_ms: int
    artifacts: list[GeneratedResourceArtifact] | None = None
    stats: dict[str, int] | None = None
    output_dir: str | None = None
    config_path: str | None = None
    error: str | None = None


def _to_artifacts(build: BuildFromIncludeResult) -> list[GeneratedResourceArtifact]:
    artifacts: list[GeneratedResourceArtifact] = []

    for datasource in build.resources.datasources:
        artifacts.append(
            GeneratedResourceArtifact(
                type="datasource",
                name=datasource.name,
                relative_path=f"datasources/{datasource.name}.datasource",
                content=datasource.content,
            )
        )

    for pipe in build.resources.pipes:
        artifacts.append(
            GeneratedResourceArtifact(
                type="pipe",
                name=pipe.name,
                relative_path=f"pipes/{pipe.name}.pipe",
                content=pipe.content,
            )
        )

    for connection in build.resources.connections:
        artifacts.append(
            GeneratedResourceArtifact(
                type="connection",
                name=connection.name,
                relative_path=f"connections/{connection.name}.connection",
                content=connection.content,
            )
        )

    return artifacts


def _write_artifacts(output_dir: Path, artifacts: list[GeneratedResourceArtifact]) -> None:
    for artifact in artifacts:
        target_path = output_dir / artifact.relative_path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text(artifact.content, encoding="utf-8")


def run_generate(options: GenerateCommandOptions | dict[str, Any] | None = None) -> GenerateCommandResult:
    start = int(time.time() * 1000)
    normalized = options if isinstance(options, GenerateCommandOptions) else GenerateCommandOptions(**(options or {}))
    cwd = Path(normalized.cwd or os.getcwd()).resolve()

    try:
        config = load_config_async(str(cwd))
        build_result = build_from_include({"include_paths": config["include"], "cwd": config["cwd"]})
        artifacts = _to_artifacts(build_result)

        resolved_output_dir: str | None = None
        if normalized.output_dir:
            output_dir = Path(normalized.output_dir)
            if not output_dir.is_absolute():
                output_dir = cwd / output_dir
            _write_artifacts(output_dir, artifacts)
            resolved_output_dir = str(output_dir)

        return GenerateCommandResult(
            success=True,
            artifacts=artifacts,
            stats={
                "datasource_count": build_result.stats["datasource_count"],
                "pipe_count": build_result.stats["pipe_count"],
                "connection_count": build_result.stats["connection_count"],
                "total_count": len(artifacts),
            },
            output_dir=resolved_output_dir,
            config_path=config["config_path"],
            duration_ms=int(time.time() * 1000) - start,
        )
    except Exception as error:
        return GenerateCommandResult(
            success=False,
            error=str(error),
            duration_ms=int(time.time() * 1000) - start,
        )

runGenerate = run_generate


__all__ = [
    "GeneratedResourceArtifact",
    "GenerateCommandOptions",
    "GenerateCommandResult",
    "run_generate",
    "runGenerate",
]
