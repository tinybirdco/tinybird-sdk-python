from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ...migrate.runner import run_migrate as run_migrate_runner


@dataclass(frozen=True, slots=True)
class MigrateCommandOptions:
    cwd: str | None = None
    patterns: list[str] | None = None
    out: str | None = None
    strict: bool = True
    dry_run: bool = False
    force: bool = False


def run_migrate_command(options: MigrateCommandOptions | dict[str, Any]) -> dict[str, Any]:
    normalized = options if isinstance(options, MigrateCommandOptions) else MigrateCommandOptions(**options)
    result = run_migrate_runner(
        {
            "cwd": normalized.cwd,
            "patterns": normalized.patterns or [],
            "out": normalized.out,
            "strict": normalized.strict,
            "dry_run": normalized.dry_run,
            "force": normalized.force,
        }
    )

    return {
        "success": result.success,
        "output_path": result.output_path,
        "migrated": result.migrated,
        "errors": result.errors,
        "dry_run": result.dry_run,
        "output_content": result.output_content,
    }


# Keep TS-style name
run_migrate = run_migrate_command


__all__ = ["MigrateCommandOptions", "run_migrate"]
