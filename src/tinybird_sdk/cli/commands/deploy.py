from __future__ import annotations

from dataclasses import dataclass
import os
import time
from typing import Any

from ...api.deploy import deploy_to_main
from ...cli.config import load_config_async
from ...generator.index import BuildFromIncludeResult, build_from_include


@dataclass(frozen=True, slots=True)
class DeployCommandOptions:
    cwd: str | None = None
    check: bool = False
    allow_destructive_operations: bool = False


@dataclass(frozen=True, slots=True)
class DeployCommandResult:
    success: bool
    duration_ms: int
    build: BuildFromIncludeResult | None = None
    deploy: dict[str, Any] | None = None
    error: str | None = None


def run_deploy(options: DeployCommandOptions | dict[str, Any] | None = None) -> DeployCommandResult:
    start = int(time.time() * 1000)
    normalized = options if isinstance(options, DeployCommandOptions) else DeployCommandOptions(**(options or {}))
    cwd = normalized.cwd or os.getcwd()

    try:
        config = load_config_async(cwd)
    except Exception as error:
        return DeployCommandResult(success=False, error=str(error), duration_ms=int(time.time() * 1000) - start)

    try:
        build_result = build_from_include({"include_paths": config["include"], "cwd": config["cwd"]})
    except Exception as error:
        return DeployCommandResult(
            success=False,
            error=f"Build failed: {error}",
            duration_ms=int(time.time() * 1000) - start,
        )

    try:
        deploy_result = deploy_to_main(
            {"base_url": config["base_url"], "token": config["token"]},
            build_result.resources,
            {"check": normalized.check, "allow_destructive_operations": normalized.allow_destructive_operations},
        )
    except Exception as error:
        return DeployCommandResult(
            success=False,
            build=build_result,
            error=f"Deploy failed: {error}",
            duration_ms=int(time.time() * 1000) - start,
        )

    if not deploy_result.get("success"):
        return DeployCommandResult(
            success=False,
            build=build_result,
            deploy=deploy_result,
            error=deploy_result.get("error"),
            duration_ms=int(time.time() * 1000) - start,
        )

    return DeployCommandResult(
        success=True,
        build=build_result,
        deploy=deploy_result,
        duration_ms=int(time.time() * 1000) - start,
    )


__all__ = ["DeployCommandOptions", "DeployCommandResult", "run_deploy"]
