from __future__ import annotations

from dataclasses import dataclass
import os
import time
from typing import Any

from ...api.branches import get_or_create_branch
from ...api.build import build_to_tinybird
from ...api.dashboard import get_branch_dashboard_url, get_local_dashboard_url
from ...api.local import LocalNotRunningError, get_local_tokens, get_local_workspace_name, get_or_create_local_workspace
from ...api.workspaces import get_workspace
from ...cli.config import LOCAL_BASE_URL, load_config_async
from ...generator.index import BuildFromIncludeResult, build_from_include


@dataclass(frozen=True, slots=True)
class BuildCommandOptions:
    cwd: str | None = None
    dry_run: bool = False
    token_override: str | None = None
    dev_mode_override: str | None = None


@dataclass(frozen=True, slots=True)
class BuildBranchInfo:
    git_branch: str | None
    tinybird_branch: str | None
    was_created: bool
    dashboard_url: str | None = None
    is_local: bool = False


@dataclass(frozen=True, slots=True)
class BuildCommandResult:
    success: bool
    duration_ms: int
    build: BuildFromIncludeResult | None = None
    deploy: dict[str, Any] | None = None
    branch_info: BuildBranchInfo | None = None
    error: str | None = None


def run_build(options: BuildCommandOptions | dict[str, Any] | None = None) -> BuildCommandResult:
    start = int(time.time() * 1000)
    normalized = options if isinstance(options, BuildCommandOptions) else BuildCommandOptions(**(options or {}))
    cwd = normalized.cwd or os.getcwd()

    try:
        config = load_config_async(cwd)
    except Exception as error:
        return BuildCommandResult(success=False, error=str(error), duration_ms=int(time.time() * 1000) - start)

    try:
        build_result = build_from_include({"include_paths": config["include"], "cwd": config["cwd"]})
    except Exception as error:
        return BuildCommandResult(
            success=False,
            error=f"Build failed: {error}",
            duration_ms=int(time.time() * 1000) - start,
        )

    if normalized.dry_run:
        return BuildCommandResult(success=True, build=build_result, duration_ms=int(time.time() * 1000) - start)

    dev_mode = normalized.dev_mode_override or config.get("dev_mode")
    branch_info: BuildBranchInfo | None = None

    if dev_mode == "local":
        try:
            local_tokens = get_local_tokens()
            if config.get("is_main_branch") or not config.get("tinybird_branch"):
                workspace = get_workspace({"base_url": config["base_url"], "token": config["token"]})
                workspace_name = workspace.name
            else:
                workspace_name = get_local_workspace_name(config.get("tinybird_branch"), config["cwd"])

            local_workspace = get_or_create_local_workspace(local_tokens, workspace_name)
            workspace_payload = local_workspace["workspace"]
            branch_info = BuildBranchInfo(
                git_branch=config.get("git_branch"),
                tinybird_branch=workspace_name,
                was_created=bool(local_workspace.get("was_created")),
                dashboard_url=get_local_dashboard_url(workspace_name),
                is_local=True,
            )
            deploy_result = build_to_tinybird(
                {"base_url": LOCAL_BASE_URL, "token": workspace_payload.token},
                build_result.resources,
            )
        except LocalNotRunningError as error:
            return BuildCommandResult(
                success=False,
                build=build_result,
                error=str(error),
                duration_ms=int(time.time() * 1000) - start,
            )
        except Exception as error:
            return BuildCommandResult(
                success=False,
                build=build_result,
                error=f"Local build failed: {error}",
                duration_ms=int(time.time() * 1000) - start,
            )
    else:
        is_main_branch = config.get("is_main_branch") or not config.get("tinybird_branch")
        if is_main_branch and not normalized.token_override:
            return BuildCommandResult(
                success=False,
                build=build_result,
                error=(
                    "Cannot deploy to main workspace with 'build' command. "
                    "Use 'tinybird deploy' to deploy to production, or switch to a feature branch."
                ),
                duration_ms=int(time.time() * 1000) - start,
            )

        effective_token = normalized.token_override or config["token"]

        if not normalized.token_override:
            try:
                branch = get_or_create_branch(
                    {"base_url": config["base_url"], "token": config["token"]},
                    config["tinybird_branch"],
                )
                if not branch.get("token"):
                    return BuildCommandResult(
                        success=False,
                        build=build_result,
                        error=f"Branch '{config['tinybird_branch']}' was created but no token was returned.",
                        duration_ms=int(time.time() * 1000) - start,
                    )

                effective_token = branch["token"]
                workspace = get_workspace({"base_url": config["base_url"], "token": config["token"]})
                dashboard_url = get_branch_dashboard_url(config["base_url"], workspace.name, config["tinybird_branch"])

                branch_info = BuildBranchInfo(
                    git_branch=config.get("git_branch"),
                    tinybird_branch=config.get("tinybird_branch"),
                    was_created=bool(branch.get("was_created")),
                    dashboard_url=dashboard_url,
                    is_local=False,
                )
            except Exception as error:
                return BuildCommandResult(
                    success=False,
                    build=build_result,
                    error=f"Failed to get/create branch: {error}",
                    duration_ms=int(time.time() * 1000) - start,
                )

        try:
            deploy_result = build_to_tinybird(
                {"base_url": config["base_url"], "token": effective_token},
                build_result.resources,
            )
        except Exception as error:
            return BuildCommandResult(
                success=False,
                build=build_result,
                error=f"Deploy failed: {error}",
                duration_ms=int(time.time() * 1000) - start,
            )

    if not deploy_result.get("success"):
        return BuildCommandResult(
            success=False,
            build=build_result,
            deploy=deploy_result,
            branch_info=branch_info,
            error=deploy_result.get("error"),
            duration_ms=int(time.time() * 1000) - start,
        )

    return BuildCommandResult(
        success=True,
        build=build_result,
        deploy=deploy_result,
        branch_info=branch_info,
        duration_ms=int(time.time() * 1000) - start,
    )


__all__ = ["BuildCommandOptions", "BuildBranchInfo", "BuildCommandResult", "run_build"]
