from __future__ import annotations

from dataclasses import dataclass
import os
import time
from typing import Any

from ...api.branches import create_branch, delete_branch, get_branch
from ...api.build import build_to_tinybird
from ...api.deploy import deploy_to_main
from ...api.local import LocalNotRunningError, get_local_tokens, get_or_create_local_workspace
from ...cli.config import LOCAL_BASE_URL, load_config_async
from ...cli.git import get_current_git_branch, sanitize_branch_name
from ...generator.index import build_from_include


@dataclass(frozen=True, slots=True)
class PreviewCommandOptions:
    cwd: str | None = None
    dry_run: bool = False
    check: bool = False
    name: str | None = None
    dev_mode_override: str | None = None


@dataclass(frozen=True, slots=True)
class PreviewCommandResult:
    success: bool
    duration_ms: int
    branch: dict[str, Any] | None = None
    build: dict[str, int] | None = None
    deploy: dict[str, str] | None = None
    error: str | None = None


def generate_preview_branch_name(git_branch: str | None) -> str:
    branch_part = sanitize_branch_name(git_branch) if git_branch else "unknown"
    return f"tmp_ci_{branch_part}"


def run_preview(options: PreviewCommandOptions | dict[str, Any] | None = None) -> PreviewCommandResult:
    start = int(time.time() * 1000)
    normalized = options if isinstance(options, PreviewCommandOptions) else PreviewCommandOptions(**(options or {}))
    cwd = normalized.cwd or os.getcwd()

    try:
        config = load_config_async(cwd)
    except Exception as error:
        return PreviewCommandResult(success=False, error=str(error), duration_ms=int(time.time() * 1000) - start)

    git_branch = get_current_git_branch()
    preview_branch_name = normalized.name or generate_preview_branch_name(git_branch)

    try:
        build_result = build_from_include({"include_paths": config["include"], "cwd": config["cwd"]})
    except Exception as error:
        return PreviewCommandResult(
            success=False,
            error=f"Build failed: {error}",
            duration_ms=int(time.time() * 1000) - start,
        )

    build_stats = {
        "datasource_count": build_result.stats["datasource_count"],
        "pipe_count": build_result.stats["pipe_count"],
    }

    if normalized.dry_run:
        return PreviewCommandResult(
            success=True,
            branch={
                "name": preview_branch_name,
                "id": "(dry-run)",
                "token": "(dry-run)",
                "url": config["base_url"],
                "created_at": "(dry-run)",
            },
            build=build_stats,
            duration_ms=int(time.time() * 1000) - start,
        )

    dev_mode = normalized.dev_mode_override or config.get("dev_mode")

    if dev_mode == "local":
        try:
            tokens = get_local_tokens()
            local_workspace = get_or_create_local_workspace(tokens, preview_branch_name)
            workspace = local_workspace["workspace"]
            deploy_result = build_to_tinybird(
                {"base_url": LOCAL_BASE_URL, "token": workspace.token},
                build_result.resources,
            )
            if not deploy_result.get("success"):
                return PreviewCommandResult(
                    success=False,
                    branch={
                        "name": preview_branch_name,
                        "id": workspace.id,
                        "token": workspace.token,
                        "url": LOCAL_BASE_URL,
                        "created_at": "",
                    },
                    build=build_stats,
                    error=deploy_result.get("error"),
                    duration_ms=int(time.time() * 1000) - start,
                )

            return PreviewCommandResult(
                success=True,
                branch={
                    "name": preview_branch_name,
                    "id": workspace.id,
                    "token": workspace.token,
                    "url": LOCAL_BASE_URL,
                    "created_at": "",
                },
                build=build_stats,
                deploy={"result": deploy_result.get("result", "success")},
                duration_ms=int(time.time() * 1000) - start,
            )
        except LocalNotRunningError as error:
            return PreviewCommandResult(success=False, error=str(error), duration_ms=int(time.time() * 1000) - start)
        except Exception as error:
            return PreviewCommandResult(success=False, error=f"Local preview failed: {error}", duration_ms=int(time.time() * 1000) - start)

    try:
        try:
            existing = get_branch({"base_url": config["base_url"], "token": config["token"]}, preview_branch_name)
            if existing:
                delete_branch({"base_url": config["base_url"], "token": config["token"]}, preview_branch_name)
        except Exception:
            pass

        branch = create_branch({"base_url": config["base_url"], "token": config["token"]}, preview_branch_name)
    except Exception as error:
        return PreviewCommandResult(
            success=False,
            error=f"Failed to create preview branch: {error}",
            duration_ms=int(time.time() * 1000) - start,
        )

    if not branch.token:
        return PreviewCommandResult(
            success=False,
            error="Preview branch created but no token returned",
            duration_ms=int(time.time() * 1000) - start,
        )

    try:
        deploy_result = deploy_to_main(
            {"base_url": config["base_url"], "token": branch.token},
            build_result.resources,
            {"check": normalized.check, "allow_destructive_operations": True},
        )
    except Exception as error:
        return PreviewCommandResult(
            success=False,
            branch={
                "name": branch.name,
                "id": branch.id,
                "token": branch.token,
                "url": config["base_url"],
                "created_at": branch.created_at,
            },
            build=build_stats,
            error=f"Deploy failed: {error}",
            duration_ms=int(time.time() * 1000) - start,
        )

    if not deploy_result.get("success"):
        return PreviewCommandResult(
            success=False,
            branch={
                "name": branch.name,
                "id": branch.id,
                "token": branch.token,
                "url": config["base_url"],
                "created_at": branch.created_at,
            },
            build=build_stats,
            error=deploy_result.get("error"),
            duration_ms=int(time.time() * 1000) - start,
        )

    return PreviewCommandResult(
        success=True,
        branch={
            "name": branch.name,
            "id": branch.id,
            "token": branch.token,
            "url": config["base_url"],
            "created_at": branch.created_at,
        },
        build=build_stats,
        deploy={"result": deploy_result.get("result", "success")},
        duration_ms=int(time.time() * 1000) - start,
    )


__all__ = ["PreviewCommandOptions", "PreviewCommandResult", "generate_preview_branch_name", "run_preview"]
