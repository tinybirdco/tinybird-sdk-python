from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import sys

from .commands.branch import run_branch_delete, run_branch_list, run_branch_status
from .commands.generate import run_generate
from .commands.info import run_info
from .commands.init import run_init
from .commands.migrate import run_migrate
from .commands.preview import run_preview
from .output import output

_SDK_OWNED_COMMANDS = {"init", "generate", "migrate", "branch", "info", "preview"}


def _print_json(payload: object) -> None:
    print(json.dumps(payload, indent=2, default=str))


def _exit_code_from_system_exit(error: SystemExit) -> int:
    code = error.code
    if code is None:
        return 0
    if isinstance(code, int):
        return code
    return 1


def _run_installed_tinybird_cli(argv: list[str]) -> int:
    try:
        from tinybird.tb.cli import cli as upstream_cli
    except ModuleNotFoundError:
        output.error("Installed Tinybird CLI dependency is required but could not be imported.")
        return 1

    try:
        upstream_cli.main(args=argv, prog_name="tinybird")
        return 0
    except SystemExit as error:
        return _exit_code_from_system_exit(error)
    except Exception as error:
        output.error(str(error))
        return 1


def create_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="tinybird", description="Tinybird Python SDK CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    init_cmd = sub.add_parser(
        "init", help="Initialize a new Tinybird project with Python SDK templates"
    )
    init_cmd.add_argument("--folder", help="Target folder for generated Python files")
    init_cmd.add_argument("--force", action="store_true", help="Overwrite existing files")

    generate_cmd = sub.add_parser(
        "generate", help="Generate Tinybird datafiles from Python definitions"
    )
    generate_cmd.add_argument("--json", action="store_true")
    generate_cmd.add_argument("-o", "--output-dir")

    migrate_cmd = sub.add_parser(
        "migrate", help="Migrate Tinybird .datasource/.pipe files to Python resources"
    )
    migrate_cmd.add_argument(
        "patterns", nargs="+", help="Files, directories, or glob patterns to migrate"
    )
    migrate_cmd.add_argument("--cwd", help="Working directory to resolve patterns from")
    migrate_cmd.add_argument(
        "-o", "--out", help="Output file path for the generated migration module"
    )
    migrate_cmd.add_argument(
        "--dry-run", action="store_true", help="Generate output without writing files"
    )
    migrate_cmd.add_argument(
        "--force", action="store_true", help="Overwrite existing output file when needed"
    )
    migrate_cmd.add_argument(
        "--strict",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Fail on migration issues (disable with --no-strict)",
    )
    migrate_cmd.add_argument("--json", action="store_true", help="Print migration result as JSON")

    branch_cmd = sub.add_parser("branch", help="Manage Tinybird branches")
    branch_sub = branch_cmd.add_subparsers(dest="branch_command", required=True)
    branch_sub.add_parser("list", help="List branches")
    branch_status_cmd = branch_sub.add_parser("status", help="Show a branch's status")
    branch_status_cmd.add_argument(
        "name", nargs="?", help="Branch name (defaults to the current project branch)"
    )
    branch_delete_cmd = branch_sub.add_parser("delete", help="Delete a branch")
    branch_delete_cmd.add_argument("name", help="Branch name to delete")

    info_cmd = sub.add_parser("info", help="Show project and workspace info")
    info_cmd.add_argument("--json", action="store_true", help="Print info as JSON")

    preview_cmd = sub.add_parser(
        "preview", help="Build and deploy resources to a temporary preview branch"
    )
    preview_cmd.add_argument(
        "--dry-run", action="store_true", help="Build without creating a preview branch"
    )
    preview_cmd.add_argument(
        "--check", action="store_true", help="Validate the preview deploy without applying it"
    )
    preview_cmd.add_argument("--name", help="Preview branch name override")
    preview_mode = preview_cmd.add_mutually_exclusive_group()
    preview_mode.add_argument(
        "--local",
        action="store_const",
        dest="dev_mode",
        const="local",
        help="Preview against Tinybird Local",
    )
    preview_mode.add_argument(
        "--branch",
        action="store_const",
        dest="dev_mode",
        const="branch",
        help="Preview against a cloud branch",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    normalized_argv = list(argv) if argv is not None else list(sys.argv[1:])

    # SDK-owned commands stay local; all other commands are delegated to Tinybird CLI.
    if not normalized_argv or normalized_argv[0] not in _SDK_OWNED_COMMANDS:
        return _run_installed_tinybird_cli(normalized_argv)

    parser = create_cli()
    args = parser.parse_args(normalized_argv)

    if args.command == "init":
        result = run_init(
            {
                "folder": args.folder,
                "force": args.force,
            }
        )
        if not result.success:
            output.error(result.error or "Init failed")
            return 1

        output.success("\n✓ Python SDK files created:")
        if result.resources_path:
            output.info(f"  {result.resources_path}")
        if result.client_path:
            output.info(f"  {result.client_path}")
        if result.main_path:
            output.info(f"  {result.main_path}")
        return 0

    if args.command == "generate":
        generate_result = run_generate({"output_dir": args.output_dir})
        if not generate_result.success:
            output.error(generate_result.error or "Generate failed")
            return 1

        if args.json:
            _print_json(asdict(generate_result))
            return 0

        stats = generate_result.stats or {
            "datasource_count": 0,
            "pipe_count": 0,
            "connection_count": 0,
            "total_count": 0,
        }
        print(
            "Generated "
            f"{stats['total_count']} resources "
            f"({stats['datasource_count']} datasources, "
            f"{stats['pipe_count']} pipes, "
            f"{stats['connection_count']} connections)"
        )
        if generate_result.output_dir:
            print(f"Written to: {generate_result.output_dir}")
        print(f"Completed in {output.format_duration(generate_result.duration_ms)}")
        return 0

    if args.command == "branch":
        if args.branch_command == "list":
            list_result = run_branch_list()
            if not list_result.success:
                output.error(list_result.error or "Failed to list branches")
                return 1
            for branch in list_result.branches:
                print(branch.get("name") or branch.get("id"))
            return 0

        if args.branch_command == "status":
            status_result = run_branch_status(args.name)
            if not status_result.success:
                output.error(status_result.error or "Failed to get branch status")
                return 1
            _print_json(status_result.branch)
            return 0

        delete_result = run_branch_delete(args.name)
        if not delete_result.success:
            output.error(delete_result.error or "Failed to delete branch")
            return 1
        output.success(f"\n✓ Branch '{args.name}' deleted")
        return 0

    if args.command == "info":
        info_result = run_info()
        if not info_result.success:
            output.error(info_result.error or "Info failed")
            return 1

        info_payload = asdict(info_result)
        if args.json:
            _print_json(info_payload)
            return 0

        output.show_info(info_payload)
        return 0

    if args.command == "preview":
        preview_result = run_preview(
            {
                "dry_run": args.dry_run,
                "check": args.check,
                "name": args.name,
                "dev_mode_override": args.dev_mode,
            }
        )
        if not preview_result.success:
            output.error(preview_result.error or "Preview failed")
            return 1

        output.success(
            f"\n✓ Preview completed in {output.format_duration(preview_result.duration_ms)}"
        )
        if preview_result.branch:
            output.info(f"Branch: {preview_result.branch['name']}")
            output.info(f"URL: {preview_result.branch['url']}")
        return 0

    migrate_result = run_migrate(
        {
            "cwd": args.cwd,
            "patterns": args.patterns,
            "out": args.out,
            "strict": args.strict,
            "dry_run": args.dry_run,
            "force": args.force,
        }
    )

    if args.json:
        _print_json(migrate_result)
        return 0 if migrate_result["success"] else 1

    if migrate_result["success"]:
        migrated_count = len(migrate_result.get("migrated") or [])
        print(f"Migrated {migrated_count} resources")
        if migrate_result.get("output_path"):
            print(f"Written to: {migrate_result['output_path']}")
        return 0

    errors = migrate_result.get("errors") or []
    if errors:
        output.error(f"Migrate failed with {len(errors)} error(s)")
        for error in errors:
            output.error(str(error))
    else:
        output.error("Migrate failed")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
