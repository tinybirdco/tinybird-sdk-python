from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import sys

from .commands.generate import run_generate
from .commands.migrate import run_migrate
from .output import output


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
        from tinybird.tb.cli import cli as upstream_cli  # type: ignore[import-not-found]
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
    parser = argparse.ArgumentParser(prog="tinybird", description="Tinybird Python SDK generate command")
    sub = parser.add_subparsers(dest="command", required=True)

    generate_cmd = sub.add_parser("generate", help="Generate Tinybird datafiles from Python definitions")
    generate_cmd.add_argument("--json", action="store_true")
    generate_cmd.add_argument("-o", "--output-dir")

    migrate_cmd = sub.add_parser("migrate", help="Migrate Tinybird .datasource/.pipe files to Python resources")
    migrate_cmd.add_argument("patterns", nargs="+", help="Files, directories, or glob patterns to migrate")
    migrate_cmd.add_argument("--cwd", help="Working directory to resolve patterns from")
    migrate_cmd.add_argument("-o", "--out", help="Output file path for the generated migration module")
    migrate_cmd.add_argument("--dry-run", action="store_true", help="Generate output without writing files")
    migrate_cmd.add_argument("--force", action="store_true", help="Overwrite existing output file when needed")
    migrate_cmd.add_argument(
        "--strict",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Fail on migration issues (disable with --no-strict)",
    )
    migrate_cmd.add_argument("--json", action="store_true", help="Print migration result as JSON")

    return parser


def main(argv: list[str] | None = None) -> int:
    normalized_argv = list(argv) if argv is not None else list(sys.argv[1:])

    # SDK-owned commands stay local; all other commands are delegated to Tinybird CLI.
    if not normalized_argv or normalized_argv[0] not in {"generate", "migrate"}:
        return _run_installed_tinybird_cli(normalized_argv)

    parser = create_cli()
    args = parser.parse_args(normalized_argv)

    if args.command == "generate":
        result = run_generate({"output_dir": args.output_dir})
        if not result.success:
            output.error(result.error or "Generate failed")
            return 1

        if args.json:
            _print_json(asdict(result))
            return 0

        stats = result.stats or {
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
        if result.output_dir:
            print(f"Written to: {result.output_dir}")
        print(f"Completed in {output.format_duration(result.duration_ms)}")
        return 0

    result = run_migrate(
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
        _print_json(result)
        return 0 if result["success"] else 1

    if result["success"]:
        migrated_count = len(result.get("migrated") or [])
        print(f"Migrated {migrated_count} resources")
        if result.get("output_path"):
            print(f"Written to: {result['output_path']}")
        return 0

    errors = result.get("errors") or []
    if errors:
        output.error(f"Migrate failed with {len(errors)} error(s)")
        for error in errors:
            output.error(str(error))
    else:
        output.error("Migrate failed")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
