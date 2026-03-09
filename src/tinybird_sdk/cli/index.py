from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import sys

from .commands.generate import run_generate
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

    return parser


def main(argv: list[str] | None = None) -> int:
    normalized_argv = list(argv) if argv is not None else list(sys.argv[1:])

    # `generate` is owned by the SDK; all other commands are delegated to Tinybird CLI.
    if not normalized_argv or normalized_argv[0] != "generate":
        return _run_installed_tinybird_cli(normalized_argv)

    parser = create_cli()
    args = parser.parse_args(normalized_argv)

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


if __name__ == "__main__":
    raise SystemExit(main())
