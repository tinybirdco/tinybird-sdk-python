from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
from typing import Any

from ..config import find_existing_config_path


RESOURCES_TEMPLATE = '''from tinybird_sdk import define_datasource, define_endpoint, engine, node, p, t


# --- Datasources ---

page_views = define_datasource("page_views", {
    "description": "Page view tracking data",
    "schema": {
        "timestamp": t.date_time(),
        "pathname": t.string(),
        "session_id": t.string(),
        "country": t.string().low_cardinality().nullable(),
    },
    "engine": engine.merge_tree({
        "sorting_key": ["pathname", "timestamp"],
    }),
})


# --- Endpoints ---

top_pages = define_endpoint("top_pages", {
    "description": "Get the most visited pages",
    "params": {
        "start_date": p.date_time(),
        "end_date": p.date_time(),
        "limit": p.int32().optional(10),
    },
    "nodes": [
        node({
            "name": "aggregated",
            "sql": """
                SELECT pathname, count() AS views
                FROM page_views
                WHERE timestamp >= {{DateTime(start_date)}}
                  AND timestamp <= {{DateTime(end_date)}}
                GROUP BY pathname
                ORDER BY views DESC
                LIMIT {{Int32(limit, 10)}}
            """,
        }),
    ],
    "output": {
        "pathname": t.string(),
        "views": t.uint64(),
    },
})
'''


def _client_template(resources_import_path: str) -> str:
    return f'''import os

from tinybird_sdk import Tinybird
from {resources_import_path} import page_views, top_pages

tinybird = Tinybird(
    {{
        "datasources": {{"page_views": page_views}},
        "pipes": {{"top_pages": top_pages}},
        "base_url": os.getenv("TINYBIRD_API_URL", "https://api.tinybird.co"),
        "token": os.getenv("TINYBIRD_TOKEN"),
    }}
)
'''


def _main_template(client_import_path: str) -> str:
    return f'''from datetime import datetime, timezone

from dotenv import load_dotenv


def main():
    load_dotenv(".env.local")

    from {client_import_path} import tinybird

    now = datetime.now(timezone.utc).isoformat(timespec="milliseconds")

    # Ingest data using the Events API
    tinybird.page_views.ingest(
        {{
            "timestamp": now,
            "session_id": "abc123",
            "pathname": "/home",
            "referrer": "https://google.com",
        }}
    )

    # Query the endpoint
    result = tinybird.top_pages.query(
        {{
            "start_date": "2026-01-01 00:00:00",
            "end_date": now,
            "limit": 5,
        }}
    )

    for row in result["data"]:
        print(row["pathname"], row["views"])


if __name__ == "__main__":
    main()
'''


@dataclass(frozen=True, slots=True)
class InitOptions:
    cwd: str | None = None
    folder: str | None = None
    force: bool = False


@dataclass(frozen=True, slots=True)
class InitResult:
    success: bool
    resources_path: str | None = None
    client_path: str | None = None
    main_path: str | None = None
    error: str | None = None


def find_existing_datafiles(cwd: str) -> list[str]:
    base = Path(cwd)
    found: list[str] = []
    for ext in ("*.datasource", "*.pipe", "*.connection"):
        found.extend(str(path.relative_to(base)) for path in base.rglob(ext))
    return sorted(found)


def _write_file(path: Path, content: str, force: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not force:
        return
    path.write_text(content, encoding="utf-8")


def _run_tinybird_cli_init(argv: list[str]) -> int:
    try:
        from tinybird.tb.cli import cli as upstream_cli  # type: ignore[import-not-found]
    except ModuleNotFoundError:
        return 1

    try:
        upstream_cli.main(args=argv, prog_name="tinybird")
        return 0
    except SystemExit as error:
        code = error.code
        if code is None:
            return 0
        return code if isinstance(code, int) else 1


def run_init(options: InitOptions | dict[str, Any] | None = None) -> InitResult:
    normalized = options if isinstance(options, InitOptions) else InitOptions(**(options or {}))
    cwd = Path(normalized.cwd or os.getcwd()).resolve()

    try:
        # 1. Delegate to tinybird CLI for interactive flow (dev mode, CI/CD, skills, login)
        cli_argv = ["init", "--type", "python"]
        if normalized.folder:
            cli_argv.extend(["--folder", normalized.folder])
        _run_tinybird_cli_init(cli_argv)

        # 2. Create Python SDK template files on top
        if normalized.folder:
            folder = Path(normalized.folder)
            if not folder.is_absolute():
                folder = cwd / folder
        else:
            src = cwd / "src"
            folder = (src / "lib") if src.is_dir() else (cwd / "lib")

        resources_path = folder / "tinybird_resources.py"
        client_path = folder / "client.py"
        main_path = cwd / "main.py"

        # Compute import paths based on folder relative to cwd
        relative_folder = str(folder.relative_to(cwd)).replace(os.sep, ".")
        resources_import = f"{relative_folder}.tinybird_resources"
        client_import = f"{relative_folder}.client"

        _write_file(resources_path, RESOURCES_TEMPLATE, normalized.force)
        _write_file(client_path, _client_template(resources_import), normalized.force)
        # Always overwrite main.py — the default from `uv init` is a placeholder
        _write_file(main_path, _main_template(client_import), force=True)

        # 3. Add the resources file to tinybird.config.json include list
        config_path = find_existing_config_path(str(cwd))
        if config_path and config_path.endswith(".json"):
            relative_resources = str(resources_path.relative_to(cwd))
            with open(config_path, "r", encoding="utf-8") as fp:
                config_data = json.load(fp)
            include = config_data.get("include", [])
            if relative_resources not in include:
                include.append(relative_resources)
                config_data["include"] = include
                with open(config_path, "w", encoding="utf-8") as fp:
                    json.dump(config_data, fp, indent=2)
                    fp.write("\n")

        return InitResult(
            success=True,
            resources_path=str(resources_path.relative_to(cwd)),
            client_path=str(client_path.relative_to(cwd)),
            main_path=str(main_path.relative_to(cwd)),
        )
    except Exception as error:
        return InitResult(success=False, error=str(error))


__all__ = ["InitOptions", "InitResult", "find_existing_datafiles", "run_init"]
